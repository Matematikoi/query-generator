from dataclasses import dataclass, field
from enum import StrEnum
from typing import Iterator
from ollama import Client
from tqdm import tqdm
from query_generator.llm.utils import (
  LLM_Message,
  get_text_after_think,
  query_llm,
  validate_and_retry_query_with_llm,
)
from query_generator.utils.file_writing import write_to_file, write_to_toml
from query_generator.utils.params import LLMFixEndpoint
from pathlib import Path
import polars as pl


@dataclass
class FixLLMLogs:
  file_path: str
  original_query: str
  fixed_query: str
  conditions_fulfilled: list[str] = field(default_factory=list)
  logs_fix: dict[str, "LLM_Message"] = field(default_factory=dict)
  logs_condition: dict[str, "LLM_Message"] = field(default_factory=dict)


class LLMFixLogColumns(StrEnum):
  FILE_PATH = "file_path"
  ORIGINAL_QUERY = "original_query"
  FIXED_QUERY = "fixed_query"
  CONDITION_NAME = "condition_name"
  CONDITION_LOG = "condition_log"
  FIX_LOGS = "fix_log"
  WAS_FIXED = "was_fixed"


def get_sql_queries_from_folder(folder_path: Path) -> list[Path]:
  """Get all SQL queries from a folder and its subfolders."""
  return list(folder_path.rglob("*.sql"))


def get_non_sql_files(folder: Path) -> Iterator[Path]:
  folder_path = Path(folder)

  for file_path in folder_path.iterdir():
    if file_path.is_file() and file_path.suffix.lower() != ".sql":
      yield file_path


def copy_non_sql_files(src_folder: Path, dest_folder: Path) -> None:
  """Copy non-SQL files from src to dest, keeping folder structure."""
  for file_path in get_non_sql_files(src_folder):
    relative_path = file_path.relative_to(src_folder)
    dest_path = dest_folder / relative_path
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_bytes(file_path.read_bytes())


def build_llm_input(
  user_petition: str, base_prompt: str, query: str
) -> LLM_Message:
  """Gets the messages to send to the LLM for a specific petition"""
  return [
    {"role": "system", "content": base_prompt},
    {
      "role": "user",
      "content": f"""
    {user_petition}
    {query}""",
    },
  ]


def llm_response_to_boolean(response: str) -> bool:
  """Convert an LLM response to a boolean."""
  response = "".join(filter(str.isalnum, response)).lower()
  return "yes" in get_text_after_think(response)


def query_fulfills_condition(
  query: str,
  params: LLMFixEndpoint,
  condition_name: str,
  llm_client: Client,
  logger: FixLLMLogs,
) -> bool:
  """Check if a query fulfills a specific condition using the LLM."""
  messages = build_llm_input(
    user_petition=params.prompts[condition_name].condition,
    base_prompt=params.llm_base_condition_prompt,
    query=query,
  )
  query_llm(llm_client, messages, params.llm_model)
  response = messages[-1]["content"].strip().lower()
  response_boolean = llm_response_to_boolean(response)
  # Logging
  logger.logs_condition[condition_name] = messages
  if response_boolean:
    logger.conditions_fulfilled.append(condition_name)

  return response_boolean


def fix_query_with_llm(
  query: str,
  params: LLMFixEndpoint,
  prompt_name: str,
  llm_client: Client,
  logger: FixLLMLogs,
) -> str:
  messages = build_llm_input(
    user_petition=params.prompts[prompt_name].fix,
    base_prompt=params.llm_base_fix_prompt,
    query=query,
  )
  query_llm(llm_client, messages, params.llm_model)
  new_query = validate_and_retry_query_with_llm(
    messages, llm_client, params.database_path, params.retry, params.llm_model
  )
  logger.logs_fix[prompt_name] = messages
  return query if new_query == "" else new_query


def get_rows_from_log(log: FixLLMLogs) -> list[dict]:
  """Get rows from a FixLLMLogs object for saving to a DataFrame."""
  rows = []
  for condition_name, condition_log in log.logs_condition.items():
    was_fixed = log.fixed_query != log.original_query
    fix_log = log.logs_fix.get(condition_name, [])
    rows.append(
      {
        LLMFixLogColumns.FILE_PATH: log.file_path,
        LLMFixLogColumns.ORIGINAL_QUERY: log.original_query,
        LLMFixLogColumns.FIXED_QUERY: log.fixed_query,
        LLMFixLogColumns.CONDITION_NAME: condition_name,
        LLMFixLogColumns.CONDITION_LOG: condition_log,
        LLMFixLogColumns.FIX_LOGS: fix_log,
        LLMFixLogColumns.WAS_FIXED: was_fixed,
      }
    )
  return rows


def get_dataframe_from_logs(logs: list[FixLLMLogs]):
  """Get a DataFrame from a list of FixLLMLogs."""
  rows = []
  for log in logs:
    rows.extend(get_rows_from_log(log))
  return pl.DataFrame(rows)


def llm_fix(params: LLMFixEndpoint) -> None:
  """Use an LLM to fix a set of queries."""
  old_queries_path = Path(params.queries_path)
  new_queries_path = Path(params.new_queries_path)
  sql_files = get_sql_queries_from_folder(old_queries_path)
  llm_client = Client()
  logs: list[FixLLMLogs] = []
  for sql_file in tqdm(sql_files, desc="Fixing queries"):
    query = sql_file.read_text()
    logger = FixLLMLogs(
      file_path=str(sql_file.relative_to(params.queries_path)),
      original_query=query,
      fixed_query=query,
    )
    fixed_query = query
    for prompt_name, _ in params.get_sorted_prompts():
      if query_fulfills_condition(
        fixed_query, params, prompt_name, llm_client, logger
      ):
        fixed_query = fix_query_with_llm(
          fixed_query, params, prompt_name, llm_client, logger
        )
    logger.fixed_query = fixed_query
    logs.append(logger)
    write_to_file(
      new_queries_path / sql_file.relative_to(params.queries_path),
      fixed_query,
    )
  logs_df = get_dataframe_from_logs(logs)
  logs_df.write_parquet(new_queries_path / "llm_fix_logs.parquet")
  copy_non_sql_files(old_queries_path, new_queries_path)
  write_to_toml(new_queries_path / "llm_fix_config.toml", params)
