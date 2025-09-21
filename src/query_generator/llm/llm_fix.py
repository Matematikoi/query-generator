from dataclasses import dataclass, field
from typing import Iterator
import duckdb
from ollama import Client
from tqdm import tqdm
from query_generator.llm.utils import LLM_Message, extract_sql, query_llm
from query_generator.utils.file_writing import write_to_file, write_to_toml
from query_generator.utils.params import LLMFixEndpoint
from pathlib import Path


@dataclass
class FixLogRow:
  file_path: str
  original_query: str
  fixed_query: str
  conditions_fulfilled: list[str] = field(default_factory=list)
  logs_fix: dict[str, "LLM_Message"] = field(default_factory=dict)
  logs_condition: dict[str, "LLM_Message"] = field(default_factory=dict)


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
  condition: str, base_prompt: str, query: str
) -> LLM_Message:
  """Gets the messages to send to the LLM for a specific petition"""
  return [
    {"role": "system", "content": base_prompt},
    {
      "role": "user",
      "content": f"""
    {condition}
    {query}""",
    },
  ]


def llm_response_to_boolean(response: str) -> bool:
  """Convert an LLM response to a boolean."""
  response = "".join(filter(str.isalnum, response)).lower()
  return response in ["yes", "true"]


def query_fulfills_condition(
  query: str,
  params: LLMFixEndpoint,
  condition_name: str,
  llm_client: Client,
  logger: FixLogRow,
) -> bool:
  """Check if a query fulfills a specific condition using the LLM."""
  messages = build_llm_input(
    condition=params.prompts[condition_name].condition,
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
  logger: FixLogRow,
) -> str:
  messages = build_llm_input(
    condition=params.prompts[prompt_name].condition,
    base_prompt=params.llm_base_fix_prompt,
    query=query,
  )
  response = query_llm(llm_client, messages, params.llm_model)
  logger.logs_fix[prompt_name] = messages
  new_query = extract_sql(response[-1]["content"])
  return query if new_query == "" else new_query


def llm_fix(params: LLMFixEndpoint) -> None:
  """Use an LLM to fix a set of queries."""
  old_queries_path = Path(params.queries_path)
  new_queries_path = Path(params.new_queries_path)
  sql_files = get_sql_queries_from_folder(old_queries_path)
  llm_client = Client()
  con = duckdb.connect(database=params.database_path, read_only=True)
  for sql_file in tqdm(sql_files, desc="Fixing queries"):
    query = sql_file.read_text()
    logger = FixLogRow(
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
    write_to_file(
      new_queries_path / sql_file.relative_to(params.queries_path),
      fixed_query,
    )
  con.close()
  # TODO save the logs to a parquet file
  copy_non_sql_files(old_queries_path, new_queries_path)
  write_to_toml(new_queries_path / "llm_fix_config.toml", params)
