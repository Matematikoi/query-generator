import json
import random
from pathlib import Path

import duckdb
import polars as pl
from ollama import Client
from tqdm import tqdm

from query_generator.llm.utils import (
  LLM_Message,
  add_retry_query_to_messages,
  extract_sql,
  query_llm,
  validate_query_duckdb,
)
from query_generator.utils.params import (
  ExtensionAndLLMEndpoint,
  LLMParams,
)


def get_random_queries(
  params: ExtensionAndLLMEndpoint,
) -> list[tuple[str, str]]:
  """Get random queries from the synthetic queries parquet file.

  Returns:
    A list of tuples containing the query and its original path."""
  assert params.llm_params is not None
  base_path = Path(params.queries_parquet).parent
  sql_files = list(base_path.rglob("*.sql"))
  random_query_paths = random.sample(sql_files, params.llm_params.total_queries)
  return [
    (p.read_text(), str(p.relative_to(base_path))) for p in random_query_paths
  ]


def get_random_prompt(params: LLMParams, query: str) -> tuple[str, LLM_Message]:
  extension_types = list(params.llm_prompts.keys())
  weights = [params.llm_prompts[e].weight for e in extension_types]
  extension_type = random.choices(extension_types, weights=weights)[0]

  return extension_type, [
    {"role": "system", "content": params.llm_base_prompt},
    {
      "role": "user",
      "content": f"""

  {params.llm_prompts[extension_type].prompt}
  {query}""",
    },
  ]


def llm_extension(
  params: ExtensionAndLLMEndpoint,
) -> None:
  llm_params = params.llm_params
  assert llm_params is not None
  llm_client = Client()
  random.seed(42)
  con = duckdb.connect(database=params.database_path, read_only=True)
  destination_path = Path(params.destination_folder)
  rows: list[dict[str, str]] = []
  log_rows: list[dict[str, str | bool]] = []
  for query, original_path in tqdm(get_random_queries(params)):
    retries = 0
    valid_query = False
    duckdb_exception = Exception("no query was found")
    while retries <= llm_params.retry and not valid_query:
      if retries == 0:
        extension_type, messages = get_random_prompt(llm_params, query)
      else:
        add_retry_query_to_messages(messages, duckdb_exception)
      query_llm(llm_client, messages, llm_params.llm_model)
      llm_extracted_query = extract_sql(messages[-1]["content"])
      valid_query, duckdb_exception = validate_query_duckdb(
        con, llm_extracted_query
      )
      retries += 1
    # Save query
    if valid_query:
      new_path = (
        destination_path / extension_type / f"{original_path.replace('/', '_')}"
      )
      new_path.parent.mkdir(parents=True, exist_ok=True)
      new_path.write_text(llm_extracted_query)
      rows.append(
        {
          "extension_type": extension_type,
          "retries": str(retries),
          "original_path": (original_path),
          "new_path": str(new_path.relative_to(destination_path)),
        }
      )

    # Adds logs even if the query is not valid
    if valid_query or retries == llm_params.retry + 1:
      log_rows.append(
        {
          "extension_type": extension_type,
          "retries": str(retries),
          "original_path": (original_path),
          "valid_query": valid_query,
          "last_duckdb_exception": str(duckdb_exception)
          if not valid_query
          else "",
          "messages": json.dumps(messages),
        }
      )
  destination_path.mkdir(parents=True, exist_ok=True)
  new_queries_df = pl.DataFrame(rows)
  new_queries_df.write_parquet(destination_path / "llm_extension.parquet")
  logs_df = pl.DataFrame(log_rows)
  logs_df.write_parquet(destination_path / "logs.parquet")
