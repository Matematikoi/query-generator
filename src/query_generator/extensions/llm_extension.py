import json
import random
import re
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
from duckdb import DuckDBPyConnection
from tqdm import tqdm

from query_generator.extensions.llm_clients import LLM_Message, LLMClient
from query_generator.tools.format_histogram import get_histogram_as_str
from query_generator.utils.params import (
  LLMParams,
)


def get_random_queries(
  queries_base_path: Path, llm_params: LLMParams
) -> list[tuple[str, str]]:
  """Get random queries from the synthetic queries parquet file.

  Returns:
    A list of tuples containing the query and its original path."""
  sql_files = list(queries_base_path.rglob("*.sql"))
  random_query_paths = random.choices(sql_files, k=llm_params.total_queries)
  return [
    (p.read_text(), str(p.relative_to(queries_base_path)))
    for p in random_query_paths
  ]


def get_random_prompt(
  params: LLMParams, query: str, context: str
) -> tuple[str, LLM_Message]:
  extension_types = list(params.prompts.weighted_prompts.keys())
  weights = [params.prompts.weighted_prompts[e].weight for e in extension_types]
  extension_type = random.choices(extension_types, weights=weights)[0]

  return extension_type, [
    {"role": "system", "content": params.prompts.base_prompt},
    {
      "role": "user",
      "content": f"""
{params.prompts.weighted_prompts[extension_type]}
```sql
{query}
```
""",
    },
  ]


def extract_sql(llm_text: str) -> str:
  if "<think>" in llm_text:
    _, _, text = llm_text.partition("</think>")
  else:
    text = llm_text
  matches = re.findall(r"```sql\s*(.*?)\s*```", text, re.DOTALL)
  return matches[-1].strip() if matches else ""


def validate_query_duckdb(
  con: DuckDBPyConnection, query: str
) -> tuple[bool, Exception]:
  try:
    con.sql(query).fetchone()
  except Exception as e:
    return False, e
  else:
    return True, Exception("no exception found")


def add_retry_query_to_messages(
  messages: LLM_Message, exception: Exception
) -> None:
  messages.append(
    {
      "role": "user",
      "content": f"""
      Fix this error with the query you provided:
      {str(exception)}
    """,
    }
  )

def get_new_query_name(cnt: int, original_path:str):
  return f"{cnt}_{original_path.replace('/', '_')}"
def write_query_llm_and_get_row(
  destination_path: Path,
  extension_type: str,
  cnt: int,
  original_path: str,
  query: str,
):
  new_path = (
    destination_path
    / extension_type
    / get_new_query_name(cnt, original_path)
  )
  new_path.parent.mkdir(parents=True, exist_ok=True)
  new_path.write_text(query)
  return {
    "extension_type": extension_type,
    "original_path": (original_path),
    "new_path": str(new_path.relative_to(destination_path)),
  }

def save_parquet(destination_path:Path, rows:list[Any]):
  destination_path.parent.mkdir(parents=True, exist_ok=True)
  pl.DataFrame(rows).write_parquet(destination_path)

def get_schema_from_statistics(
  params: LLMParams,
) -> str:
  """Get the schema of a db from the parquet stats file."""
  if params.statistics_parquet is None:
    return ""
  df_stats = pl.read_parquet(params.statistics_parquet)
  return get_histogram_as_str(df_stats)


def llm_extension(
  llm_params: LLMParams,
  llm_client: LLMClient,
  llm_config_params: str,
  input_queries_base_path: Path,
  destination_path: Path,
) -> int:
  """Generate new queries using LLM prompts.

  Returns:
    The number of generated queries.
  """
  random.seed(42)
  con = duckdb.connect(database=llm_params.database_path, read_only=True)
  schema_context: str = get_schema_from_statistics(llm_params)
  rows: list[dict[str, str]] = []
  log_rows: list[dict[str, str | bool]] = []
  sampled_queries = get_random_queries(input_queries_base_path, llm_params)
  for cnt, (query, original_path) in tqdm(  # type:ignore
    enumerate(sampled_queries),
    desc="LLM-Extension",
    total=len(sampled_queries)
  ):
    retries = 0
    valid_query = False
    duckdb_exception = Exception("no query was found")
    llm_extracted_query = ""
    extension_type, messages = get_random_prompt(
      llm_params, query, schema_context
    )
    while retries <= llm_params.retry and not valid_query:
      if retries > 0:
        add_retry_query_to_messages(messages, duckdb_exception)
      llm_client.query(messages, llm_config_params)
      llm_extracted_query = extract_sql(messages[-1]["content"])
      valid_query, duckdb_exception = validate_query_duckdb(
        con, llm_extracted_query
      )
      retries += 1
    # Save query
    if valid_query:
      rows.append(
        {
          **write_query_llm_and_get_row(
            destination_path,
            extension_type,
            cnt,
            original_path,
            llm_extracted_query,
          ),
          "retries": str(retries),
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
          "new_path": get_new_query_name(cnt, original_path),
        }
      )
      save_parquet(destination_path/"llm_extension.parquet", rows)
      save_parquet(destination_path/"logs.parquet", log_rows)

  print(f"Total LLM queries generated: {len(rows)}.")
  return len(rows)
