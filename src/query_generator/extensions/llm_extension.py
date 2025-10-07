import json
import random
import re
from pathlib import Path

import duckdb
import polars as pl
from duckdb import DuckDBPyConnection
from ollama import Client
from tqdm import tqdm

from query_generator.tools.format_histogram import get_histogram_as_str
from query_generator.utils.params import (
  ExtensionAndLLMEndpoint,
  LLMParams,
)

LLM_Message = list[dict[str, str]]


def query_llm(client: Client, messages: LLM_Message, model: str) -> None:
  """Send a single request to the LLM and return its response."""
  response = client.chat(model=model, messages=messages, stream=False)
  response_str = response.message.content
  if not response_str:
    messages.append(
      {"role": "assistant", "content": "I can't help you with that"}
    )
  else:
    messages.append({"role": "assistant", "content": response_str})


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


def get_random_prompt(
  params: LLMParams, query: str, context: str
) -> tuple[str, LLM_Message]:
  extension_types = list(params.llm_prompts.keys())
  weights = [params.llm_prompts[e].weight for e in extension_types]
  extension_type = random.choices(extension_types, weights=weights)[0]

  return extension_type, [
    {"role": "system", "content": params.llm_base_prompt},
    {
      "role": "user",
      "content": f"""
Here is the context you need for your task. You have the schema, and
some stats of all the columns along with the most common values:
{context}

Here is your specific task:

{params.llm_prompts[extension_type].prompt}

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


def get_schema_from_statistics(
  params: LLMParams,
) -> str:
  """Get the schema of a db from the parquet stats file."""
  if params.statistics_parquet is None:
    return ""
  df_stats = pl.read_parquet(params.statistics_parquet)
  return get_histogram_as_str(df_stats)


def llm_extension(
  params: ExtensionAndLLMEndpoint,
) -> None:
  llm_params = params.llm_params
  assert llm_params is not None
  llm_client = Client()
  random.seed(42)
  con = duckdb.connect(database=params.database_path, read_only=True)
  destination_path = Path(params.destination_folder)
  schema_context: str = get_schema_from_statistics(llm_params)
  rows: list[dict[str, str]] = []
  log_rows: list[dict[str, str | bool]] = []
  for query, original_path in tqdm(get_random_queries(params)):
    retries = 0
    valid_query = False
    duckdb_exception = Exception("no query was found")
    while retries <= llm_params.retry and not valid_query:
      if retries == 0:
        extension_type, messages = get_random_prompt(
          llm_params, query, schema_context
        )
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
