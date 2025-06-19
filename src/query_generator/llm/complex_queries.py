import random
import re
from collections import defaultdict
from pathlib import Path

import polars as pl
from duckdb import DuckDBPyConnection
from ollama import Client
from tqdm import tqdm

from query_generator.duckdb_connection.setup import setup_duckdb
from query_generator.utils.params import (
  ComplexQueryGenerationParametersEndpoint,
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
  params: ComplexQueryGenerationParametersEndpoint,
) -> list[tuple[str, Path]]:
  sql_files = list(Path(params.queries_path).rglob("*.sql"))
  random_query_paths = random.sample(sql_files, params.total_queries)
  return [
    (p.read_text(), p.relative_to(params.queries_path))
    for p in random_query_paths
  ]


def get_random_prompt(
  params: ComplexQueryGenerationParametersEndpoint, query: str
) -> tuple[str, LLM_Message]:
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


def create_complex_queries(
  params: ComplexQueryGenerationParametersEndpoint,
) -> None:
  llm_client = Client()
  random.seed(params.seed)
  con = setup_duckdb(params.dataset, 0)
  destination_path = Path(params.destination_folder)
  query_counter: dict[str, int] = defaultdict(int)
  rows: list[dict[str, str]] = []
  for query, original_path in tqdm(get_random_queries(params)):
    retries = 0
    valid_query = False
    duckdb_exception = Exception("no query was found")
    while retries <= params.retry and not valid_query:
      if retries == 0:
        extension_type, messages = get_random_prompt(params, query)
      else:
        add_retry_query_to_messages(messages, duckdb_exception)
      query_llm(llm_client, messages, params.llm_model)
      llm_extracted_query = extract_sql(messages[-1]["content"])
      valid_query, duckdb_exception = validate_query_duckdb(
        con, llm_extracted_query
      )
      retries += 1

    if valid_query:
      query_counter[extension_type] = 1 + query_counter[extension_type]
      new_path = (
        destination_path
        / extension_type
        / f"{query_counter[extension_type]}.sql"
      )
      new_path.parent.mkdir(parents=True, exist_ok=True)
      new_path.write_text(llm_extracted_query)
      rows.append(
        {
          "extension_type": extension_type,
          "retries": str(retries),
          "original_path": str(original_path),
          "new_path": str(new_path.relative_to(destination_path)),
        }
      )
  new_queries_df = pl.DataFrame(rows)
  new_queries_df.write_csv(destination_path / "complex_queries.csv")
