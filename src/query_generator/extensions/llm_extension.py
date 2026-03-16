import logging
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from tqdm import tqdm

from query_generator.duckdb_connection.query_validation import (
  DuckDBQueryExecutor,
)
from query_generator.extensions.llm_clients import (
  LLM_Message,
  LLMClientFactory,
)
from query_generator.tools.format_histogram import get_histogram_as_str
from query_generator.utils.params import (
  LLMParams,
)

logger = logging.getLogger(__name__)


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
  if matches:
    return matches[-1].strip()
  logger.warning("Error: Unable to find query in LLM response")
  return ""


def add_retry_query_to_messages(
  messages: LLM_Message, exception: Exception | None
) -> None:
  messages.append(
    {
      "role": "user",
      "content": f"""
      Fix this error with the query you provided:
      {
        str(exception)
        if exception is not None
        else "No exception provided. Please ensure your query is valid."
      }
    """,
    }
  )


def get_new_query_name(cnt: int, original_path: str):
  return f"{cnt}_{original_path.replace('/', '_')}"


def write_query_llm_and_get_row(
  destination_path: Path,
  extension_type: str,
  cnt: int,
  original_path: str,
  query: str,
):
  new_path = (
    destination_path / extension_type / get_new_query_name(cnt, original_path)
  )
  new_path.parent.mkdir(parents=True, exist_ok=True)
  new_path.write_text(query)
  return {
    "extension_type": extension_type,
    "original_path": (original_path),
    "new_path": str(new_path.relative_to(destination_path)),
  }


def save_parquet(destination_path: Path, rows: list[Any]):
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


def log_not_valid_query(duckdb_exception: Exception | None, query: str) -> None:
  logger.warning(
    f"Generated query is not valid. Exception:\n{
      duckdb_exception
      if duckdb_exception is not None
      else 'No exception provided.'
    }"
  )
  logger.debug(f"Query that failed:\n{query}")


@dataclass
class QueryResult:
  """Result of processing a single query through the LLM retry loop."""

  valid: bool
  query: str
  extension_type: str
  original_path: str
  cnt: int
  retries: int
  duckdb_exception: Exception | None
  messages: LLM_Message
  client_logs: dict[str, Any]

  def to_row(self, destination_path: Path) -> dict[str, str]:
    """Build the row dict for the llm_extension parquet (valid queries only)."""
    return {
      **write_query_llm_and_get_row(
        destination_path,
        self.extension_type,
        self.cnt,
        self.original_path,
        self.query,
      ),
      "retries": str(self.retries),
    }

  def to_log_row(self) -> dict[str, Any]:
    """Build the log row dict (always appended, valid or not)."""
    return {
      "extension_type": self.extension_type,
      "retries": str(self.retries),
      "original_path": self.original_path,
      "valid_query": self.valid,
      "last_duckdb_exception": str(self.duckdb_exception)
      if not self.valid
      else "",
      "messages": self.messages,
      "new_path": get_new_query_name(self.cnt, self.original_path),
      "client_logs": self.client_logs,
    }


def _process_single_query(  # noqa: PLR0913
  llm_client_factory: LLMClientFactory,
  llm_config_params: str,
  llm_params: LLMParams,
  query_validator: DuckDBQueryExecutor,
  cnt: int,
  query: str,
  original_path: str,
  schema_context: str,
) -> QueryResult:
  """Run LLM + retry loop for one query. Returns the outcome."""
  llm_client = llm_client_factory.build()
  extension_type, messages = get_random_prompt(
    llm_params, query, schema_context
  )

  valid_query = False
  duckdb_exception: Exception | None = Exception("no query was found")
  llm_extracted_query = ""

  for attempt in range(llm_params.retry + 1):
    if attempt > 0:
      add_retry_query_to_messages(messages, duckdb_exception)
    logger.info("Starting query #%d, attempt #%d", cnt, attempt + 1)
    llm_client.query(messages, llm_config_params)
    logger.debug("LLM response received.")
    llm_extracted_query = extract_sql(messages[-1]["content"])
    valid_query, duckdb_exception = query_validator.is_query_valid(
      llm_extracted_query
    )
    if valid_query:
      break
    log_not_valid_query(duckdb_exception, llm_extracted_query)

  return QueryResult(
    valid=valid_query,
    query=llm_extracted_query,
    extension_type=extension_type,
    original_path=original_path,
    cnt=cnt,
    retries=attempt + 1,
    duckdb_exception=duckdb_exception,
    messages=messages,
    client_logs=llm_client.get_logs(),
  )


def llm_extension(
  llm_params: LLMParams,
  llm_client_factory: LLMClientFactory,
  llm_config_params: str,
  input_queries_base_path: Path,
  destination_path: Path,
) -> int:
  """Generate new queries using LLM prompts.

  Returns:
    The number of generated queries.
  """
  random.seed(42)
  query_validator = DuckDBQueryExecutor(
    llm_params.database_path, llm_params.duckdb_timeout_seconds
  )
  schema_context: str = get_schema_from_statistics(llm_params)
  rows: list[dict[str, str]] = []
  log_rows: list[dict[str, Any]] = []
  sampled_queries = get_random_queries(input_queries_base_path, llm_params)

  for cnt, (query, original_path) in tqdm(  # type:ignore
    enumerate(sampled_queries), desc="LLM-Extension", total=len(sampled_queries)
  ):
    result = _process_single_query(
      llm_client_factory=llm_client_factory,
      llm_config_params=llm_config_params,
      llm_params=llm_params,
      query_validator=query_validator,
      cnt=cnt,
      query=query,
      original_path=original_path,
      schema_context=schema_context,
    )

    if result.valid:
      logger.debug("Generated a valid query.")
      rows.append(result.to_row(destination_path))
    else:
      logger.error(
        "Failed to generate a valid query after %d retries.",
        llm_params.retry,
      )

    log_rows.append(result.to_log_row())
    save_parquet(destination_path / "llm_extension.parquet", rows)
    save_parquet(destination_path / "logs.parquet", log_rows)

  logger.info("Total LLM queries generated: %d.", len(rows))
  return len(rows)
