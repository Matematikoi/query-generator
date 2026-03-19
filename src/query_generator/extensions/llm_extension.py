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


@dataclass
class SampledQuery:
  """A query with pre-selected extension type and function samples."""

  query: str
  path: str
  extension_type: str
  function_samples: list[tuple[str, str]]


@dataclass
class QueryProcessor:
  """Shared state for processing queries through the LLM retry loop."""

  llm_client_factory: LLMClientFactory
  llm_config_params: str
  llm_params: LLMParams
  query_validator: DuckDBQueryExecutor
  schema_context: str


def get_random_queries(
  queries_base_path: Path, llm_params: LLMParams
) -> list[SampledQuery]:
  """Get random queries with pre-selected extension types and samples."""
  sql_files = sorted(queries_base_path.rglob("*.sql"))
  random_query_paths = random.choices(sql_files, k=llm_params.total_queries)

  extension_types = list(llm_params.prompts.weighted_prompts.keys())
  weights = [
    llm_params.prompts.weighted_prompts[e].weight for e in extension_types
  ]
  selected_types = random.choices(
    extension_types, weights=weights, k=llm_params.total_queries
  )

  n = min(
    llm_params.number_of_function_examples,
    len(llm_params.function_examples),
  )
  selected_samples = [
    random.sample(llm_params.function_examples, n)
    if llm_params.function_examples
    else []
    for _ in range(llm_params.total_queries)
  ]

  return [
    SampledQuery(
      query=p.read_text(),
      path=str(p.relative_to(queries_base_path)),
      extension_type=ext_type,
      function_samples=func_samples,
    )
    for p, ext_type, func_samples in zip(
      random_query_paths, selected_types, selected_samples, strict=False
    )
  ]


def format_function_examples(
  function_samples: list[tuple[str, str]],
) -> str:
  """Format pre-sampled function examples for the prompt.

  Returns empty string when the list is empty."""
  if not function_samples:
    return ""
  header = (
    "Add the following SQL functions to the modified query. "
    "Skip any that are impossible to fit:\n"
  )
  entries: list[str] = []
  for dotted_name, sql in function_samples:
    parts = dotted_name.rsplit(".", 1)
    name = parts[-1]
    category = parts[0] if len(parts) > 1 else ""
    label = f"{name} ({category})" if category else name
    entries.append(f"- Function: {label}\n  Example:\n  ```sql\n  {sql}\n  ```")
  return header + "\n\n".join(entries)


def get_random_prompt(
  params: LLMParams,
  query: str,
  extension_type: str,
  function_samples: list[tuple[str, str]],
) -> LLM_Message:
  """Build the LLM prompt. Pure function — no random calls."""
  function_text = format_function_examples(function_samples)
  prompt_text = params.prompts.weighted_prompts[extension_type].prompt

  user_content = f"""This is the query you will be modifying. \
You can base yourself upon it:

```sql
{query}
```

{prompt_text}

{function_text}

Return only the modified query in ```sql ``` format.
"""

  return [
    {"role": "system", "content": params.prompts.base_prompt},
    {"role": "user", "content": user_content},
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
  function_names: list[str]

  def to_row(self, destination_path: Path) -> dict[str, Any]:
    """Build the row dict for the llm_extension parquet."""
    return {
      **write_query_llm_and_get_row(
        destination_path,
        self.extension_type,
        self.cnt,
        self.original_path,
        self.query,
      ),
      "retries": str(self.retries),
      "function_names": self.function_names,
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
      "function_names": self.function_names,
    }


def process_single_query(
  processor: QueryProcessor,
  cnt: int,
  sampled: SampledQuery,
) -> QueryResult:
  """Run LLM + retry loop for one query. Returns the outcome."""
  llm_client = processor.llm_client_factory.build()
  messages = get_random_prompt(
    processor.llm_params,
    sampled.query,
    sampled.extension_type,
    sampled.function_samples,
  )
  function_names = [name for name, _ in sampled.function_samples]

  valid_query = False
  duckdb_exception: Exception | None = Exception("no query was found")
  llm_extracted_query = ""

  for attempt in range(processor.llm_params.retry + 1):
    if attempt > 0:
      add_retry_query_to_messages(messages, duckdb_exception)
    logger.info("Starting query #%d, attempt #%d", cnt, attempt + 1)
    llm_client.query(messages, processor.llm_config_params)
    logger.debug("LLM response received.")
    llm_extracted_query = extract_sql(messages[-1]["content"])
    valid_query, duckdb_exception = processor.query_validator.is_query_valid(
      llm_extracted_query
    )
    if valid_query:
      break
    log_not_valid_query(duckdb_exception, llm_extracted_query)

  return QueryResult(
    valid=valid_query,
    query=llm_extracted_query,
    extension_type=sampled.extension_type,
    original_path=sampled.path,
    cnt=cnt,
    retries=attempt + 1,
    duckdb_exception=duckdb_exception,
    messages=messages,
    client_logs=llm_client.get_logs(),
    function_names=function_names,
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
  processor = QueryProcessor(
    llm_client_factory=llm_client_factory,
    llm_config_params=llm_config_params,
    llm_params=llm_params,
    query_validator=DuckDBQueryExecutor(
      llm_params.database_path, llm_params.duckdb_timeout_seconds
    ),
    schema_context=get_schema_from_statistics(llm_params),
  )
  rows: list[dict[str, str]] = []
  log_rows: list[dict[str, Any]] = []
  sampled_queries = get_random_queries(input_queries_base_path, llm_params)

  for cnt, sampled in tqdm(  # type:ignore
    enumerate(sampled_queries),
    desc="LLM-Extension",
    total=len(sampled_queries),
  ):
    result = process_single_query(processor, cnt, sampled)

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
