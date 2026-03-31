"""Batch LLM extension using OpenAI Batch API."""

import logging
import random
from pathlib import Path
from typing import Any

from tqdm import tqdm

from query_generator.database_connection.abc import QueryValidator
from query_generator.database_connection.factory import build_query_validator
from query_generator.extensions.llm_clients import (
  BatchClient,
  BatchRequest,
  BatchResult,
  OpenAIBatchClient,
)
from query_generator.extensions.llm_extension import (
  SampledQuery,
  add_retry_query_to_messages,
  extract_sql,
  get_random_prompt,
  get_random_queries,
  log_not_valid_query,
  save_parquet,
  write_query_llm_and_get_row,
)
from query_generator.utils.params import LLMParams

logger = logging.getLogger(__name__)


def build_batch_requests(
  sampled_queries: list[SampledQuery],
  llm_params: LLMParams,
) -> tuple[list[BatchRequest], dict[str, dict[str, Any]]]:
  """Build BatchRequest list and a metadata dict keyed by custom_id.

  Returns:
    (requests, metadata) where metadata[custom_id] contains messages,
    extension_type, original_path, the original query, and
    function_names.
  """
  requests: list[BatchRequest] = []
  metadata: dict[str, dict[str, Any]] = {}
  for idx, sampled in enumerate(sampled_queries):
    custom_id = f"req-{idx}"
    messages = get_random_prompt(
      llm_params,
      sampled.query,
      sampled.extension_type,
      sampled.function_samples,
    )
    function_names = [name for name, _ in sampled.function_samples]
    requests.append(
      BatchRequest(
        custom_id=custom_id,
        messages=messages,
        model=llm_params.model,
      )
    )
    metadata[custom_id] = {
      "messages": messages,
      "extension_type": sampled.extension_type,
      "original_path": sampled.path,
      "query": sampled.query,
      "idx": idx,
      "function_names": function_names,
    }
  return requests, metadata


def _submit_and_collect(
  batch_client: BatchClient,
  requests: list[BatchRequest],
  batch_size: int,
  poll_interval: float,
  round_num: int,
) -> list[BatchResult]:
  """Chunk requests into batches, submit each, poll, and collect."""
  all_results: list[BatchResult] = []
  chunks = [
    requests[i : i + batch_size] for i in range(0, len(requests), batch_size)
  ]
  for chunk in tqdm(chunks, desc=f"Submitting batches round {round_num}"):  # type:ignore
    batch_id = batch_client.submit_batch(chunk)
    status = batch_client.poll_batch(batch_id, poll_interval)
    if status != "completed":
      logger.error("Batch %s ended with status: %s", batch_id, status)
      all_results.extend(
        BatchResult(
          custom_id=req.custom_id,
          content=None,
          error=f"Batch ended with status: {status}",
        )
        for req in chunk
      )
      continue
    results = batch_client.download_results(batch_id)
    all_results.extend(results)
  return all_results


def _validate_results(
  results: list[BatchResult],
  metadata: dict[str, dict[str, Any]],
  query_validator: QueryValidator,
  round_num: int,
) -> tuple[
  list[dict[str, Any]],
  list[dict[str, Any]],
  list[BatchRequest],
  dict[str, dict[str, Any]],
]:
  """Validate batch results against DuckDB.

  Returns:
    (valid_entries, log_entries, failed_requests_for_retry,
     updated_metadata)
  """
  valid_entries: list[dict[str, Any]] = []
  log_entries: list[dict[str, Any]] = []
  retry_requests: list[BatchRequest] = []
  retry_metadata: dict[str, dict[str, Any]] = {}

  for result in tqdm(results, desc=f"Validating round {round_num}"):  # type:ignore
    meta = metadata.get(result.custom_id)
    if meta is None:
      logger.warning("No metadata for custom_id %s", result.custom_id)
      continue

    idx = meta["idx"]
    extension_type = meta["extension_type"]
    original_path = meta["original_path"]
    messages = meta["messages"]
    function_names = meta.get("function_names", [])

    if result.error or not result.content:
      error_msg = result.error or "Empty response"
      log_entries.append(
        {
          "extension_type": extension_type,
          "retries": str(round_num),
          "original_path": original_path,
          "valid_query": False,
          "last_duckdb_exception": error_msg,
          "messages": messages,
          "new_path": "",
          "client_logs": "batch_api",
          "function_names": function_names,
        }
      )
      # Add to retry with LLM error
      updated_messages = list(messages)
      updated_messages.append(
        {
          "role": "assistant",
          "content": result.content or "I can't help you with that",
        }
      )
      add_retry_query_to_messages(updated_messages, Exception(error_msg))
      retry_requests.append(
        BatchRequest(
          custom_id=result.custom_id,
          messages=updated_messages,
          model=meta.get("model", ""),
        )
      )
      retry_metadata[result.custom_id] = {
        **meta,
        "messages": updated_messages,
      }
      continue

    # Extract SQL and validate
    messages_with_response = list(messages)
    messages_with_response.append(
      {"role": "assistant", "content": result.content}
    )
    sql = extract_sql(result.content)
    valid, duckdb_exception = query_validator.is_query_valid(sql)

    if valid:
      valid_entries.append(
        {
          "idx": idx,
          "extension_type": extension_type,
          "original_path": original_path,
          "query": sql,
          "retries": round_num,
          "messages": messages_with_response,
          "function_names": function_names,
        }
      )
      log_entries.append(
        {
          "extension_type": extension_type,
          "retries": str(round_num),
          "original_path": original_path,
          "valid_query": True,
          "last_duckdb_exception": "",
          "messages": messages_with_response,
          "new_path": "",
          "client_logs": "batch_api",
          "function_names": function_names,
        }
      )
    else:
      log_not_valid_query(duckdb_exception, sql)
      log_entries.append(
        {
          "extension_type": extension_type,
          "retries": str(round_num),
          "original_path": original_path,
          "valid_query": False,
          "last_duckdb_exception": str(duckdb_exception),
          "messages": messages_with_response,
          "new_path": "",
          "client_logs": "batch_api",
          "function_names": function_names,
        }
      )
      # Build retry with DuckDB error
      add_retry_query_to_messages(messages_with_response, duckdb_exception)
      retry_requests.append(
        BatchRequest(
          custom_id=result.custom_id,
          messages=messages_with_response,
          model=meta.get("model", ""),
        )
      )
      retry_metadata[result.custom_id] = {
        **meta,
        "messages": messages_with_response,
      }

  return valid_entries, log_entries, retry_requests, retry_metadata


def batch_llm_extension(
  llm_params: LLMParams,
  input_queries_base_path: Path,
  destination_path: Path,
) -> int:
  """Generate new queries using the OpenAI Batch API.

  Returns:
    The number of valid generated queries.
  """
  random.seed(42)
  batch_client: BatchClient = OpenAIBatchClient()
  query_validator = build_query_validator(llm_params)

  sampled_queries = get_random_queries(input_queries_base_path, llm_params)
  requests, metadata = build_batch_requests(sampled_queries, llm_params)

  # Set model on all requests (already set in build_batch_requests)
  all_valid: list[dict[str, Any]] = []
  all_logs: list[dict[str, Any]] = []
  rows: list[dict[str, str]] = []

  current_requests = requests
  current_metadata = metadata

  for round_num in range(llm_params.retry + 1):
    if not current_requests:
      break

    logger.info(
      "Batch round %d: submitting %d requests.",
      round_num,
      len(current_requests),
    )

    results = _submit_and_collect(
      batch_client,
      current_requests,
      llm_params.batch_size,
      llm_params.batch_poll_interval_seconds,
      round_num,
    )

    valid_entries, log_entries, retry_requests, retry_metadata = (
      _validate_results(results, current_metadata, query_validator, round_num)
    )

    # Save valid queries
    for entry in valid_entries:
      row = write_query_llm_and_get_row(
        destination_path,
        entry["extension_type"],
        entry["idx"],
        entry["original_path"],
        entry["query"],
      )
      row["retries"] = str(entry["retries"])
      row["function_names"] = entry["function_names"]
      rows.append(row)

    all_valid.extend(valid_entries)
    all_logs.extend(log_entries)

    # Incremental save
    save_parquet(destination_path / "llm_extension.parquet", rows)
    save_parquet(destination_path / "logs.parquet", all_logs)

    current_requests = retry_requests
    current_metadata = retry_metadata

    logger.info(
      "Round %d: %d valid, %d to retry.",
      round_num,
      len(valid_entries),
      len(retry_requests),
    )

  logger.info("Total batch LLM queries generated: %d.", len(rows))
  return len(rows)
