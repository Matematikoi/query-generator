"""Tests for batch_llm_extension using mocked batch APIs."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb

from query_generator.extensions.batch_llm_extension import batch_llm_extension
from query_generator.extensions.llm_clients import (
  BatchResult,
  OpenAIBatchClient,
)
from query_generator.utils.params import LLMEngineParams, LLMParams

VALID_SQL = "SELECT 1"
VALID_RESPONSE = f"```sql\n{VALID_SQL}\n```"
INVALID_RESPONSE = "```sql\nSELECT * FROM nonexistent_table_xyz\n```"


def _make_llm_params(
  db_path: str,
  total_queries: int = 2,
  retry: int = 1,
  batch_size: int = 50,
) -> LLMParams:
  """Create LLMParams for batch testing."""
  prompts_path = (
    Path(__file__).parent.parent.parent
    / "params_config"
    / "prompts"
    / "basic_prompt.toml"
  )
  schema_path = (
    Path(__file__).parent.parent.parent
    / "params_config"
    / "schemas"
    / "dev.txt"
  )
  return LLMParams(
    total_queries=total_queries,
    retry=retry,
    model="gpt-4o-mini",
    provider="openai",
    batch_size=batch_size,
    batch_poll_interval_seconds=0.01,
    engine_params=LLMEngineParams(
      database_path=db_path,
      prompts_path=str(prompts_path),
      schema_path=str(schema_path),
    ),
  )


def _setup_queries_dir(base: Path, count: int = 2) -> Path:
  """Create a temp directory with dummy .sql files."""
  for i in range(count):
    p = base / f"query_{i}.sql"
    p.write_text("SELECT 1")
  return base


def _mock_batch_client(results_by_round: list[list[BatchResult]]):
  """Create a mock OpenAIBatchClient returning results per round."""
  mock_client = MagicMock()
  mock_client.submit_batch.return_value = "batch-123"
  mock_client.poll_batch.return_value = "completed"
  mock_client.download_results.side_effect = results_by_round
  return mock_client


@patch("query_generator.extensions.batch_llm_extension.OpenAIBatchClient")
def test_all_valid_first_round(
  mock_client_cls: MagicMock, tmp_path: Path
) -> None:
  """All queries succeed on the first batch round."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=2)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  round0_results = [
    BatchResult(custom_id="req-0", content=VALID_RESPONSE, error=None),
    BatchResult(custom_id="req-1", content=VALID_RESPONSE, error=None),
  ]
  mock_client = _mock_batch_client([round0_results])
  mock_client_cls.return_value = mock_client

  params = _make_llm_params(db_path, total_queries=2, retry=1)
  result = batch_llm_extension(
    llm_params=params,
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 2
  assert mock_client.submit_batch.call_count == 1
  assert (dest / "llm_extension.parquet").exists()
  assert (dest / "logs.parquet").exists()


@patch("query_generator.extensions.batch_llm_extension.OpenAIBatchClient")
def test_retry_succeeds(mock_client_cls: MagicMock, tmp_path: Path) -> None:
  """First round fails, retry round succeeds."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  round0_results = [
    BatchResult(custom_id="req-0", content=INVALID_RESPONSE, error=None),
  ]
  round1_results = [
    BatchResult(custom_id="req-0", content=VALID_RESPONSE, error=None),
  ]
  mock_client = _mock_batch_client([round0_results, round1_results])
  mock_client_cls.return_value = mock_client

  params = _make_llm_params(db_path, total_queries=1, retry=1)
  result = batch_llm_extension(
    llm_params=params,
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 1
  assert mock_client.submit_batch.call_count == 2


@patch("query_generator.extensions.batch_llm_extension.OpenAIBatchClient")
def test_all_retries_exhausted(
  mock_client_cls: MagicMock, tmp_path: Path
) -> None:
  """All retry rounds return invalid SQL — result is 0."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  round0_results = [
    BatchResult(custom_id="req-0", content=INVALID_RESPONSE, error=None),
  ]
  round1_results = [
    BatchResult(custom_id="req-0", content=INVALID_RESPONSE, error=None),
  ]
  mock_client = _mock_batch_client([round0_results, round1_results])
  mock_client_cls.return_value = mock_client

  params = _make_llm_params(db_path, total_queries=1, retry=1)
  result = batch_llm_extension(
    llm_params=params,
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 0
  assert mock_client.submit_batch.call_count == 2


@patch("query_generator.extensions.batch_llm_extension.OpenAIBatchClient")
def test_partial_success_with_retry(
  mock_client_cls: MagicMock, tmp_path: Path
) -> None:
  """One query succeeds, one fails and succeeds on retry."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=2)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  round0_results = [
    BatchResult(custom_id="req-0", content=VALID_RESPONSE, error=None),
    BatchResult(custom_id="req-1", content=INVALID_RESPONSE, error=None),
  ]
  round1_results = [
    BatchResult(custom_id="req-1", content=VALID_RESPONSE, error=None),
  ]
  mock_client = _mock_batch_client([round0_results, round1_results])
  mock_client_cls.return_value = mock_client

  params = _make_llm_params(db_path, total_queries=2, retry=1)
  result = batch_llm_extension(
    llm_params=params,
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 2
  assert mock_client.submit_batch.call_count == 2


@patch("query_generator.extensions.batch_llm_extension.OpenAIBatchClient")
def test_batch_api_error(mock_get_client: MagicMock, tmp_path: Path) -> None:
  """Batch returns an API-level error for a request."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  round0_results = [
    BatchResult(custom_id="req-0", content=None, error="rate_limit_exceeded"),
  ]
  round1_results = [
    BatchResult(custom_id="req-0", content=VALID_RESPONSE, error=None),
  ]
  mock_client = _mock_batch_client([round0_results, round1_results])
  mock_get_client.return_value = mock_client

  params = _make_llm_params(db_path, total_queries=1, retry=1)
  result = batch_llm_extension(
    llm_params=params,
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 1


@patch("query_generator.extensions.llm_clients.OpenAI")
def test_openai_batch_client(_mock_openai: MagicMock) -> None:
  """Verify OpenAIBatchClient can be instantiated."""
  client = OpenAIBatchClient()
  assert isinstance(client, OpenAIBatchClient)
