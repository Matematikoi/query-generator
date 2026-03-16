"""Tests for the batch adapter-based LLM extension loop."""

import json
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock, patch

from query_generator.extensions.llm_clients import (
  AnthropicBatchAdapter,
  OpenAIBatchAdapter,
  PromptRequest,
  PromptResult,
)
from query_generator.extensions.llm_extension import llm_extension
from query_generator.utils.params import LLMParams

# A minimal valid SQL query for DuckDB (no tables needed)
VALID_SQL = "SELECT 1"
VALID_RESPONSE = f"```sql\n{VALID_SQL}\n```"
INVALID_RESPONSE = "```sql\nSELECT * FROM nonexistent_table_xyz\n```"
EMPTY_RESPONSE = "I can't help with that"


@dataclass
class MockBatchAdapter:
  """Mock adapter that returns predefined responses per round."""

  # List of response maps per round. Each map: custom_id -> response_content
  rounds: list[dict[str, str]] = field(default_factory=list)
  call_count: int = 0

  def submit(
    self,
    requests: list[PromptRequest],
    model: str,
  ) -> list[PromptResult]:
    round_responses = (
      self.rounds[self.call_count] if self.call_count < len(self.rounds) else {}
    )
    self.call_count += 1
    results = []
    for req in requests:
      content = round_responses.get(req.custom_id, "")
      results.append(
        PromptResult(
          custom_id=req.custom_id,
          response_content=content,
          success=bool(content),
        )
      )
    return results


def _make_llm_params(
  db_path: str,
  total_queries: int = 2,
  retry: int = 1,
) -> LLMParams:
  """Create LLMParams pointing at a real in-memory DuckDB path."""
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
    database_path=db_path,
    total_queries=total_queries,
    retry=retry,
    prompts_path=str(prompts_path),
    schema_path=str(schema_path),
  )


def _setup_queries_dir(base: Path, count: int = 2) -> Path:
  """Create a temp directory with dummy .sql files."""
  for i in range(count):
    p = base / f"query_{i}.sql"
    p.write_text("SELECT 1")
  return base


def test_all_valid_first_round(tmp_path: Path) -> None:
  """All queries succeed on the first round — no retries needed."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=2)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  import duckdb

  duckdb.connect(db_path).close()

  params = _make_llm_params(db_path, total_queries=2, retry=1)

  # Both queries will get a valid response
  adapter = MockBatchAdapter(
    rounds=[
      {
        # We don't know the exact custom_ids ahead of time,
        # so use a MatchAll adapter instead
      }
    ]
  )

  # Use a simpler adapter that always returns valid SQL
  @dataclass
  class AlwaysValidAdapter:
    def submit(self, requests: list[PromptRequest], model: str):
      return [
        PromptResult(
          custom_id=r.custom_id,
          response_content=VALID_RESPONSE,
          success=True,
        )
        for r in requests
      ]

  result = llm_extension(
    llm_params=params,
    adapter=AlwaysValidAdapter(),
    model="test-model",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )
  assert result == 2


def test_retry_succeeds_second_round(tmp_path: Path) -> None:
  """First round returns invalid SQL, second round returns valid SQL."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  import duckdb

  duckdb.connect(db_path).close()

  params = _make_llm_params(db_path, total_queries=1, retry=1)

  call_count = 0

  @dataclass
  class RetryAdapter:
    def submit(self, requests: list[PromptRequest], model: str):
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        # First round: invalid SQL
        return [
          PromptResult(
            custom_id=r.custom_id,
            response_content=INVALID_RESPONSE,
            success=True,
          )
          for r in requests
        ]
      # Second round: valid SQL
      return [
        PromptResult(
          custom_id=r.custom_id,
          response_content=VALID_RESPONSE,
          success=True,
        )
        for r in requests
      ]

  result = llm_extension(
    llm_params=params,
    adapter=RetryAdapter(),
    model="test-model",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )
  assert result == 1
  assert call_count == 2


def test_all_fail_exhausts_retries(tmp_path: Path) -> None:
  """All rounds return invalid SQL — 0 valid queries after retries."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  import duckdb

  duckdb.connect(db_path).close()

  params = _make_llm_params(db_path, total_queries=1, retry=2)

  @dataclass
  class AlwaysInvalidAdapter:
    def submit(self, requests: list[PromptRequest], model: str):
      return [
        PromptResult(
          custom_id=r.custom_id,
          response_content=INVALID_RESPONSE,
          success=True,
        )
        for r in requests
      ]

  result = llm_extension(
    llm_params=params,
    adapter=AlwaysInvalidAdapter(),
    model="test-model",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )
  assert result == 0


def test_empty_llm_response_triggers_retry(tmp_path: Path) -> None:
  """LLM returns empty content — should retry."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  import duckdb

  duckdb.connect(db_path).close()

  params = _make_llm_params(db_path, total_queries=1, retry=1)

  call_count = 0

  @dataclass
  class EmptyThenValidAdapter:
    def submit(self, requests: list[PromptRequest], model: str):
      nonlocal call_count
      call_count += 1
      if call_count == 1:
        return [
          PromptResult(
            custom_id=r.custom_id,
            response_content="",
            success=False,
          )
          for r in requests
        ]
      return [
        PromptResult(
          custom_id=r.custom_id,
          response_content=VALID_RESPONSE,
          success=True,
        )
        for r in requests
      ]

  result = llm_extension(
    llm_params=params,
    adapter=EmptyThenValidAdapter(),
    model="test-model",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )
  assert result == 1
  assert call_count == 2


def _make_prompt_requests(count: int = 2) -> list[PromptRequest]:
  """Create dummy PromptRequest objects for adapter tests."""
  return [
    PromptRequest(
      custom_id=f"req_{i}",
      messages=[
        {"role": "system", "content": "You are a SQL expert."},
        {"role": "user", "content": f"Write query {i}"},
      ],
      extension_type="subquery",
      original_path=f"query_{i}.sql",
      cnt=i,
    )
    for i in range(count)
  ]


def test_openai_adapter_submit() -> None:
  """OpenAI adapter: upload → create batch → poll → download results."""
  mock_openai_module = MagicMock()
  mock_client = MagicMock()
  mock_openai_module.OpenAI.return_value = mock_client

  # files.create returns an uploaded file object
  mock_uploaded = MagicMock()
  mock_uploaded.id = "file-abc123"
  mock_client.files.create.return_value = mock_uploaded

  # batches.create returns a batch with status "completed" immediately
  mock_batch = MagicMock()
  mock_batch.id = "batch-xyz"
  mock_batch.status = "completed"
  mock_batch.output_file_id = "file-output-456"
  mock_client.batches.create.return_value = mock_batch

  # files.content returns JSONL with results
  results_jsonl = "\n".join(
    [
      json.dumps(
        {
          "custom_id": "req_0",
          "response": {
            "body": {
              "choices": [{"message": {"content": "```sql\nSELECT 1\n```"}}]
            }
          },
        }
      ),
      json.dumps(
        {
          "custom_id": "req_1",
          "response": {
            "body": {
              "choices": [{"message": {"content": "```sql\nSELECT 2\n```"}}]
            }
          },
        }
      ),
    ]
  )
  mock_output_content = MagicMock()
  mock_output_content.text = results_jsonl
  mock_client.files.content.return_value = mock_output_content

  # Inject mock module into sys.modules
  old_module = sys.modules.get("openai")
  sys.modules["openai"] = mock_openai_module
  try:
    adapter = OpenAIBatchAdapter(poll_interval=0.0)
    requests = _make_prompt_requests(2)
    results = adapter.submit(requests, "gpt-4o")
  finally:
    if old_module is not None:
      sys.modules["openai"] = old_module
    else:
      del sys.modules["openai"]

  assert len(results) == 2
  assert results[0].custom_id == "req_0"
  assert results[0].response_content == "```sql\nSELECT 1\n```"
  assert results[0].success is True
  assert results[1].custom_id == "req_1"
  assert results[1].response_content == "```sql\nSELECT 2\n```"
  assert results[1].success is True

  # Verify API calls were made
  mock_client.files.create.assert_called_once()
  mock_client.batches.create.assert_called_once()
  mock_client.files.content.assert_called_once_with("file-output-456")


def test_anthropic_adapter_submit() -> None:
  """Anthropic adapter: create batch → poll → retrieve results."""
  mock_anthropic_module = MagicMock()
  mock_client = MagicMock()
  mock_anthropic_module.Anthropic.return_value = mock_client

  # batches.create returns a batch that is already ended
  mock_batch = MagicMock()
  mock_batch.id = "msgbatch-001"
  mock_batch.processing_status = "ended"
  mock_client.messages.batches.create.return_value = mock_batch

  # Build mock results
  mock_result_0 = MagicMock()
  mock_result_0.custom_id = "req_0"
  mock_result_0.result.type = "succeeded"
  mock_block_0 = MagicMock()
  mock_block_0.type = "text"
  mock_block_0.text = "```sql\nSELECT 1\n```"
  mock_result_0.result.message.content = [mock_block_0]

  mock_result_1 = MagicMock()
  mock_result_1.custom_id = "req_1"
  mock_result_1.result.type = "succeeded"
  mock_block_1 = MagicMock()
  mock_block_1.type = "text"
  mock_block_1.text = "```sql\nSELECT 2\n```"
  mock_result_1.result.message.content = [mock_block_1]

  mock_client.messages.batches.results.return_value = [
    mock_result_0,
    mock_result_1,
  ]

  # Inject mock module into sys.modules
  old_module = sys.modules.get("anthropic")
  sys.modules["anthropic"] = mock_anthropic_module
  try:
    adapter = AnthropicBatchAdapter(poll_interval=0.0)
    requests = _make_prompt_requests(2)
    results = adapter.submit(requests, "claude-sonnet-4-20250514")
  finally:
    if old_module is not None:
      sys.modules["anthropic"] = old_module
    else:
      del sys.modules["anthropic"]

  assert len(results) == 2
  assert results[0].custom_id == "req_0"
  assert results[0].response_content == "```sql\nSELECT 1\n```"
  assert results[0].success is True
  assert results[1].custom_id == "req_1"
  assert results[1].response_content == "```sql\nSELECT 2\n```"
  assert results[1].success is True

  # Verify API calls were made
  mock_client.messages.batches.create.assert_called_once()
  mock_client.messages.batches.results.assert_called_once_with("msgbatch-001")
