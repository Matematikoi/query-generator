"""Tests for llm_extension using mocked OpenAILLMClient."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb

from query_generator.extensions.llm_clients import (
  LLMClientFactory,
  OpenAILLMClient,
  get_llm_client_factory,
)
from query_generator.extensions.llm_extension import llm_extension
from query_generator.utils.params import LLMParams

VALID_SQL = "SELECT 1"
VALID_RESPONSE = f"```sql\n{VALID_SQL}\n```"
INVALID_RESPONSE = "```sql\nSELECT * FROM nonexistent_table_xyz\n```"


def _make_llm_params(
  db_path: str,
  total_queries: int = 2,
  retry: int = 1,
) -> LLMParams:
  """Create LLMParams pointing at a real DuckDB path."""
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
    model="gpt-4o-mini",
    provider="openai",
    prompts_path=str(prompts_path),
    schema_path=str(schema_path),
  )


def _setup_queries_dir(base: Path, count: int = 2) -> Path:
  """Create a temp directory with dummy .sql files."""
  for i in range(count):
    p = base / f"query_{i}.sql"
    p.write_text("SELECT 1")
  return base


def _make_mock_openai_response(content: str) -> MagicMock:
  """Build a mock OpenAI chat completion response."""
  response = MagicMock()
  response.usage.completion_tokens = 10
  response.usage.prompt_tokens = 5
  choice = MagicMock()
  choice.message.content = content if content else None
  response.choices = [choice]
  return response


@patch("query_generator.extensions.llm_clients.OpenAI")
def test_openai_valid_first_attempt(
  mock_openai_cls: MagicMock, tmp_path: Path
) -> None:
  """All queries succeed on the first attempt — no retries needed."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=2)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  mock_openai_cls.return_value.chat.completions.create.return_value = (
    _make_mock_openai_response(VALID_RESPONSE)
  )

  params = _make_llm_params(db_path, total_queries=2, retry=1)
  factory = LLMClientFactory(factory=OpenAILLMClient, init_kwargs={})

  result = llm_extension(
    llm_params=params,
    llm_client_factory=factory,
    llm_config_params="gpt-4o-mini",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 2


@patch("query_generator.extensions.llm_clients.OpenAI")
def test_openai_retry_succeeds(
  mock_openai_cls: MagicMock, tmp_path: Path
) -> None:
  """First call returns invalid SQL, second returns valid."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  mock_openai_cls.return_value.chat.completions.create.side_effect = [
    _make_mock_openai_response(INVALID_RESPONSE),
    _make_mock_openai_response(VALID_RESPONSE),
  ]

  params = _make_llm_params(db_path, total_queries=1, retry=1)
  factory = LLMClientFactory(factory=OpenAILLMClient, init_kwargs={})

  result = llm_extension(
    llm_params=params,
    llm_client_factory=factory,
    llm_config_params="gpt-4o-mini",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 1
  assert mock_openai_cls.return_value.chat.completions.create.call_count == 2


@patch("query_generator.extensions.llm_clients.OpenAI")
def test_openai_empty_response_triggers_retry(
  mock_openai_cls: MagicMock, tmp_path: Path
) -> None:
  """Empty LLM response triggers a retry, second attempt succeeds."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  mock_openai_cls.return_value.chat.completions.create.side_effect = [
    _make_mock_openai_response(""),
    _make_mock_openai_response(VALID_RESPONSE),
  ]

  params = _make_llm_params(db_path, total_queries=1, retry=1)
  factory = LLMClientFactory(factory=OpenAILLMClient, init_kwargs={})

  result = llm_extension(
    llm_params=params,
    llm_client_factory=factory,
    llm_config_params="gpt-4o-mini",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 1
  assert mock_openai_cls.return_value.chat.completions.create.call_count == 2


@patch("query_generator.extensions.llm_clients.OpenAI")
def test_openai_all_retries_exhausted(
  mock_openai_cls: MagicMock, tmp_path: Path
) -> None:
  """All retries return invalid SQL — result is 0."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  mock_openai_cls.return_value.chat.completions.create.return_value = (
    _make_mock_openai_response(INVALID_RESPONSE)
  )

  params = _make_llm_params(db_path, total_queries=1, retry=2)
  factory = LLMClientFactory(factory=OpenAILLMClient, init_kwargs={})

  result = llm_extension(
    llm_params=params,
    llm_client_factory=factory,
    llm_config_params="gpt-4o-mini",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 0
  # 1 initial + 2 retries = 3 calls
  assert mock_openai_cls.return_value.chat.completions.create.call_count == 3


@patch("query_generator.extensions.llm_clients.OpenAI")
def test_openai_flex_passes_service_tier(mock_openai_cls: MagicMock) -> None:
  """OpenAILLMClient with service_tier='flex' forwards it to the API call."""
  mock_openai_cls.return_value.chat.completions.create.return_value = (
    _make_mock_openai_response(VALID_RESPONSE)
  )

  client = OpenAILLMClient(service_tier="flex")
  messages: list[dict[str, str]] = [{"role": "user", "content": "Hello"}]
  client.query(messages, "gpt-4o-mini")

  mock_openai_cls.return_value.chat.completions.create.assert_called_once()
  call_kwargs = (
    mock_openai_cls.return_value.chat.completions.create.call_args.kwargs
  )
  assert call_kwargs["service_tier"] == "flex"


def test_get_llm_client_factory_openai_flex() -> None:
  """get_llm_client_factory('openai-flex') returns factory with flex tier."""
  factory = get_llm_client_factory("openai-flex")
  assert factory.init_kwargs == {"service_tier": "flex"}
