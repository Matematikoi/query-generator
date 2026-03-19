"""Tests for llm_extension using mocked OllamaLLMClient."""

import random
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb

from query_generator.extensions.llm_clients import (
  LLM_Message,
  LLMClientFactory,
  OllamaLLMClient,
)
from query_generator.extensions.llm_extension import (
  QueryResult,
  format_function_examples,
  get_random_prompt,
  get_random_queries,
  llm_extension,
)
from query_generator.utils.params import LLMParams

VALID_SQL = "SELECT 1"
VALID_RESPONSE = f"```sql\n{VALID_SQL}\n```"
INVALID_RESPONSE = "```sql\nSELECT * FROM nonexistent_table_xyz\n```"
EMPTY_RESPONSE = "I can't help you with that"


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
    model="llama3:latest",
    prompts_path=str(prompts_path),
    schema_path=str(schema_path),
  )


def _setup_queries_dir(base: Path, count: int = 2) -> Path:
  """Create a temp directory with dummy .sql files."""
  for i in range(count):
    p = base / f"query_{i}.sql"
    p.write_text("SELECT 1")
  return base


def _make_mock_chat_response(content: str) -> MagicMock:
  """Build a mock ollama chat response."""
  response = MagicMock()
  response.eval_count = 10
  response.prompt_eval_count = 5
  response.message.content = content if content else None
  return response


@patch("query_generator.extensions.llm_clients.Client")
def test_all_valid_first_attempt(
  mock_client_cls: MagicMock, tmp_path: Path
) -> None:
  """All queries succeed on the first attempt — no retries needed."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=2)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  mock_client_cls.return_value.chat.return_value = _make_mock_chat_response(
    VALID_RESPONSE
  )

  params = _make_llm_params(db_path, total_queries=2, retry=1)
  factory = LLMClientFactory(factory=OllamaLLMClient, init_kwargs={})

  result = llm_extension(
    llm_params=params,
    llm_client_factory=factory,
    llm_config_params="test-model",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 2


@patch("query_generator.extensions.llm_clients.Client")
def test_retry_succeeds(mock_client_cls: MagicMock, tmp_path: Path) -> None:
  """First call returns invalid SQL, second returns valid."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  mock_client_cls.return_value.chat.side_effect = [
    _make_mock_chat_response(INVALID_RESPONSE),
    _make_mock_chat_response(VALID_RESPONSE),
  ]

  params = _make_llm_params(db_path, total_queries=1, retry=1)
  factory = LLMClientFactory(factory=OllamaLLMClient, init_kwargs={})

  result = llm_extension(
    llm_params=params,
    llm_client_factory=factory,
    llm_config_params="test-model",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 1
  assert mock_client_cls.return_value.chat.call_count == 2


@patch("query_generator.extensions.llm_clients.Client")
def test_all_retries_exhausted(
  mock_client_cls: MagicMock, tmp_path: Path
) -> None:
  """All retries return invalid SQL — result is 0."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  mock_client_cls.return_value.chat.return_value = _make_mock_chat_response(
    INVALID_RESPONSE
  )

  params = _make_llm_params(db_path, total_queries=1, retry=2)
  factory = LLMClientFactory(factory=OllamaLLMClient, init_kwargs={})

  result = llm_extension(
    llm_params=params,
    llm_client_factory=factory,
    llm_config_params="test-model",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 0
  # 1 initial + 2 retries = 3 calls
  assert mock_client_cls.return_value.chat.call_count == 3


@patch("query_generator.extensions.llm_clients.Client")
def test_empty_response_triggers_retry(
  mock_client_cls: MagicMock, tmp_path: Path
) -> None:
  """Empty LLM response triggers a retry, second attempt succeeds."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=1)
  dest = tmp_path / "output"
  dest.mkdir()

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()

  mock_client_cls.return_value.chat.side_effect = [
    _make_mock_chat_response(""),
    _make_mock_chat_response(VALID_RESPONSE),
  ]

  params = _make_llm_params(db_path, total_queries=1, retry=1)
  factory = LLMClientFactory(factory=OllamaLLMClient, init_kwargs={})

  result = llm_extension(
    llm_params=params,
    llm_client_factory=factory,
    llm_config_params="test-model",
    input_queries_base_path=queries_dir,
    destination_path=dest,
  )

  assert result == 1
  assert mock_client_cls.return_value.chat.call_count == 2


def test_format_function_examples_returns_empty_when_no_examples() -> None:
  """When function_samples is empty, returns ''."""
  assert format_function_examples([]) == ""


def test_format_function_examples_returns_correct_format() -> None:
  """format_function_examples returns structured entries."""
  samples = [
    ("math.aggregate.SUM", "SELECT SUM(x) FROM t"),
    ("string.transform.UPPER", "SELECT UPPER(name) FROM t"),
    ("math.aggregate.AVG", "SELECT AVG(x) FROM t"),
  ]
  result = format_function_examples(samples)
  assert result.startswith(
    "If possible, try to use the following SQL functions"
  )
  assert "- Function: SUM (math.aggregate)" in result
  assert "- Function: UPPER (string.transform)" in result
  assert "- Function: AVG (math.aggregate)" in result
  assert result.count("```sql") == 3
  assert "SELECT SUM(x) FROM t" in result


def test_get_random_prompt_includes_function_examples(
  tmp_path: Path,
) -> None:
  """get_random_prompt includes function examples in the user message."""
  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()
  params = LLMParams(
    database_path=db_path,
    total_queries=1,
    retry=0,
    model="test",
    prompts_path=str(
      Path(__file__).parent.parent.parent
      / "params_config"
      / "prompts"
      / "basic_prompt.toml"
    ),
    schema_path=str(
      Path(__file__).parent.parent.parent
      / "params_config"
      / "schemas"
      / "dev.txt"
    ),
  )
  extension_types = list(params.prompts.weighted_prompts.keys())
  function_samples = [
    ("math.aggregate.SUM", "SELECT SUM(x) FROM t"),
    ("string.transform.UPPER", "SELECT UPPER(name) FROM t"),
  ]
  messages = get_random_prompt(
    params, "SELECT 1", extension_types[0], function_samples
  )
  user_content = messages[1]["content"]
  assert "try to use the following SQL functions" in user_content
  assert "- Function:" in user_content
  assert "```sql" in user_content


def test_get_random_prompt_omits_function_section_when_empty(
  tmp_path: Path,
) -> None:
  """get_random_prompt omits function section when samples are empty."""
  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()
  params = LLMParams(
    database_path=db_path,
    total_queries=1,
    retry=0,
    model="test",
    prompts_path=str(
      Path(__file__).parent.parent.parent
      / "params_config"
      / "prompts"
      / "basic_prompt.toml"
    ),
    schema_path=str(
      Path(__file__).parent.parent.parent
      / "params_config"
      / "schemas"
      / "dev.txt"
    ),
  )
  extension_types = list(params.prompts.weighted_prompts.keys())
  messages = get_random_prompt(params, "SELECT 1", extension_types[0], [])
  user_content = messages[1]["content"]
  assert "try to use the following SQL functions" not in user_content
  assert "```sql" in user_content


def test_get_random_queries_reproducible(tmp_path: Path) -> None:
  """Same seed produces identical results."""
  queries_dir = tmp_path / "queries"
  queries_dir.mkdir()
  _setup_queries_dir(queries_dir, count=5)

  db_path = str(tmp_path / "test.duckdb")
  duckdb.connect(db_path).close()
  function_examples_path = (
    Path(__file__).parent.parent.parent
    / "params_config"
    / "functions"
    / "standard_sql_functions.toml"
  )
  params = LLMParams(
    database_path=db_path,
    total_queries=10,
    retry=0,
    model="test",
    prompts_path=str(
      Path(__file__).parent.parent.parent
      / "params_config"
      / "prompts"
      / "basic_prompt.toml"
    ),
    schema_path=str(
      Path(__file__).parent.parent.parent
      / "params_config"
      / "schemas"
      / "dev.txt"
    ),
    function_examples_path=str(function_examples_path),
    number_of_function_examples=3,
  )

  random.seed(42)
  result1 = get_random_queries(queries_dir, params)

  random.seed(42)
  result2 = get_random_queries(queries_dir, params)

  assert result1 == result2


def test_function_names_in_log_row(tmp_path: Path) -> None:
  """QueryResult.to_log_row() includes function_names."""
  qr = QueryResult(
    valid=True,
    query="SELECT 1",
    extension_type="group_by",
    original_path="q.sql",
    cnt=0,
    retries=1,
    duckdb_exception=None,
    messages=[],
    client_logs={},
    function_names=[
      "math.aggregate.SUM",
      "string.transform.UPPER",
    ],
  )
  log_row = qr.to_log_row()
  assert "function_names" in log_row
  assert log_row["function_names"] == [
    "math.aggregate.SUM",
    "string.transform.UPPER",
  ]


def test_function_names_in_to_row(tmp_path: Path) -> None:
  """QueryResult.to_row() includes function_names."""
  dest = tmp_path / "output"
  dest.mkdir()

  qr = QueryResult(
    valid=True,
    query="SELECT 1",
    extension_type="group_by",
    original_path="q.sql",
    cnt=0,
    retries=1,
    duckdb_exception=None,
    messages=[],
    client_logs={},
    function_names=["math.aggregate.SUM"],
  )
  row = qr.to_row(dest)
  assert "function_names" in row
  assert row["function_names"] == ["math.aggregate.SUM"]
