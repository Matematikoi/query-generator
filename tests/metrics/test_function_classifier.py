"""Tests for the SQL function classifier."""

from pathlib import Path

import pytest
import tomllib

from query_generator.metrics.function_classifier import (
  FunctionRecord,
  FunctionRecordFields,
  parse_sql_functions,
)
from query_generator.utils.definitions import SQLDialect

_FUNCTIONS_DIR = (
  Path(__file__).parent.parent.parent / "params_config" / "functions"
)

_TOML_PATH = _FUNCTIONS_DIR / "minimal_example.toml"
_DUCKDB_TOML_PATH = _FUNCTIONS_DIR / "duckdb_functions.toml"
_SPARK_TOML_PATH = _FUNCTIONS_DIR / "spark_functions.toml"
_DUCKDB_CODEX_TOML_PATH = _FUNCTIONS_DIR / "duckdb_functions_codex.toml"
_PYSPARK_CODEX_TOML_PATH = _FUNCTIONS_DIR / "pyspark_functions_codex.toml"


def _load_toml_examples_from(
  path: Path,
) -> list[tuple[str, str, str, str]]:
  """Load (category, subcategory, name, sql) from a function TOML file."""
  with open(path, "rb") as f:
    data = tomllib.load(f)
  entries: list[tuple[str, str, str, str]] = []
  for category, subcategories in data.items():
    if not isinstance(subcategories, dict):
      continue
    for subcategory, expressions in subcategories.items():
      if not isinstance(expressions, dict):
        continue
      for name, sql in expressions.items():
        entries.append((category, subcategory, name, sql))
  return entries


def _load_toml_examples() -> list[tuple[str, str, str, str]]:
  """Load (category, subcategory, name, sql) from the minimal_example.toml."""
  return _load_toml_examples_from(_TOML_PATH)


_TOML_EXAMPLES = _load_toml_examples()
_DUCKDB_EXAMPLES = _load_toml_examples_from(_DUCKDB_TOML_PATH)
_SPARK_EXAMPLES = _load_toml_examples_from(_SPARK_TOML_PATH)
_DUCKDB_CODEX_EXAMPLES = _load_toml_examples_from(_DUCKDB_CODEX_TOML_PATH)
_PYSPARK_CODEX_EXAMPLES = _load_toml_examples_from(_PYSPARK_CODEX_TOML_PATH)


def _assert_expected_classification(
  category: str,
  subcategory: str,
  name: str,
  sql: str,
  dialect: SQLDialect | None = None,
) -> None:
  result = parse_sql_functions(sql, dialect=dialect)
  assert isinstance(result, list), f"Expected list, got {type(result)}"
  found = any(
    r[FunctionRecordFields.CATEGORY] == category
    and r[FunctionRecordFields.SUBCATEGORY] == subcategory
    for r in result
  )
  seen = [
    (r[FunctionRecordFields.CATEGORY], r[FunctionRecordFields.SUBCATEGORY])
    for r in result
  ]
  assert found, (
    f"[{name}] Expected ({category!r}, {subcategory!r}) but got {seen}"
  )


def test_all_keys_present_in_every_record():
  sql = "SELECT SUM(ss_sales_price) FROM store_sales"
  result = parse_sql_functions(sql)
  assert len(result) > 0
  for record in result:
    for field in FunctionRecordFields:
      assert field in record


@pytest.mark.parametrize(
  "category,subcategory,name,sql",
  _TOML_EXAMPLES,
  ids=[f"{cat}.{sub}.{name}" for cat, sub, name, _ in _TOML_EXAMPLES],
)
def test_classification_correctness(
  category: str, subcategory: str, name: str, sql: str
):
  _assert_expected_classification(category, subcategory, name, sql)


def test_arithmetic_binary():
  result = parse_sql_functions("SELECT 1 + 1")
  assert len(result) == 1
  assert result[0]["category"] == "scalar"
  assert result[0]["subcategory"] == "arithmetic"
  assert result[0]["name"] == "Add"


def test_neg_unary():
  result = parse_sql_functions("SELECT -5")
  assert len(result) == 1
  assert result[0]["category"] == "scalar"
  assert result[0]["subcategory"] == "arithmetic"
  assert result[0]["name"] == "Neg"


def test_mixed_func_and_binary():
  """ABS(a - b) should return both scalar.numeric (Abs) and scalar.arithmetic (Sub)."""
  result = parse_sql_functions("SELECT ABS(a - b)")
  cats = {(r["category"], r["subcategory"], r["name"]) for r in result}
  assert ("scalar", "numeric", "Abs") in cats
  assert ("scalar", "arithmetic", "Sub") in cats


def test_comparison_binary():
  result = parse_sql_functions("SELECT 1 = 1")
  assert len(result) == 1
  assert result[0]["category"] == "scalar"
  assert result[0]["subcategory"] == "comparison"
  assert result[0]["name"] == "EQ"


def test_logical_not():
  result = parse_sql_functions("SELECT NOT TRUE")
  assert any(
    r["subcategory"] == "logical" and r["name"] == "Not" for r in result
  )


def test_select_one_returns_empty():
  assert parse_sql_functions("SELECT 1") == []


def test_bad_sql_returns_empty():
  result = parse_sql_functions("")
  assert isinstance(result, list)


def test_function_record_fields_values():
  sql = "SELECT COUNT(*) FROM store_sales"
  result = parse_sql_functions(sql)
  assert len(result) > 0
  record: FunctionRecord = result[0]
  assert record[FunctionRecordFields.CATEGORY] == "agg"
  assert record[FunctionRecordFields.SUBCATEGORY] == "core"
  assert record[FunctionRecordFields.NAME] == "Count"
  assert isinstance(record[FunctionRecordFields.EXPRESSION], str)
  assert len(record[FunctionRecordFields.EXPRESSION]) > 0


@pytest.mark.parametrize(
  "category,subcategory,name,sql",
  _DUCKDB_EXAMPLES,
  ids=[f"{cat}.{sub}.{name}" for cat, sub, name, _ in _DUCKDB_EXAMPLES],
)
def test_duckdb_classification_correctness(
  category: str, subcategory: str, name: str, sql: str
):
  _assert_expected_classification(category, subcategory, name, sql)


@pytest.mark.parametrize(
  "category,subcategory,name,sql",
  _SPARK_EXAMPLES,
  ids=[f"{cat}.{sub}.{name}" for cat, sub, name, _ in _SPARK_EXAMPLES],
)
def test_spark_classification_correctness(
  category: str, subcategory: str, name: str, sql: str
):
  _assert_expected_classification(category, subcategory, name, sql)


@pytest.mark.parametrize(
  "category,subcategory,name,sql",
  _DUCKDB_CODEX_EXAMPLES,
  ids=[f"{cat}.{sub}.{name}" for cat, sub, name, _ in _DUCKDB_CODEX_EXAMPLES],
)
def test_duckdb_codex_classification_correctness(
  category: str, subcategory: str, name: str, sql: str
):
  _assert_expected_classification(
    category,
    subcategory,
    name,
    sql,
    dialect=SQLDialect.DUCKDB,
  )


@pytest.mark.parametrize(
  "category,subcategory,name,sql",
  _PYSPARK_CODEX_EXAMPLES,
  ids=[f"{cat}.{sub}.{name}" for cat, sub, name, _ in _PYSPARK_CODEX_EXAMPLES],
)
def test_pyspark_codex_classification_correctness(
  category: str, subcategory: str, name: str, sql: str
):
  _assert_expected_classification(
    category,
    subcategory,
    name,
    sql,
    dialect=SQLDialect.SPARK,
  )
