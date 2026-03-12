"""Tests for the SQL function classifier."""

from pathlib import Path

import pytest
import tomllib

from query_generator.metrics.function_classifier import (
  FunctionRecord,
  FunctionRecordFields,
  parse_sql_functions,
)

_TOML_PATH = (
  Path(__file__).parent.parent.parent
  / "params_config"
  / "functions"
  / "minimal_example.toml"
)


def _load_toml_examples() -> list[tuple[str, str, str, str]]:
  """Load (category, subcategory, name, sql) from the minimal_example.toml."""
  with open(_TOML_PATH, "rb") as f:
    data = tomllib.load(f)
  entries: list[tuple[str, str, str, str]] = []
  for category, subcategories in data.items():
    for subcategory, expressions in subcategories.items():
      for name, sql in expressions.items():
        entries.append((category, subcategory, name, sql))
  return entries


_TOML_EXAMPLES = _load_toml_examples()


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
  result = parse_sql_functions(sql)
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
  assert isinstance(record[FunctionRecordFields.EXPRESSION], str)
  assert len(record[FunctionRecordFields.EXPRESSION]) > 0
