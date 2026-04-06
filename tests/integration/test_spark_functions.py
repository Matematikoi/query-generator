"""Integration tests: validate Spark SQL function examples via EXPLAIN."""

import tomllib
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

_TOML_PATH = Path("params_config/functions/spark_functions.toml")


def _load_examples() -> list[tuple[str, str, str, str]]:
  data = tomllib.loads(_TOML_PATH.read_text())
  entries = []
  for category, subcats in data.items():
    if not isinstance(subcats, dict):
      continue
    for subcategory, funcs in subcats.items():
      if not isinstance(funcs, dict):
        continue
      for name, sql in funcs.items():
        entries.append((category, subcategory, name, sql))
  return entries


_EXAMPLES = _load_examples()


@pytest.mark.integration
@pytest.mark.parametrize(
  "category,subcategory,name,sql",
  _EXAMPLES,
  ids=[f"{cat}.{sub}.{name}" for cat, sub, name, _ in _EXAMPLES],
)
def test_spark_function_is_valid(
  spark: SparkSession,
  category: str,
  subcategory: str,
  name: str,
  sql: str,
) -> None:
  spark.sql(f"EXPLAIN {sql}")
