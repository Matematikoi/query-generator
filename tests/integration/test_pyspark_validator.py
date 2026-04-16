"""Integration tests for PySparkQueryValidator."""

import pytest
from pyspark.sql import SparkSession

from query_generator.database_connection.pyspark_validation import (
  PySparkQueryValidator,
)
from tests.integration.conftest import PARQUET_PATH


@pytest.mark.integration
def test_get_synthetic_query_cardinality(spark: SparkSession) -> None:
  """PySparkQueryValidator.get_synthetic_query_cardinality returns correct COUNT(*)."""
  validator = PySparkQueryValidator(
    parquet_path=str(PARQUET_PATH),
    timeout_seconds=30.0,
  )
  # Reuse the session fixture so no new JVM is spawned.
  validator._spark = spark

  result = validator.get_synthetic_query_cardinality(
    "SELECT COUNT(*) FROM customer"
  )
  assert result == 10000, f"Expected 10000, got {result}"


@pytest.mark.integration
def test_get_synthetic_query_cardinality_returns_minus_one_on_error(
  spark: SparkSession,
) -> None:
  """Returns -1 when the query is invalid."""
  validator = PySparkQueryValidator(
    parquet_path=str(PARQUET_PATH),
    timeout_seconds=30.0,
  )
  validator._spark = spark

  result = validator.get_synthetic_query_cardinality(
    "SELECT COUNT(*) FROM nonexistent_table"
  )
  assert result == -1
