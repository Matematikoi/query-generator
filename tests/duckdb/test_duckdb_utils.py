import datetime
from pathlib import Path

import pytest

from query_generator.duckdb_connection.setup import generate_db
from query_generator.duckdb_connection.utils import (
  get_distinct_count,
  get_equi_height_histogram,
  get_frequent_non_null_values,
)
from query_generator.synthetic_queries.synthetic_query_generator import (
  get_result_from_duckdb,
)
from query_generator.tools.histograms import DuckDBHistogramParser
from query_generator.utils.definitions import Dataset
from query_generator.utils.params import GenerateDBEndpoint
from tests.utils import is_float

TEMP_DB_PATH = "/tmp/tests/small_tpcds_0.1.db"


@pytest.fixture(scope="module")
def duckdb_connection():
  """Fixture to set up and tear down a DuckDB connection."""
  con = generate_db(GenerateDBEndpoint(Dataset.TPCDS, TEMP_DB_PATH, 0.1))
  yield con
  con.close()
  db_path = Path(TEMP_DB_PATH)
  if db_path.exists():
    db_path.unlink()


def test_distinct_values(duckdb_connection):
  """Test the setup of DuckDB."""
  # Setup DuckDB
  con = duckdb_connection
  assert get_distinct_count(con, "call_center", "cc_call_center_sk") == 1


@pytest.mark.parametrize(
  "query, expected_result",
  [
    ("SELECT COUNT(*) FROM customer", 10000),
    ("SELECT 1", 1),
  ],
)
def test_duck_db_execution(query, expected_result, duckdb_connection):
  """Test the execution of queries in DuckDB."""
  # Setup DuckDB
  con = duckdb_connection
  val = get_result_from_duckdb(query, con)
  assert val == expected_result, f"Expected {expected_result}, but got {val}"


def test_histogram(duckdb_connection):
  con = duckdb_connection
  histogram = get_equi_height_histogram(con, "item", "i_current_price", 5)
  histogram_parser = DuckDBHistogramParser(histogram, "float")
  assert len(histogram) == 5
  assert len(histogram_parser.bins) == 5
  assert len(histogram_parser.counts) == 5
  assert len(histogram_parser.lower_bounds) == 5
  assert len(histogram_parser.upper_bounds) == 5
  assert histogram_parser.lower_bounds[0] is None
  assert is_float(histogram_parser.upper_bounds[0])
  for h in range(1, 5):
    assert is_float(histogram_parser.lower_bounds[h])
    assert is_float(histogram_parser.upper_bounds[h])


def test_most_common_values_datetime(duckdb_connection):
  con = duckdb_connection
  most_common_values = get_frequent_non_null_values(
    con, "item", "i_rec_end_date", 2
  )
  assert len(most_common_values) == 2
  for value in most_common_values:
    assert isinstance(value.value, datetime.date)
    assert isinstance(value.count, int)
    assert value.count == 300


def test_most_common_values_string(duckdb_connection):
  con = duckdb_connection
  most_common_values = get_frequent_non_null_values(con, "item", "i_item_id", 2)
  assert len(most_common_values) == 2
  for value in most_common_values:
    assert isinstance(value.value, str)
    assert isinstance(value.count, int)


def test_most_common_values_float(duckdb_connection):
  con = duckdb_connection
  most_common_values = get_frequent_non_null_values(
    con, "item", "i_current_price", 2
  )
  assert len(most_common_values) == 2
  for value in most_common_values:
    assert is_float(value.value)
    assert isinstance(value.count, int)


def test_most_common_values_int(duckdb_connection):
  con = duckdb_connection
  most_common_values = get_frequent_non_null_values(con, "item", "i_item_sk", 2)
  assert len(most_common_values) == 2
  for value in most_common_values:
    assert is_float(value.value)
    assert isinstance(value.count, int)
