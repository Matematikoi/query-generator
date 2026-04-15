import datetime
from pathlib import Path

import pytest

from query_generator.duckdb_connection.setup import generate_db
from query_generator.duckdb_connection.utils import (
  get_distinct_count,
  get_equi_height_histogram,
  get_frequent_non_null_values,
  get_null_count,
  DuckDBColumnInfo,
)
from query_generator.database_connection.duckdb_validation import (
  DuckDBQueryExecutor,
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
  column_info = DuckDBColumnInfo(
    con=con, table="call_center", column="cc_call_center_sk"
  )

  assert get_distinct_count(column_info) == 1


def test_distinct_values_sample(duckdb_connection):
  """Test the setup of DuckDB."""
  # Setup DuckDB
  con = duckdb_connection
  column_info = DuckDBColumnInfo(
    con=con, table="web_returns", column="wr_return_quantity"
  )

  value_1 = get_distinct_count(column_info, sample_size=1000)
  value_2 = get_distinct_count(column_info, sample_size=1000)
  assert value_1 == value_2


def test_null_values(duckdb_connection):
  """Test null counting for a column."""
  con = duckdb_connection
  column_info = DuckDBColumnInfo(con=con, table="item", column="i_rec_end_date")
  expected = con.execute(
    f"SELECT COUNT_IF({column_info.column} IS NULL) FROM {column_info.table}"
  ).fetchall()[0][0]

  assert get_null_count(column_info) == expected


@pytest.mark.parametrize(
  "query, expected_result",
  [
    ("SELECT COUNT(*) FROM customer", 10000),
    ("SELECT 1", 1),
  ],
)
def test_duck_db_execution(query, expected_result, duckdb_connection):
  """Test get_synthetic_query_cardinality on DuckDBQueryExecutor."""
  executor = DuckDBQueryExecutor(
    database_path=TEMP_DB_PATH, timeout_seconds=10.0
  )
  # Reuse the fixture's open connection to avoid a read-only/write conflict.
  executor._persistent_con = duckdb_connection
  val = executor.get_synthetic_query_cardinality(query)
  assert val == expected_result, f"Expected {expected_result}, but got {val}"


def test_histogram(duckdb_connection):
  con = duckdb_connection
  params = DuckDBColumnInfo(con=con, table="item", column="i_current_price")
  histogram = get_equi_height_histogram(params, 5)
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
    DuckDBColumnInfo(con=con, table="item", column="i_rec_end_date"), 2, None
  )
  assert len(most_common_values) == 2
  for value in most_common_values:
    assert isinstance(value.value, datetime.date)
    assert isinstance(value.count, int)
    assert value.count == 300


def test_most_common_values_string(duckdb_connection):
  con = duckdb_connection
  most_common_values = get_frequent_non_null_values(
    DuckDBColumnInfo(con=con, table="item", column="i_item_id"), 2, None
  )
  assert len(most_common_values) == 2
  for value in most_common_values:
    assert isinstance(value.value, str)
    assert isinstance(value.count, int)


def test_most_common_values_float(duckdb_connection):
  con = duckdb_connection
  most_common_values = get_frequent_non_null_values(
    DuckDBColumnInfo(con=con, table="item", column="i_current_price"), 2, None
  )
  assert len(most_common_values) == 2
  for value in most_common_values:
    assert is_float(value.value)
    assert isinstance(value.count, int)


def test_most_common_values_int(duckdb_connection):
  con = duckdb_connection
  most_common_values = get_frequent_non_null_values(
    DuckDBColumnInfo(con=con, table="item", column="i_item_sk"), 2, None
  )
  assert len(most_common_values) == 2
  for value in most_common_values:
    assert is_float(value.value)
    assert isinstance(value.count, int)
