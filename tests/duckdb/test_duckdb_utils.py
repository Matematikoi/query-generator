import datetime

from query_generator.duckdb_connection.setup import setup_duckdb
from query_generator.duckdb_connection.utils import (
  get_distinct_count,
  get_equi_height_histogram,
  get_frequent_non_null_values,
)
from query_generator.tools.histograms import DuckDBHistogramParser
from query_generator.utils.definitions import Dataset
from tests.utils import is_float


def test_distinct_values():
  """Test the setup of DuckDB."""
  # Setup DuckDB
  con = setup_duckdb(Dataset.TPCDS, 0.1)
  assert get_distinct_count(con, "call_center", "cc_call_center_sk") == 1


def test_histogram():
  con = setup_duckdb(Dataset.TPCDS, 0.1)
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


def test_most_common_values_datetime():
  con = setup_duckdb(Dataset.TPCDS, 0.1)
  most_common_values = get_frequent_non_null_values(
    con, "item", "i_rec_end_date", 2
  )
  assert len(most_common_values) == 2
  for value in most_common_values:
    print(type(value.value))
    assert isinstance(value.value, datetime.date)
    assert isinstance(value.count, int)
    assert value.count == 300


def test_most_common_values_string():
  con = setup_duckdb(Dataset.TPCDS, 0.1)
  most_common_values = get_frequent_non_null_values(con, "item", "i_item_id", 2)
  assert len(most_common_values) == 2
  for value in most_common_values:
    assert isinstance(value.value, str)
    assert isinstance(value.count, int)


def test_most_common_values_float():
  con = setup_duckdb(Dataset.TPCDS, 0.1)
  most_common_values = get_frequent_non_null_values(
    con, "item", "i_current_price", 2
  )
  assert len(most_common_values) == 2
  for value in most_common_values:
    assert is_float(value.value)
    assert isinstance(value.count, int)


def test_most_common_values_int():
  con = setup_duckdb(Dataset.TPCDS, 0.1)
  most_common_values = get_frequent_non_null_values(con, "item", "i_item_sk", 2)
  assert len(most_common_values) == 2
  for value in most_common_values:
    assert is_float(value.value)
    assert isinstance(value.count, int)
