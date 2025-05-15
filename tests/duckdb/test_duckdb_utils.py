from query_generator.duckdb_connection.setup import setup_duckdb
from query_generator.duckdb_connection.utils import (
  get_distinct_count,
  get_equi_height_histogram,
)
from query_generator.utils.definitions import Dataset



def test_distinct_values():
  """Test the setup of DuckDB."""
  # Setup DuckDB
  con = setup_duckdb(Dataset.TPCDS, 0.1 )
  assert get_distinct_count(con, "call_center", "cc_call_center_sk") == 1


def test_histogram():
  con = setup_duckdb(Dataset.TPCDS, 0.1 )
  histogram = get_equi_height_histogram(con, "item", "i_item_id", 5)
  assert len(histogram) == 5
