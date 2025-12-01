from pathlib import Path

import pytest

from query_generator.duckdb_connection.query_validation import (
  DuckDBQueryExecutor,
)
from query_generator.duckdb_connection.setup import generate_db
from query_generator.utils.definitions import Dataset
from query_generator.utils.exceptions import DuckDBTimeoutError
from query_generator.utils.params import GenerateDBEndpoint

TEMP_DB_PATH = "/tmp/tests/small_test.db"


@pytest.fixture
def setup_and_teardown_db():
  """Fixture to handle setup and teardown for DuckDB tests."""
  db_path = Path(TEMP_DB_PATH)
  if db_path.exists():
    db_path.unlink()
  yield
  if db_path.exists():
    db_path.unlink()


def test_dev_duckdb_setup_tpch(setup_and_teardown_db):
  """Test the setup of DuckDB."""
  # Setup DuckDB
  con = generate_db(GenerateDBEndpoint(Dataset.TPCH, TEMP_DB_PATH, 0.0))
  assert con is not None, "DuckDB connection should not be None"
  assert con.execute("SELECT 1").fetchall() == [(1,)], "DuckDB should return 1"
  assert con.sql("show tables").fetchall() == [
    ("customer",),
    ("lineitem",),
    ("nation",),
    ("orders",),
    ("part",),
    ("partsupp",),
    ("region",),
    ("supplier",),
  ], "DuckDB should have the TPCH tables"
  con.close()


def test_dev_duckdb_setup_tpcds(setup_and_teardown_db):
  """Test the setup of DuckDB."""
  # Setup DuckDB
  con = generate_db(GenerateDBEndpoint(Dataset.TPCDS, TEMP_DB_PATH, 0.0))
  assert con is not None, "DuckDB connection should not be None"
  assert con.execute("SELECT 1").fetchall() == [(1,)], "DuckDB should return 1"
  assert con.sql("show tables").fetchall() == [
    ("call_center",),
    ("catalog_page",),
    ("catalog_returns",),
    ("catalog_sales",),
    ("customer",),
    ("customer_address",),
    ("customer_demographics",),
    ("date_dim",),
    ("household_demographics",),
    ("income_band",),
    ("inventory",),
    ("item",),
    ("promotion",),
    ("reason",),
    ("ship_mode",),
    ("store",),
    ("store_returns",),
    ("store_sales",),
    ("time_dim",),
    ("warehouse",),
    ("web_page",),
    ("web_returns",),
    ("web_sales",),
    ("web_site",),
  ], "DuckDB should have the TPCDS tables"
  con.close()


def test_duckdb_timeout(setup_and_teardown_db):
  """Test validation actually timeouts"""
  con = generate_db(GenerateDBEndpoint(Dataset.TPCDS, TEMP_DB_PATH, 0.0))
  con.close()
  validator = DuckDBQueryExecutor(TEMP_DB_PATH, 1)
  long_running_query = """
  SELECT COUNT(*)
  FROM range(0, 100000000) t1(i)
  CROSS JOIN range(0, 100000000) t2(j);
  """
  valid, db_exeception = validator.is_query_valid(long_running_query)
  assert not valid
  assert isinstance(db_exeception, DuckDBTimeoutError)
  validator.conn.close()


@pytest.mark.parametrize(
  "query,expected_output_size",
  [
    ("select 1 where 1 = 0;", 0),
    ("select 1;", 1),
    ("select 1 union all select 2;", 2),
    # Test the ; will not break it
    ("select 1 where 1 = 0;", 0),
    ("select 1;", 1),
    ("select 1 union all select 2;", 2),
  ],
)
def test_duckdb_count_output(
  setup_and_teardown_db, query: str, expected_output_size: int
):
  """Ensure output size calculation matches row counts for simple queries."""
  con = generate_db(GenerateDBEndpoint(Dataset.TPCDS, TEMP_DB_PATH, 0.0))
  con.close()
  validator = DuckDBQueryExecutor(TEMP_DB_PATH, 1)

  output_size = validator.get_query_output_size(query)

  assert output_size == expected_output_size
  validator.conn.close()
