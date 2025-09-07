import pytest

from query_generator.synthetic_queries.synthetic_query_generator import (
  get_result_from_duckdb,
)
from query_generator.duckdb_connection.setup import setup_duckdb
from query_generator.utils.definitions import Dataset
from query_generator.utils.params import GenerateDBEndpoint
from pathlib import Path

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
  con = setup_duckdb(GenerateDBEndpoint(
    Dataset.TPCH, TEMP_DB_PATH, 0.0))
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


def test_dev_duckdb_setup_tpcds(setup_and_teardown_db):
  """Test the setup of DuckDB."""
  # Setup DuckDB
  con = setup_duckdb(GenerateDBEndpoint(
    Dataset.TPCDS, TEMP_DB_PATH, 0.0))
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


@pytest.mark.parametrize(
  "query, expected_result",
  [
    ("SELECT COUNT(*) FROM customer", 10000),
    ("SELECT 1", 1),
  ],
)
def test_duck_db_execution(query, expected_result,setup_and_teardown_db):
  """Test the execution of queries in DuckDB."""
  # Setup DuckDB
  con = setup_duckdb(GenerateDBEndpoint(
    Dataset.TPCDS, TEMP_DB_PATH, 0.1))
  val = get_result_from_duckdb(query, con)
  assert val == expected_result, f"Expected {expected_result}, but got {val}"
