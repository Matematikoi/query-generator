from pathlib import Path
from typing import Generator

import duckdb
import pytest

PARQUET_PATH = Path("tmp/database_parquet/TPCDS_0.1")
DUCKDB_PATH = Path("tmp/database_TPCDS_0.1.duckdb")


@pytest.fixture(scope="session")
def spark() -> Generator:
  import logging
  import os

  import pyspark
  from pyspark.sql import SparkSession

  os.environ["SPARK_HOME"] = pyspark.__path__[0]
  logging.getLogger("py4j").setLevel(logging.INFO)

  if not PARQUET_PATH.exists():
    pytest.skip(
      f"Parquet database not found at {PARQUET_PATH}. Run generate-db first."
    )

  session = (
    SparkSession.builder.master("local[*]")
    .appName("integration-tests")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.log.level", "WARN")
    .getOrCreate()
  )

  for table_dir in sorted(PARQUET_PATH.iterdir()):
    if table_dir.is_dir():
      session.read.parquet(str(table_dir)).createOrReplaceTempView(
        table_dir.name
      )

  yield session
  session.stop()


@pytest.fixture(scope="session")
def duckdb_conn() -> duckdb.DuckDBPyConnection:
  if not DUCKDB_PATH.exists():
    pytest.skip(
      f"DuckDB database not found at {DUCKDB_PATH}. Run generate-db first."
    )
  return duckdb.connect(str(DUCKDB_PATH), read_only=True)
