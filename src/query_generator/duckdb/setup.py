import os

import duckdb

from query_generator.utils.definitions import Dataset


def load_and_install_libraries() -> None:
  duckdb.install_extension("TPCDS")
  duckdb.install_extension("TPCH")
  duckdb.load_extension("TPCDS")
  duckdb.load_extension("TPCH")


def generate_data(
  scale_factor: float | int, dataset: Dataset, con: duckdb.DuckDBPyConnection
) -> None:
  if dataset == Dataset.TPCDS:
    con.execute(f"CALL dsdgen(sf = {scale_factor})")
  elif dataset == Dataset.TPCH:
    con.execute(f"CALL dbgen(sf = {scale_factor})")
  else:
    raise ValueError(f"{dataset} is not supported")


def setup_duckdb(
  scale_factor: int | float, dataset: Dataset
) -> duckdb.DuckDBPyConnection:
  """Installs TPCDS and TPCH datasets in DuckDB.

  If the scale factor required is not generated, it will generate it.
  It returns a duckdb connection to the database.
  """
  load_and_install_libraries()
  db_path = f"data/duckdb/{dataset.value}/{scale_factor}.db"
  if os.path.exists(db_path):
    print(f"Database {db_path} already exists")
    return duckdb.connect(db_path)

  os.makedirs(os.path.dirname(db_path), exist_ok=True)
  con = duckdb.connect(db_path)
  generate_data(scale_factor, dataset, con)
  print(f"Database {db_path} created.")
  return con
