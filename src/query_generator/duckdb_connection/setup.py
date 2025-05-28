import os

import duckdb

from query_generator.utils.definitions import Dataset
from query_generator.utils.exceptions import (
  MissingScaleFactorError,
  PartiallySupportedDatasetError,
  UnkownDatasetError,
)


def load_and_install_libraries() -> None:
  duckdb.install_extension("TPCDS")
  duckdb.install_extension("TPCH")
  duckdb.load_extension("TPCDS")
  duckdb.load_extension("TPCH")


def generate_data(
  scale_factor: float | int,
  dataset: Dataset,
  con: duckdb.DuckDBPyConnection,
) -> None:
  if dataset == Dataset.TPCDS:
    con.execute(f"CALL dsdgen(sf = {scale_factor})")
  elif dataset == Dataset.TPCH:
    con.execute(f"CALL dbgen(sf = {scale_factor})")
  elif dataset == Dataset.JOB:
    raise PartiallySupportedDatasetError(dataset.value)
  else:
    raise UnkownDatasetError(dataset)


def get_path(
  dataset: Dataset,
  scale_factor: float | int | None,
) -> str:
  if dataset in [Dataset.TPCDS, Dataset.TPCH]:
    return f"data/duckdb/{dataset.value}/{scale_factor}.db"
  if dataset == Dataset.JOB:
    return f"data/duckdb/{dataset.value}/job.db"
  raise UnkownDatasetError(dataset.value)


def setup_duckdb(
  dataset: Dataset,
  scale_factor: int | float | None = None,
) -> duckdb.DuckDBPyConnection:
  """Installs TPCDS and TPCH datasets in DuckDB.

  If the scale factor required is not generated, it will generate it.
  It returns a duckdb connection to the database.

  Args:
      dataset (Dataset): The dataset to set up (TPCDS, TPCH, JOB).
      scale_factor (int | float | None): The scale factor for the dataset.
          It is only none for JOB dataset.
  """
  load_and_install_libraries()
  db_path = get_path(dataset, scale_factor)
  if os.path.exists(db_path):
    print(f"Database {db_path} already exists")
    return duckdb.connect(db_path)

  if scale_factor is None:
    # scale factor can only be ommited for JOB dataset
    # and currently we can't generate it
    raise MissingScaleFactorError(dataset.value)

  os.makedirs(os.path.dirname(db_path), exist_ok=True)
  con = duckdb.connect(db_path)
  generate_data(scale_factor, dataset, con)
  print(f"Database {db_path} created.")
  return con
