
from pathlib import Path

import duckdb

from query_generator.utils.definitions import Dataset
from query_generator.utils.exceptions import (
  DatabaseGenerationNotImplementedError,
  PartiallySupportedDatasetError,
  UnkownDatasetError,
)
from query_generator.utils.params import GenerateDBEndpoint


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


def setup_duckdb(params: GenerateDBEndpoint) -> duckdb.DuckDBPyConnection:
  """Installs TPCDS and TPCH datasets in DuckDB.

  If the scale factor required is not generated, it will generate it.
  It returns a duckdb connection to the database.

  Args:
      dataset (Dataset): The dataset to set up (TPCDS, TPCH, JOB).
      scale_factor (int | float | None): The scale factor for the dataset.
          It is only none for JOB dataset.
  """
  load_and_install_libraries()
  db_path = Path(params.db_path)

  if params.dataset not in [Dataset.TPCDS, Dataset.TPCH]:
    raise UnkownDatasetError(params.dataset.value)

  if params.scale_factor is None:
    # scale factor can only be ommited for JOB dataset
    # and currently we can't generate it
    raise DatabaseGenerationNotImplementedError(params.dataset.value)

  db_path.parent.mkdir(parents=True, exist_ok=True)
  con = duckdb.connect(db_path)
  generate_data(params.scale_factor, params.dataset, con)
  print(f"Database {db_path} created.")
