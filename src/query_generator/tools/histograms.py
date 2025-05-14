import duckdb

from query_generator.utils.definitions import Dataset


def query_histograms(
  dataset: Dataset, scale_factor: int | float, con: duckdb.DuckDBPyConnection
) -> None:
  """Creates histograms for the given dataset.
  Args:
      dataset (Dataset): The dataset to create histograms for.
      scale_factor (int): The scale factor for the histograms.
      con (duckdb.DuckDBPyConnection): The connection to the database.
  """

  return
