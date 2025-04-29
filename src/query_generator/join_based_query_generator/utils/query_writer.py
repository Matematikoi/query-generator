import os

import polars as pl

from query_generator.utils.definitions import (
  Dataset,
  Extension,
  GeneratedQueryFeatures,
)


class Writer:
  def __init__(self, dataset: Dataset, extension: Extension) -> None:
    self.extension = extension
    self.dataset = dataset

  def write_query(self, query: GeneratedQueryFeatures) -> None:
    """
    Write the generated queries to a file.
    Args:
        queries (List[str]): List of SQL queries.
        file_name (str): Name of the output file.
    """
    folder = (
      "data/generated_queries/"
      f"{self.extension.value}/{self.dataset.value}/{query.fact_table}_{query.template_number}"
    )
    if not os.path.exists(folder):
      os.makedirs(folder)
    file_name = f"{query.template_number}_{query.predicate_number}.sql"
    with open(os.path.join(folder, file_name), "w") as f:
      f.write(query.query)

  def get_binning_folder(self) -> str:
    """
    Get the folder path for the binning queries.
    Returns:
        str: The folder path for the binning queries.
    """
    path = f"data/generated_queries/{self.extension.value}/{self.dataset.value}"
    if not os.path.exists(path):
      os.makedirs(path)
    return path

  def write_query_to_bin(self, bin: int, query: GeneratedQueryFeatures) -> None:
    folder = f"{self.get_binning_folder()}/bin_{bin}"
    if not os.path.exists(folder):
      os.makedirs(folder)
    file_name = f"{query.template_number}_{query.predicate_number}.sql"
    with open(os.path.join(folder, file_name), "w") as f:
      f.write(query.query)

  def write_dataframe(self, df: pl.DataFrame) -> None:
    folder = self.get_binning_folder()
    path = f"{folder}/{self.dataset.value}_binning.csv"
    df.write_csv(path)
