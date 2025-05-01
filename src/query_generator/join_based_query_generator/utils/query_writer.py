import os
from pathlib import Path

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

  def get_binning_folder(self) -> Path:
    path = Path(
      f"data/generated_queries/{self.extension.value}/{self.dataset.value}"
    )
    path.mkdir(parents=True, exist_ok=True)
    return path

  def write_query_to_batch(
    self, batch: int, query: GeneratedQueryFeatures
  ) -> str:
    """
    Returns relative path of the file to the final CSV
    """
    batch_dir = Path(self.get_binning_folder()) / f"batch_{batch}"

    batch_dir.mkdir(parents=True, exist_ok=True)

    file_path = (
      batch_dir / f"{query.template_number}_{query.predicate_number}.sql"
    )

    file_path.write_text(query.query, encoding="utf-8")

    return str(file_path.relative_to(self.get_binning_folder()))

  def write_dataframe(self, input_dataframe: pl.DataFrame) -> None:
    folder = self.get_binning_folder()
    path = f"{folder}/{self.dataset.value}_batches.csv"
    input_dataframe.write_csv(path)
