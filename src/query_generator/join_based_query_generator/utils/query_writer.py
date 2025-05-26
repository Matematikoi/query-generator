import os
from pathlib import Path

import polars as pl

from query_generator.utils.definitions import (
  BatchGeneratedQueryFeatures,
  Dataset,
  Extension,
  GeneratedQueryFeatures,
)
from query_generator.utils.exceptions import OverwriteFileError


def write_parquet(df_to_write: pl.DataFrame, path: Path) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  df_to_write.write_parquet(path)


# TODO(Gabriel):  http://localhost:8080/tktview/46fca17ee0
#  Delete this code and everything that
#  touches it [46fca17ee0ab9e46]
def write_redundant_histogram_csv(
  redundant_histogram: pl.DataFrame, path: Path
) -> None:
  def join_redundant_array(array: list[str]) -> str:
    return f"[{', '.join([f"'{i}'" for i in array])}]"

  redundant_histogram.with_columns(
    pl.col("bins")
    .map_elements(join_redundant_array, return_dtype=pl.Utf8)
    .alias("bins"),
    pl.col("hists")
    .map_elements(join_redundant_array, return_dtype=pl.Utf8)
    .alias("hists"),
  ).select(
    [
      pl.col("table"),
      pl.col("column"),
      pl.col("dtype"),
      pl.col("distinct_count"),
      pl.col("bins"),
      pl.col("hists"),
    ]
  ).write_csv(path)


class Writer:
  def __init__(self, dataset: Dataset, extension: Extension) -> None:
    self.extension = extension
    self.dataset = dataset

  def write_query(self, query: GeneratedQueryFeatures) -> None:
    """Write the generated queries to a file.

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
      f"data/generated_queries/{self.extension.value}/{self.dataset.value}",
    )
    path.mkdir(parents=True, exist_ok=True)
    return path

  def write_query_to_batch(self, query: BatchGeneratedQueryFeatures) -> str:
    """Returns relative path of the file to the final CSV"""
    batch_dir = Path(self.get_binning_folder()) / query.prefix

    batch_dir.mkdir(parents=True, exist_ok=True)

    file_path = (
      batch_dir / f"{query.fact_table}_{query.template_number}_"
      f"{query.predicate_number}.sql"
    )

    self._do_not_overwrite(file_path)
    file_path.write_text(query.query, encoding="utf-8")
    return str(file_path.relative_to(self.get_binning_folder()))

  def write_dataframe(self, input_dataframe: pl.DataFrame) -> None:
    folder = self.get_binning_folder()
    path = f"{folder}/{self.dataset.value}_batches.csv"
    input_dataframe.write_csv(path)

  def _do_not_overwrite(self, path: Path) -> None:
    """Check if the file already exists and do not overwrite it."""
    if path.exists():
      raise OverwriteFileError(path)
