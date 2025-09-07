from pathlib import Path

import polars as pl

from query_generator.utils.definitions import (
  BatchGeneratedQueryToWrite,
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
  def __init__(self, destination_folder: str) -> None:
    self.destination_folder = Path(destination_folder)

  # TODO(Gabriel): https://chiselapp.com/user/matematikoi/repository/query-generation/tktview/8dd46fc66a
  # this should be a pathlib
  def write_query(self, query: GeneratedQueryFeatures) -> None:
    """Write the generated queries to a file.

    Args:
        queries (List[str]): List of SQL queries.
        file_name (str): Name of the output file.

    """

    file = (
      self.destination_folder
      / f"{query.fact_table}_{query.template_number}"
      / f"{query.template_number}_{query.predicate_number}.sql"
    )
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(query.query, encoding="utf-8")
    print("Query written to:", file)

  def write_query_to_batch(self, query: BatchGeneratedQueryToWrite) -> str:
    """Returns relative path of the file to the final CSV"""
    prefix = f"batch_{query.batch_number}"
    batch_dir = self.destination_folder / prefix

    batch_dir.mkdir(parents=True, exist_ok=True)

    file_path = (
      batch_dir / f"{query.fact_table}_{query.template_number}_"
      f"{query.predicate_number}.sql"
    )

    self._do_not_overwrite(file_path)
    file_path.write_text(query.query, encoding="utf-8")
    return str(file_path.relative_to(self.destination_folder))

  def write_dataframe(
    self, input_dataframe: pl.DataFrame, name: str = "output.parquet"
  ) -> None:
    file_path = self.destination_folder / "output.parquet"
    input_dataframe.write_parquet(file_path)

  def write_toml(self, input_toml: str) -> None:
    path = self.destination_folder / "parameters.toml"
    path.write_text(input_toml, encoding="utf-8")

  def _do_not_overwrite(self, path: Path) -> None:
    """Check if the file already exists and do not overwrite it."""
    if path.exists():
      raise OverwriteFileError(path)
