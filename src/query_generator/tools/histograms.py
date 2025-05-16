from pathlib import Path

import duckdb
import polars as pl

from query_generator.duckdb_connection.utils import (
  RawDuckDBHistograms,
  get_columns,
  get_distinct_count,
  get_equi_height_histogram,
  get_tables,
)
from query_generator.utils.definitions import Dataset


class DuckDBHistogramParser:
  """Class to represent a histogram in DuckDB."""

  def __init__(
    self, raw_histogram: list[RawDuckDBHistograms], duckdb_type: str
  ):
    self.bins = [data.bin for data in raw_histogram]
    self.counts = [data.count for data in raw_histogram]
    self._get_lower_upper_bounds()

  def _get_lower_upper_bounds(self) -> None:
    self.lower_bounds: list[str | None] = []
    self.upper_bounds: list[str] = []
    if len(self.bins) == 0:
      return
    # First bin is always special because it has a format
    # of "x <= 6" or "x <= AAAAAAAAAAAA" or "x <= 1998-01-01"
    self.lower_bounds.append(None)
    self.upper_bounds.append(self.bins[0][5:])
    # the rest of them are standard like
    # "AAAAAAAAKBAAAAAA < x <= AAAAAAAAOAAAAAAA"
    # "12 < x <= 18"
    # "2000-01-02 < x <= 2001-01-01"
    for bin in self.bins[1:]:
      lower_bound, upper_bound = bin.split(" < x <= ")
      self.lower_bounds.append(lower_bound)
      self.upper_bounds.append(upper_bound)

  def get_equiwidth_histogram_array(self) -> list[str]:
    return self.upper_bounds


def query_histograms(
  dataset: Dataset, histogram_size: int, con: duckdb.DuckDBPyConnection
) -> None:
  """Creates histograms for the given dataset.
  Args:
      dataset (Dataset): The dataset to create histograms for.
      scale_factor (int): The scale factor for the histograms.
      con (duckdb.DuckDBPyConnection): The connection to the database.
  """
  rows: list[dict[str, str | int | list[str]]] = []
  tables = get_tables(con)
  for table in tables:
    columns = get_columns(con, table)
    for column in columns:
      # Get Histogram array
      histogram_raw = get_equi_height_histogram(
        con, table, column.column_name, histogram_size
      )
      histogram_parser = DuckDBHistogramParser(
        histogram_raw, column.column_type
      )
      histogram_array = histogram_parser.get_equiwidth_histogram_array()

      # Get distinct count
      distinct_count = get_distinct_count(con, table, column.column_name)
      rows.append(
        {
          "table": table,
          "column": column.column_name,
          "histogram": histogram_array,
          "distinct_count": distinct_count,
          "dtype": column.column_type,
        }
      )

  path = Path(f"data/generated_histograms/{dataset.value}/histograms.parquet")
  path.parent.mkdir(parents=True, exist_ok=True)
  pl.DataFrame(rows).write_parquet(path)
