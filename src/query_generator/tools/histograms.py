from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
from tqdm import tqdm

from query_generator.duckdb_connection.utils import (
  RawDuckDBHistograms,
  RawDuckDBMostCommonValues,
  RawDuckDBTableDescription,
  get_columns,
  get_distinct_count,
  get_equi_height_histogram,
  get_frequent_non_null_values,
  get_histogram_excluding_common_values,
  get_tables,
)
from query_generator.utils.definitions import Dataset

LIMIT_FOR_DISTINCT_VALUES = 1000


@dataclass
class HistogramParams:
  con: duckdb.DuckDBPyConnection
  table: str
  column: RawDuckDBTableDescription
  histogram_size: int


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


def get_most_common_values(
  con: duckdb.DuckDBPyConnection,
  table: str,
  column: str,
  common_value_size: int,
  distinct_count: int,
) -> list[RawDuckDBMostCommonValues]:
  result: list[RawDuckDBMostCommonValues] = []
  if distinct_count < LIMIT_FOR_DISTINCT_VALUES:
    result = get_frequent_non_null_values(con, table, column, common_value_size)
  return result


def get_histogram_array(histogram_params: HistogramParams) -> list[str]:
  histogram_raw = get_equi_height_histogram(
    histogram_params.con,
    histogram_params.table,
    histogram_params.column.column_name,
    histogram_params.histogram_size,
  )
  histogram_parser = DuckDBHistogramParser(
    histogram_raw, histogram_params.column.column_type
  )
  return histogram_parser.get_equiwidth_histogram_array()


def get_histogram_array_excluding_common_values(
  histogram_params: HistogramParams,
  common_values_size: int,
  distinct_count: int,
) -> list[str]:
  histogram_array: list[RawDuckDBHistograms] = []
  if (
    distinct_count < LIMIT_FOR_DISTINCT_VALUES
    and distinct_count > common_values_size
  ):
    histogram_array = get_histogram_excluding_common_values(
      histogram_params.con,
      histogram_params.table,
      histogram_params.column.column_name,
      histogram_params.histogram_size,
      common_values_size,
    )
  histogram_parser = DuckDBHistogramParser(
    histogram_array,
    histogram_params.column.column_type,
  )
  return histogram_parser.get_equiwidth_histogram_array()


def query_histograms(
  dataset: Dataset,
  histogram_size: int,
  common_values_size: int,
  con: duckdb.DuckDBPyConnection,
  *,
  include_mvc: bool,
) -> None:
  """Creates histograms for the given dataset.
  Args:
      dataset (Dataset): The dataset to create histograms for.
      scale_factor (int): The scale factor for the histograms.
      con (duckdb.DuckDBPyConnection): The connection to the database.
  """
  rows: list[dict[str, Any]] = []
  tables = get_tables(con)
  for table in tqdm(tables, position=0):
    columns = get_columns(con, table)
    pbar = tqdm(columns, desc="Starting…", position=1, leave=False)
    for column in pbar:
      pbar.set_description(
        f"Processing table {table} column {column.column_name}"
      )
      histogram_params = HistogramParams(con, table, column, histogram_size)
      # Get Histogram array
      histogram_array = get_histogram_array(histogram_params)

      # Get distinct count
      distinct_count = get_distinct_count(con, table, column.column_name)

      row_dict: dict[str, Any] = {
        "table": table,
        "column": column.column_name,
        "histogram": histogram_array,
        "distinct_count": distinct_count,
        "dtype": column.column_type,
      }
      if include_mvc:
        # Get most common values
        most_common_values = get_most_common_values(
          con,
          table,
          column.column_name,
          common_values_size,
          distinct_count,
        )

        # Get histogram array excluding common values
        histogram_array_excluding_common_values = (
          get_histogram_array_excluding_common_values(
            histogram_params,
            common_values_size,
            distinct_count,
          )
        )
        row_dict |= {
          "most_common_values": [
            {"value": value.value, "count": value.count}
            for value in most_common_values
          ],
          "histogram-mcv": histogram_array_excluding_common_values,
        }
      rows.append(row_dict)

  path = Path(f"data/generated_histograms/{dataset.value}/histograms.parquet")
  path.parent.mkdir(parents=True, exist_ok=True)
  pl.DataFrame(rows).write_parquet(path)
