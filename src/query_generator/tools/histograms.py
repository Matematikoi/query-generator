from dataclasses import dataclass
from enum import Enum
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
from query_generator.utils.exceptions import InvalidHistogramTypeError

LIMIT_FOR_DISTINCT_VALUES = 1000


class RedundantHistogramsDataType(Enum):
  """
  This class was made for compatibility with old code that
  generated this histogram:
  https://github.com/udao-moo/udao-spark-optimizer-dev/blob/main
  /playground/assets/data_stats/regrouped_job_hist.csv
  """

  INTEGER = "int"
  STRING = "string"


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
) -> pl.DataFrame:
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
  return pl.DataFrame(rows)


def get_basic_element_of_redundant_histogram(
  dtype: str,
) -> str:
  if dtype == RedundantHistogramsDataType.INTEGER.value:
    return "0"
  if dtype == RedundantHistogramsDataType.STRING.value:
    return "A"
  raise InvalidHistogramTypeError(dtype)


def force_histogram_to_lenght(
  original_histogram: list[str],
  desired_length: int,
  dtype: str,
) -> list[str]:
  if len(original_histogram) == desired_length:
    return original_histogram
  if len(original_histogram) == 0:
    return [get_basic_element_of_redundant_histogram(dtype)] * desired_length

  base, extra = divmod(desired_length, len(original_histogram))
  result: list[str] = []
  for i, item in enumerate(original_histogram):
    result.extend([item] * (base + (1 if i < extra else 0)))
  return result


def get_redundant_bins(
  histogram_df: pl.DataFrame, desired_length: int
) -> pl.DataFrame:
  return histogram_df.with_columns(
    pl.struct(["histogram", "dtype"])
    .map_elements(
      lambda row: force_histogram_to_lenght(
        row["histogram"], desired_length, row["dtype"]
      ),
      return_dtype=pl.List(pl.Utf8),
    )
    .alias("redundant_histogram")
  )


def get_redundant_histogram_type(histogram_df: pl.DataFrame) -> pl.DataFrame:
  return histogram_df.with_columns(
    pl.when(pl.col("dtype") == "VARCHAR")
    .then(pl.lit(RedundantHistogramsDataType.STRING.value))
    .when(pl.col("dtype") == "INTEGER")
    .then(pl.lit(RedundantHistogramsDataType.INTEGER.value))
    .otherwise(pl.col("dtype"))
    .alias("dtype")
  )


def get_redundant_histograms_name_convention(
  histogram_df: pl.DataFrame,
) -> pl.DataFrame:
  """
  We only want to comply with old code. This is bad naming convention
  """
  return histogram_df.rename(
    {"histogram": "bins", "redundant_histogram": "hists"}
  )


def make_redundant_histograms(
  histogram_path: Path, desired_length: int
) -> pl.DataFrame:
  histogram_df = pl.read_parquet(histogram_path)
  modified_dtype_df = get_redundant_histogram_type(histogram_df)
  redundant_histogram = get_redundant_bins(modified_dtype_df, desired_length)
  return get_redundant_histograms_name_convention(redundant_histogram)
