import logging
from dataclasses import dataclass
from enum import StrEnum
from functools import reduce
from typing import Any, TypedDict

import duckdb
import polars as pl
from commonstrings import PyCommon_multiple_strings
from tqdm import tqdm

from query_generator.duckdb_connection.utils import (
  DuckDBColumnInfo,
  RawDuckDBHistograms,
  RawDuckDBMostCommonValues,
  RawDuckDBTableDescription,
  get_columns,
  get_distinct_count,
  get_equi_height_histogram,
  get_frequent_non_null_values,
  get_histogram_excluding_common_values,
  get_null_count,
  get_sample_of_str_from_column,
  get_tables,
)
from query_generator.utils.exceptions import (
  NoBasicHistogramElementError,
)
from query_generator.utils.params import HistogramEndpoint

logger = logging.getLogger(__name__)


class MostCommonValuesColumns(StrEnum):
  VALUE = "value"
  COUNT = "count"


@dataclass
class CandidateEntry:
  support: int
  substring: str


class CommonSubstring(TypedDict):
  """A common substring with its probability threshold and occurrence count."""

  probability: float
  substring: str
  support: int
  support_probability: float


class RedundantHistogramsDataType(StrEnum):
  """
  This class was made for compatibility with old code that
  generated this histogram:
  https://github.com/udao-moo/udao-spark-optimizer-dev/blob/main
  /playground/assets/data_stats/regrouped_job_hist.csv
  """

  INTEGER = "int"
  STRING = "string"
  DATE = "DATE"


class HistogramColumns(StrEnum):
  TABLE = "table"
  COLUMN = "column"
  HISTOGRAM = "histogram"
  DISTINCT_COUNT = "distinct_count"
  DTYPE = "dtype"
  MOST_COMMON_VALUES = "most_common_values"
  HISTOGRAM_MCV = "histogram-mcv"  # histogram excluding most common values
  TABLE_SIZE = "table_size"
  NULL_COUNT = "null_count"
  SAMPLE_SIZE = "sample_size"
  COMMON_SUBSTRINGS = "common_substrings"


@dataclass
class HistogramParams:
  col_info: DuckDBColumnInfo
  histogram_size: int
  histogram_sample_size: int | None


class DuckDBHistogramParser:
  """Class to represent a histogram in DuckDB."""

  def __init__(
    self, raw_histogram: list[RawDuckDBHistograms], duckdb_type: str
  ) -> None:
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
      if " < x <= " in bin:
        lower_bound, upper_bound = bin.split(" < x <= ")
        self.lower_bounds.append(lower_bound)
        self.upper_bounds.append(upper_bound)

  def get_equiwidth_histogram_array(self) -> list[str]:
    return self.upper_bounds


def binary_search_common_str_tree(
  tree: PyCommon_multiple_strings, support: int, minimum_candidates: int
) -> list[CandidateEntry]:
  for i in range(25, 0, -1):
    data: dict[int, list[str]] = tree.filter_substrings_by_length(
      length_input=i, times=(support, None)
    )
    data_size = reduce(lambda cum, kv: cum + len(kv), data.values(), 0)
    if data_size >= minimum_candidates:
      result = []
      for support_for_candidate, candidates in data.items():
        for candidate in candidates:
          result.append(
            CandidateEntry(support=support_for_candidate, substring=candidate)
          )
      return result
  return []


def get_common_substrings(
  params: DuckDBColumnInfo,
  sample_size: int | None,
  like_strings_per_threshold: int,
  distinct_count: int,
) -> list[CommonSubstring]:
  """Calculates common substrings of a column."""
  get_list_of_str = get_sample_of_str_from_column(params, sample_size)
  if not get_list_of_str:
    return []
  tree = PyCommon_multiple_strings()
  tree.from_strings(get_list_of_str)
  sampled_str_count = len(get_list_of_str)
  number_of_candidates = min(like_strings_per_threshold, distinct_count)
  result: list[CommonSubstring] = []
  for support in range(5, 101, 5):  # [5,10,15,...,100]
    count_threshold = max(1, int(support / 100.0 * sampled_str_count))
    candidates = binary_search_common_str_tree(
      tree, count_threshold, number_of_candidates
    )
    for candidate in candidates:
      result.append(
        CommonSubstring(
          probability=support / 100.0,
          substring=candidate.substring,
          support=candidate.support,
          support_probability=candidate.support / sampled_str_count,
        )
      )
  return result


def get_most_common_values(
  params: DuckDBColumnInfo,
  common_value_size: int,
  sample_size: int | None,
) -> list[RawDuckDBMostCommonValues]:
  return get_frequent_non_null_values(params, common_value_size, sample_size)


def get_histogram_array(params: HistogramParams) -> list[str]:
  histogram_raw = get_equi_height_histogram(
    params.col_info,
    params.histogram_size,
    params.histogram_sample_size,
  )
  histogram_parser = DuckDBHistogramParser(
    histogram_raw, params.col_info.column
  )
  return histogram_parser.get_equiwidth_histogram_array()


def get_histogram_array_excluding_common_values(
  params: HistogramParams,
  common_values_size: int,
  distinct_count: int,
  column_type: str,
) -> list[str]:
  histogram_array: list[RawDuckDBHistograms] = []
  if distinct_count > common_values_size:
    histogram_array = get_histogram_excluding_common_values(
      params.col_info,
      params.histogram_size,
      common_values_size,
      params.histogram_sample_size,
    )
  histogram_parser = DuckDBHistogramParser(
    histogram_array,
    column_type,
  )
  return histogram_parser.get_equiwidth_histogram_array()


def query_histograms(
  params: HistogramEndpoint,
  con: duckdb.DuckDBPyConnection,
) -> pl.DataFrame:
  """Creates histograms for the given dataset.
  Args:
    histogram_size (int): Size of the histogram.
    common_values_size (int): Size of the most common values.
    histogram_sample_size (int): Max sampled rows per table used for
      histogram-related queries.
    con (duckdb.DuckDBPyConnection): DuckDB connection object.
    include_mvc (bool): Whether to include most common values in the histogram
  """
  rows: list[dict[str, Any]] = []
  tables: list[str] = get_tables(con)
  table_iter = tqdm(tables, position=0)
  for table in table_iter:  # type: ignore
    logger.debug(f"Processing table {table}")
    columns: list[RawDuckDBTableDescription] = get_columns(con, table)
    pbar = tqdm(columns, desc="Starting…", position=1, leave=False)

    # Get table size
    table_size = get_size_of_table(con, table)
    actual_sample_size = min(params.sample_size, table_size)
    sample_size_for_query = (
      actual_sample_size if actual_sample_size < table_size else None
    )
    column: RawDuckDBTableDescription
    for column in pbar:  # type: ignore
      logger.debug(f"Processing column {column} of table {table}")
      pbar.set_description(  # type: ignore
        f"Processing table {table} column {column.column_name}"
      )
      column_info = DuckDBColumnInfo(
        con=con, table=table, column=column.column_name
      )
      histogram_params = HistogramParams(
        column_info,
        params.histogram_size,
        sample_size_for_query,
      )
      # Get Histogram array
      histogram_array = get_histogram_array(histogram_params)

      # Get distinct count
      distinct_count = get_distinct_count(column_info, sample_size_for_query)

      # Get null Count
      null_count = get_null_count(column_info, sample_size_for_query)

      # Get common substr information
      common_substrings = None
      if column.column_type in ["TEXT", "VARCHAR", "BPCHAR", "CHAR", "STRING"]:
        common_substrings = get_common_substrings(
          column_info,
          sample_size_for_query,
          params.like_strings_per_threshold,
          distinct_count,
        )

      row_dict: dict[str, Any] = {
        HistogramColumns.TABLE: table,
        HistogramColumns.COLUMN: column.column_name,
        HistogramColumns.HISTOGRAM: histogram_array,
        HistogramColumns.DISTINCT_COUNT: distinct_count,
        HistogramColumns.DTYPE: column.column_type,
        HistogramColumns.TABLE_SIZE: table_size,
        HistogramColumns.NULL_COUNT: null_count,
        HistogramColumns.SAMPLE_SIZE: actual_sample_size,
        HistogramColumns.COMMON_SUBSTRINGS: common_substrings,
      }
      if params.include_mcv:
        # Get most common values
        most_common_values = get_most_common_values(
          column_info, params.common_values_size, sample_size_for_query
        )

        # Get histogram array excluding common values
        histogram_array_excluding_mcv = (
          get_histogram_array_excluding_common_values(
            histogram_params,
            params.common_values_size,
            distinct_count,
            column.column_type,
          )
        )

        row_dict |= {
          HistogramColumns.MOST_COMMON_VALUES.value: [
            {
              MostCommonValuesColumns.VALUE: value.value,
              MostCommonValuesColumns.COUNT: value.count,
            }
            for value in most_common_values
          ],
          HistogramColumns.HISTOGRAM_MCV.value: histogram_array_excluding_mcv,
        }

      rows.append(row_dict)
  return pl.DataFrame(rows)


def get_size_of_table(
  con: duckdb.DuckDBPyConnection,
  table: str,
) -> int:
  result = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
  return result[0] if result else 0


def get_basic_element_of_redundant_histogram(
  dtype: str,
) -> str:
  if dtype == "INTEGER":
    return "0"
  if dtype == "VARCHAR":
    return "A"
  if dtype == "DATE":
    return "1970-01-01"
  raise NoBasicHistogramElementError(dtype)


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
    pl.struct([HistogramColumns.HISTOGRAM, HistogramColumns.DTYPE])
    .map_elements(
      lambda row: force_histogram_to_lenght(
        row[HistogramColumns.HISTOGRAM],
        desired_length,
        row[HistogramColumns.DTYPE],
      ),
      return_dtype=pl.List(pl.Utf8),
    )
    .alias("redundant_histogram")
  )


def make_redundant_histograms(
  histogram_df: pl.DataFrame, desired_length: int
) -> pl.DataFrame:
  if desired_length == 0:
    return histogram_df
  return get_redundant_bins(histogram_df, desired_length)
