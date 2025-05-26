import math
import random
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum

import polars as pl

from query_generator.tools.histograms import HistogramColumns
from query_generator.utils.definitions import Dataset
from query_generator.utils.exceptions import (
  InvalidHistogramTypeError,
  UnkwonDatasetError,
)

SupportedHistogramType = float | int | str
SuportedHistogramArrayType = list[float] | list[int] | list[str]


class HistogramDataType(Enum):
  INT = "int"
  FLOAT = "float"
  DATE = "date"
  STRING = "string"

class PredicateType(Enum):
  RANGE = "range"
  EQUALITY = "equality"
  IN = "in"

@dataclass
class Predicate:
  table: str
  column: str
  min_value: SupportedHistogramType
  max_value: SupportedHistogramType
  dtype: HistogramDataType
  predicate_type: PredicateType


class PredicateGenerator:
  def __init__(self, dataset: Dataset):
    self.dataset = dataset
    self.histogram: pl.DataFrame = self.read_histogram()

  def _parse_bin(
    self, hist_array: list[str], dtype: HistogramDataType
  ) -> SuportedHistogramArrayType:
    """Parse the bin string representation to a list of values.

    Args:
        bin_str (str): String representation of bins.
        dtype (str): Data type of the values.

    Returns:
        list: List of parsed values.

    """
    if dtype == HistogramDataType.INT:
      return [int(float(x)) for x in hist_array]
    if dtype == HistogramDataType.FLOAT:
      return [float(x) for x in hist_array]
    if dtype == HistogramDataType.DATE:
      return hist_array
    if dtype == HistogramDataType.STRING:
      return hist_array
    raise InvalidHistogramTypeError(dtype)

  def read_histogram(self) -> pl.DataFrame:
    """Read the histogram data for the specified dataset.

    Args:
        dataset: The dataset type (TPCH or TPCDS).

    Returns:
        pd.DataFrame: DataFrame containing the histogram data.

    """
    if self.dataset == Dataset.TPCH:
      path = "data/histograms/histogram_tpch.parquet"
    elif self.dataset == Dataset.TPCDS:
      path = "data/histograms/histogram_tpcds.parquet"
    elif self.dataset == Dataset.JOB:
      path = "data/histograms/histogram_job.parquet"
    else:
      raise UnkwonDatasetError(self.dataset.value)
    return pl.read_parquet(path).filter(pl.col("histogram") != [])

  def _get_histogram_type(self, dtype: str) -> HistogramDataType:
    if dtype in ["INTEGER", "BIGINT"]:
      return HistogramDataType.INT
    if dtype.startswith("DECIMAL"):
      return HistogramDataType.FLOAT
    if dtype == "DATE":
      return HistogramDataType.DATE
    if dtype == "VARCHAR":
      return HistogramDataType.STRING
    raise InvalidHistogramTypeError(dtype)

  def get_random_predicates(
    self,
    tables: list[str],
    num_predicates: int,
    row_retention_probability: float,
  ) -> Iterator[Predicate]:
    """Generate random predicates based on the histogram data.

    Args:
        tables (str): List of tables to select predicates from.
        num_predicates (int): Number of predicates to generate.
        row_retention_probability (float): Probability of retaining rows.

    Returns:
        List[Predicate]: List of generated predicates.

    """
    selected_tables_histogram = self.histogram.filter(
      pl.col(HistogramColumns.TABLE.value).is_in(tables)
    )

    for row in selected_tables_histogram.sample(n=num_predicates).iter_rows(
      named=True
    ):
      table = row[HistogramColumns.TABLE.value]
      column = row[HistogramColumns.COLUMN.value]
      bins = row[HistogramColumns.HISTOGRAM.value]
      dtype = self._get_histogram_type(row[HistogramColumns.DTYPE.value])
      min_value, max_value = self._get_min_max_from_bins(
        bins, row_retention_probability, dtype
      )
      predicate = Predicate(
        table=table,
        column=column,
        min_value=min_value,
        max_value=max_value,
        dtype=dtype,
        predicate_type=PredicateType.RANGE,
      )
      yield predicate

  def _get_min_max_from_bins(
    self,
    bins: list[str],
    row_retention_probability: float,
    dtype: HistogramDataType,
  ) -> tuple[SupportedHistogramType, SupportedHistogramType]:
    """Convert the bins string representation to a tuple of min and max values.

    Args:
        bins (str): String representation of bins.
        row_retention_probability (float): Probability of retaining rows.

    Returns:
        tuple: Tuple containing min and max values.

    """
    histogram_array: SuportedHistogramArrayType = self._parse_bin(bins, dtype)
    subrange_length = math.ceil(
      row_retention_probability * len(histogram_array)
    )
    start_index = random.randint(0, len(histogram_array) - subrange_length)

    min_value = histogram_array[start_index]
    max_value = histogram_array[
      min(start_index + subrange_length, len(histogram_array) - 1)
    ]
    return min_value, max_value
