import math
import random
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum

import polars as pl

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


class PredicateGenerator:
  @dataclass
  class Predicate:
    table: str
    column: str
    min_value: SupportedHistogramType
    max_value: SupportedHistogramType
    dtype: HistogramDataType

  def __init__(self, dataset: Dataset):
    self.dataset = dataset
    self.histogram: pl.DataFrame = self.read_histogram()

  def _parse_bin(
    self, bin_str: str, dtype: HistogramDataType
  ) -> SuportedHistogramArrayType:
    """Parse the bin string representation to a list of values.

    Args:
        bin_str (str): String representation of bins.
        dtype (str): Data type of the values.

    Returns:
        list: List of parsed values.

    """
    if bin_str == "[]":
      return []
    inner = bin_str[1:-1]
    hist_array = inner.split(", ")
    if dtype == HistogramDataType.INT:
      return [int(float(x)) for x in hist_array]
    if dtype == HistogramDataType.FLOAT:
      return [float(x) for x in hist_array]
    if dtype == HistogramDataType.DATE:
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
      path = "data/histograms/raw_tpch_hist.csv"
    elif self.dataset == Dataset.TPCDS:
      path = "data/histograms/raw_tpcds_hist.csv"
    else:
      raise UnkwonDatasetError(self.dataset.value)
    return pl.read_csv(path).filter(
      (pl.col("dtype") != "string") & (pl.col("bins") != "[]")
    )

  def _get_histogram_type(self, dtype: str) -> HistogramDataType:
    if dtype in ["int", "bigint"]:
      return HistogramDataType.INT
    if dtype.startswith("decimal"):
      return HistogramDataType.FLOAT
    if dtype == "date":
      return HistogramDataType.DATE
    raise InvalidHistogramTypeError(dtype)

  def get_random_predicates(
    self,
    tables: list[str],
    num_predicates: int,
    row_retention_probability: float,
  ) -> Iterator["PredicateGenerator.Predicate"]:
    """Generate random predicates based on the histogram data.

    Args:
        tables (str): List of tables to select predicates from.
        num_predicates (int): Number of predicates to generate.
        row_retention_probability (float): Probability of retaining rows.

    Returns:
        List[PredicateGenerator.Predicate]: List of generated predicates.

    """
    selected_tables_histogram = self.histogram.filter(
      pl.col("table").is_in(tables)
    )

    for row in selected_tables_histogram.sample(n=num_predicates).iter_rows(
      named=True
    ):
      table = row["table"]
      column = row["column"]
      bins = row["bins"]
      dtype = self._get_histogram_type(row["dtype"])
      min_value, max_value = self._get_min_max_from_bins(
        bins, row_retention_probability, dtype
      )
      predicate = PredicateGenerator.Predicate(
        table=table,
        column=column,
        min_value=min_value,
        max_value=max_value,
        dtype=dtype,
      )
      yield predicate

  def _get_min_max_from_bins(
    self, bins: str, row_retention_probability: float, dtype: HistogramDataType
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
