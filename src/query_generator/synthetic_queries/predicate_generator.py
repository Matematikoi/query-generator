import math
import random
from abc import ABC
from collections.abc import Iterator
from dataclasses import dataclass
from enum import Enum

import numpy as np
import polars as pl

from query_generator.tools.histograms import (
  HistogramColumns,
  MostCommonValuesColumns,
)
from query_generator.utils.definitions import (
  PredicateOperatorProbability,
  PredicateParameters,
)
from query_generator.utils.exceptions import (
  InvalidHistogramError,
)

SupportedHistogramType = float | int | str
SuportedHistogramArrayType = list[float] | list[int] | list[str]


MAX_DISTINCT_COUNT_FOR_RANGE = 500
PROBABILITY_TO_CHOOSE_EQUALITY = 0.8
PREDICATE_IN_SIZE = 5


class PredicateTypes(Enum):
  IN = "in"
  RANGE = "range"
  EQUALITY = "equality"


class HistogramDataType(Enum):
  INT = "int"
  FLOAT = "float"
  DATE = "date"
  STRING = "string"


@dataclass
class Predicate(ABC):
  table: str
  column: str
  dtype: HistogramDataType


@dataclass
class PredicateRange(Predicate):
  min_value: SupportedHistogramType
  max_value: SupportedHistogramType


@dataclass
class PredicateEquality(Predicate):
  equality_value: SupportedHistogramType


@dataclass
class PredicateIn(Predicate):
  in_values: SuportedHistogramArrayType


class PredicateGenerator:
  def __init__(self, predicate_params: PredicateParameters) -> None:
    self.predicate_params = predicate_params
    self.histogram: pl.DataFrame = pl.read_parquet(
      predicate_params.histogram_path
    ).filter(pl.col(HistogramColumns.HISTOGRAM.value) != [])

  def _cast_array(
    self, str_array: list[str], dtype: HistogramDataType
  ) -> SuportedHistogramArrayType:
    """Parse the bin string representation to a list of values.

    Args:
        bin_str (str): String representation of bins.
        dtype (str): Data type of the values.

    Returns:
        list: List of parsed values.
    """
    if dtype == HistogramDataType.INT:
      return [int(float(x)) for x in str_array]
    if dtype == HistogramDataType.FLOAT:
      return [float(x) for x in str_array]
    if dtype == HistogramDataType.DATE:
      return str_array
    if dtype == HistogramDataType.STRING:
      return str_array
    raise InvalidHistogramError(dtype)

  def _cast_element(
    self, value: str, dtype: HistogramDataType
  ) -> SupportedHistogramType:
    if dtype == HistogramDataType.INT:
      return int(float(value))
    if dtype == HistogramDataType.FLOAT:
      return float(value)
    if dtype == HistogramDataType.DATE:
      return value
    if dtype == HistogramDataType.STRING:
      return value
    raise InvalidHistogramError(dtype)

  def _get_histogram_type(self, dtype: str) -> HistogramDataType:
    if dtype in ["INTEGER", "BIGINT"]:
      return HistogramDataType.INT
    if dtype.startswith("DECIMAL"):
      return HistogramDataType.FLOAT
    if dtype == "DATE":
      return HistogramDataType.DATE
    if dtype == "VARCHAR":
      return HistogramDataType.STRING
    raise ValueError(dtype)

  def _choose_predicate_type(
    self, operator_weights: PredicateOperatorProbability
  ) -> PredicateTypes:
    weights = [
      operator_weights.operator_equal,
      operator_weights.operator_in,
      operator_weights.operator_range,
    ]
    return random.choices(
      [
        PredicateTypes.EQUALITY,
        PredicateTypes.IN,
        PredicateTypes.RANGE,
      ],
      weights=weights,
    )[0]

  def get_random_predicates(
    self,
    tables: list[str],
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
      pl.col(HistogramColumns.TABLE).is_in(tables)
    )

    for row in selected_tables_histogram.sample(
      n=self.predicate_params.extra_predicates
    ).iter_rows(named=True):
      table = row[HistogramColumns.TABLE]
      column = row[HistogramColumns.COLUMN]
      dtype = self._get_histogram_type(row[HistogramColumns.DTYPE])
      predicate_type = self._choose_predicate_type(
        self.predicate_params.operator_weights
      )

      if predicate_type == PredicateTypes.RANGE:
        yield self._get_range_predicate(
          table, column, row[HistogramColumns.HISTOGRAM], dtype
        )
      elif predicate_type == PredicateTypes.IN:
        array = self._get_in_array(
          row[HistogramColumns.MOST_COMMON_VALUES],
          row[HistogramColumns.TABLE_SIZE],
          row[HistogramColumns.HISTOGRAM_MCV],
        )
        if array is not None:
          yield self._get_in_predicate(array, table, column, dtype)
      elif predicate_type == PredicateTypes.EQUALITY:
        value = self._get_equality_value(
          row[HistogramColumns.MOST_COMMON_VALUES],
          row[HistogramColumns.TABLE_SIZE],
        )
        if value is not None:
          yield self._get_equality_predicate(value, table, column, dtype)

  def _get_in_predicate(
    self, array: list[str], table: str, column: str, dtype: HistogramDataType
  ) -> PredicateIn:
    cast_array = self._cast_array(array, dtype)
    return PredicateIn(table, column, dtype, cast_array)

  def _get_in_array(
    self,
    most_common_values: list[dict[str, int | str]],
    table_size: int,
    histogram: list[str],
  ) -> list[str] | None:
    """
    Gets the array for the IN operator
    """
    value = self._get_equality_value(most_common_values, table_size)
    if value is None:
      return None
    noise_values = random.sample(
      histogram,
      k=min(self.predicate_params.extra_values_for_in, len(histogram)),
    )
    return [value] + noise_values

  def _get_equality_predicate(
    self, value: str, table: str, column: str, dtype: HistogramDataType
  ) -> PredicateEquality:
    cast_value = self._cast_element(value, dtype)
    return PredicateEquality(
      table=table, column=column, dtype=dtype, equality_value=cast_value
    )

  def _get_equality_value(
    self,
    most_common_values: list[dict[str, int | str]],
    table_size: int,
  ) -> str | None:
    mcv_probabilities: list[float] = [
      float(table_size) / float(v[MostCommonValuesColumns.COUNT])
      for v in most_common_values
    ]
    mcv_probabilities_np = np.array(mcv_probabilities)
    filtered_indices = np.where(
      mcv_probabilities_np
      > self.predicate_params.equality_lower_bound_probability
    )[0]
    if len(filtered_indices) == 0:
      return None
    idx = random.choice(filtered_indices)
    value = most_common_values[idx][MostCommonValuesColumns.VALUE]
    assert isinstance(value, str)
    return value

  def _get_range_predicate(
    self,
    table: str,
    column: str,
    bins: list[str],
    dtype: HistogramDataType,
  ) -> PredicateRange:
    min_value, max_value = self._get_min_max_from_bins(bins, dtype)
    return PredicateRange(
      table=table,
      column=column,
      min_value=min_value,
      max_value=max_value,
      dtype=dtype,
    )

  def _get_min_max_from_bins(
    self,
    bins: list[str],
    dtype: HistogramDataType,
  ) -> tuple[SupportedHistogramType, SupportedHistogramType]:
    """Convert the bins string representation to a tuple of min and max values.

    Args:
        bins (str): String representation of bins.
        row_retention_probability (float): Probability of retaining rows.

    Returns:
        tuple: Tuple containing min and max values.

    """
    histogram_array: SuportedHistogramArrayType = self._cast_array(bins, dtype)
    subrange_length = math.ceil(
      self.predicate_params.row_retention_probability * len(histogram_array)
    )
    start_index = random.randint(0, len(histogram_array) - subrange_length)

    min_value = histogram_array[start_index]
    max_value = histogram_array[
      min(start_index + subrange_length, len(histogram_array) - 1)
    ]
    return min_value, max_value
