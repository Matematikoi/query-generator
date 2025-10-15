from pathlib import Path
from unittest import mock

import polars as pl
import pytest

from query_generator.synthetic_queries.predicate_generator import (
  HistogramDataType,
  PredicateGenerator,
)
from query_generator.tools.histograms import HistogramColumns
from query_generator.utils.definitions import Dataset, PredicateParameters
from query_generator.utils.exceptions import InvalidHistogramError
from tests.utils import get_precomputed_histograms


def test_read_histograms():
  for dataset in Dataset:
    predicate_generator = PredicateGenerator(
      PredicateParameters(
        histogram_path=get_precomputed_histograms(dataset),
        extra_predicates=None,
        row_retention_probability=None,
        operator_weights=None,
        equality_lower_bound_probability=None,
        extra_values_for_in=None,
      ),
    )
    histogram = predicate_generator.histogram
    assert not histogram.is_empty()

    assert histogram[HistogramColumns.DTYPE.value].dtype == pl.Utf8
    assert histogram[HistogramColumns.COLUMN.value].dtype == pl.Utf8
    assert histogram[HistogramColumns.DTYPE.value].dtype == pl.Utf8
    assert histogram[HistogramColumns.HISTOGRAM.value].dtype == pl.List(pl.Utf8)
    assert histogram[HistogramColumns.DISTINCT_COUNT.value].dtype == pl.Int64


@pytest.mark.parametrize(
  "mock_rand,bins_array, row_retention_probability, min_index, max_index,dtype",
  [
    (0, [1, 2, 3, 4, 5], 0.2, 0, 1, HistogramDataType.INT),
    (3, [1, 2, 3, 4, 5], 0.2, 3, 4, HistogramDataType.INT),
    (0, [10, 20, 30, 40], 0.2, 0, 1, HistogramDataType.INT),
    (2, [10, 20, 30, 40], 0.2, 2, 3, HistogramDataType.INT),
    (
      0,
      [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
      0.2,
      0,
      2,
      HistogramDataType.FLOAT,
    ),
    (
      3,
      [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
      0.2,
      3,
      5,
      HistogramDataType.FLOAT,
    ),
    (
      0,
      ["1976-05-16", "1976-05-17", "1976-05-18", "1976-05-19", "1976-05-20"],
      0.2,
      0,
      1,
      HistogramDataType.DATE,
    ),
    (
      0,
      ["a", "b", "c", "d", "e"],
      0.2,
      0,
      1,
      HistogramDataType.STRING,
    ),
  ],
)
def test_get_min_max_from_bins(
  mock_rand,
  bins_array,
  row_retention_probability,
  min_index,
  max_index,
  dtype,
):
  with mock.patch(
    "query_generator.synthetic_queries.predicate_generator.random.randint",
    return_value=mock_rand,
  ):
    predicate_generator = PredicateGenerator(
      PredicateParameters(
        histogram_path=get_precomputed_histograms(Dataset.TPCDS),
        extra_predicates=None,
        row_retention_probability=row_retention_probability,
        operator_weights=None,
        equality_lower_bound_probability=None,
        extra_values_for_in=None,
      ),
    )
    min_value, max_value = predicate_generator._get_min_max_from_bins(
      bins_array, dtype
    )
  assert min_value == bins_array[min_index]
  assert max_value == bins_array[max_index]

@pytest.mark.parametrize(
  "input_type, expected_type",
  [
    ("INTEGER", HistogramDataType.INT),
    ("BIGINT", HistogramDataType.INT),
    ("DECIMAL(10,2)", HistogramDataType.FLOAT),
    ("DECIMAL(7,4)", HistogramDataType.FLOAT),
    ("DATE", HistogramDataType.DATE),
    ("VARCHAR", HistogramDataType.STRING),
  ],
)
def test_get_valid_histogram_type(input_type, expected_type):
  predicate_generator = PredicateGenerator(
    PredicateParameters(
        histogram_path=get_precomputed_histograms(Dataset.TPCDS),
        extra_predicates=None,
        row_retention_probability=None,
        operator_weights=None,
        equality_lower_bound_probability=None,
        extra_values_for_in=None,
      ),
  )
  assert predicate_generator._get_histogram_type(input_type) == expected_type
