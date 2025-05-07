from unittest import mock

import pytest

import polars as pl
from query_generator.predicate_generator.histogram import (
  HistogramDataType,
  PredicateGenerator,
)
from query_generator.utils.definitions import Dataset
from query_generator.utils.exceptions import UnsupportedTypeError


@pytest.mark.parametrize("dataset", [Dataset.TPCH, Dataset.TPCDS])
def test_read_histograms(dataset):
  predicate_generator = PredicateGenerator(dataset)
  histogram = predicate_generator.read_histogram()
  assert not histogram.is_empty()

  assert histogram["table"].dtype == pl.Utf8
  assert histogram["column"].dtype == pl.Utf8
  assert histogram["dtype"].dtype == pl.Utf8
  assert histogram["bins"].dtype == pl.Utf8
  assert histogram["distinct_count"].dtype == pl.Int64


@pytest.mark.parametrize(
  "mock_rand,bins_array, bins, row_retention_probability, min_index, max_index,dtype",
  [
    (0, [1, 2, 3, 4, 5], "[1, 2, 3, 4, 5]", 0.2, 0, 1, HistogramDataType.INT),
    (3, [1, 2, 3, 4, 5], "[1, 2, 3, 4, 5]", 0.2, 3, 4, HistogramDataType.INT),
    (0, [10, 20, 30, 40], "[10, 20, 30, 40]", 0.2, 0, 1, HistogramDataType.INT),
    (2, [10, 20, 30, 40], "[10, 20, 30, 40]", 0.2, 2, 3, HistogramDataType.INT),
    (
      0,
      [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
      "[0.1, 0.2, 0.3, 0.4, 0.5, 0.6]",
      0.2,
      0,
      2,
      HistogramDataType.FLOAT,
    ),
    (
      3,
      [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
      "[0.1, 0.2, 0.3, 0.4, 0.5, 0.6]",
      0.2,
      3,
      5,
      HistogramDataType.FLOAT,
    ),
    (
      0,
      ["1976-05-16", "1976-05-17", "1976-05-18", "1976-05-19", "1976-05-20"],
      "[1976-05-16, 1976-05-17, 1976-05-18, 1976-05-19, 1976-05-20]",
      0.2,
      0,
      1,
      HistogramDataType.DATE,
    ),
  ],
)
def test_get_min_max_from_bins(
  mock_rand,
  bins_array,
  bins,
  row_retention_probability,
  min_index,
  max_index,
  dtype,
):
  with mock.patch(
    "query_generator.predicate_generator.histogram.random.randint",
    return_value=mock_rand,
  ):
    predicate_generator = PredicateGenerator(Dataset.TPCH)
    min_value, max_value = predicate_generator._get_min_max_from_bins(
      bins, row_retention_probability, dtype
    )
  assert min_value == bins_array[min_index]
  assert max_value == bins_array[max_index]

def test_get_invalid_histogram_type():
  predicate_generator = PredicateGenerator(Dataset.TPCH)
  with pytest.raises(UnsupportedTypeError):
    predicate_generator._get_histogram_type("not_supported_type")


def test_get_valid_histogram_type():
  predicate_generator = PredicateGenerator(Dataset.TPCH)
  assert predicate_generator._get_histogram_type("int") == HistogramDataType.INT
  assert predicate_generator._get_histogram_type("bigint") == HistogramDataType.INT
  assert (
    predicate_generator._get_histogram_type("decimal(10,2)")
    == HistogramDataType.FLOAT
  )
  assert predicate_generator._get_histogram_type("date") == HistogramDataType.DATE