from unittest import mock

import pytest

from query_generator.predicate_generator.histogram import PredicateGenerator
from query_generator.utils.definitions import Dataset


@pytest.mark.parametrize("dataset", [Dataset.TPCH, Dataset.TPCDS])
def test_read_histograms(dataset):
  predicate_generator = PredicateGenerator(dataset)
  histogram = predicate_generator.read_histogram()
  print(histogram["table"].dtype)
  assert not histogram.empty
  assert histogram["table"].dtype == object
  assert histogram["column"].dtype == object
  assert histogram["dtype"].dtype == object
  assert histogram["bins"].dtype == object
  assert histogram["distinct_count"].dtype == int


@pytest.mark.parametrize(
  "mock_rand,bins_array, bins, row_retention_probability, min_index, max_index",
  [
    (0, [1, 2, 3, 4, 5], "[1, 2, 3, 4, 5]", 0.2, 0, 1),
    (0, [10, 20, 30, 40], "[10, 20, 30, 40]", 0.2, 0, 1),
    (
      0,
      [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
      "[0.1, 0.2, 0.3, 0.4, 0.5, 0.6]",
      0.2,
      0,
      2,
    ),
  ],
)
def test_get_min_max_from_bins(
  mock_rand, bins_array, bins, row_retention_probability, min_index, max_index
):
  with mock.patch(
    "query_generator.predicate_generator.histogram.random.randint",
    return_value=mock_rand,
  ):
    predicate_generator = PredicateGenerator(Dataset.TPCH)
    min_value, max_value = predicate_generator._get_min_max_from_bins(
      bins, row_retention_probability
    )
  assert min_value == bins_array[min_index]
  assert max_value == bins_array[max_index]
