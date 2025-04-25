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
