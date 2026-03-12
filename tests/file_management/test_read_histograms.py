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
        minimum_like_support_probability=None,
      ),
    )
    histogram = predicate_generator.histogram
    assert not histogram.is_empty()

    assert histogram[HistogramColumns.DTYPE].dtype == pl.Utf8
    assert histogram[HistogramColumns.COLUMN].dtype == pl.Utf8
    assert histogram[HistogramColumns.DTYPE].dtype == pl.Utf8
    assert histogram[HistogramColumns.HISTOGRAM].dtype == pl.List(pl.Utf8)
    assert histogram[HistogramColumns.DISTINCT_COUNT].dtype == pl.Int64


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
        minimum_like_support_probability=None,
      ),
    )
    min_value, max_value = predicate_generator._get_min_max_from_bins(
      bins_array, dtype
    )
  assert min_value == bins_array[min_index]
  assert max_value == bins_array[max_index]


def _make_predicate_generator(tmp_path, rows: list[dict]) -> PredicateGenerator:
  """Build a PredicateGenerator backed by a minimal in-memory histogram."""
  df = pl.DataFrame(
    rows,
    schema={
      HistogramColumns.TABLE: pl.Utf8,
      HistogramColumns.COLUMN: pl.Utf8,
      HistogramColumns.HISTOGRAM: pl.List(pl.Utf8),
      HistogramColumns.DISTINCT_COUNT: pl.Int64,
      HistogramColumns.DTYPE: pl.Utf8,
      HistogramColumns.TABLE_SIZE: pl.Int64,
      HistogramColumns.NULL_COUNT: pl.Int64,
      HistogramColumns.SAMPLE_SIZE: pl.Int64,
      HistogramColumns.COMMON_SUBSTRINGS: pl.List(
        pl.Struct(
          {"substring": pl.Utf8, "support": pl.Int64, "support_probability": pl.Float64}
        )
      ),
      HistogramColumns.MOST_COMMON_VALUES: pl.List(
        pl.Struct({"value": pl.Utf8, "count": pl.Int64})
      ),
      HistogramColumns.HISTOGRAM_MCV: pl.List(pl.Utf8),
    },
  )
  path = tmp_path / "histogram.parquet"
  df.write_parquet(path)
  return PredicateGenerator(
    PredicateParameters(
      histogram_path=path,
      extra_predicates=None,
      row_retention_probability=0.5,
      operator_weights=None,
      equality_lower_bound_probability=None,
      extra_values_for_in=None,
      minimum_like_support_probability=None,
    )
  )


def _base_row(distinct_count: int, bins: list[str]) -> dict:
  return {
    HistogramColumns.TABLE: "t",
    HistogramColumns.COLUMN: "c",
    HistogramColumns.HISTOGRAM: bins,
    HistogramColumns.DISTINCT_COUNT: distinct_count,
    HistogramColumns.DTYPE: "VARCHAR",
    HistogramColumns.TABLE_SIZE: 1000,
    HistogramColumns.NULL_COUNT: 0,
    HistogramColumns.SAMPLE_SIZE: 1000,
    HistogramColumns.COMMON_SUBSTRINGS: [],
    HistogramColumns.MOST_COMMON_VALUES: [],
    HistogramColumns.HISTOGRAM_MCV: [],
  }


def test_single_distinct_count_filtered_out(tmp_path):
  """Columns with distinct_count == 1 must be excluded from the histogram."""
  gen = _make_predicate_generator(
    tmp_path,
    [_base_row(distinct_count=1, bins=["United States"])],
  )
  assert gen.histogram.is_empty()


def test_multiple_distinct_count_not_filtered(tmp_path):
  """Columns with distinct_count > 1 must be kept in the histogram."""
  gen = _make_predicate_generator(
    tmp_path,
    [_base_row(distinct_count=2, bins=["a", "b"])],
  )
  assert len(gen.histogram) == 1


def test_try_range_predicate_returns_none_when_collapsed(tmp_path):
  """_try_range_predicate must return None when min == max."""
  gen = _make_predicate_generator(
    tmp_path,
    [_base_row(distinct_count=2, bins=["United States", "United States"])],
  )
  with mock.patch(
    "query_generator.synthetic_queries.predicate_generator.random.randint",
    return_value=0,
  ):
    result = gen._try_range_predicate("t", "c", ["United States"], HistogramDataType.STRING)
  assert result is None


def test_try_range_predicate_returns_predicate_when_valid(tmp_path):
  """_try_range_predicate must return a PredicateRange when min != max."""
  from query_generator.synthetic_queries.predicate_generator import PredicateRange

  gen = _make_predicate_generator(
    tmp_path,
    [_base_row(distinct_count=5, bins=["a", "b", "c", "d", "e"])],
  )
  with mock.patch(
    "query_generator.synthetic_queries.predicate_generator.random.randint",
    return_value=0,
  ):
    result = gen._try_range_predicate("t", "c", ["a", "b", "c", "d", "e"], HistogramDataType.STRING)
  assert isinstance(result, PredicateRange)
  assert result.min_value != result.max_value


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
      minimum_like_support_probability=None,
    ),
  )
  assert predicate_generator._get_histogram_type(input_type) == expected_type
