from unittest import mock

import pytest

from query_generator.duckdb.binning import (
  BinningSnowflakeParameters,
  SearchParameters,
  get_bin_from_value,
  run_snowflake_binning,
)
from query_generator.utils.definitions import Dataset


@pytest.mark.parametrize(
  "value, params, expected",
  [
    (5, BinningSnowflakeParameters(None, None, 0, 10, 5, None), 3),
    (0, BinningSnowflakeParameters(None, None, 0, 10, 5, None), 0),
    (10, BinningSnowflakeParameters(None, None, 0, 10, 5, None), 5),
    (11, BinningSnowflakeParameters(None, None, 0, 10, 5, None), 6),
    # after the upper bound should stick to the last bin
    (20, BinningSnowflakeParameters(None, None, 0, 10, 5, None), 6),
    (5, BinningSnowflakeParameters(None, None, 0, 11, 5, None), 3),
    (0, BinningSnowflakeParameters(None, None, 0, 11, 5, None), 0),
    (10, BinningSnowflakeParameters(None, None, 0, 11, 5, None), 5),
    (11, BinningSnowflakeParameters(None, None, 0, 11, 5, None), 5),
    (20, BinningSnowflakeParameters(None, None, 0, 11, 5, None), 6),
  ],
)
def test_binning(value, params, expected):
  val = get_bin_from_value(value, params)
  assert val == expected, f"Expected {expected}, but got {val}"


@pytest.mark.parametrize(
  "extra_predicates, expected_call_count",
  [
    ([1], 134),
    ([1, 2], 134 * 2),
  ],
)
def test_binning_calls(extra_predicates, expected_call_count):
  with mock.patch(
    "query_generator.duckdb.binning.Writer.write_query_to_bin"
  ) as mock_writer:
    with mock.patch(
      "query_generator.duckdb.binning.get_result_from_duckdb"
    ) as mock_connect:
      mock_connect.return_value = 0
      run_snowflake_binning(
        BinningSnowflakeParameters(
          scale_factor=0,
          dataset=Dataset.TPCDS,
          lower_bound=0,
          upper_bound=10000,
          total_bins=10,
          con=None,
        ),
        search_params=SearchParameters(
          max_hops=[1],
          extra_predicates=extra_predicates,
          row_retention_probability=[0.2],
        ),
      )
    assert mock_writer.call_count == expected_call_count, (
      f"Expected {expected_call_count} calls to write_query, "
      f"but got {mock_writer.call_count}"
    )
