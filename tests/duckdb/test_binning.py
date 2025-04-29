import pytest

from query_generator.duckdb.binning import (
  BinningSnowflakeParameters,
  get_bin_from_value,
)


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
