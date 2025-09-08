import tomllib
from unittest import mock

from cattrs import structure
import polars as pl
import pytest

from query_generator.synthetic_queries.synthetic_query_generator import (
  SyntheticQueriesParams,
  generate_synthetic_queries,
)
from query_generator.filter.filter import make_bins
from query_generator.utils.params import SyntheticQueriesEndpoint


@pytest.mark.parametrize(
  "count_star, upper_bound, total_bins, expected_bin",
  [
    (5, 10, 5, 3),
    (0, 10, 5, 0),
    (10, 10, 5, 5),
    (11, 10, 5, 6),
    (20, 10, 5, 6),
    (5, 11, 5, 3),
    (0, 11, 5, 0),
    (10, 11, 5, 5),
    (11, 11, 5, 5),
    (20, 11, 5, 6),
  ],
)
def test_cherry_pick(count_star, upper_bound, total_bins, expected_bin):
  # Create a DataFrame with a single value
  test_df = pl.DataFrame({"count_star": [count_star]})
  result_df = make_bins(test_df, upper_bound, total_bins)

  # Extract the computed bin
  computed_bin = result_df["bin"][0]

  assert computed_bin == expected_bin, (
    f"For count_star={count_star}, upper_bound={upper_bound}, "
    f"total_bins={total_bins}, expected bin={expected_bin}"
    " but got {computed_bin}"
  )


@pytest.mark.parametrize(
  "extra_predicates, expected_call_count, unique_joins",
  [
    ("[1]", 120 * 1 + 14, "false"),
    ("[1]", 120 * 1 + 14, "true"),
    # Inventory is small and prooduces 14 queries total
    ("[1, 2]", 120 * 2 + 14, "true"),
    ("[1, 2]", 120 * 2 + 14 * 2, "false"),
  ],
)
def test_binning_calls(extra_predicates, expected_call_count, unique_joins):
  with (
    mock.patch(
      "query_generator.synthetic_queries.utils.query_writer.Writer.write_query_to_batch"
    ) as mock_writer,
    mock.patch(
      "query_generator.synthetic_queries.synthetic_query_generator.get_result_from_duckdb"
    ) as mock_connect,
    mock.patch(
      "query_generator.synthetic_queries.synthetic_query_generator.checkpoint_queries_parquet"
    ),
    mock.patch(
      "query_generator.synthetic_queries.utils.query_writer.Writer.write_toml"
    ),
  ):
    mock_connect.return_value = 0
    data_toml = f"""
      dataset = "TPCDS"
      output_folder = ""
      duckdb_database = ""
      dev = true
      max_hops = [1]
      extra_predicates = {extra_predicates}
      row_retention_probability = [0.2]
      unique_joins = {unique_joins}
      max_queries_per_fact_table = 10
      max_queries_per_signature = 2
      keep_edge_probability = [0.2]
      equality_lower_bound_probability = [0]
      extra_values_for_in = 3

      [operator_weights]
      operator_in = 1
      operator_range = 3
      operator_equal = 3
      """
    user_input = structure(tomllib.loads(data_toml), SyntheticQueriesEndpoint)
    generate_synthetic_queries(
      params=SyntheticQueriesParams(
        con=None,
        user_input=user_input,
      ),
    )
    assert mock_writer.call_count == expected_call_count, (
      f"Expected {expected_call_count} calls to write_query, "
      f"but got {mock_writer.call_count}"
    )
