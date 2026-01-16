import polars as pl

from query_generator.duckdb_connection.trace_collection import DuckDBTraceEnum
from query_generator.metrics.plot_histograms import _build_collapsed_hue_expr


def test_build_collapsed_hue_expr_collapses_expected_templates():
  hue_column = DuckDBTraceEnum.query_folder.value
  df = pl.DataFrame(
    {
      hue_column: [
        "group_by_1",
        "group_by_order_by_2",
        "recursive_query_simple",
        "union",
      ]
    }
  )

  rules = {
    "group_by": "group_by*",
    "recursive": "recursive_*",
  }
  out = df.with_columns(
    _build_collapsed_hue_expr(hue_column, rules).alias("collapsed")
  )

  assert out["collapsed"].to_list() == [
    "group_by",
    "group_by",
    "recursive",
    "union",
  ]


def test_build_collapsed_hue_expr_first_match_wins_by_insertion_order():
  hue_column = DuckDBTraceEnum.query_folder.value
  df = pl.DataFrame({hue_column: ["group_by_order_by_1", "group_by_1"]})

  rules = {
    "group_by_order_by": "group_by_order_by*",
    "group_by": "group_by*",
  }
  out = df.with_columns(
    _build_collapsed_hue_expr(hue_column, rules).alias("collapsed")
  )

  assert out["collapsed"].to_list() == ["group_by_order_by", "group_by"]


def test_build_collapsed_hue_expr_no_rules_is_identity():
  hue_column = DuckDBTraceEnum.query_folder.value
  df = pl.DataFrame({hue_column: ["group_by_1", "union"]})

  out = df.with_columns(
    _build_collapsed_hue_expr(hue_column, {}).alias("collapsed")
  )

  assert out["collapsed"].to_list() == ["group_by_1", "union"]


def test_build_collapsed_hue_expr_supports_leading_and_trailing_glob():
  hue_column = DuckDBTraceEnum.query_folder.value
  df = pl.DataFrame({hue_column: ["group_by_1", "x_group_by_y", "union"]})

  rules = {"group_by": "*group_by*"}
  out = df.with_columns(
    _build_collapsed_hue_expr(hue_column, rules).alias("collapsed")
  )

  assert out["collapsed"].to_list() == ["group_by", "group_by", "union"]
