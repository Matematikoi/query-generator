import logging
import re
from collections.abc import Mapping
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import seaborn as sns

from query_generator.duckdb_connection.trace_collection import DuckDBTraceEnum
from query_generator.metrics.duckdb_parser import DuckDBMetricsName
from query_generator.utils.params import GetMetricsEndpoint

logger = logging.getLogger(__name__)

HISTOGRAMS_WITH_LOG: dict[DuckDBMetricsName, bool] = {
  DuckDBMetricsName.latency_duckdb: True,
  DuckDBMetricsName.query_plan_size: False,
  DuckDBMetricsName.query_plan_length: False,
  DuckDBMetricsName.query_size_tokens: False,
  DuckDBMetricsName.cumulative_cardinality_duckdb: True,
  DuckDBMetricsName.cumulative_rows_scanned_duckdb: True,
  DuckDBMetricsName.cardinality_over_rows_scanned: True,
  DuckDBMetricsName.output_cardinality: True,
}


def _glob_to_rust_regex(glob_pattern: str) -> str:
  # `fnmatch.translate` generates Python-regex constructs (e.g. `(?>...)`, `\Z`)
  # that are rejected by Rust's regex engine (used by Polars). Implement a small
  # subset of glob semantics that is compatible with Rust regex:
  # - `*` => `.*`
  # - `?` => `.`
  # Everything else is escaped literally.
  escaped = re.escape(glob_pattern)
  escaped = escaped.replace(r"\*", ".*").replace(r"\?", ".")
  return f"^{escaped}$"


def _build_collapsed_hue_expr(
  hue_column: str, rules: Mapping[str, str]
) -> pl.Expr:
  collapse_expr: pl.Expr = pl.col(hue_column)
  for new_name, glob_pattern in reversed(list(rules.items())):
    regex_pattern = _glob_to_rust_regex(glob_pattern)
    collapse_expr = (
      pl.when(pl.col(hue_column).str.contains(regex_pattern, literal=False))
      .then(pl.lit(new_name))
      .otherwise(collapse_expr)
    )
  return collapse_expr


def plot_numerical_histogram(
  params: GetMetricsEndpoint,
  metrics_df: pl.DataFrame,
  column: DuckDBMetricsName,
):
  """Plot and save a histogram for the selected numeric column."""
  output_dir = Path(params.output_folder) / "histograms"
  output_dir.mkdir(parents=True, exist_ok=True)

  sns.set_theme(style="whitegrid")
  hue_column = DuckDBTraceEnum.query_folder.value
  collapsed_hue_column = "hue_collapsed"
  collapse_expr = _build_collapsed_hue_expr(
    hue_column, params.group_by_templates
  )
  collapsed_df = metrics_df.with_columns(
    collapse_expr.alias(collapsed_hue_column)
  )
  col_name = str(column.value)
  if col_name not in collapsed_df.columns:
    logger.warning("Column %s not found in metrics_df; skipping.", col_name)
    return

  filtered_df = collapsed_df.filter(
    pl.col(col_name).is_not_null() & pl.col(collapsed_hue_column).is_not_null()
  )
  log_axis = HISTOGRAMS_WITH_LOG[column]
  if log_axis:
    filtered_df = filtered_df.filter(pl.col(col_name) > 0)

  if filtered_df.height == 0:
    logger.warning("Column %s is empty; skipping histogram.", col_name)
    return

  bins = 50
  if log_axis:
    col_min = filtered_df[col_name].min()  # type: ignore
    col_max = filtered_df[col_name].max()  # type: ignore
    assert isinstance(col_min, int | float)
    assert isinstance(col_max, int | float)
    if col_min is None or col_max is None or col_min <= 0:
      logger.warning(
        "Column %s has non-positive values; cannot plot log histogram.",
        col_name,
      )
      return
    if col_min == col_max:
      col_min *= 0.9
      col_max *= 1.1
    bins = np.logspace(np.log10(col_min), np.log10(col_max), num=51)

  plt.figure(figsize=(8, 6))
  sns.histplot(
    data=filtered_df.select([col_name, collapsed_hue_column]).to_pandas(),
    x=col_name,
    hue=collapsed_hue_column,
    bins=bins,
    multiple="stack",
  )
  plt.title(f"{col_name} distribution")
  plt.xlabel(f"{col_name}{' (log scale)' if log_axis else ''}")
  if log_axis:
    plt.xscale("log")
  plt.ylabel("Count")
  plt.tight_layout()
  if col_name in params.x_axis_limits:
    plt.xlim(*params.x_axis_limits[col_name])
  if col_name in params.y_axis_limits:
    plt.ylim(*params.y_axis_limits[col_name])

  output_path = output_dir / f"{col_name}.png"
  plt.savefig(output_path)
  plt.close()
  logger.info("Saved histogram for %s to %s", col_name, output_path)


def plot_metrics(params: GetMetricsEndpoint, metrics_df: pl.DataFrame):
  for column in HISTOGRAMS_WITH_LOG:
    plot_numerical_histogram(params, metrics_df, column)
