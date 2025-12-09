import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import seaborn as sns

from query_generator.duckdb_connection.trace_collection import DuckDBTraceEnum
from query_generator.metrics.duckdb_parser import DuckDBMetricsName
from query_generator.utils.params import GetMetricsEndpoint

logger = logging.getLogger(__name__)


def plot_numerical_histogram(
  params: GetMetricsEndpoint,
  metrics_df: pl.DataFrame,
  column: str,
  *,
  log_axis: bool = False,
):
  """Plot and save a histogram for the selected numeric column."""
  output_dir = Path(params.output_folder) / "histograms"
  output_dir.mkdir(parents=True, exist_ok=True)

  sns.set_theme(style="whitegrid")
  hue_column = DuckDBTraceEnum.query_folder.value
  collapsed_hue_column = "hue_collapsed"
  collapsed_df = metrics_df.with_columns(
    pl.when(pl.col(hue_column).str.contains("group_by", literal=False))
    .then(pl.lit("group_by"))
    .otherwise(pl.col(hue_column))
    .alias(collapsed_hue_column)
  )
  col_name = str(column)
  if col_name not in collapsed_df.columns:
    logger.warning("Column %s not found in metrics_df; skipping.", col_name)
    return

  filtered_df = collapsed_df.filter(
    pl.col(col_name).is_not_null() & pl.col(collapsed_hue_column).is_not_null()
  )
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

  output_path = output_dir / f"{col_name}.png"
  plt.savefig(output_path)
  plt.close()
  logger.info("Saved histogram for %s to %s", col_name, output_path)


def plot_metrics(params: GetMetricsEndpoint, metrics_df: pl.DataFrame):
  columns_for_histograms_with_log = [
    (DuckDBMetricsName.latency_duckdb.value, True),
    (DuckDBMetricsName.query_plan_size.value, False),
    (DuckDBMetricsName.query_plan_length.value, False),
    (DuckDBMetricsName.query_size_tokens.value, False),
    (DuckDBMetricsName.cumulative_cardinality_duckdb.value, True),
    (DuckDBMetricsName.cumulative_rows_scanned_duckdb.value, True),
    (DuckDBMetricsName.rows_scanned_over_cardinality, True),
  ]
  for column, log_axis in columns_for_histograms_with_log:
    plot_numerical_histogram(params, metrics_df, column, log_axis=log_axis)
