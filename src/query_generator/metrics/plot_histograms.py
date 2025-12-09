import logging
from pathlib import Path

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns

from query_generator.duckdb_connection.trace_collection import DuckDBTraceEnum
from query_generator.metrics.duckdb_parser import DuckDBMetricsName
from query_generator.utils.params import GetMetricsEndpoint

logger = logging.getLogger(__name__)


def plot_numerical_histograms(
  params: GetMetricsEndpoint, metrics_df: pl.DataFrame, columns: list[str]
):
  """Plot and save histograms for the selected numeric columns."""
  output_dir = Path(params.output_folder) / "histograms"
  output_dir.mkdir(parents=True, exist_ok=True)

  sns.set_theme(style="whitegrid")
  hue_column = DuckDBTraceEnum.query_folder.value
  collapsed_hue_column = "hue_collapsed"
  collapsed_df = metrics_df.with_columns(
    pl.when(
      pl.col(hue_column).str.contains("group_by", literal=False)
    )
    .then(pl.lit("group_by"))
    .otherwise(pl.col(hue_column))
    .alias(collapsed_hue_column)
  )
  for col in columns:
    col_name = str(col)
    if col_name not in collapsed_df.columns:
      logger.warning("Column %s not found in metrics_df; skipping.", col_name)
      continue

    filtered_df = collapsed_df.filter(
      pl.col(col_name).is_not_null() & pl.col(collapsed_hue_column).is_not_null()
    )
    if filtered_df.height == 0:
      logger.warning("Column %s is empty; skipping histogram.", col_name)
      continue

    plt.figure(figsize=(8, 6))
    sns.histplot(
      data=filtered_df.select([col_name, collapsed_hue_column]).to_pandas(),
      x=col_name,
      hue=collapsed_hue_column,
      bins=50,
      multiple="stack",
    )
    plt.title(f"{col_name} distribution")
    plt.xlabel(col_name)
    plt.ylabel("Count")
    plt.tight_layout()

    output_path = output_dir / f"{col_name}.png"
    plt.savefig(output_path)
    plt.close()
    logger.info("Saved histogram for %s to %s", col_name, output_path)


def plot_metrics(params: GetMetricsEndpoint, metrics_df: pl.DataFrame):
  columns_for_histograms = [
    DuckDBMetricsName.latency_duckdb.value,
    DuckDBMetricsName.query_plan_size.value,
    DuckDBMetricsName.query_plan_length.value,
    DuckDBMetricsName.query_size_tokens.value,
    DuckDBMetricsName.cumulative_cardinality_duckdb.value,
    DuckDBMetricsName.cumulative_rows_scanned_duckdb.value,
  ]
  plot_numerical_histograms(params, metrics_df, columns_for_histograms)
