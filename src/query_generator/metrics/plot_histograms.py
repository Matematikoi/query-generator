from pathlib import Path
import logging

import matplotlib.pyplot as plt
import polars as pl
import seaborn as sns

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
    for col in columns:
        col_name = str(col)
        if col_name not in metrics_df.columns:
            logger.warning("Column %s not found in metrics_df; skipping.", col_name)
            continue

        series = metrics_df[col_name].drop_nulls()
        if series.len() == 0:
            logger.warning("Column %s is empty; skipping histogram.", col_name)
            continue

        plt.figure(figsize=(8, 6))
        sns.histplot(series, bins=50)
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
        DuckDBMetricsName.latency_duckdb,
        DuckDBMetricsName.query_plan_size,
        DuckDBMetricsName.query_plan_length,
        DuckDBMetricsName.query_size_tokens,
        DuckDBMetricsName.cumulative_cardinality_duckdb,
        DuckDBMetricsName.cumulative_rows_scanned_duckdb,
    ]
    plot_numerical_histograms(params, metrics_df, columns_for_histograms)
