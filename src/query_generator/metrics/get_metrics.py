import logging
import multiprocessing as mp
from multiprocessing.pool import Pool

import polars as pl

from query_generator.duckdb_connection.trace_collection import DuckDBTraceEnum
from query_generator.metrics.duckdb_parser import DuckDBTraceParser
from query_generator.metrics.plot_histograms import plot_metrics
from query_generator.synthetic_queries.utils.query_writer import (
  write_parquet,
)
from query_generator.utils.params import GetMetricsEndpoint

logger = logging.getLogger(__name__)


def _get_pool() -> Pool:
  """Lazily create a process pool to escape the GIL."""
  workers = mp.cpu_count() - 1
  start_method = "fork"
  ctx = mp.get_context(start_method)
  return ctx.Pool(processes=workers)


def apply_template_occurrence_limit(
  params: GetMetricsEndpoint, traces_df: pl.DataFrame
) -> pl.DataFrame:
  """Apply template occurrence limits to the traces DataFrame."""
  if not params.template_occurrence_limit:
    return traces_df
  template_keys = list(params.template_occurrence_limit.keys())
  template_col = pl.col(DuckDBTraceEnum.query_folder)
  restricted_df = traces_df.filter(template_col.is_in(template_keys)).sort(
    pl.col(DuckDBTraceEnum.relative_path)
  )
  unrestricted_df = traces_df.filter(~template_col.is_in(template_keys))
  restricted_dfs = []
  for template, limit in params.template_occurrence_limit.items():
    template_df = restricted_df.filter(template_col == template).head(limit)
    restricted_dfs.append(template_df)
  return pl.concat([unrestricted_df] + restricted_dfs, how="vertical")


def get_metrics(params: GetMetricsEndpoint) -> None:
  """Get metrics according to given queries."""
  traces_df = pl.read_parquet(params.input_parquet)
  trace_expr = pl.col(DuckDBTraceEnum.duckdb_trace)
  success_expr = pl.col(DuckDBTraceEnum.trace_success)
  min_trace_length = 2
  valid_trace_expr = (
    trace_expr.is_not_null()
    & success_expr
    & (trace_expr.str.len_chars() > min_trace_length)
  )
  filtered_df = apply_template_occurrence_limit(
    params, traces_df.filter(valid_trace_expr)
  )
  traces = filtered_df[DuckDBTraceEnum.duckdb_trace].to_list()
  with _get_pool() as pool:
    metrics = pool.map(DuckDBTraceParser.get_metrics_from_raw_trace, traces)
  metrics_df = pl.DataFrame(metrics)
  result_df = pl.concat([filtered_df, metrics_df], how="horizontal")
  write_parquet(result_df, params.output_folder / "metrics.parquet")
  logger.info("Metrics collected")
  plot_metrics(params, result_df)
