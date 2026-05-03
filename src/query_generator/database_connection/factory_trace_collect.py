from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

from query_generator.duckdb_connection.trace_collection import (
  DuckDBTraceOuputDataFrameRow,
  DuckDBTraceParams,
  duckdb_collect_one_trace,
)
from query_generator.spark_connection.catalog_setup import (
  ensure_catalog_initialized,
)
from query_generator.spark_connection.trace_collection import (
  SparkTraceParams,
  SparkTraceRow,
  spark_collect_one_trace,
)
from query_generator.utils.definitions import ValidatorEngine
from query_generator.utils.params import FixTransformEndpoint

logger = logging.getLogger(__name__)

GeneralTraceRow = DuckDBTraceOuputDataFrameRow | SparkTraceRow


def build_trace_collector(
  params: FixTransformEndpoint,
) -> Callable[[str, Path], tuple[GeneralTraceRow, bool]]:
  """Return a (sql, sql_file) -> (trace_row, was_transformed) callable.

  Sets up engine-specific resources (Spark catalog) before returning.
  """
  engine = params.engine

  if engine.validator_engine == ValidatorEngine.PYSPARK:
    catalog_path = Path(engine.database_path) / ".spark" / "catalog"
    ensure_catalog_initialized(
      parquet_path=Path(engine.database_path),
      catalog_path=catalog_path,
      spark_config=engine.spark_config,
    )
    spark_trace_params = SparkTraceParams(
      queries_path=params.queries_folder,
      parquet_path=engine.database_path,
      catalog_path=str(catalog_path),
      timeout_seconds=params.timeout_seconds,
      output_folder=params.destination_folder,
      spark_config=engine.spark_config,
    )

    def _spark_collect(sql: str, sql_file: Path) -> tuple[SparkTraceRow, bool]:
      return spark_collect_one_trace(sql, sql_file, spark_trace_params), True

    return _spark_collect

  trace_params = DuckDBTraceParams(
    queries_path=params.queries_folder,
    duckdb_path=engine.database_path,
    timeout_seconds=params.timeout_seconds,
    fetch_limit=params.max_output_size,
    output_folder=params.destination_folder,
    max_memory_gb=params.max_memory_gb,
  )

  def _duckdb_collect(
    sql: str, sql_file: Path
  ) -> tuple[DuckDBTraceOuputDataFrameRow, bool]:
    trace = duckdb_collect_one_trace(sql, sql_file, trace_params)
    if trace.trace_success:
      return trace, True
    logger.info("Transformation failed, falling back to original query trace.")
    return duckdb_collect_one_trace(
      sql_file.read_text(), sql_file, trace_params
    ), False

  return _duckdb_collect
