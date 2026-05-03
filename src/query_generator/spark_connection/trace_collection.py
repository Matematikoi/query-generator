from __future__ import annotations

import logging
import multiprocessing
import os
import queue
import uuid
from dataclasses import dataclass
from enum import StrEnum
from multiprocessing import Queue
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
_MP_CTX = multiprocessing.get_context("spawn")


@dataclass
class SparkTraceParams:
  queries_path: str
  parquet_path: str
  catalog_path: str
  timeout_seconds: float
  output_folder: str
  spark_config: dict[str, str]


class SparkTraceEnum(StrEnum):
  """Column names for SparkTraceRow."""

  relative_path = "relative_path"
  query_folder = "query_folder"
  query_name = "query_name"
  spark_log = "spark_log"
  error = "error"
  trace_success = "trace_success"


@dataclass
class SparkTraceRow:
  relative_path: str
  query_folder: str
  query_name: str
  spark_log: str
  error: str
  trace_success: bool


@dataclass
class SparkTraceWorkerInput:
  """Bundles per-query worker inputs — mirrors PySparkWorkerInput pattern."""

  query: str
  query_uuid: str
  log_dir: Path
  params: SparkTraceParams


def _spark_trace_worker(
  worker_input: SparkTraceWorkerInput,
  q: Queue,
  ready_queue: Queue,
) -> None:
  spark = None
  try:
    os.environ["SPARK_HOME"] = pyspark.__path__[0]
    params = worker_input.params
    catalog_path = Path(params.catalog_path)
    metastore = catalog_path / "metastore_db"
    warehouse = catalog_path / "warehouse"

    master = params.spark_config.get("master") or "local[*]"
    builder = (
      SparkSession.builder.master(master)
      .appName("trace-collector")
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.log.level", "WARN")
      .config("spark.eventLog.enabled", "true")
      .config(
        "spark.eventLog.dir",
        f"file://{worker_input.log_dir.resolve().as_posix()}",
      )
      .config("spark.eventLog.compress", "false")
      .enableHiveSupport()
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.sql.warehouse.dir", str(warehouse))
      .config(
        "spark.hadoop.javax.jdo.option.ConnectionURL",
        f"jdbc:derby:;databaseName={metastore};create=false",
      )
      .config(
        "spark.hadoop.javax.jdo.option.ConnectionDriverName",
        "org.apache.derby.jdbc.EmbeddedDriver",
      )
      .config("spark.sql.cbo.enabled", "true")
    )
    for k, v in params.spark_config.items():
      builder = builder.config(k, v)

    spark = builder.getOrCreate()
    # Signal parent that the session is ready — timeout starts NOW.
    ready_queue.put("READY")
    spark.sparkContext.setLocalProperty(
      "spark.job.description",
      f"query_uuid:{worker_input.query_uuid}",
    )
    spark.sql(worker_input.query).collect()
    q.put(("ok", ""))
  except Exception as exc:
    q.put(("error", str(exc)))
  finally:
    if spark is not None:
      spark.stop()


def _collect_log_content(log_dir: Path) -> str | None:
  """Return content of the first non-empty events_* file found under log_dir.

  Skips Hadoop .crc checksum files that are written alongside the event log.
  """
  for f in sorted(log_dir.rglob("events_*")):
    if f.is_file() and f.stat().st_size > 0:
      return f.read_text()
  return None


def spark_collect_one_trace(
  sql: str,
  sql_file: Path,
  params: SparkTraceParams,
) -> SparkTraceRow:
  queries_path = Path(params.queries_path)
  relative_path = str(sql_file.relative_to(queries_path))

  query_uuid = str(uuid.uuid4())
  log_dir = (Path(params.output_folder) / "SPARK_TRACES" / query_uuid).resolve()
  log_dir.mkdir(parents=True, exist_ok=True)

  worker_input = SparkTraceWorkerInput(
    query=sql,
    query_uuid=query_uuid,
    log_dir=log_dir,
    params=params,
  )
  q: Queue = _MP_CTX.Queue()
  ready_queue: Queue = _MP_CTX.Queue()
  p = _MP_CTX.Process(
    target=_spark_trace_worker,
    args=(worker_input, q, ready_queue),
  )
  p.start()
  try:
    ready_queue.get(timeout=20)
  except queue.Empty:
    logger.warning(
      "Spark session did not start within 20s for %s; proceeding to timeout.",
      sql_file,
    )
  p.join(params.timeout_seconds)

  error = ""
  if p.is_alive():
    logger.warning(
      "Spark trace timed out after %ss for %s",
      params.timeout_seconds,
      sql_file,
    )
    p.terminate()
    p.join(5)
    if p.is_alive():
      p.kill()
      p.join()
    error = f"Timeout after {params.timeout_seconds}s"
  elif not q.empty():
    status, msg = q.get()
    if status == "error":
      error = msg

  spark_log = _collect_log_content(log_dir)
  if spark_log is None:
    return SparkTraceRow(
      relative_path=relative_path,
      query_folder=sql_file.parent.name,
      query_name=sql_file.stem,
      spark_log="",
      error=error or "No log files produced",
      trace_success=False,
    )

  return SparkTraceRow(
    relative_path=relative_path,
    query_folder=sql_file.parent.name,
    query_name=sql_file.stem,
    spark_log=spark_log,
    error=error,
    trace_success=True,
  )
