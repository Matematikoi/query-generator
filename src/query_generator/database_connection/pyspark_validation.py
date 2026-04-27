import logging
import multiprocessing
import os
import threading
import uuid
from dataclasses import dataclass, field
from multiprocessing import Queue
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

from query_generator.database_connection.duckdb_validation import (
  QueryExecution,
)
from query_generator.database_connection.query_validator_abc import (
  QueryValidator,
)

logger = logging.getLogger(__name__)

_MP_CTX = multiprocessing.get_context("spawn")


@dataclass
class PySparkWorkerInput:
  parquet_path: str
  timeout_seconds: float
  limit_output_size: int
  spark_config: dict[str, str] = field(default_factory=dict)


def _run_pyspark_query_worker(
  query: str,
  q: Queue,
  params: PySparkWorkerInput,
  ready_queue: Queue,
) -> None:
  """Execute a Spark SQL query in an isolated process."""
  spark = None
  try:
    os.environ["SPARK_HOME"] = pyspark.__path__[0]
    logging.getLogger("py4j").setLevel(logging.INFO)
    master = params.spark_config.get("master") or "local[*]"
    builder = (
      SparkSession.builder.master(master)
      .appName("query-validator")
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.log.level", "WARN")
    )
    for k, v in params.spark_config.items():
      builder = builder.config(k, v)
    spark = builder.getOrCreate()

    base = Path(params.parquet_path)
    for table_dir in sorted(base.iterdir()):
      if table_dir.is_dir():
        table = spark.read.parquet(str(table_dir))
        table.createOrReplaceTempView(table_dir.name)

    ready_queue.put("READY")

    result_df = spark.sql(query)
    rows = result_df.limit(params.limit_output_size).take(
      params.limit_output_size
    )
    row_tuples = [tuple(row) for row in rows]
    q.put(QueryExecution(result=row_tuples, exception=None, timed_out=False))
  except Exception as exc:
    # Convert to plain Exception so it can be pickled across processes.
    # PySpark exceptions often fail to deserialize in the parent process.
    # timed_out is always False here — timeouts are detected at the
    # parent-process level via p.join(timeout) in _execute_with_timeout.
    q.put(
      QueryExecution(
        result=None, exception=Exception(str(exc)), timed_out=False
      )
    )
  finally:
    if spark is not None:
      spark.stop()


def _run_persistent_spark_query(
  spark: SparkSession,
  query: str,
  job_group: str,
  q: Queue,
) -> None:
  """Run a COUNT(*) query on a persistent SparkSession, put result in q."""
  try:
    spark.sparkContext.setJobGroup(job_group, query, interruptOnCancel=True)
    rows = spark.sql(query).take(1)
    q.put(int(rows[0][0]) if rows else -1)
  except Exception as exc:
    logger.debug("Cardinality query failed: %s | query: %s", exc, query)
    q.put(-1)


class PySparkQueryValidator(QueryValidator):
  """Query validator using PySpark with parquet files as data source.

  Reads parquet directories structured as database_path/table_name/data.parquet
  and registers each as a temporary view (metadata-only, no data loaded).
  Each query runs in a separate process for isolation against crashes and hangs.
  """

  def __init__(
    self,
    parquet_path: str,
    timeout_seconds: float,
    limit_output_size: int = 1_000,
    spark_config: dict[str, str] | None = None,
  ) -> None:
    output_size_buffer = 100
    self.parquet_path = parquet_path
    self.timeout_seconds = timeout_seconds
    self.limit_output_size = limit_output_size + output_size_buffer
    self.spark_config: dict[str, str] = spark_config or {}
    self.worker_input = PySparkWorkerInput(
      parquet_path=parquet_path,
      timeout_seconds=timeout_seconds,
      limit_output_size=self.limit_output_size,
      spark_config=self.spark_config,
    )
    self._spark: SparkSession | None = None

  def _execute_with_timeout(
    self, query: str, description: str
  ) -> QueryExecution:
    logger.debug("Start %s.", description)
    q: Queue = _MP_CTX.Queue()
    ready_queue: Queue = _MP_CTX.Queue()

    p = _MP_CTX.Process(
      target=_run_pyspark_query_worker,
      args=(query, q, self.worker_input, ready_queue),
    )
    p.start()
    logger.debug("PySpark worker process started (pid=%s).", p.pid)

    ready_queue.get()
    logger.debug(
      "Spark session ready, starting %ss query timeout (pid=%s).",
      self.timeout_seconds,
      p.pid,
    )

    p.join(self.timeout_seconds)

    if p.is_alive():
      logger.warning(
        "%s exceeded %s seconds; process terminated.",
        description,
        self.timeout_seconds,
      )
      p.terminate()
      p.join(5)
      if p.is_alive():
        p.kill()
        p.join()
      return QueryExecution(
        result=None,
        exception=TimeoutError(
          f"PySpark query exceeded {self.timeout_seconds}s"
        ),
        timed_out=True,
      )

    execution = (
      q.get()
      if not q.empty()
      else QueryExecution(
        result=None,
        exception=Exception("No result returned from PySpark worker."),
        timed_out=False,
      )
    )

    logger.debug(
      "%s finished with timed_out=%s, exception=%s",
      description,
      execution.timed_out,
      execution.exception,
    )

    return execution

  def is_query_valid(self, query: str) -> tuple[bool, Exception | None]:
    execution = self._execute_with_timeout(query, "PySpark query validation")
    if execution.exception:
      return False, execution.exception
    return True, None

  def get_query_output_size(self, query: str) -> tuple[int | None, bool]:
    execution = self._execute_with_timeout(
      query, "PySpark output size calculation"
    )
    result = len(execution.result) if execution.result is not None else None
    logger.debug("Query exception: %s", execution.exception)
    return result, execution.timed_out

  def get_synthetic_query_cardinality(self, query: str) -> int:
    """Run a COUNT(*) query and return its scalar result.

    Uses a persistent SparkSession — no new process per call. Timeout via
    Spark job group cancellation. Returns -1 on error or timeout.
    """
    if self._spark is None:
      os.environ["SPARK_HOME"] = pyspark.__path__[0]
      logging.getLogger("py4j").setLevel(logging.INFO)
      master = self.spark_config.get("master") or "local[*]"
      builder = (
        SparkSession.builder.master(master)
        .appName("query-validator")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.log.level", "WARN")
      )
      for k, v in self.spark_config.items():
        builder = builder.config(k, v)
      self._spark = builder.getOrCreate()
      base = Path(self.parquet_path)
      for table_dir in sorted(base.iterdir()):
        if table_dir.is_dir():
          table = self._spark.read.parquet(str(table_dir))
          table.createOrReplaceTempView(table_dir.name)

    job_group = str(uuid.uuid4())
    q: Queue = Queue()
    t = threading.Thread(
      target=_run_persistent_spark_query,
      args=(self._spark, query, job_group, q),
      daemon=True,
    )
    t.start()
    t.join(self.timeout_seconds)
    if t.is_alive():
      self._spark.sparkContext.cancelJobGroup(job_group)
      t.join()
      logger.debug(
        "Cardinality query timed out after %ss | query: %s",
        self.timeout_seconds,
        query,
      )
      return -1
    return q.get() if not q.empty() else -1
