import logging
import os
from dataclasses import dataclass
from multiprocessing import Process, Queue
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


@dataclass
class PySparkWorkerInput:
  parquet_path: str
  timeout_seconds: float
  limit_output_size: int


def _run_pyspark_query_worker(
  query: str,
  q: Queue,
  params: PySparkWorkerInput,
) -> None:
  """Execute a Spark SQL query in an isolated process."""
  spark = None
  try:
    os.environ["SPARK_HOME"] = pyspark.__path__[0]
    logging.getLogger("py4j").setLevel(logging.INFO)
    spark = (
      SparkSession.builder.master("local[*]")
      .appName("query-validator")
      .config("spark.ui.showConsoleProgress", "false")
      .config("spark.log.level", "WARN")
      .getOrCreate()
    )

    base = Path(params.parquet_path)
    for table_dir in sorted(base.iterdir()):
      if table_dir.is_dir():
        table = spark.read.parquet(str(table_dir))
        table.createOrReplaceTempView(table_dir.name)

    result_df = spark.sql(query)
    rows = result_df.limit(params.limit_output_size).collect()
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
  ) -> None:
    output_size_buffer = 100
    self.parquet_path = parquet_path
    self.timeout_seconds = timeout_seconds
    self.limit_output_size = limit_output_size + output_size_buffer
    self.worker_input = PySparkWorkerInput(
      parquet_path=parquet_path,
      timeout_seconds=timeout_seconds,
      limit_output_size=self.limit_output_size,
    )

  def _execute_with_timeout(
    self, query: str, description: str
  ) -> QueryExecution:
    logger.debug("Start %s.", description)
    q: Queue = Queue()

    p = Process(
      target=_run_pyspark_query_worker,
      args=(query, q, self.worker_input),
    )
    p.start()
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
