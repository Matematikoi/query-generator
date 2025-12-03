import contextlib
import logging
import threading
from dataclasses import dataclass
from multiprocessing import Process, Queue

import duckdb

from query_generator.utils.exceptions import DuckDBTimeoutError

logger = logging.getLogger(__name__)


@dataclass
class QueryExecution:
  result: list[tuple] | None
  exception: Exception | None
  timed_out: bool


@dataclass
class QueryWorkerInput:
  database_path: str
  memory_gb: int
  timeout_seconds: float
  limit_output_size: int


COUNT_CTE_NAME = "cte_for_count"


def _run_query_worker(
  query: str,
  q: Queue,
  params: QueryWorkerInput,
) -> None:
  """Execute a query in an isolated process and return results via queue."""
  conn = None
  timer: threading.Timer | None = None
  timed_out = False

  def _interrupt() -> None:
    nonlocal timed_out
    timed_out = True
    with contextlib.suppress(Exception):
      assert conn is not None
      conn.interrupt()

  try:
    conn = duckdb.connect(database=params.database_path, read_only=True)
    conn.execute(f"SET memory_limit = '{params.memory_gb}GB';")
    conn.execute("SET enable_progress_bar = false;")
    conn.execute("SET enable_progress_bar_print = false;")

    if params.timeout_seconds and params.timeout_seconds > 0:
      timer = threading.Timer(params.timeout_seconds, _interrupt)
      timer.daemon = True
      timer.start()

    cur = conn.execute(query)
    rows = cur.fetchmany(params.limit_output_size)
    q.put(QueryExecution(result=rows, exception=None, timed_out=timed_out))
  except Exception as exc:  # pragma: no cover - defensive
    q.put(QueryExecution(result=None, exception=exc, timed_out=timed_out))
  finally:
    with contextlib.suppress(Exception):
      if timer is not None:
        timer.cancel()
    with contextlib.suppress(Exception):
      if conn is not None:
        conn.close()


class DuckDBQueryExecutor:
  """Simple class for executing queries under timout constraints.

  It works with a DuckDB database in read-only mode. Each query is executed in
  a separate process to isolate potential crashes."""

  def __init__(
    self,
    database_path: str,
    timeout_seconds: float,
    memory_gb: int = 5,
    limit_output_size: int = 1_000,
  ) -> None:
    output_size_buffer = 100
    self.database_path = database_path
    self.timeout_seconds = timeout_seconds
    self.memory_gb = memory_gb
    self.limit_output_size = limit_output_size + output_size_buffer
    self.query_worker_input = QueryWorkerInput(
      database_path=database_path,
      memory_gb=memory_gb,
      timeout_seconds=timeout_seconds,
      limit_output_size=self.limit_output_size,
    )

  def _execute_with_timeout(
    self, query: str, description: str
  ) -> QueryExecution:
    logger.debug("Start %s.", description)
    q: Queue = Queue()

    p = Process(
      target=_run_query_worker,
      args=(
        query,
        q,
        self.query_worker_input,
      ),
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
      p.join()
      return QueryExecution(
        result=None,
        exception=DuckDBTimeoutError(self.timeout_seconds),
        timed_out=True,
      )

    execution = (
      q.get()
      if not q.empty()
      else QueryExecution(
        result=None,
        exception=Exception("No result returned from worker process."),
        timed_out=False,
      )
    )

    logger.debug(
      "%s finished with timed_out=%s ,exception=%s",
      description,
      execution.timed_out,
      execution.exception,
    )

    return execution

  def is_query_valid(self, query: str) -> tuple[bool, Exception]:
    execution = self._execute_with_timeout(query, "DuckDB query validation")
    if execution.exception:
      return False, execution.exception
    return True, Exception("No exception found while running the query")

  def get_query_output_size(self, query: str) -> tuple[int | None, bool]:
    """Get the output size of a query.

    It returns a tuple of (output_size, timed_out). If the query fails
    to execute, output_size is None
    """
    execution = self._execute_with_timeout(
      query,
      "DuckDB output size calculation",
    )
    result = len(execution.result) if execution.result is not None else None
    logger.debug(
      "Query exception: %s",
      execution.exception,
    )
    return result, execution.timed_out
