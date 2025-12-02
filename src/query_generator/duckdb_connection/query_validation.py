import logging
import threading
from dataclasses import dataclass

import duckdb
from sqlglot import exp, parse_one

from query_generator.duckdb_connection.utils import get_tables
from query_generator.utils.exceptions import DuckDBTimeoutError

logger = logging.getLogger(__name__)


@dataclass
class QueryExecution:
  result: tuple | None
  exception: Exception | None
  timed_out: bool


COUNT_CTE_NAME = "cte_for_count"


class DuckDBQueryExecutor:
  """Simple class for executing queries under timout constraints.

  It works with a DuckDB database in read-only mode. It will test the
  connection before each query and will reconnect if any problem arise.
  Works for fetch-one queries only for now."""

  def __init__(
    self, database_path: str, timeout_seconds: float, memory_gb: int = 5
  ) -> None:
    self.database_path = database_path
    self.timeout_seconds = timeout_seconds
    self.memory_gb = memory_gb
    self.connect_to_database()
    self.get_tables()

  def get_tables(self) -> None:
    self.tables = get_tables(self.conn)

  def reconnect_database_and_test(self) -> None:
    """Restart connection and reconnects if any problem arise."""
    self.connect_to_database()
    try:
      # Simple query to verify the connection responds correctly
      self.conn.execute(f"SELECT (*) FROM {self.tables[0]};").fetchall()
    except Exception:
      logger.exception("Error in basic SQL query, restarting connection")
      self.conn.close()
      self.connect_to_database()
    else:
      logger.debug("Database tested, no connection problem found.")

  def _interrupt_connection(self, interrupted: threading.Event) -> None:
    interrupted.set()
    try:
      self.conn.interrupt()
    except Exception:
      logger.exception("Failed to interrupt DuckDB connection.")

  def _execute_with_timeout(
    self, query: str, description: str
  ) -> QueryExecution:
    logger.debug("Start %s.", description)
    interrupted = threading.Event()
    timer = threading.Timer(
      self.timeout_seconds,
      self._interrupt_connection,
      args=(interrupted,),
    )
    timer.start()
    result: tuple | None = None
    exception: Exception | None = None
    timed_out = False
    try:
      row = self.conn.execute(query).fetchone()
      result = row
    except Exception as exc:
      timed_out = interrupted.is_set()
      if timed_out:
        exception = DuckDBTimeoutError(self.timeout_seconds)
        logger.warning(
          "%s exceeded %s seconds; connection interrupted.",
          description,
          self.timeout_seconds,
        )
      else:
        exception = exc
    finally:
      timer.cancel()
      self.conn.close()
      logger.debug(
        "%s finished with timed_out=%s ,exception=%s",
        description,
        timed_out,
        exception,
      )

    return QueryExecution(
      result=result, exception=exception, timed_out=timed_out
    )

  def is_query_valid(self, query: str) -> tuple[bool, Exception]:
    self.reconnect_database_and_test()
    execution = self._execute_with_timeout(query, "DuckDB query validation")
    if execution.exception:
      return False, execution.exception

    return True, Exception("No exception found while running the query")

  def connect_to_database(self) -> None:
    self.conn = duckdb.connect(database=self.database_path, read_only=True)
    self.conn.execute(f"SET memory_limit = '{self.memory_gb}GB';")
    self.conn.execute("SET enable_progress_bar = false;")
    self.conn.execute("SET enable_progress_bar_print = false;")

  def _wrap_query_with_count(self, sql: str) -> str | None:
    """Wrap a query inside a CTE and count its rows to avoid syntax issues."""
    with contextlib.suppress(Exception):
      original: exp.Expression = parse_one(sql)

      cte_alias = exp.TableAlias(this=exp.to_identifier(COUNT_CTE_NAME))
      cte = exp.CTE(this=original.copy(), alias=cte_alias)
      with_clause = exp.With(expressions=[cte])

      outer_select = exp.select(exp.func("COUNT", exp.Star())).from_(
        exp.to_table(COUNT_CTE_NAME)
      )
      outer_select.set("with", with_clause)

      return outer_select.sql(pretty=True)

    return None

  def get_query_output_size(self, query: str) -> int:
    """Returns the output size of a query.

    If the query fails, it returns -1."""
    self.reconnect_database_and_test()
    count_query = self._wrap_query_with_count(query)
    if count_query is None:
      logger.error("Failed to wrap query for counting. Skipping.")
      return -1
    execution = self._execute_with_timeout(
      count_query,
      "DuckDB output size calculation",
    )
    result = -1
    if execution.exception is None and execution.result is not None:
      result = execution.result[0]
    assert result is not None
    logger.debug(
      "Query result was: %s\nwith exception %s",
      execution.result,
      execution.exception,
    )
    return int(result)
