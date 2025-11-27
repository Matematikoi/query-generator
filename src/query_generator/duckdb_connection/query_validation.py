import logging
import threading

import duckdb

from query_generator.duckdb_connection.utils import get_tables
from query_generator.utils.exceptions import DuckDBTimeoutError

logger = logging.getLogger(__name__)


class DuckDBQueryValidator:
  def __init__(self, database_path: str, timeout_seconds: float) -> None:
    self.database_path = database_path
    self.timeout_seconds = timeout_seconds
    self.connect_to_database()
    self.get_tables()

  def get_tables(self) -> None:
    self.tables = get_tables(self.conn)

  def test_and_fix_connection(self) -> None:
    """Test connection and reconnects if any problem arise."""
    try:
      # Simple query to verify the connection responds correctly
      self.conn.execute(f"SELECT (*) FROM {self.tables[0]};").fetchall()
    except Exception as e:
      logger.exception(f"Error in basic SQL query, restarting connection:\n{e}")
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

  def is_query_valid(self, query: str) -> tuple[bool, Exception]:
    self.test_and_fix_connection()
    interrupted = threading.Event()
    timer = threading.Timer(
      self.timeout_seconds,
      self._interrupt_connection,
      args=(interrupted,),
    )
    timer.start()
    try:
      self.conn.sql(query).fetchone()
    except Exception as exc:
      if interrupted.is_set():
        logger.warning(
          "Query validation exceeded %s seconds; connection interrupted.",
          self.timeout_seconds,
        )
        return False, DuckDBTimeoutError(self.timeout_seconds)
      return False, exc
    finally:
      timer.cancel()

    return True, Exception("No exception found while running the query")

  def connect_to_database(self) -> None:
    self.conn = duckdb.connect(database=self.database_path, read_only=True)
