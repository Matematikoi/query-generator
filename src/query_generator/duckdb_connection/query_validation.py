import logging
import threading

import duckdb

from query_generator.duckdb_connection.utils import get_tables

TIMEOUT_FOR_QUERY_VALIDATION_SECONDS = 5
logger = logging.getLogger(__name__)


class DuckDBQueryValidator:
  def __init__(self, database_path: str) -> None:
    self.database_path = database_path
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
      logger.exception("Error in basic SQL query, restarting connection")
      logger.debug(f"Error produced: {e}")
      self.conn.close()
      self.connect_to_database()
    else:
      logger.debug("Database tested, no connection problem found.")

  def _execute_query_for_validation(
    self, query: str, exceptions: list[Exception]
  ) -> None:
    try:
      self.conn.sql(query).fetchone()
    except Exception as exc:
      exceptions.append(exc)

  def is_query_valid(self, query: str) -> tuple[bool, Exception]:
    self.test_and_fix_connection()
    exceptions: list[Exception] = []

    query_thread = threading.Thread(
      target=self._execute_query_for_validation,
      args=(query, exceptions),
      daemon=True,
    )
    query_thread.start()
    query_thread.join(timeout=TIMEOUT_FOR_QUERY_VALIDATION_SECONDS)

    if query_thread.is_alive():
      logger.warning(
        "Query validation exceeded %s seconds; interrupting.",
        TIMEOUT_FOR_QUERY_VALIDATION_SECONDS,
      )
      try:
        self.conn.interrupt()
      except Exception:
        logger.exception("Failed to interrupt DuckDB connection.")
      query_thread.join()

    if exceptions:
      logger.warning("DuckDB failed to run the provided query.")
      logger.debug(f"Query that fail to run: \n```sql\n{query}\n```")
      return False, exceptions[0]

    return True, Exception("No exception found while running the query")

  def connect_to_database(self) -> None:
    self.conn = duckdb.connect(database=self.database_path, read_only=True)
