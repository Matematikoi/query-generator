"""Collect DuckDB JSON profiles (full trace).

- Runs each query in a separate process (robustness) **and** uses an in-process
    timer to call `conn.interrupt()` for precise per-query timeouts.
- Captures **DETAILED** JSON profile fully in memory using
    `SET explain_output='json'; EXPLAIN ANALYZE <query>` (no temp files).
- Output Parquet schema: (relative_path, query_folder, query_name, duckdb_trace)
"""

from __future__ import annotations

import contextlib
import threading
from dataclasses import dataclass
from enum import StrEnum
from multiprocessing import Process, Queue
from pathlib import Path

import duckdb

CHECKPOINT_FREQUENCY = 100  # Save Parquet every N queries


@dataclass
class DuckDBTraceParams:
  """Dataclass for getting the duckdb traces."""

  queries_path: str
  duckdb_path: str
  timeout_seconds: float
  fetch_limit: int
  output_folder: str

  def get_queries_path(self) -> Path:
    """Get the queries path as a Path object."""
    return Path(self.queries_path)

  def get_duckdb_path(self) -> Path:
    """Get the duckdb path as a Path object."""
    return Path(self.duckdb_path)

  def get_output_path(self) -> Path:
    return Path(self.output_folder)


class DuckDBTraceEnum(StrEnum):
  """Rows for DuckDBTraceOuputDataFrameRow."""

  relative_path = "relative_path"
  query_folder = "query_folder"
  query_name = "query_name"
  duckdb_trace = "duckdb_trace"
  error = "error"
  trace_success = "trace_success"
  duckdb_output = "duckdb_output"


@dataclass
class DuckDBTraceOuputDataFrameRow:
  """Rows for the output parquet file."""

  relative_path: str
  query_folder: str
  query_name: str
  duckdb_trace: str
  error: str
  trace_success: bool
  duckdb_output: list[str]


def _profile_worker(
  query: str,
  query_path: Path,
  params: DuckDBTraceParams,
  out_q: Queue[tuple[bool, list[str], Path | None, str]],
) -> None:
  """Execute one SQL with DuckDB JSON profiling.

  This mirrors the server pattern you shared: enable JSON profiling to a
  concrete path, run the query (with `.show()`), then read the profile file
  and send the JSON string via the queue. We still keep per‑query timeouts via
  `conn.interrupt()`.
  """
  base_path: Path = params.get_queries_path()
  output_path: Path = params.get_output_path()
  db_path: str = params.get_duckdb_path().as_posix()
  timeout_seconds = float(params.timeout_seconds)
  con = None
  timer: threading.Timer | None = None
  try:
    trace_file = (
      output_path
      / "DUCKDB_TRACES"
      / query_path.relative_to(base_path).with_suffix(".json")
    )
    trace_file.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(db_path, read_only=True)
    # Match the detailed JSON profiling behavior
    con.execute("PRAGMA profiling_mode='detailed';")
    con.execute("PRAGMA enable_profiling=json;")
    con.execute(f"PRAGMA profiling_output='{trace_file.as_posix()}';")

    if timeout_seconds and timeout_seconds > 0:

      def _interrupt() -> None:
        with contextlib.suppress(Exception):
          assert con is not None
          con.interrupt()

      timer = threading.Timer(timeout_seconds, _interrupt)
      timer.daemon = True
      timer.start()

    # Execute the original text and force materialization/printing
    cur = con.execute(query)
    rows = cur.fetchmany(params.fetch_limit + 10)
    result = [str(r) for r in rows]

    out_q.put((True, result, trace_file, ""))
  except Exception as e:
    out_q.put((False, [], None, str(e)))
  finally:
    with contextlib.suppress(Exception):
      assert timer is not None
      assert con is not None
      timer.cancel()
      timer.join(timeout=0.1)
      con.close()


def duckdb_collect_one_trace(
  sql: str, sql_file: Path, params: DuckDBTraceParams
) -> DuckDBTraceOuputDataFrameRow:
  """Get DuckDB traces for the queries and store them in a Parquet file.

  Uses only the fields/methods provided by DuckDBTracesParams:
      - queries_path, duckdb_path, timeout_seconds
  """
  queries_path = Path(params.queries_path)

  ok = False
  result = []
  json_path = None
  error = ""
  q: Queue = Queue()
  p = Process(
    target=_profile_worker,
    args=(
      sql,
      sql_file,
      params,
      q,
    ),
  )
  p.start()
  p.join(params.timeout_seconds)

  if p.is_alive():
    p.terminate()
    p.join()
  elif not q.empty():
    ok, result, json_path, error = q.get()

  return DuckDBTraceOuputDataFrameRow(
    relative_path=str(sql_file.relative_to(queries_path)),
    query_folder=sql_file.parent.name,
    query_name=sql_file.stem,
    duckdb_trace=json_path.read_text()
    if ok and json_path is not None and json_path.is_file()
    else "",
    duckdb_output=result if ok else [],
    error=error if not ok else "",
    trace_success=ok,
  )
