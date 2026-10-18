"""Helpers for collecting results from worker processes."""

from __future__ import annotations

import queue
import time
from multiprocessing.process import BaseProcess
from multiprocessing.queues import Queue
from typing import Any

POLL_INTERVAL_SECONDS = 0.1


def wait_for_worker_result(
  process: BaseProcess, result_queue: Queue[Any], timeout_seconds: float
) -> tuple[Any | None, bool]:
  """Read the worker's result from the queue before joining the process.

  A process that put data on a multiprocessing Queue does not exit until that
  data is flushed to the underlying pipe. Joining it before reading the queue
  deadlocks once the payload exceeds the pipe buffer (~64KB), so the queue
  must be drained first.

  Returns:
      (result, timed_out). The result is None if the worker exited without
      putting anything on the queue or if the timeout elapsed.
  """
  deadline = time.monotonic() + timeout_seconds
  while True:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
      return None, True
    try:
      result = result_queue.get(timeout=min(remaining, POLL_INTERVAL_SECONDS))
    except queue.Empty:
      if process.is_alive():
        continue
      # The worker may have exited right after the last poll.
      try:
        result = result_queue.get_nowait()
      except queue.Empty:
        process.join()
        return None, False
    join_or_terminate(process, deadline - time.monotonic())
    return result, False


def join_or_terminate(process: BaseProcess, timeout_seconds: float) -> None:
  """Join the process, terminating it if it does not exit in time."""
  process.join(max(timeout_seconds, POLL_INTERVAL_SECONDS))
  if process.is_alive():
    process.terminate()
    process.join()
