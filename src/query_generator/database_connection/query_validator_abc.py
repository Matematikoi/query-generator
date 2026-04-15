from abc import ABC, abstractmethod


class QueryValidator(ABC):
  """Abstract base class for query validators."""

  @abstractmethod
  def is_query_valid(self, query: str) -> tuple[bool, Exception | None]:
    """Validate a query. Returns (is_valid, exception_or_none)."""

  @abstractmethod
  def get_query_output_size(self, query: str) -> tuple[int | None, bool]:
    """Get query output row count. Returns (row_count_or_none, timed_out)."""

  @abstractmethod
  def get_synthetic_query_cardinality(self, query: str) -> int:
    """Run a COUNT(*) query and return its scalar result.

    Uses a persistent connection — no new process per call. Returns -1 on
    error or timeout.
    """
