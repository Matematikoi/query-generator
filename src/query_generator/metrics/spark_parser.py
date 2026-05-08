import json
import logging
from enum import StrEnum
from typing import TypedDict

import sqlparse

from query_generator.spark_connection.spark_log_parser import (
  SparkNode,
  parse_spark_log_str,
)

logger = logging.getLogger(__name__)


class OperatorCount(TypedDict):
  operator: str
  count: int


class SparkMetricsName(StrEnum):
  wall_time_ms = "wall_time_ms"
  number_of_stages = "number_of_stages"
  number_of_joins = "number_of_joins"
  shuffle_bytes_written = "shuffle_bytes_written"
  number_of_physical_operators = "number_of_physical_operators"
  operator_distribution = "operator_distribution"
  parsed_spark_trace = "parsed_spark_trace"
  query_size_bytes = "query_size_bytes"
  query_size_tokens = "query_size_tokens"
  query_keywords = "query_keywords"


class SparkTraceMetrics(TypedDict):
  wall_time_ms: int
  number_of_stages: int
  number_of_joins: int
  shuffle_bytes_written: int
  number_of_physical_operators: int
  operator_distribution: list[OperatorCount]
  parsed_spark_trace: str
  query_size_bytes: int
  query_size_tokens: int
  query_keywords: list[str]


class SparkTraceParser:
  def __init__(self, raw_log: str, sql: str = "") -> None:
    results = parse_spark_log_str(raw_log)
    if len(results) > 1:
      logger.warning(
        "Found %d UUID-tagged executions in one log; using the first one.",
        len(results),
      )
    self.metrics_data = results[0] if results else None
    self.sql = sql
    try:
      parsed_statements = sqlparse.parse(sql)
      self._parsed_query: list[sqlparse.sql.Token] = (
        list(parsed_statements[0].flatten()) if parsed_statements else []
      )
    except Exception:
      logger.warning(
        "Failed to parse SQL with sqlparse; SQL metrics will be empty."
      )
      self._parsed_query = []

  def _count_joins(self, node: SparkNode) -> int:
    count = 1 if "Join" in node["node_name"] else 0
    return count + sum(self._count_joins(c) for c in node["children"])

  def _sum_metric(self, node: SparkNode, key: str) -> int:
    total = int(node["metrics"].get(key, 0))
    return total + sum(self._sum_metric(c, key) for c in node["children"])

  _INFRASTRUCTURE_NODES: frozenset[str] = frozenset(
    {
      "InputAdapter",
      "ColumnarToRow",
      "AdaptiveSparkPlan",
      "ResultQueryStage",
      "ShuffleQueryStage",
      "BroadcastQueryStage",
      "LogicalQueryStage",
      "AQEShuffleRead",
      "ReusedExchange",
      "LogicalRelation",
      "EmptyRelation",
    }
  )

  def _is_infrastructure(self, node_name: str) -> bool:
    return node_name in self._INFRASTRUCTURE_NODES or node_name.startswith(
      "WholeStageCodegen"
    )

  def _normalize_node_name(self, node_name: str) -> str:
    if node_name.startswith("Scan parquet"):
      return "Scan parquet"
    return node_name

  def get_wall_time_ms(self) -> int:
    assert self.metrics_data is not None
    return self.metrics_data["global_metrics"]["wall_time_ms"]

  def get_number_of_stages(self) -> int:
    assert self.metrics_data is not None
    return len(self.metrics_data["stages_info"])

  def get_number_of_joins(self) -> int:
    assert self.metrics_data is not None
    return self._count_joins(self.metrics_data["spark_plan_info"])

  def get_shuffle_bytes_written(self) -> int:
    assert self.metrics_data is not None
    return self._sum_metric(
      self.metrics_data["spark_plan_info"], "shuffle_bytes_written"
    )

  def get_number_of_physical_operators(self) -> int:
    assert self.metrics_data is not None

    def _count(node: SparkNode) -> int:
      own = 0 if self._is_infrastructure(node["node_name"]) else 1
      return own + sum(_count(c) for c in node["children"])

    return _count(self.metrics_data["spark_plan_info"])

  def get_operator_distribution(self) -> list[OperatorCount]:
    assert self.metrics_data is not None
    counts: dict[str, int] = {}

    def _collect(node: SparkNode) -> None:
      if not self._is_infrastructure(node["node_name"]):
        name = self._normalize_node_name(node["node_name"])
        counts[name] = counts.get(name, 0) + 1
      for child in node["children"]:
        _collect(child)

    _collect(self.metrics_data["spark_plan_info"])
    return [
      {"operator": op, "count": cnt}
      for op, cnt in sorted(counts.items(), key=lambda x: x[1], reverse=True)
    ]

  def get_query_size_bytes(self) -> int:
    return len(self.sql.encode("utf-8"))

  def get_query_size_tokens(self) -> int:
    return sum(
      1 for t in self._parsed_query if not t.is_whitespace and not t.is_newline
    )

  def get_query_keywords(self) -> list[str]:
    return [t.normalized for t in self._parsed_query if t.is_keyword]

  def get_parsed_spark_trace(self) -> str:
    assert self.metrics_data is not None
    return json.dumps(self.metrics_data)

  def get_metrics(self) -> SparkTraceMetrics | None:
    if self.metrics_data is None:
      return None
    return {
      "wall_time_ms": self.get_wall_time_ms(),
      "number_of_stages": self.get_number_of_stages(),
      "number_of_joins": self.get_number_of_joins(),
      "shuffle_bytes_written": self.get_shuffle_bytes_written(),
      "number_of_physical_operators": self.get_number_of_physical_operators(),
      "operator_distribution": self.get_operator_distribution(),
      "parsed_spark_trace": self.get_parsed_spark_trace(),
      "query_size_bytes": self.get_query_size_bytes(),
      "query_size_tokens": self.get_query_size_tokens(),
      "query_keywords": self.get_query_keywords(),
    }

  @staticmethod
  def get_metrics_from_raw_log(
    raw_log: str, sql: str = ""
  ) -> SparkTraceMetrics | None:
    parser = SparkTraceParser(raw_log, sql)
    return parser.get_metrics()
