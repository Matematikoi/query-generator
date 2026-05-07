import json
import logging
from enum import StrEnum
from typing import TypedDict

from query_generator.spark_connection.spark_log_parser import (
  SparkNode,
  parse_spark_log_str,
)

logger = logging.getLogger(__name__)


class SparkMetricsName(StrEnum):
  wall_time_ms = "wall_time_ms"
  number_of_stages = "number_of_stages"
  number_of_joins = "number_of_joins"
  shuffle_bytes_written = "shuffle_bytes_written"
  parsed_spark_trace = "parsed_spark_trace"


class SparkTraceMetrics(TypedDict):
  wall_time_ms: int
  number_of_stages: int
  number_of_joins: int
  shuffle_bytes_written: int
  parsed_spark_trace: str


class SparkTraceParser:
  def __init__(self, raw_log: str) -> None:
    results = parse_spark_log_str(raw_log)
    if len(results) > 1:
      logger.warning(
        "Found %d UUID-tagged executions in one log; using the first one.",
        len(results),
      )
    self.metrics_data = results[0] if results else None

  def _count_joins(self, node: SparkNode) -> int:
    count = 1 if "Join" in node["node_name"] else 0
    return count + sum(self._count_joins(c) for c in node["children"])

  def _sum_metric(self, node: SparkNode, key: str) -> int:
    total = int(node["metrics"].get(key, 0))
    return total + sum(self._sum_metric(c, key) for c in node["children"])

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
      "parsed_spark_trace": self.get_parsed_spark_trace(),
    }

  @staticmethod
  def get_metrics_from_raw_log(raw_log: str) -> SparkTraceMetrics | None:
    parser = SparkTraceParser(raw_log)
    return parser.get_metrics()
