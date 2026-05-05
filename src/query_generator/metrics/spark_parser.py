from enum import StrEnum
from typing import TypedDict

from query_generator.spark_connection.spark_log_parser import (
  parse_spark_log_str,
)


class SparkMetricsName(StrEnum):
  wall_time_ms = "wall_time_ms"
  number_of_stages = "number_of_stages"


class SparkTraceMetrics(TypedDict):
  wall_time_ms: int
  number_of_stages: int


class SparkTraceParser:
  def __init__(self, raw_log: str) -> None:
    results = parse_spark_log_str(raw_log)
    self.metrics_data = results[0] if results else None

  @staticmethod
  def get_metrics_from_raw_log(raw_log: str) -> SparkTraceMetrics | None:
    parser = SparkTraceParser(raw_log)
    return parser.get_metrics()

  def get_metrics(self) -> SparkTraceMetrics | None:
    if self.metrics_data is None:
      return None
    return {
      "wall_time_ms": self.metrics_data["global_metrics"]["wall_time_ms"],
      "number_of_stages": len(self.metrics_data["stages_info"]),
    }
