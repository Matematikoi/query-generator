import json

from query_generator.metrics.spark_parser import SparkTraceParser

MINIMAL_NDJSON = "\n".join(
  [
    json.dumps(
      {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
        "executionId": 1,
        "description": "query_uuid:00000000-0000-0000-0000-000000000001",
        "time": 1000,
        "sparkPlanInfo": {
          "nodeName": "Project",
          "simpleString": "Project [a#1]",
          "metadata": {},
          "metrics": [],
          "children": [],
        },
      }
    ),
    json.dumps(
      {
        "Event": "SparkListenerJobStart",
        "Job ID": 0,
        "Stage IDs": [0],
        "Properties": {"spark.sql.execution.id": "1"},
      }
    ),
    json.dumps(
      {
        "Event": "SparkListenerStageCompleted",
        "Stage Info": {
          "Stage ID": 0,
          "Stage Name": "collect at Query.scala:1",
          "Number of Tasks": 1,
          "Submission Time": 1100,
          "Completion Time": 2000,
          "Accumulables": [],
        },
      }
    ),
    json.dumps(
      {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
        "executionId": 1,
        "time": 3000,
      }
    ),
  ]
)


def test_get_metrics_from_raw_log_wall_time_ms():
  from query_generator.metrics.spark_parser import SparkTraceParser

  metrics = SparkTraceParser.get_metrics_from_raw_log(MINIMAL_NDJSON)
  assert metrics["wall_time_ms"] == 2000


def test_get_metrics_from_raw_log_number_of_stages():
  from query_generator.metrics.spark_parser import SparkTraceParser

  metrics = SparkTraceParser.get_metrics_from_raw_log(MINIMAL_NDJSON)
  assert metrics["number_of_stages"] == 1


def test_get_metrics_from_raw_log_returns_none_on_empty():
  assert SparkTraceParser.get_metrics_from_raw_log("") is None
