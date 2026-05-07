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

JOIN_NDJSON = "\n".join(
  [
    json.dumps(
      {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
        "executionId": 1,
        "description": "query_uuid:00000000-0000-0000-0000-000000000002",
        "time": 1000,
        "sparkPlanInfo": {
          "nodeName": "Project",
          "simpleString": "Project",
          "metadata": {},
          "metrics": [],
          "children": [
            {
              "nodeName": "BroadcastHashJoin",
              "simpleString": "BroadcastHashJoin",
              "metadata": {},
              "metrics": [],
              "children": [
                {
                  "nodeName": "SortMergeJoin",
                  "simpleString": "SortMergeJoin",
                  "metadata": {},
                  "metrics": [],
                  "children": [],
                }
              ],
            }
          ],
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

SHUFFLE_NDJSON = "\n".join(
  [
    json.dumps(
      {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
        "executionId": 1,
        "description": "query_uuid:00000000-0000-0000-0000-000000000003",
        "time": 1000,
        "sparkPlanInfo": {
          "nodeName": "Project",
          "simpleString": "Project",
          "metadata": {},
          "metrics": [{"name": "shuffle bytes written", "accumulatorId": 1}],
          "children": [
            {
              "nodeName": "Exchange",
              "simpleString": "Exchange",
              "metadata": {},
              "metrics": [
                {"name": "shuffle bytes written", "accumulatorId": 2}
              ],
              "children": [],
            }
          ],
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
          "Accumulables": [
            {"ID": 1, "Value": "100", "Metadata": "sql"},
            {"ID": 2, "Value": "200", "Metadata": "sql"},
          ],
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

MIXED_PLAN_NDJSON = "\n".join(
  [
    json.dumps(
      {
        "Event": "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
        "executionId": 1,
        "description": "query_uuid:00000000-0000-0000-0000-000000000004",
        "time": 1000,
        "sparkPlanInfo": {
          "nodeName": "AdaptiveSparkPlan",
          "simpleString": "AdaptiveSparkPlan",
          "metadata": {},
          "metrics": [],
          "children": [
            {
              "nodeName": "WholeStageCodegen (1)",
              "simpleString": "WholeStageCodegen (1)",
              "metadata": {},
              "metrics": [],
              "children": [
                {
                  "nodeName": "Project",
                  "simpleString": "Project",
                  "metadata": {},
                  "metrics": [],
                  "children": [
                    {
                      "nodeName": "InputAdapter",
                      "simpleString": "InputAdapter",
                      "metadata": {},
                      "metrics": [],
                      "children": [
                        {
                          "nodeName": "Filter",
                          "simpleString": "Filter",
                          "metadata": {},
                          "metrics": [],
                          "children": [
                            {
                              "nodeName": "Scan parquet spark_catalog.default.customer",
                              "simpleString": "Scan parquet",
                              "metadata": {},
                              "metrics": [],
                              "children": [],
                            }
                          ],
                        }
                      ],
                    }
                  ],
                }
              ],
            }
          ],
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
  metrics = SparkTraceParser.get_metrics_from_raw_log(MINIMAL_NDJSON)
  assert metrics["wall_time_ms"] == 2000


def test_get_metrics_from_raw_log_number_of_stages():
  metrics = SparkTraceParser.get_metrics_from_raw_log(MINIMAL_NDJSON)
  assert metrics["number_of_stages"] == 1


def test_get_metrics_from_raw_log_returns_none_on_empty():
  assert SparkTraceParser.get_metrics_from_raw_log("") is None


def test_number_of_joins_counts_all_join_nodes():
  metrics = SparkTraceParser.get_metrics_from_raw_log(JOIN_NDJSON)
  assert metrics["number_of_joins"] == 2


def test_number_of_joins_zero_when_no_joins():
  metrics = SparkTraceParser.get_metrics_from_raw_log(MINIMAL_NDJSON)
  assert metrics["number_of_joins"] == 0


def test_shuffle_bytes_written_sums_across_nodes():
  metrics = SparkTraceParser.get_metrics_from_raw_log(SHUFFLE_NDJSON)
  assert metrics["shuffle_bytes_written"] == 300


def test_shuffle_bytes_written_zero_when_absent():
  metrics = SparkTraceParser.get_metrics_from_raw_log(MINIMAL_NDJSON)
  assert metrics["shuffle_bytes_written"] == 0


def test_number_of_physical_operators_excludes_infrastructure():
  metrics = SparkTraceParser.get_metrics_from_raw_log(MIXED_PLAN_NDJSON)
  assert metrics["number_of_physical_operators"] == 3


def test_number_of_physical_operators_minimal():
  metrics = SparkTraceParser.get_metrics_from_raw_log(MINIMAL_NDJSON)
  assert metrics["number_of_physical_operators"] == 1


def test_operator_distribution_normalized_scan():
  metrics = SparkTraceParser.get_metrics_from_raw_log(MIXED_PLAN_NDJSON)
  dist = {
    entry["operator"]: entry["count"]
    for entry in metrics["operator_distribution"]
  }
  assert dist == {"Project": 1, "Filter": 1, "Scan parquet": 1}


def test_operator_distribution_minimal():
  metrics = SparkTraceParser.get_metrics_from_raw_log(MINIMAL_NDJSON)
  dist = {
    entry["operator"]: entry["count"]
    for entry in metrics["operator_distribution"]
  }
  assert dist == {"Project": 1}
