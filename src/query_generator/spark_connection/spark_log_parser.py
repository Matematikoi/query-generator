from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypedDict

from query_generator.utils.exceptions import SparkUUIDNotFoundError

logger = logging.getLogger(__name__)


class SparkNode(TypedDict):
  node_name: str
  simple_string: str
  metadata: dict
  metrics: dict[str, int | float]
  children: list[SparkNode]


class GlobalMetrics(TypedDict):
  wall_time_ms: int


class StageInfo(TypedDict):
  stage_id: int
  stage_name: str
  duration_ms: int | None
  submission_time_ms: int
  completion_time_ms: int
  number_of_tasks: int


class SparkMetrics(TypedDict):
  query_uuid: str
  error: str | None
  global_metrics: GlobalMetrics
  spark_plan_info: SparkNode
  physical_plan_description: str
  stages_info: list[StageInfo]


@dataclass
class _EventLog:
  exec_starts: dict[int, dict] = field(default_factory=dict)
  adaptive_updates: dict[int, dict] = field(default_factory=dict)
  exec_ends: dict[int, dict] = field(default_factory=dict)
  job_starts: list[dict] = field(default_factory=list)
  stage_completeds: list[dict] = field(default_factory=list)
  driver_accum_updates: list[dict] = field(default_factory=list)


def _extract_uuid(description: str) -> str:
  match = re.search(r"query_uuid:([0-9a-f-]{36})", description)
  if not match:
    raise SparkUUIDNotFoundError(description)
  return match.group(1)


def _build_accum_map(
  stage_events: list[dict],
  driver_events: list[dict],
) -> dict[int, int | float]:
  accum_map: dict[int, int | float] = {}
  for stage_event in stage_events:
    for accum in stage_event["Stage Info"]["Accumulables"]:
      if accum.get("Metadata") == "sql":
        accum_map[accum["ID"]] = int(accum["Value"])
  accum_map.update(
    {
      accum_id: value
      for d in driver_events
      for accum_id, value in d["accumUpdates"]
    }
  )
  return accum_map


def _resolve_node(node: dict, accum_map: dict[int, int | float]) -> SparkNode:
  metrics: dict[str, int | float] = {}
  for metric_def in node.get("metrics", []):
    accum_id = metric_def["accumulatorId"]
    if accum_id in accum_map:
      name = re.sub(r"[^a-z0-9]+", "_", metric_def["name"].lower()).strip("_")
      metrics[name] = accum_map[accum_id]
  return SparkNode(
    node_name=node["nodeName"],
    simple_string=node["simpleString"],
    metadata=node.get("metadata", {}),
    metrics=metrics,
    children=[
      _resolve_node(child, accum_map) for child in node.get("children", [])
    ],
  )


def _build_stage_info(stage_event: dict) -> StageInfo:
  info = stage_event["Stage Info"]
  duration_ms = None
  for accum in info.get("Accumulables", []):
    if accum.get("Name") == "duration" and accum.get("Metadata") == "sql":
      duration_ms = int(accum["Value"])
      break
  return StageInfo(
    stage_id=info["Stage ID"],
    stage_name=info["Stage Name"],
    duration_ms=duration_ms,
    submission_time_ms=info.get("Submission Time", 0),
    completion_time_ms=info.get("Completion Time", 0),
    number_of_tasks=info["Number of Tasks"],
  )


def _collect_events(raw_lines: Iterable[bytes]) -> _EventLog:
  log = _EventLog()
  for raw_line in raw_lines:
    stripped = raw_line.strip()
    if not stripped:
      continue
    try:
      event = json.loads(stripped)
    except json.JSONDecodeError:
      logger.warning("Skipping malformed JSON line in Spark log.")
      continue
    event_type = event.get("Event", "")

    if "SQLExecutionStart" in event_type:
      log.exec_starts[event["executionId"]] = event
    elif "SQLAdaptiveExecutionUpdate" in event_type:
      log.adaptive_updates[event["executionId"]] = event
    elif "SQLExecutionEnd" in event_type:
      log.exec_ends[event["executionId"]] = event
    elif event_type == "SparkListenerJobStart":
      log.job_starts.append(event)
    elif event_type == "SparkListenerStageCompleted":
      log.stage_completeds.append(event)
    elif "DriverAccumUpdates" in event_type:
      log.driver_accum_updates.append(event)

  return log


def _build_execution_result(
  exec_id: int,
  exec_start: dict,
  exec_end: dict,
  log: _EventLog,
) -> SparkMetrics | None:
  query_uuid = _extract_uuid(exec_start["description"])
  wall_time_ms = exec_end["time"] - exec_start["time"]
  error_msg: str = exec_end.get("errorMessage", "")
  error = error_msg if error_msg else None

  execution_stage_ids: set[int] = set()
  for job in log.job_starts:
    props = job.get("Properties", {})
    try:
      if int(props.get("spark.sql.execution.id", -1)) == exec_id:
        execution_stage_ids.update(job["Stage IDs"])
    except ValueError:
      pass

  if not execution_stage_ids:
    return None

  relevant_stages = [
    s
    for s in log.stage_completeds
    if s["Stage Info"]["Stage ID"] in execution_stage_ids
  ]
  relevant_driver = [
    d for d in log.driver_accum_updates if d["executionId"] == exec_id
  ]

  accum_map = _build_accum_map(relevant_stages, relevant_driver)
  plan_source = log.adaptive_updates.get(exec_id, exec_start)
  spark_plan_info = _resolve_node(plan_source["sparkPlanInfo"], accum_map)
  physical_plan_description = plan_source.get("physicalPlanDescription", "")
  stages_info = [_build_stage_info(s) for s in relevant_stages]

  return SparkMetrics(
    query_uuid=query_uuid,
    error=error,
    global_metrics=GlobalMetrics(wall_time_ms=wall_time_ms),
    spark_plan_info=spark_plan_info,
    physical_plan_description=physical_plan_description,
    stages_info=stages_info,
  )


def _parse_raw_lines(raw_lines: Iterable[bytes]) -> list[SparkMetrics]:
  log = _collect_events(raw_lines)

  results: list[SparkMetrics] = []
  for exec_id, exec_start in log.exec_starts.items():
    if not exec_start["description"].startswith("query_uuid:"):
      continue
    exec_end = log.exec_ends.get(exec_id)
    if exec_end is None:
      continue
    result = _build_execution_result(exec_id, exec_start, exec_end, log)
    if result is not None:
      results.append(result)

  best: dict[str, SparkMetrics] = {}
  for r in results:
    uid = r["query_uuid"]
    if (
      uid not in best
      or r["global_metrics"]["wall_time_ms"]
      > best[uid]["global_metrics"]["wall_time_ms"]
    ):
      best[uid] = r
  return list(best.values())


def parse_spark_log_str(content: str) -> list[SparkMetrics]:
  """Parse a Spark event log from a string instead of a file."""
  return _parse_raw_lines(line.encode() for line in content.splitlines())


def parse_spark_log_file(file: Path) -> list[SparkMetrics]:
  """Parse a Spark event log from a file path."""
  with open(file, "rb") as f:
    return _parse_raw_lines(f)
