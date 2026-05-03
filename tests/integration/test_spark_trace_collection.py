import json
import tempfile
import pytest
from pathlib import Path

from query_generator.spark_connection.catalog_setup import (
  ensure_catalog_initialized,
)
from query_generator.spark_connection.trace_collection import (
  SparkTraceParams,
  spark_collect_one_trace,
)
from tests.integration.conftest import PARQUET_PATH


@pytest.fixture(scope="module")
def catalog() -> Path:
  cat = PARQUET_PATH / ".spark" / "catalog"
  ensure_catalog_initialized(PARQUET_PATH, cat, {})
  return cat


def _make_params(catalog: Path, work_dir: Path) -> SparkTraceParams:
  return SparkTraceParams(
    queries_path=str(work_dir),
    parquet_path=str(PARQUET_PATH),
    catalog_path=str(catalog),
    timeout_seconds=60.0,
    output_folder=str(work_dir / "out"),
    spark_config={},
  )


@pytest.mark.integration
def test_spark_collect_one_trace_success(catalog: Path) -> None:
  with tempfile.TemporaryDirectory(dir="/tmp") as d:
    work_dir = Path(d)
    sql_file = work_dir / "q.sql"
    sql_file.write_text("SELECT COUNT(*) FROM customer")

    params = _make_params(catalog, work_dir)
    row = spark_collect_one_trace(
      "SELECT COUNT(*) FROM customer", sql_file, params
    )

  assert row.trace_success is True
  assert row.spark_log != ""
  events = [
    json.loads(line) for line in row.spark_log.splitlines() if line.strip()
  ]
  event_names = {e.get("Event", "") for e in events}
  assert any("SQLExecutionStart" in name for name in event_names)


@pytest.mark.integration
def test_spark_collect_one_trace_relative_path(catalog: Path) -> None:
  with tempfile.TemporaryDirectory(dir="/tmp") as d:
    work_dir = Path(d)
    subdir = work_dir / "subfolder"
    subdir.mkdir()
    sql_file = subdir / "q.sql"
    sql_file.write_text("SELECT COUNT(*) FROM customer")

    params = _make_params(catalog, work_dir)
    row = spark_collect_one_trace(
      "SELECT COUNT(*) FROM customer", sql_file, params
    )

  assert row.relative_path == "subfolder/q.sql"
  assert row.query_folder == "subfolder"
  assert row.query_name == "q"
