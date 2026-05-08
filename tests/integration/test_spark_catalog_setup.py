import tempfile
from pathlib import Path

import pytest

from query_generator.spark_connection.catalog_setup import (
  ensure_catalog_initialized,
)
from tests.integration.conftest import PARQUET_PATH


@pytest.mark.integration
def test_ensure_catalog_initialized_creates_metastore() -> None:
  with tempfile.TemporaryDirectory(dir="/tmp") as d:
    catalog_path = Path(d) / "catalog"
    ensure_catalog_initialized(PARQUET_PATH, catalog_path, {})
    assert (catalog_path / "metastore_db").exists()


@pytest.mark.integration
def test_ensure_catalog_initialized_is_idempotent() -> None:
  with tempfile.TemporaryDirectory(dir="/tmp") as d:
    catalog_path = Path(d) / "catalog"
    ensure_catalog_initialized(PARQUET_PATH, catalog_path, {})
    ensure_catalog_initialized(PARQUET_PATH, catalog_path, {})
    assert (catalog_path / "metastore_db").exists()
