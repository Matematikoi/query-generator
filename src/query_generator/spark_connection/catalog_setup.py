from __future__ import annotations

import logging
import os
from pathlib import Path

import pyspark
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def ensure_catalog_initialized(
  parquet_path: Path,
  catalog_path: Path,
  spark_config: dict[str, str],
) -> None:
  """Initialise a Hive metastore at catalog_path if not already present.

  Idempotent: returns immediately when metastore_db/ already exists.
  Skips hidden directories (names starting with '.') when registering tables.
  """
  metastore = catalog_path / "metastore_db"
  if metastore.exists():
    logger.info(
      "Spark catalog already initialised at %s, skipping.", catalog_path
    )
    return

  warehouse = catalog_path / "warehouse"
  warehouse.mkdir(parents=True, exist_ok=True)

  os.environ["SPARK_HOME"] = pyspark.__path__[0]
  master = spark_config.get("master") or "local[*]"
  builder = (
    SparkSession.builder.master(master)
    .appName("catalog-setup")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.log.level", "WARN")
    .enableHiveSupport()
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.sql.warehouse.dir", str(warehouse))
    .config(
      "spark.hadoop.javax.jdo.option.ConnectionURL",
      f"jdbc:derby:;databaseName={metastore};create=true",
    )
    .config(
      "spark.hadoop.javax.jdo.option.ConnectionDriverName",
      "org.apache.derby.jdbc.EmbeddedDriver",
    )
    .config("spark.sql.cbo.enabled", "true")
  )
  for k, v in spark_config.items():
    builder = builder.config(k, v)

  spark = builder.getOrCreate()
  try:
    table_dirs = sorted(
      d
      for d in parquet_path.iterdir()
      if d.is_dir() and not d.name.startswith(".")
    )
    for table_dir in table_dirs:
      logger.info(f"Collecting Stats for table {table_dir.name}")
      name = table_dir.name
      path = str(table_dir.resolve())
      spark.sql(
        f"CREATE TABLE IF NOT EXISTS `{name}` USING PARQUET LOCATION '{path}'"
      )
      spark.sql(f"ANALYZE TABLE `{name}` COMPUTE STATISTICS")
      spark.sql(f"ANALYZE TABLE `{name}` COMPUTE STATISTICS FOR ALL COLUMNS")
      logger.debug("Registered table %s", name)
    logger.info("Spark catalog initialised with %d tables.", len(table_dirs))
  finally:
    spark.stop()
