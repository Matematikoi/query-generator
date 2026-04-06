from query_generator.database_connection.duckdb_validation import (
  DuckDBQueryExecutor,
)
from query_generator.database_connection.pyspark_validation import (
  PySparkQueryValidator,
)
from query_generator.database_connection.query_validator_abc import (
  QueryValidator,
)
from query_generator.utils.definitions import ValidatorEngine


def build_query_validator(
  database_path: str,
  validation_timeout_seconds: int | float,
  validator_engine: ValidatorEngine,
) -> QueryValidator:
  """Build the appropriate query validator based on validator_engine.

  When validator_engine is DUCKDB, database_path should point to a .duckdb file.
  When validator_engine is PYSPARK, database_path should point to a parquet
  directory with structure: database_path/table_name/data.parquet
  """
  if validator_engine == ValidatorEngine.DUCKDB:
    return DuckDBQueryExecutor(database_path, validation_timeout_seconds)
  if validator_engine == ValidatorEngine.PYSPARK:
    return PySparkQueryValidator(database_path, validation_timeout_seconds)
  msg = f"Unknown validator engine: {validator_engine}"
  raise ValueError(msg)
