from query_generator.database_connection.duckdb_validation import (
  DuckDBQueryExecutor,
)
from query_generator.database_connection.query_validator_abc import (
  QueryValidator,
)
from query_generator.utils.definitions import ValidatorEngine
from query_generator.utils.params import LLMParams


def build_query_validator(llm_params: LLMParams) -> QueryValidator:
  """Build the appropriate query validator based on llm_params.validator_engine.

  When validator_engine is DUCKDB, database_path should point to a .duckdb file.
  When validator_engine is PYSPARK, database_path should point to a parquet
  directory with structure: database_path/table_name/data.parquet
  """
  if llm_params.validator_engine == ValidatorEngine.DUCKDB:
    return DuckDBQueryExecutor(
      llm_params.database_path, llm_params.duckdb_timeout_seconds
    )
  if llm_params.validator_engine == ValidatorEngine.PYSPARK:
    from query_generator.database_connection.pyspark_validation import (
      PySparkQueryValidator,
    )

    return PySparkQueryValidator(
      llm_params.database_path, llm_params.duckdb_timeout_seconds
    )
  msg = f"Unknown validator engine: {llm_params.validator_engine}"
  raise ValueError(msg)
