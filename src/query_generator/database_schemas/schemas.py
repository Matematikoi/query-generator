from typing import Any

from query_generator.database_schemas.tpcds import get_tpcds_table_info
from query_generator.database_schemas.tpch import get_tpch_table_info
from query_generator.utils.definitions import Dataset
from query_generator.utils.exceptions import (
  PartiallySupportedDatasetError,
  UnkwonDatasetError,
)


def get_schema(dataset: Dataset) -> tuple[dict[str, dict[str, Any]], list[str]]:
  """Get the schema of the database based on the dataset.

  Args:
      dataset (Dataset): The dataset to get the schema for.

  Returns:
      Tuple[Dict[str, Dict[str, Any]], List[str]]: A tuple containing the schema
      as a dictionary and a list of fact tables

  """
  if dataset == Dataset.TPCDS:
    return get_tpcds_table_info()
  if dataset == Dataset.TPCH:
    return get_tpch_table_info()
  if dataset == Dataset.JOB:
    raise PartiallySupportedDatasetError(dataset.value)
  raise UnkwonDatasetError(dataset)
