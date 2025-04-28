from typing import Any, Dict, List, Tuple

from query_generator.database_schemas.tpcds import get_tpcds_table_info
from query_generator.database_schemas.tpch import get_tpch_table_info
from query_generator.utils.definitions import Dataset


def get_schema(dataset: Dataset) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
  """
  Get the schema of the database based on the dataset.
  Args:
      dataset (Dataset): The dataset to get the schema for.
  Returns:
      Tuple[Dict[str, Dict[str, Any]], List[str]]: A tuple containing the schema
      as a dictionary and a list of fact tables
  """
  if dataset == Dataset.TPCDS:
    return get_tpcds_table_info()
  elif dataset == Dataset.TPCH:
    return get_tpch_table_info()
  else:
    raise ValueError(f"Unknown dataset: {dataset}")
