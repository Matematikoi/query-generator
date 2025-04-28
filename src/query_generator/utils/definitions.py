from dataclasses import dataclass
from enum import Enum


class Extension(Enum):
  SNOWFLAKE = "SNOWFLAKE"
  STAR_JOIN = "STAR_JOIN"


class Dataset(Enum):
  TPCDS = "TPCDS"
  TPCH = "TPCH"


@dataclass
class QueryGenerationParameters:
  max_hops: int
  max_queries_per_signature: int
  max_queries_per_fact_table: int
  keep_edge_prob: float
  dataset: Dataset
  extra_predicates: int
  row_retention_probability: float
