from dataclasses import dataclass
from enum import Enum


class Extension(Enum):
  SNOWFLAKE = "SNOWFLAKE"
  SNOWFLAKE_SEARCH_PARAMS = "SNOWFLAKE_SEARCH_PARAMS"
  BINNING_CHERRY_PICKING = "BINNING_CHERRY_PICKING"


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


@dataclass
class GeneratedQueryFeatures:
  query: str
  template_number: int
  predicate_number: int
  fact_table: str


@dataclass
class BatchGeneratedQueryFeatures(GeneratedQueryFeatures):
  batch_number: int
