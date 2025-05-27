from dataclasses import dataclass
from enum import Enum


class Extension(Enum):
  SNOWFLAKE = "SNOWFLAKE"
  SNOWFLAKE_SEARCH_PARAMS = "SNOWFLAKE_SEARCH_PARAMS"
  BINNING_CHERRY_PICKING = "BINNING_CHERRY_PICKING"


class Utility(Enum):
  HISTOGRAM = "HISTOGRAM"


class Dataset(Enum):
  TPCDS = "TPCDS"
  TPCH = "TPCH"
  JOB = "JOB"


@dataclass
class PredicateOperatorProbability:
  """Probability of using a specific predicate operator.

  They are based on choice with weights for each operator.

  The weights will be normalized to sum to 1.
  """

  operator_in: float
  operator_equal: float
  operator_range: float


@dataclass
class QueryGenerationParameters:
  max_hops: int
  max_queries_per_signature: int
  max_queries_per_fact_table: int
  keep_edge_prob: float
  dataset: Dataset
  extra_predicates: int
  row_retention_probability: float
  seen_subgraphs: dict[int, bool]
  operator_probabilities: PredicateOperatorProbability


@dataclass
class GeneratedQueryFeatures:
  query: str
  template_number: int
  predicate_number: int
  fact_table: str


@dataclass
class BatchGeneratedQueryFeatures(GeneratedQueryFeatures):
  batch_number: int
  prefix: str
