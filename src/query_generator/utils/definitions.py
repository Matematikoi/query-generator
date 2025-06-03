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
  """

  operator_in: float
  operator_equal: float
  operator_range: float


@dataclass
class PredicateParameters:
  extra_predicates: int
  row_retention_probability: float
  operator_weights: PredicateOperatorProbability
  equality_lower_bound_probability: float
  extra_values_for_in: int


# TODO(Gabriel): http://localhost:8080/tktview/205e90a1fa
@dataclass
class QueryGenerationParameters:
  dataset: Dataset
  max_hops: int
  max_queries_per_signature: int
  max_queries_per_fact_table: int
  keep_edge_probability: float
  seen_subgraphs: dict[int, bool]
  predicate_parameters: PredicateParameters


@dataclass
class GeneratedPredicateTypes:
  """Class to hold the types of predicates generated for a query."""

  equality: int = 0
  range: int = 0
  in_values: int = 0


@dataclass
class GeneratedQueryFeatures:
  query: str
  template_number: int
  predicate_number: int
  fact_table: str
  total_subgraph_edges: int
  generated_predicate_types: GeneratedPredicateTypes


@dataclass
class BatchGeneratedQueryFeatures(GeneratedQueryFeatures):
  batch_number: int
  prefix: str
