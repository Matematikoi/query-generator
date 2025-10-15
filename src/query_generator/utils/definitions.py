from dataclasses import dataclass
from enum import Enum
from pathlib import Path


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
  histogram_path: Path
  extra_predicates: int
  row_retention_probability: float
  operator_weights: PredicateOperatorProbability
  equality_lower_bound_probability: float
  extra_values_for_in: int


# TODO(Gabriel): http://localhost:8080/tktview/205e90a1fa
@dataclass
class SyntheticQueryGenerationParameters:
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
  subgraph_signature: int


@dataclass
class BatchGeneratedQueryToWrite:
  batch_number: int
  fact_table: str
  template_number: int
  predicate_number: int
  query: str


@dataclass
class ComplexQueryLLMPrompt:
  """Class to hold the prompt for complex query generation.
  Attributes:
    prompt (str): The prompt text to be used for LLM query generation.
    weight (float): The weight of the prompt, It defines the probability
    of using this prompt in the query generation process.
  """

  prompt: str
  weight: float
