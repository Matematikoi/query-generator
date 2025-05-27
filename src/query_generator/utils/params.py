import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from cattrs import structure

from query_generator.utils.definitions import Dataset


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
class SearchParametersEndpoint:
  dataset: Dataset
  dev: bool
  max_hops: list[int]
  extra_predicates: list[int]
  row_retention_probability: list[float]
  unique_joins: bool
  operator_probabilities: PredicateOperatorProbability
  max_queries_per_fact_table: int
  max_queries_per_signature: int
  keep_edge_prob: float


@dataclass
class QueryGenerationEndpoint:
  max_hops: int
  max_queries_per_signature: int
  max_queries_per_fact_table: int
  keep_edge_prob: float
  dataset: Dataset
  extra_predicates: int
  row_retention_probability: float


T = TypeVar("T")


def read_and_parse_toml(path: Path, cls: type[T]) -> T:
  toml_dict = tomllib.loads(path.read_text())
  return structure(toml_dict, cls)
