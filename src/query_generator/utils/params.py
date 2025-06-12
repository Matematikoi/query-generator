import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from cattrs import structure

from query_generator.utils.definitions import (
  Dataset,
  PredicateOperatorProbability,
  PredicateParameters,
)


@dataclass
class SearchParametersEndpoint:
  """
  Represents the parameters used for configuring search queries, including
  query builder, subgraph, and predicate options.

  This class is designed to support both the `IN` and `=` statements in
  query generation.

  Attributes:
    dataset (Dataset): The dataset to be queried.
    dev (bool): Flag indicating whether to use development settings.
    max_queries_per_fact_table (int): Maximum number of queries per fact
      table.
    max_queries_per_signature (int): Maximum number of queries per
      signature.
    unique_joins (bool): Whether to enforce unique joins in the subgraph.
    max_hops (list[int]): Maximum number of hops allowed in the subgraph.
    keep_edge_probability (float): Probability of retaining an edge in the
      subgraph.
    extra_predicates (list[int]): Number of additional predicates to include
      in the query.
    row_retention_probability (list[float]): Probability of retaining a row
      for range predicates
    operator_weights (PredicateOperatorProbability): Probability
      distribution for predicate operators.
    equality_lower_bound_probability (float): Lower bound probability when
      using the `=` and the `IN` operators
  """

  # Query Builder
  dataset: Dataset
  dev: bool
  max_queries_per_fact_table: int
  max_queries_per_signature: int
  # Subgraph
  unique_joins: bool
  max_hops: list[int]
  keep_edge_probability: list[float]
  # Predicates
  extra_predicates: list[int]
  row_retention_probability: list[float]
  operator_weights: PredicateOperatorProbability
  equality_lower_bound_probability: list[float]
  extra_values_for_in: int


@dataclass
class SnowflakeEndpoint:
  """
  Represents the parameters used for configuring query generation,
  including query builder, subgraph, and predicate options.

  Attributes:
    dataset (Dataset): The dataset to be used for query generation.
    max_queries_per_signature (int): Maximum number of queries to generate
      per signature.
    max_queries_per_fact_table (int): Maximum number of queries to generate
      per fact table.
    max_hops (int): Maximum number of hops allowed in the subgraph.
    keep_edge_probability (float): Probability of retaining an edge in the
      subgraph.
    extra_predicates (int): Number of extra predicates to add to the query.
    row_retention_probability (float): Probability of retaining a row after
      applying predicates.
    operator_weights (PredicateOperatorProbability): Probability
      distribution for predicate operators.
    equality_lower_bound_probability (float): Probability of using a lower
      bound for equality predicates.
  """

  # Query builder
  dataset: Dataset
  max_queries_per_signature: int
  max_queries_per_fact_table: int
  # Subgraph
  max_hops: int
  keep_edge_probability: float
  # Predicates
  predicate_parameters: PredicateParameters


T = TypeVar("T")


def read_and_parse_toml(path: Path, cls: type[T]) -> T:
  toml_dict = tomllib.loads(path.read_text())
  return structure(toml_dict, cls)
