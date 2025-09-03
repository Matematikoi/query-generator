import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from cattrs import structure

from query_generator.utils.definitions import (
  ComplexQueryLLMPrompt,
  Dataset,
  PredicateOperatorProbability,
  PredicateParameters,
)


@dataclass
class ComplexQueryGenerationParametersEndpoint:
  """Uses an LLM to generate complex queries based on synthetic queries.

  Attributes:
  - llm_base_prompt (str): The base prompt to use for the LLM.
  - llm_model (str): The model to use for the LLM from ollama.
  - queries_path (str): The path to the synthetic queries files.
  - total_queries (int): The total number of queries to generate.
  - seed (int): The seed to use for random operations.
  - dataset (Dataset): The dataset to use for query generation.
  - destination_folder (str): The folder to save the generated queries.
  - retry (int): The number of times to retry generating a query if it fails.
  - llm_prompts (dict[str, ComplexQueryLLMPrompt]): A dictionary of
      additional prompts to use for the LLM.
      This dictionary maps any operations e.g. `group_by` to a prompt
      configuration. You need to specify two attributes for each prompt,
      a `prompt` that will be used and a `weight` that defines the
      probability of using that prompt.
      The weights do not need to sum up to 1, they will be normalized
      automatically.
      The higher the weight, the more likely the prompt will be used.

  examples of toml files can be found in:
  `params_config/complex_queries/*toml`
  """

  llm_base_prompt: str
  llm_model: str
  queries_path: str
  total_queries: int
  seed: int
  dataset: Dataset
  destination_folder: str
  retry: int
  llm_prompts: dict[str, ComplexQueryLLMPrompt]


@dataclass
class SearchParametersEndpoint:
  """
  Represents the parameters used for configuring search queries, including
  query builder, subgraph, and predicate options.

  This class is designed to support both the `IN` and `=` statements in
  query generation.

  Attributes:
  - dataset (Dataset): The dataset to be queried.
  - dev (bool): Flag indicating whether to use development settings.
  - max_queries_per_fact_table (int): Maximum number of queries per fact
      table.
  - max_queries_per_signature (int): Maximum number of queries per
      signature.
  - unique_joins (bool): Whether to enforce unique joins in the subgraph.
  - max_hops (list[int]): Maximum number of hops allowed in the subgraph.
  - keep_edge_probability (float): Probability of retaining an edge in the
      subgraph.
  - extra_predicates (list[int]): Number of additional predicates to include
      in the query.
  - row_retention_probability (list[float]): Probability of retaining a row
      for range predicates
  - operator_weights (PredicateOperatorProbability): Probability
      distribution for predicate operators.
  - equality_lower_bound_probability (float): Lower bound probability when
      using the `=` and the `IN` operators

  Examples of toml files can be found in:
  `params_config/search_params/*toml`
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
  - dataset (Dataset): The dataset to be used for query generation.
    The currently supported datasets are TPC-H, TPC-DS, and JOB.
  - max_queries_per_signature (int): Maximum number of queries to generate
      per signature.
  - max_queries_per_fact_table (int): Maximum number of queries to generate
      per fact table.
  - max_hops (int): Maximum number of hops allowed in the subgraph.
  - keep_edge_probability (float): Probability of retaining an edge in the
      subgraph.
  - extra_predicates (int): Number of extra predicates to add to the query.
  - row_retention_probability (float): Probability of retaining a row after
      applying predicates.
  - operator_weights (PredicateOperatorProbability): Probability
      distribution for predicate operators.
  - equality_lower_bound_probability (float): Probability of using a lower
      bound for equality predicates.

  Examples of toml files can be found in:
  `params_config/snowflake/*toml`
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


@dataclass
class CherryPickBase:
  queries_per_bin: int
  upper_bound: int
  total_bins: int
  seed: int = 42


T = TypeVar("T")


def read_and_parse_toml(path: Path, cls: type[T]) -> T:
  toml_dict = tomllib.loads(path.read_text())
  return structure(toml_dict, cls)
