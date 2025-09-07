import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

import cattrs
import toml
from cattrs import structure

from query_generator.utils.definitions import (
  ComplexQueryLLMPrompt,
  Dataset,
  PredicateOperatorProbability,
)
from query_generator.utils.toml_examples import TOML_EXAMPLE


@dataclass
class LLMExtensionEndpoint:
  __doc__ = f"""Uses LLM to generate complex queries based on synthetic queries.

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

  Example:
  ```toml
  {TOML_EXAMPLE["llm_augmentation"]}
  ```
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
class SyntheticQueriesEndpoint:
  __doc__ = f"""
  Represents the parameters used for configuring search queries, including
  query builder, subgraph, and predicate options.

  This class is designed to support both the `IN` and `=` statements in
  query generation.

  Attributes:
  - duckdb_database (str): The path to the DuckDB database file.
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

  Example:
  ```toml
  {TOML_EXAMPLE["synthetic_generation"]}
  ```
  """

  # Query Builder
  max_queries_per_fact_table: int
  max_queries_per_signature: int
  # Subgraph
  dataset: Dataset
  unique_joins: bool
  max_hops: list[int]
  keep_edge_probability: list[float]
  # Predicates
  extra_predicates: list[int]
  row_retention_probability: list[float]
  operator_weights: PredicateOperatorProbability
  equality_lower_bound_probability: list[float]
  extra_values_for_in: int
  # Paths
  duckdb_database: str
  output_folder: str


@dataclass
class GenerateDBEndpoint:
  __doc__ = f"""
  Parameters for generating a DuckDB database with TPCDS or TPCH datasets.

  Attributes:
  - dataset (Dataset): The dataset to be used (TPCDS, TPCH).
  - scale_factor (int | float | None): The scale factor for the dataset.
      It is only None for JOB dataset.
  - db_path (str): The path where the DuckDB database will be stored.
  Examples of toml files can be found in:
  `params_config/generate_db/*toml`

  Example:
  ```toml
  {TOML_EXAMPLE["generate_db"]}
  ```
  """

  # Query builder
  dataset: Dataset
  db_path: str
  scale_factor: float | None = None


@dataclass
class CherryPickBase:
  queries_per_bin: int
  upper_bound: int
  total_bins: int
  seed: int = 42


@dataclass
class FilterEndpoint:
  __doc__ = f"""Filter synthetic queries based on various criteria.

  Two filtering methods are available:
  1. Null Filter: Removes queries with null values in the `count_star`
      column.
  2. Cherry-Pick Filter: Divides queries into bins based on the `count_star`
      values and randomly selects a specified number of queries from each bin.
  Attributes:
  - filter_null (bool): Whether to filter out null values from the results.
  - cherry_pick (bool): Whether to cherry-pick queries based on specific
      criteria.
  - cherry_pick_config (CherryPickBase): Configuration for cherry-picking
      queries. This is required if `cherry_pick` is set to True.
  Examples of toml files can be found in:
  `params_config/filter/*toml`

  Example:
  ```toml
  {TOML_EXAMPLE["filter"]}
  ```
  """
  input_parquet: str
  destination_folder: str
  filter_null: bool
  cherry_pick: bool
  cherry_pick_config: CherryPickBase | None = None


T = TypeVar("T")


def read_and_parse_toml(path: Path, cls: type[T]) -> T:
  toml_dict = tomllib.loads(path.read_text())
  return structure(toml_dict, cls)


def get_toml_from_params(
  params: Any,
) -> str:
  converter = cattrs.Converter()
  params_dict = converter.unstructure(params)
  return toml.dumps(params_dict)
