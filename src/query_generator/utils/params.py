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
class LLMParams:
  """Params used for the LLM endpoint"""

  llm_base_prompt: str
  llm_model: str
  total_queries: int
  retry: int
  llm_prompts: dict[str, ComplexQueryLLMPrompt]
  statistics_parquet: str | None = None


@dataclass
class UnionParams:
  """Params used for the union extension
  Attributes:
  - max_queries (int): The maximum number of queries to union. Default is 5.
  - probability (float): The probability of using UNION instead of UNION ALL.
      Default is 0.5.
  """

  max_queries: int = 5
  probability: float = 0.5


@dataclass
class ExtensionAndLLMEndpoint:
  __doc__ = f"""Uses LLM to generate complex queries based on synthetic queries.

  Attributes:
  - database_path (str): The path to the DuckDB database file.
  - seed (int): The seed for random number generation to ensure reproducibility.
    by default, it is set to 42.
  - queries_parquet (str): The path to the parquet file containing synthetic
      queries.
  - llm_extension (bool): Whether to use the LLM extension.
  - union_extension (bool): Whether to use the union extension.
  - destination_folder (str): The folder to save the generated complex queries.
  - union_max_queries (int): The maximum number of queries to union if the
      union extension is used. Default is 5.
  - llm_params (LLMParams | None): The parameters for the LLM. see below for
      details.

  
  Attributes Union params:
  - max_queries (int): The maximum number of queries to union. Default is 5.
  - probability (float): The probability of using UNION instead of UNION ALL.
      Default is 0.5.

  Attributes llm params:
  - llm_base_prompt (str): The base prompt to use for the LLM.
  - llm_model (str): The model to use for the LLM from ollama.
  - total_queries (int): The total number of queries to generate with LLM.
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
  {TOML_EXAMPLE["extension_and_llm"]}
  ```
  """
  database_path: str
  queries_parquet: str
  llm_extension: bool
  union_extension: bool
  destination_folder: str
  llm_params: LLMParams | None = None
  union_params: UnionParams | None = None


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
  - scale_factor (int | float): The scale factor for the dataset.
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


@dataclass
class HistogramEndpoint:
  __doc__ = f"""Parameters for generating histograms from a database

  Attributes:
  - output_folder (str): The folder to save the generated histogram parquet
      file.
  - database_path (str): The path to the DuckDB database to use for generating
      histograms.
  - histogram_size (int): The number of bins to use for the histogram.
      Default is 51.
  - common_values_size (int): The number of common values to include in the
      histogram. Default is 10.
  - include_mvc (bool): Whether to include most common values (MCV) in the
      histogram. Default is False.
  Examples of toml files can be found in:
  `params_config/histogram/*toml`
  Example:
  ```toml
  {TOML_EXAMPLE["histogram"]}
  ```
  """
  output_folder: str
  database_path: str
  histogram_size: int = 51
  common_values_size: int = 10
  include_mcv: bool = False

@dataclass
class FixTransformEndpoint:
  __doc__ = f"""Adds LIMIT to sql queries according to output size.

  Attributes:
  - traces_parquet (str): The path to the parquet file containing the traces
      with the output sizes of the queries.
  - queries_folder (str): The folder containing the sql queries to
      which the LIMIT will be added.
  - destination_folder (str): The folder to save the formatted queries.
  - max_output_size (int): The maximum output size for the queries. Queries
      with an output size greater than this value will have a LIMIT added.
  Examples of toml files can be found in:
  `params_config/add_limit/*toml`
  Example:
  ```toml
  {TOML_EXAMPLE["fix_transform"]}
  ```
  """
  traces_parquet: str
  queries_folder: str
  destination_folder: str
  max_output_size: int

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
