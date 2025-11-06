import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

import cattrs
import toml
from attrs import define, field
from cattrs import structure

from query_generator.utils.definitions import (
  ComplexQueryLLMPrompt,
  Dataset,
  PredicateOperatorProbability,
)
from query_generator.utils.toml_examples import TOML_EXAMPLE, EndpointName


def get_markdown_documentation(name: EndpointName) -> str:
  """Returns the markdown documentation for the given endpoint name."""

  return f"""



  {
    (
      Path(__file__).parent.parent.parent.parent
      / "docs"
      / "endpoints"
      / f"{name}.md"
    ).read_text()
  }
  
  You can find example toml files in `./params_config/{name}/*.toml`
  """


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


@define
class LLMPrompts:
  """Base class for the prompts used in an LLM"""

  base_prompt: str
  weighted_prompts: dict[str, ComplexQueryLLMPrompt]


@define
class LLMParams:
  """Params used for the LLM endpoint"""

  database_path: str
  total_queries: int
  retry: int
  prompts_path: Path = field(converter=Path)
  schema_path: Path = field(converter=Path)
  prompts: LLMPrompts = field(init=False)
  statistics_parquet: str | None = None

  @prompts.default  # type: ignore
  def _make_llm_prompts(self) -> LLMPrompts:
    raw_prompts = read_and_parse_toml(self.prompts_path, LLMPrompts)
    raw_prompts.base_prompt = raw_prompts.base_prompt.format(
      schema = self.schema_path.read_text())
    return raw_prompts


@dataclass
class ExtensionAndOllamaEndpoint:
  __doc__ = f"""Makes complex queries from synthetic ones, mainly using ollama.
{get_markdown_documentation(EndpointName.EXTENSIONS_WITH_OLLAMA)}

# Example

```toml
{TOML_EXAMPLE[EndpointName.EXTENSIONS_WITH_OLLAMA]}
```
## Example prompts.toml

```toml
{TOML_EXAMPLE[EndpointName.PROMPTS]}
```

"""
  queries_parquet: str
  llm_extension: bool
  union_extension: bool
  destination_folder: str
  ollama_model: str | None = None
  llm_params: LLMParams | None = None
  union_params: UnionParams | None = None


@dataclass
class SyntheticQueriesEndpoint:
  __doc__ = f"""Generates Synthetic queries based on column statistics.
{get_markdown_documentation(EndpointName.SYNTHETIC_GENERATION)}

# Example

```toml
{TOML_EXAMPLE[EndpointName.SYNTHETIC_GENERATION]}
```
  """

  # Subgraph
  dataset: Dataset
  unique_joins: bool
  max_hops: list[int]
  keep_edge_probability: list[float]
  # Query Builder
  max_signatures_per_fact_table: int
  max_queries_per_signature: int
  # Predicates
  extra_predicates: list[int]
  row_retention_probability: list[float]
  operator_weights: PredicateOperatorProbability
  equality_lower_bound_probability: list[float]
  extra_values_for_in: int
  # Paths
  duckdb_database: str
  output_folder: str
  histogram_path: str


@dataclass
class GenerateDBEndpoint:
  __doc__ = f"""Generate a DuckDB database with TPCDS or TPCH datasets.
{get_markdown_documentation(EndpointName.GENERATE_DB)}

# Example

```toml
{TOML_EXAMPLE[EndpointName.GENERATE_DB]}
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
{get_markdown_documentation(EndpointName.FILTER)}

# Example

```toml
{TOML_EXAMPLE[EndpointName.FILTER]}
```
  """
  input_parquet: str
  destination_folder: str
  filter_null: bool
  cherry_pick: bool
  cherry_pick_config: CherryPickBase | None = None


@dataclass
class HistogramEndpoint:
  __doc__ = f"""Generates column statistics from a database.
{get_markdown_documentation(EndpointName.HISTOGRAM)}

# Example

```toml
{TOML_EXAMPLE[EndpointName.HISTOGRAM]}
```
  """
  output_folder: str
  database_path: str
  histogram_size: int = 51
  common_values_size: int = 10
  redundant_histogram_size = 0


@dataclass
class FixTransformEndpoint:
  __doc__ = f"""Adds LIMIT to sql queries according to output size.
{get_markdown_documentation(EndpointName.FIX_TRANSFORM)}

# Example

```toml
{TOML_EXAMPLE[EndpointName.FIX_TRANSFORM]}
```
  """
  queries_folder: str
  destination_folder: str
  max_output_size: int
  duckdb_database: str
  timeout_seconds: float


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
