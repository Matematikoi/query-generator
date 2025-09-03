import inspect
import re
import tomllib
from dataclasses import MISSING, dataclass, fields
from pathlib import Path
from typing import Any, TypeVar, get_type_hints

from cattrs import structure

from query_generator.utils.definitions import (
  ComplexQueryLLMPrompt,
  Dataset,
  PredicateOperatorProbability,
  PredicateParameters,
)


@dataclass
class ComplexQueryGenerationParametersEndpoint:
  """Basic Docstring"""

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
    dev (bool): Flag indicating whether to use development settings.
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


def no_rewrap(s: str) -> str:
  # Preserve formatting for all paragraphs
  s = s.strip()
  return "\b\n" + s.replace("\n\n", "\n\n\b\n")


def _toml_literal(literal: Any) -> str:
  if isinstance(literal, bool):
    return "true" if literal else "false"
  if isinstance(literal, (int | float)):
    return str(literal)
  # strings / placeholders
  return f'"{literal}"'


def md_hard_breaks(s: str) -> str:
  # add two spaces at EOL for every single newline (but keep blank lines)
  return re.sub(r"(?<!\n)\n(?!\n)", "  \n", s)


def _placeholder(name: str, type: Any) -> str:
  # reasonable TOML placeholders by type
  try:
    if type is int:
      return "0"
    if type is float:
      return "0.0"
    if type is bool:
      return "true"
    if type is str:
      return f'"/path/to/{name}"' if "path" in name else f'"<{name}>"'
  except Exception:
    pass
  return f'"<{name}>"'


def build_help_from_dataclass(cls: Any) -> str:
  doc = md_hard_breaks(inspect.getdoc(cls) or "")
  hints = get_type_hints(cls)
  lines = [doc, "", "TOML keys:"]
  for field in fields(cls):
    type = hints.get(field.name, field.type)
    type_name = getattr(type, "__name__", str(type))
    if field.default is MISSING:
      lines.append(f"- {field.name} ({type_name}, required)")
    else:
      lines.append(f"- {field.name} ({type_name}, default={field.default})")

  # Example TOML block
  ex_lines = []
  for field in fields(cls):
    type = hints.get(field.name, field.type)
    if field.default is MISSING:
      ex_val = _placeholder(field.name, type)
    else:
      ex_val = _toml_literal(field.default)
    ex_lines.append(f"{field.name} = {ex_val}")

  example = "```toml\n# Example\n" + "\n".join(ex_lines) + "\n```"
  lines += ["", "Example config:", example]
  return no_rewrap("\n".join(lines))


T = TypeVar("T")


def read_and_parse_toml(path: Path, cls: type[T]) -> T:
  toml_dict = tomllib.loads(path.read_text())
  return structure(toml_dict, cls)
