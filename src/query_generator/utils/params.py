import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from cattrs import structure

from query_generator.utils.definitions import Dataset


@dataclass
class SearchParametersEndpoint:
  dataset: Dataset
  dev: bool
  max_hops: list[int]
  extra_predicates: list[int]
  row_retention_probability: list[float]
  unique_joins: bool


@dataclass
class QueryGenerationParameters:
  max_hops: int
  max_queries_per_signature: int
  max_queries_per_fact_table: int
  keep_edge_prob: float
  dataset: Dataset
  extra_predicates: int
  row_retention_probability: float
  # seen_subgraphs: dict[int, bool]


T = TypeVar(
  "T"
)  # define a generic type variable :contentReference[oaicite:0]{index=0}


def read_and_parse_toml(path: Path, cls: type[T]) -> T:
  toml_dict = tomllib.loads(path.read_text())
  return structure(toml_dict, cls)
