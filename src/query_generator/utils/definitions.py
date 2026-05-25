from dataclasses import dataclass
from enum import Enum, StrEnum
from pathlib import Path

from query_generator.utils.exceptions import (
  InvalidForeignKeyError,
  TableNotFoundError,
)


class Dataset(Enum):
  TPCDS = "TPCDS"
  TPCH = "TPCH"
  JOB = "JOB"


class ValidatorEngine(StrEnum):
  DUCKDB = "duckdb"
  PYSPARK = "pyspark"


class SQLDialect(StrEnum):
  DUCKDB = "duckdb"
  SPARK = "spark"


@dataclass
class PredicateOperatorProbability:
  """Probability of using a specific predicate operator.

  They are based on choice with weights for each operator.
  """

  operator_in: float
  operator_equal: float
  operator_range: float
  operator_like: float
  operator_not_like: float


@dataclass
class ForeignKey:
  column: str
  ref_table: str
  ref_column: str


@dataclass
class TableSchema:
  alias: str | None
  columns: list[str]
  foreign_keys: list[ForeignKey]


@dataclass
class Schema:
  tables: dict[str, TableSchema]
  fact_tables: list[str]

  def __post_init__(self) -> None:
    for ft in self.fact_tables:
      if ft not in self.tables:
        raise TableNotFoundError(ft)
    for table_name, table in self.tables.items():
      for fk in table.foreign_keys:
        if fk.column not in table.columns:
          raise InvalidForeignKeyError(table_name, fk.column)
        if fk.ref_table not in self.tables:
          raise TableNotFoundError(fk.ref_table)
        if fk.ref_column not in self.tables[fk.ref_table].columns:
          raise InvalidForeignKeyError(fk.ref_table, fk.ref_column)


@dataclass
class PredicateParameters:
  histogram_path: Path
  extra_predicates: int
  row_retention_probability: float
  operator_weights: PredicateOperatorProbability
  equality_lower_bound_probability: float
  extra_values_for_in: int
  minimum_like_support_probability: float
  or_probability: float = 0.2
  max_predicate_attempts: int = 10


# TODO(Gabriel): http://localhost:8080/tktview/205e90a1fa
@dataclass
class SyntheticQueryGenerationParameters:
  schema: Schema
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
  like: int = 0
  not_like: int = 0


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
