import random
from collections.abc import Iterator
from typing import Any

from pypika import OracleQuery, Table
from pypika import functions as fn
from pypika.queries import QueryBuilder
from pypika.terms import Criterion

from query_generator.database_schemas.schemas import get_schema

# fmt: off
from query_generator.synthetic_queries.\
  utils.subgraph_generator import (
  SubGraphGenerator,
)

# fmt: on
from query_generator.synthetic_queries.foreign_key_graph import ForeignKeyGraph
from query_generator.synthetic_queries.predicate_generator import (
  HistogramDataType,
  PredicateEquality,
  PredicateGenerator,
  PredicateIn,
  PredicateLike,
  PredicateNotLike,
  PredicateRange,
  SupportedHistogramType,
)
from query_generator.utils.definitions import (
  GeneratedPredicateTypes,
  GeneratedQueryFeatures,
  PredicateParameters,
  SyntheticQueryGenerationParameters,
)
from query_generator.utils.utils import set_seed


def _build_predicate_tree(
  criteria: list[Criterion], or_probability: float
) -> Criterion | None:
  """Combine a list of criteria into a random binary tree of AND/OR nodes.

  Args:
    criteria: Leaf criteria to combine.
    or_probability: Probability [0.0, 1.0] that each merge uses OR
      instead of AND.

  Returns:
    A single combined Criterion, or None if criteria is empty.
  """
  if not criteria:
    return None
  remaining = list(criteria)
  while len(remaining) > 1:
    i, j = random.sample(range(len(remaining)), 2)
    a, b = remaining[i], remaining[j]
    combined = (a | b) if random.random() < or_probability else (a & b)
    # Remove higher index first to avoid shifting
    for idx in sorted([i, j], reverse=True):
      remaining.pop(idx)
    remaining.append(combined)
  return remaining[0]


class QueryBuilderPypika:
  def __init__(
    self,
    subgraph_generator: SubGraphGenerator,
    # TODO(Gabriel): http://localhost:8080/tktview/b9400c203a38f3aef46ec250d98563638ba7988b
    tables_schema: Any,
    predicate_params: PredicateParameters,
  ) -> None:
    self.sub_graph_gen = subgraph_generator
    self.table_to_pypika_table = {
      i: Table(i, alias=tables_schema[i]["alias"]) for i in tables_schema
    }
    self.predicate_gen = PredicateGenerator(predicate_params)
    self.tables_schema = tables_schema

  def get_subgraph_tables(
    self,
    subgraph: list[ForeignKeyGraph.Edge],
  ) -> list[str]:
    return list(
      set(
        [edge.reference_table.name for edge in subgraph]
        + [edge.table.name for edge in subgraph],
      ),
    )

  def generate_query_from_subgraph(
    self,
    subgraph: list[ForeignKeyGraph.Edge],
  ):
    subgraph_tables = self.get_subgraph_tables(subgraph)
    query = OracleQuery().select(fn.Count("*"))
    for table in subgraph_tables:
      query = query.from_(self.table_to_pypika_table[table])
      random_column = random.choice(
        list(self.tables_schema[table]["columns"].keys())
      )
      query = query.select(
        fn.Count(self.table_to_pypika_table[table][random_column])
      )

    for edge in subgraph:
      query = query.where(
        self.table_to_pypika_table[edge.table.name][edge.column]
        == self.table_to_pypika_table[edge.reference_table.name][
          edge.reference_column
        ],
      )
    return query

  def add_predicates(
    self,
    subgraph: list[ForeignKeyGraph.Edge],
    query: QueryBuilder,
  ) -> tuple[QueryBuilder, GeneratedPredicateTypes]:
    subgraph_tables = self.get_subgraph_tables(subgraph)
    predicate_types = GeneratedPredicateTypes()
    criteria: list[Criterion] = []
    for predicate in self.predicate_gen.get_random_predicates(
      subgraph_tables,
    ):
      if isinstance(predicate, PredicateRange):
        criteria.append(self._build_criterion_range(predicate))
        predicate_types.range += 1
      if isinstance(predicate, PredicateEquality):
        criteria.append(self._build_criterion_equality(predicate))
        predicate_types.equality += 1
      if isinstance(predicate, PredicateIn):
        criteria.append(self._build_criterion_in(predicate))
        predicate_types.in_values += 1
      if isinstance(predicate, PredicateLike):
        criteria.append(self._build_criterion_like(predicate))
        predicate_types.like += 1
      if isinstance(predicate, PredicateNotLike):
        criteria.append(self._build_criterion_not_like(predicate))
        predicate_types.not_like += 1
    tree = _build_predicate_tree(
      criteria, self.predicate_gen.predicate_params.or_probability
    )
    if tree is not None:
      query = query.where(tree)  # type: ignore
    return query, predicate_types

  def _cast_if_needed(
    self, value: SupportedHistogramType, dtype: HistogramDataType
  ) -> Any:
    """Cast the value to the appropriate type if needed."""
    if dtype == HistogramDataType.DATE:
      return fn.Cast(value, "date")
    return value

  def _build_criterion_range(self, predicate: PredicateRange) -> Criterion:
    """Build a range criterion: col >= min AND col <= max."""
    return (  # type: ignore
      self.table_to_pypika_table[predicate.table][predicate.column]
      >= self._cast_if_needed(predicate.min_value, predicate.dtype)
    ) & (
      self.table_to_pypika_table[predicate.table][predicate.column]
      <= self._cast_if_needed(predicate.max_value, predicate.dtype)
    )

  def _build_criterion_equality(
    self, predicate: PredicateEquality
  ) -> Criterion:
    """Build an equality criterion: col = value."""
    return (  # type: ignore
      self.table_to_pypika_table[predicate.table][predicate.column]
      == predicate.equality_value
    )

  def _build_criterion_in(self, predicate: PredicateIn) -> Criterion:
    """Build an IN criterion: col IN (v1, v2, ...)."""
    return self.table_to_pypika_table[predicate.table][predicate.column].isin(  # type: ignore
      [self._cast_if_needed(i, predicate.dtype) for i in predicate.in_values]
    )

  def _build_criterion_like(self, predicate: PredicateLike) -> Criterion:
    """Build a LIKE criterion: col LIKE pattern."""
    return self.table_to_pypika_table[predicate.table][predicate.column].like(  # type: ignore
      predicate.pattern
    )

  def _build_criterion_not_like(self, predicate: PredicateNotLike) -> Criterion:
    """Build a NOT LIKE criterion: col NOT LIKE pattern."""
    return self.table_to_pypika_table[predicate.table][
      predicate.column
    ].not_like(  # type: ignore
      predicate.pattern
    )


class QueryGenerator:
  def __init__(self, params: SyntheticQueryGenerationParameters) -> None:
    set_seed()
    self.params = params
    self.tables_schema, self.fact_tables = get_schema(params.dataset)
    self.foreign_key_graph = ForeignKeyGraph(self.tables_schema)
    self.subgraph_generator = SubGraphGenerator(
      self.foreign_key_graph,
      params.keep_edge_probability,
      params.max_hops,
      params.seen_subgraphs,
    )
    self.query_builder = QueryBuilderPypika(
      self.subgraph_generator,
      self.tables_schema,
      params.predicate_parameters,
    )

  def generate_queries(self) -> Iterator[GeneratedQueryFeatures]:
    for fact_table in self.fact_tables:
      for cnt, subgraph in enumerate(
        self.subgraph_generator.generate_subgraph(
          fact_table,
          self.params.max_queries_per_fact_table,
        ),
      ):
        for idx in range(1, self.params.max_queries_per_signature + 1):
          query = self.query_builder.generate_query_from_subgraph(subgraph)
          query, predicate_types = self.query_builder.add_predicates(
            subgraph,
            query,
          )

          yield GeneratedQueryFeatures(
            query=query.get_sql(),  # type: ignore
            template_number=cnt,
            predicate_number=idx,
            fact_table=fact_table,
            total_subgraph_edges=len(subgraph),
            generated_predicate_types=predicate_types,
            subgraph_signature=self.foreign_key_graph.get_subgraph_signature(
              subgraph
            ),
          )
