import random
from collections.abc import Iterator
from typing import Any

from pypika import OracleQuery, Table
from pypika import functions as fn

from query_generator.data_structures.foreign_key_graph import ForeignKeyGraph
from query_generator.database_schemas.schemas import get_schema

# fmt: off
from query_generator.join_based_query_generator.\
  utils.subgraph_generator import (
  SubGraphGenerator,
)

# fmt: on
from query_generator.join_based_query_generator.utils.query_writer import (
  Writer,
)
from query_generator.predicate_generator.predicate_generator import (
  HistogramDataType,
  PredicateEquality,
  PredicateGenerator,
  PredicateIn,
  PredicateRange,
  SupportedHistogramType,
)
from query_generator.utils.definitions import (
  Dataset,
  Extension,
  GeneratedQueryFeatures,
  PredicateParameters,
  QueryGenerationParameters,
)
from query_generator.utils.exceptions import InvalidHistogramTypeError
from query_generator.utils.utils import set_seed


class QueryBuilder:
  def __init__(
    self,
    subgraph_generator: SubGraphGenerator,
    # TODO(Gabriel): http://localhost:8080/tktview/b9400c203a38f3aef46ec250d98563638ba7988b
    tables_schema: Any,
    dataset: Dataset,
    predicate_params: PredicateParameters,
  ) -> None:
    self.sub_graph_gen = subgraph_generator
    self.table_to_pypika_table = {
      i: Table(i, alias=tables_schema[i]["alias"]) for i in tables_schema
    }
    self.predicate_gen = PredicateGenerator(dataset, predicate_params)
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
  ) -> OracleQuery:
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
    query: OracleQuery,
  ) -> OracleQuery:
    subgraph_tables = self.get_subgraph_tables(subgraph)
    for predicate in self.predicate_gen.get_random_predicates(
      subgraph_tables,
    ):
      if isinstance(predicate, PredicateRange):
        return self._add_range(query, predicate)
      if isinstance(predicate, PredicateEquality):
        return self._add_equality(query, predicate)
      if isinstance(predicate, PredicateIn):
        return self._add_in(query, predicate)
      raise InvalidHistogramTypeError(str(predicate.dtype))
    return query

  def _cast_if_needed(
    self, value: SupportedHistogramType, dtype: HistogramDataType
  ) -> Any:
    """Cast the value to the appropriate type if needed."""
    if dtype == HistogramDataType.DATE:
      return fn.Cast(value, "date")
    return value

  def _add_range(
    self, query: OracleQuery, predicate: PredicateRange
  ) -> OracleQuery:
    return query.where(
      self.table_to_pypika_table[predicate.table][predicate.column]
      >= self._cast_if_needed(predicate.min_value, predicate.dtype),
    ).where(
      self.table_to_pypika_table[predicate.table][predicate.column]
      <= self._cast_if_needed(predicate.max_value, predicate.dtype)
    )

  def _add_equality(
    self, query: OracleQuery, predicate: PredicateEquality
  ) -> OracleQuery:
    return query.where(
      self.table_to_pypika_table[predicate.table][predicate.column]
      == predicate.equality_value
    )

  def _add_in(self, query: OracleQuery, predicate: PredicateIn) -> OracleQuery:
    return query.where(
      self.table_to_pypika_table[predicate.table][predicate.column].isin(
        [self._cast_if_needed(i, predicate.dtype) for i in predicate.in_values]
      )
    )


class QueryGenerator:
  def __init__(self, params: QueryGenerationParameters) -> None:
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
    self.query_builder = QueryBuilder(
      self.subgraph_generator,
      self.tables_schema,
      params.dataset,
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
        query = self.query_builder.generate_query_from_subgraph(subgraph)
        for idx in range(1, self.params.max_queries_per_signature + 1):
          query = self.query_builder.add_predicates(
            subgraph,
            query,
          )

          yield GeneratedQueryFeatures(
            query=query.get_sql(),
            template_number=cnt,
            predicate_number=idx,
            fact_table=fact_table,
          )


def generate_and_write_queries(params: QueryGenerationParameters) -> None:
  """Generate and write queries to a file.

  Args:
      params (QueryGenerationParameters): Query generation parameters.

  """
  query_writer = Writer(
    params.dataset,
    Extension.SNOWFLAKE,
  )
  query_generator = QueryGenerator(params)
  for query in query_generator.generate_queries():
    query_writer.write_query(query)
