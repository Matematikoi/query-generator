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
from query_generator.predicate_generator.histogram import PredicateGenerator
from query_generator.utils.definitions import (
  Dataset,
  Extension,
  GeneratedQueryFeatures,
  QueryGenerationParameters,
)
from query_generator.utils.utils import set_seed


class QueryBuilder:
  def __init__(
    self,
    subgraph_generator: SubGraphGenerator,
    # TODO(Gabriel): http://localhost:8080/tktview/b9400c203a38f3aef46ec250d98563638ba7988b
    tables_schema: Any,
    dataset: Dataset,
  ) -> None:
    self.sub_graph_gen = subgraph_generator
    self.table_to_pypika_table = {
      i: Table(i, alias=tables_schema[i]["alias"]) for i in tables_schema
    }
    self.predicate_gen = PredicateGenerator(dataset)

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
    extra_predicates: int,
    row_retention_probability: float,
  ) -> OracleQuery:
    subgraph_tables = self.get_subgraph_tables(subgraph)
    for predicate in self.predicate_gen.get_random_predicates(
      subgraph_tables,
      extra_predicates,
      row_retention_probability,
    ):
      query = query.where(
        self.table_to_pypika_table[predicate.table][predicate.column]
        >= predicate.min_value,
      ).where(
        self.table_to_pypika_table[predicate.table][predicate.column]
        <= predicate.max_value,
      )
    return query


class QueryGenerator:
  def __init__(self, params: QueryGenerationParameters) -> None:
    set_seed()
    self.params = params
    self.tables_schema, self.fact_tables = get_schema(params.dataset)
    self.foreign_key_graph = ForeignKeyGraph(self.tables_schema)
    self.subgraph_generator = SubGraphGenerator(
      self.foreign_key_graph,
      params.keep_edge_prob,
      params.max_hops,
      params.seen_subgraphs,
    )
    self.query_builder = QueryBuilder(
      self.subgraph_generator,
      self.tables_schema,
      params.dataset,
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
            self.params.extra_predicates,
            self.params.row_retention_probability,
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
