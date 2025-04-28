from typing import Any, List

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
  QueryWriter,
)
from query_generator.predicate_generator.histogram import PredicateGenerator
from query_generator.utils.definitions import Dataset, QueryGenerationParameters
from query_generator.utils.utils import set_seed


class QueryBuilder:
  def __init__(
    self,
    subgraph_generator: SubGraphGenerator,
    # TODO: this has to go, and be replaced by [(table, alias)]
    tables_schema: Any,
    dataset: Dataset,
  ) -> None:
    self.sub_graph_gen = subgraph_generator
    self.table_to_pypika_table = {
      i: Table(i, alias=tables_schema[i]["alias"]) for i in tables_schema
    }
    self.predicate_gen = PredicateGenerator(dataset)

  def get_subgraph_tables(
    self, subgraph: List[ForeignKeyGraph.Edge]
  ) -> List[str]:
    return list(
      set(
        [edge.reference_table.name for edge in subgraph]
        + [edge.table.name for edge in subgraph]
      )
    )

  def generate_query_from_subgraph(
    self,
    subgraph: List[ForeignKeyGraph.Edge],
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
        ]
      )
    return query

  def add_predicates(
    self,
    subgraph: List[ForeignKeyGraph.Edge],
    query: OracleQuery,
    extra_predicates: int,
    row_retention_probability: float,
  ) -> OracleQuery:
    subgraph_tables = self.get_subgraph_tables(subgraph)
    for predicate in self.predicate_gen.get_random_predicates(
      subgraph_tables, extra_predicates, row_retention_probability
    ):
      query = query.where(
        self.table_to_pypika_table[predicate.table][predicate.column]
        >= predicate.min_value
      ).where(
        self.table_to_pypika_table[predicate.table][predicate.column]
        <= predicate.max_value
      )
    return query


def generate_and_write_queries(params: QueryGenerationParameters) -> None:
  set_seed()
  tables_schema, fact_tables = get_schema(params.dataset)
  foreign_key_graph = ForeignKeyGraph(tables_schema)
  subgraph_generator = SubGraphGenerator(
    foreign_key_graph, params.keep_edge_prob, params.max_hops
  )
  # TODO: This should have their own predicate generator,
  # which should be away from query builder
  query_builder = QueryBuilder(
    subgraph_generator, tables_schema, params.dataset
  )
  for fact_table in fact_tables:
    for cnt, subgraph in enumerate(
      subgraph_generator.generate_subgraph(
        fact_table, params.max_queries_per_fact_table
      )
    ):
      query = query_builder.generate_query_from_subgraph(subgraph)
      for idx in range(1, params.max_queries_per_signature + 1):
        query = query_builder.add_predicates(
          subgraph,
          query,
          params.extra_predicates,
          params.row_retention_probability,
        )

        query_writer = QueryWriter(
          f"data/generated_queries/snowflake/{params.dataset.value}/{cnt}"
        )
        query_writer.write_query(
          query.get_sql(),
          f"{cnt}-{idx + 1}.sql",  # The script needs to start from 1
        )
