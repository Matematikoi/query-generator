import random
from collections.abc import Callable
from typing import Any, Dict, List, Tuple

import numpy as np
from pypika import OracleQuery, Table
from pypika import functions as fn

from query_generator.database_schemas.tpcds import (
  get_tpcds_table_info,
)
from query_generator.database_schemas.tpch import (
  get_tpch_table_info,
)

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
from query_generator.utils.definitions import Dataset
from query_generator.utils.exceptions import GraphExploredError
from query_generator.utils.utils import set_seed
from query_generator.data_structures.foreign_key_graph import ForeignKeyGraph


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
    row_retention_probability,
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


# TODO: This should really be a dataclass
def generate_and_write_queries(
  schema_function: Callable[[], Tuple[Dict[str, Dict[str, Any]], List[str]]],
  max_hops: int,
  max_queries_per_signature: int,
  max_queries_per_fact_table: int,
  keep_edge_prob: float,
  dataset: Dataset,
  extra_predicates: int,
  row_retention_probability: float,
) -> None:
  """
  Generate random SQL queries for a given schema and dataset.
  Args:
      schema_function (Callable): Function to get the schema.
      max_hops (int): Maximum number of hops for the subgraph.
      max_queries_per_signature (int): Maximum number of queries per signature.
      dataset (datasetType): The dataset type (TPCH or TPCDS).
  """
  set_seed()
  tables_schema, fact_tables = schema_function()
  foreign_key_graph = ForeignKeyGraph(tables_schema)
  subgraph_generator = SubGraphGenerator(
    foreign_key_graph, keep_edge_prob, max_hops
  )
  # TODO: This should have their own predicate generator,
  # which should be away from query builder
  query_builder = QueryBuilder(subgraph_generator, tables_schema, dataset)
  for fact_table in fact_tables:
    for cnt, subgraph in enumerate(
      subgraph_generator.generate_subgraph(
        fact_table, max_queries_per_fact_table
      )
    ):
      query = query_builder.generate_query_from_subgraph(subgraph)
      for idx in range(1, max_queries_per_signature):
        query = query_builder.add_predicates(
          subgraph, query, extra_predicates, row_retention_probability
        )

        query_writer = QueryWriter(
          f"data/generated_queries/snowflake/{dataset.value}/{cnt}"
        )
        query_writer.write_query(
          query.get_sql(),
          f"{cnt}-{idx + 1}.sql",  # The script needs to start from 1
        )


# TODO query writer should happen at this level
# TODO query subgraph should also happen at this level
def run_snowflake_generator(
  dataset: Dataset, max_hops: int, max_queries_per_template: int
) -> None:
  if dataset == Dataset.TPCH:
    generate_and_write_queries(
      get_tpch_table_info,
      max_hops,
      max_queries_per_template,
      100,
      0.2,
      Dataset.TPCH,
      3,
      0.2,
    )
  elif dataset == Dataset.TPCDS:
    generate_and_write_queries(
      get_tpcds_table_info,
      max_hops,
      max_queries_per_template,
      100,
      0.2,
      Dataset.TPCDS,
      3,
      0.2,
    )
  else:
    raise ValueError(f"Unsupported dataset: {dataset}")
