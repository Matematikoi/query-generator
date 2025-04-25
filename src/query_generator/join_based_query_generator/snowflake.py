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


class QueryBuilder:
  def __init__(
    self,
    tables_schema: Dict[str, Dict[str, Any]],
    dataset: Dataset,
    **kwargs: Any,
  ) -> None:
    self.sub_graph_gen = SubGraphGenerator(tables_schema, **kwargs)
    self.table_to_pypika_table = {
      i: Table(i, alias=tables_schema[i]["alias"]) for i in tables_schema
    }
    self.predicate_gen = PredicateGenerator(dataset)

  def generate_random_sql_queries(
    self,
    fact_table: str,
    extra_predicates: int,
    row_retention_probability: float,
    query_count: int,
  ) -> list[str]:
    """
    Generate a random query for the given fact table.
    Args:
        fact_table (str): Name of the fact table.
    Returns:
        str: Generated SQL query.
    """
    edges = self.sub_graph_gen.get_unseen_random_subgraph(fact_table)

    subgraph_tables = [fact_table] + list(
      set([edge.reference_table.name for edge in edges])
    )

    queries = []
    for _ in range(query_count):
      query = OracleQuery().select(fn.Count("*"))
      for table in subgraph_tables:
        query = query.from_(self.table_to_pypika_table[table])

      for edge in edges:
        query = query.where(
          self.table_to_pypika_table[edge.table.name][edge.column]
          == self.table_to_pypika_table[edge.reference_table.name][
            edge.reference_column
          ]
        )

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
      queries.append(query.get_sql())
    return queries


def generate_and_write_queries(
  schema_function: Callable[[], Tuple[Dict[str, Dict[str, Any]], List[str]]],
  max_hops: int,
  max_queries_per_signature: int,
  dataset: Dataset = Dataset.TPCDS,
) -> None:
  """
  Generate random SQL queries for a given schema and dataset.
  Args:
      schema_function (Callable): Function to get the schema.
      max_hops (int): Maximum number of hops for the subgraph.
      max_queries_per_signature (int): Maximum number of queries per signature.
      dataset (datasetType): The dataset type (TPCH or TPCDS).
  """

  max_signatures_per_fact_table = 100
  tables_schema, fact_tables = schema_function()
  kwargs = {"keep_edge_prob": 0.5, "max_hops": max_hops}
  query_builder = QueryBuilder(tables_schema, dataset, **kwargs)
  cnt = 0
  for fact_table in fact_tables:
    for query_signature_count in range(max_signatures_per_fact_table):
      query_writer = QueryWriter(
        f"data/generated_queries/snowflake/{dataset.value}/{cnt}"
      )
      try:
        for idx, query in enumerate(
          query_builder.generate_random_sql_queries(
            fact_table, 3, 0.5, max_queries_per_signature
          )
        ):
          query_writer.write_query(
            query,
            f"{cnt}-{idx + 1}.sql",  # The script needs to start from 1
          )
        cnt += 1
      except GraphExploredError:
        # The exception is failing to find a new subgraph after 1000 attempts
        print(
          f"{fact_table} made a a total of {query_signature_count} signatures"
        )
        break

      if query_signature_count == max_signatures_per_fact_table - 1:
        print(
          f"{fact_table} made a total of {query_signature_count} signatures"
        )


def run_snowflake_generator(
  dataset: Dataset, max_hops: int, max_queries_per_template: int
) -> None:
  seed = 80
  np.random.seed(seed)
  random.seed(seed)
  if dataset == Dataset.TPCH:
    generate_and_write_queries(
      get_tpch_table_info,
      max_hops,
      max_queries_per_template,
      Dataset.TPCH,
    )
  elif dataset == Dataset.TPCDS:
    generate_and_write_queries(
      get_tpcds_table_info,
      max_hops,
      max_queries_per_template,
      Dataset.TPCDS,
    )
  else:
    raise ValueError(f"Unsupported dataset: {dataset}")
