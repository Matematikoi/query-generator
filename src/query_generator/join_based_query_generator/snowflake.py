import os
import random
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Tuple

import numpy as np
import pandas as pd
from pypika import OracleQuery, Table
from pypika import functions as fn

from query_generator.data_structures.foreign_key_graph import ForeignKeyGraph
from query_generator.database_schemas.tpcds import (
  get_tpcds_table_info,
)
from query_generator.database_schemas.tpch import (
  get_tpch_table_info,
)
from query_generator.utils.definitions import BenchmarkType


class GraphExploredError(Exception):
  pass


class SubGraphGenerator:
  def __init__(
    self,
    tables_schema: Dict[str, Dict[str, Any]],
    keep_edge_prob: float = 0.5,
    max_hops: int = 2,
  ) -> None:
    self.hops = max_hops
    self.keep_edge_prob = keep_edge_prob
    self.graph = ForeignKeyGraph(tables_schema)
    self.seen_subgraphs: Dict[int, bool] = {}

  def get_random_subgraph(self, fact_table: str) -> List[ForeignKeyGraph.Edge]:
    """
    Starting from the fact table, for each edge of the current table we
    decide based on the keep_edge_probability whether to keep the edge or not.

    We repeat this process up until the maximum number of hops.
    """

    @dataclass
    class JoinDepthNode:
      table: str
      depth: int

    queue: deque[JoinDepthNode] = deque()
    queue.append(JoinDepthNode(fact_table, 0))
    edges_subgraph = []

    while queue:
      current_node = queue.popleft()
      if current_node.depth >= self.hops:
        continue

      current_edges = self.graph.get_edges(current_node.table)
      for current_edge in current_edges:
        if random.random() < self.keep_edge_prob:
          edges_subgraph.append(current_edge)
          queue.append(
            JoinDepthNode(
              current_edge.reference_table.name, current_node.depth + 1
            )
          )

    return edges_subgraph

  def get_unseen_random_subgraph(
    self, fact_table: str
  ) -> list[ForeignKeyGraph.Edge]:
    """
    Generate a random subgraph starting from the fact table.
    Args:
        fact_table (str): Name of the fact table.
    Returns:
        List[ForeignKeyGraph.Edge]: List of edges in the generated subgraph.
    """
    cnt = 0
    while True:
      cnt += 1
      if cnt > 1000:
        raise GraphExploredError(
          "Unable to find a new subgraph after 1000 attempts."
        )
      edges = self.get_random_subgraph(fact_table)
      edges_signature = self.graph.get_subgraph_signature(edges)
      if len(edges) == 0:
        continue  # no edges found, retry
      if edges_signature not in self.seen_subgraphs:
        self.seen_subgraphs[edges_signature] = True
        return edges


class PredicateGenerator:
  @dataclass
  class Predicate:
    table: str
    column: str
    min_value: float | int
    max_value: float | int

  def __init__(self, benchmark: BenchmarkType):
    self.histogram: pd.DataFrame = self.read_histogram(benchmark)

  def read_histogram(self, benchmark: BenchmarkType) -> pd.DataFrame:
    """
    Read the histogram data for the specified benchmark.
    Args:
        benchmark (BenchmarkType): The benchmark type (TPCH or TPCDS).
    Returns:
        pd.DataFrame: DataFrame containing the histogram data.
    """
    current_file_path = os.path.abspath(__file__)
    parent_dir = current_file_path
    for _ in range(4):
      parent_dir = os.path.dirname(parent_dir)

    if benchmark == BenchmarkType.TPCH:
      df = pd.read_csv(
        os.path.join(
          parent_dir, "playground/assets/data_stats/raw_tpch_hist.csv"
        )
      )
    elif benchmark == BenchmarkType.TPCDS:
      df = pd.read_csv(
        os.path.join(
          parent_dir, "playground/assets/data_stats/raw_tpcds_hist.csv"
        )
      )
    else:
      raise ValueError(f"Unsupported benchmark histogram: {benchmark}")
    # Remove rows with empty bins or that are dates
    df = df[(df["bins"] != "[]") & (df["dtype"] != "date")]
    return df

  def get_random_predicates(
    self,
    tables: List[str],
    num_predicates: int,
    row_retention_probability: float = 0.2,
  ) -> Iterator["PredicateGenerator.Predicate"]:
    """
    Generate random predicates based on the histogram data.
    Args:
        tables (str): List of tables to select predicates from.
        num_predicates (int): Number of predicates to generate.
        row_retention_probability (float): Probability of retaining rows.
    Returns:
        List[PredicateGenerator.Predicate]: List of generated predicates.
    """
    selected_tables_histogram = self.histogram[
      self.histogram["table"].isin(tables)
    ]

    for _, row in selected_tables_histogram.sample(num_predicates).iterrows():
      table = row["table"]
      column = row["column"]
      bins = row["bins"]
      min_value, max_value = self._get_min_max_from_bins(
        bins, row_retention_probability
      )
      predicate = PredicateGenerator.Predicate(
        table=table, column=column, min_value=min_value, max_value=max_value
      )
      yield predicate

  def _get_min_max_from_bins(
    self, bins: str, row_retention_probability: float
  ) -> Tuple[float | int, float | int]:
    """
    Convert the bins string representation to a tuple of min and max values.
    Args:
        bins (str): String representation of bins.
        row_retention_probability (float): Probability of retaining rows.
    Returns:
        tuple: Tuple containing min and max values.
    """
    number_array: List[int | float] = eval(bins)
    subrange_length = max(
      1, round(row_retention_probability / 100 * len(number_array))
    )
    start_index = random.randint(0, len(number_array) - subrange_length)

    min_value = number_array[start_index]
    max_value = number_array[start_index + subrange_length - 1]
    return min_value, max_value


class QueryBuilder:
  def __init__(
    self,
    tables_schema: Dict[str, Dict[str, Any]],
    benchmark: BenchmarkType,
    **kwargs: Any,
  ) -> None:
    self.sub_graph_gen = SubGraphGenerator(tables_schema, **kwargs)
    self.table_to_pypika_table = {
      i: Table(i, alias=tables_schema[i]["alias"]) for i in tables_schema
    }
    self.predicate_gen = PredicateGenerator(benchmark)

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


class QueryWriter:
  def __init__(self, output_dir: str) -> None:
    self.output_dir = output_dir
    if not os.path.exists(self.output_dir):
      os.makedirs(self.output_dir)

  def write_query(self, query: str, file_name: str) -> None:
    """
    Write the generated queries to a file.
    Args:
        queries (List[str]): List of SQL queries.
        file_name (str): Name of the output file.
    """
    with open(os.path.join(self.output_dir, file_name), "w") as f:
      f.write(query)


def generate_and_write_queries(
  schema_function: Callable[[], Tuple[Dict[str, Dict[str, Any]], List[str]]],
  max_hops: int,
  max_queries_per_signature: int,
  benchmark: BenchmarkType = BenchmarkType.TPCDS,
) -> None:
  """
  Generate random SQL queries for a given schema and benchmark.
  Args:
      schema_function (Callable): Function to get the schema.
      max_hops (int): Maximum number of hops for the subgraph.
      max_queries_per_signature (int): Maximum number of queries per signature.
      benchmark (BenchmarkType): The benchmark type (TPCH or TPCDS).
  """

  max_signatures_per_fact_table = 100
  tables_schema, fact_tables = schema_function()
  kwargs = {"keep_edge_prob": 0.5, "max_hops": max_hops}
  query_builder = QueryBuilder(tables_schema, benchmark, **kwargs)
  cnt = 0
  for fact_table in fact_tables:
    for query_signature_count in range(max_signatures_per_fact_table):
      query_writer = QueryWriter(
        os.path.join(
          os.path.dirname(os.path.abspath(__file__)),
          f"generated_queries/{benchmark.value}/{cnt}",
        )
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


def main() -> None:
  seed = 80
  np.random.seed(seed)
  random.seed(seed)
  generate_and_write_queries(get_tpch_table_info, 4, 5, BenchmarkType.TPCH)
  generate_and_write_queries(get_tpcds_table_info, 2, 1, BenchmarkType.TPCDS)


if __name__ == "__main__":
  main()
