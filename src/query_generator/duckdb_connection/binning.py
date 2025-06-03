import threading
from dataclasses import dataclass
from itertools import product
from typing import Any

import duckdb
import polars as pl
from tqdm import tqdm

from query_generator.join_based_query_generator.snowflake import (
  QueryGenerator,
)
from query_generator.join_based_query_generator.utils.query_writer import (
  Writer,
)
from query_generator.utils.definitions import (
  BatchGeneratedQueryFeatures,
  Extension,
  PredicateParameters,
  QueryGenerationParameters,
)
from query_generator.utils.params import SearchParametersEndpoint


@dataclass
class SearchParameters:
  user_input: SearchParametersEndpoint
  scale_factor: int | float
  con: duckdb.DuckDBPyConnection


def get_result_from_duckdb(
  query: str, con: duckdb.DuckDBPyConnection, timeout: float = 5.0
) -> int:
  did_timeout = False

  def _interrupt() -> None:
    nonlocal did_timeout
    did_timeout = True
    con.interrupt()

  timer = threading.Timer(timeout, _interrupt)
  timer.start()
  try:
    result = con.sql(query).fetchall()[0][0]
    return int(result)
  except duckdb.BinderException:
    return -1
  except duckdb.Error:
    return -1
  finally:
    timer.cancel()


def get_total_iterations(search_params: SearchParametersEndpoint) -> int:
  """Get the total number of iterations for the Snowflake binning process.

  Args:
    search_params (SearchParameters): The parameters for the Snowflake
    binning process.

  Returns:
    int: The total number of iterations.

  """
  return (
    len(search_params.max_hops)
    * len(search_params.extra_predicates)
    * len(search_params.row_retention_probability)
    * len(search_params.equality_lower_bound_probability)
  )


def run_snowflake_param_search(
  search_params: SearchParameters,
) -> None:
  """Run the Snowflake binning process. Binning is equiwidth binning.

  Args:
    parameters (BinningSnowflakeParameters): The parameters for
    the Snowflake binning process.

  """
  query_writer = Writer(
    search_params.user_input.dataset,
    Extension.SNOWFLAKE_SEARCH_PARAMS,
  )
  rows: list[dict[str, str | int | float]] = []
  total_iterations = get_total_iterations(search_params.user_input)
  batch_number = 0
  seen_subgraphs: dict[int, bool] = {}
  for (
    max_hops,
    extra_predicates,
    row_retention_probability,
    equality_lower_bound_probability,
  ) in tqdm(
    product(
      search_params.user_input.max_hops,
      search_params.user_input.extra_predicates,
      search_params.user_input.row_retention_probability,
      search_params.user_input.equality_lower_bound_probability,
    ),
    total=total_iterations,
    desc="Progress",
  ):
    batch_number += 1
    query_generator = QueryGenerator(
      QueryGenerationParameters(
        dataset=search_params.user_input.dataset,
        max_hops=max_hops,
        max_queries_per_fact_table=search_params.user_input.max_queries_per_fact_table,
        max_queries_per_signature=search_params.user_input.max_queries_per_signature,
        keep_edge_probability=search_params.user_input.keep_edge_probability,
        seen_subgraphs=seen_subgraphs,
        predicate_parameters=PredicateParameters(
          extra_predicates=extra_predicates,
          row_retention_probability=row_retention_probability,
          operator_weights=search_params.user_input.operator_weights,
          equality_lower_bound_probability=equality_lower_bound_probability,
          extra_values_for_in=search_params.user_input.extra_values_for_in,
        ),
      )
    )
    for query in query_generator.generate_queries():
      selected_rows = get_result_from_duckdb(query.query, search_params.con)
      if selected_rows == -1:
        continue  # invalid query

      prefix = f"batch_{batch_number}"

      relative_path = query_writer.write_query_to_batch(
        BatchGeneratedQueryFeatures(
          batch_number=batch_number,
          query=query.query,
          template_number=query.template_number,
          predicate_number=query.predicate_number,
          fact_table=query.fact_table,
          prefix=prefix,
          total_subgraph_edges=query.total_subgraph_edges,
          generated_predicate_types=query.generated_predicate_types,
        )
      )
      # Adds query to the DataFrame
      rows.append(
        {
          "relative_path": relative_path,
          "count_star": selected_rows,
          "prefix": prefix,
          "template_number": query.template_number,
          "predicate_number": query.predicate_number,
          "extra_predicates": extra_predicates,
          "fact_table": query.fact_table,
          "max_hops": max_hops,
          "row_retention_probability": row_retention_probability,
          "equality_lower_bound_probability": equality_lower_bound_probability,
          "total_subgraph_edges": query.total_subgraph_edges,
          "predicates_range": query.generated_predicate_types.range,
          "predicates_in_values": query.generated_predicate_types.in_values,
          "predicates_equality": query.generated_predicate_types.equality,
        },
      )
    # Update the seen subgraphs with the new ones
    if search_params.user_input.unique_joins:
      seen_subgraphs = query_generator.subgraph_generator.seen_subgraphs
    checkpoint_queries_csv(rows, query_writer)
  checkpoint_queries_csv(rows, query_writer)


def checkpoint_queries_csv(rows: list[Any], query_writer: Writer) -> None:
  df_queries = pl.DataFrame(rows)
  query_writer.write_dataframe(df_queries)
