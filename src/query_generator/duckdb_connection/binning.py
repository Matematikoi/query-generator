from dataclasses import dataclass
from itertools import product

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
  QueryGenerationParameters,
)
from query_generator.utils.params import SearchParametersEndpoint


@dataclass
class SearchParameters:
  user_input: SearchParametersEndpoint
  scale_factor: int | float
  con: duckdb.DuckDBPyConnection


def get_result_from_duckdb(query: str, con: duckdb.DuckDBPyConnection) -> int:
  try:
    result = int(con.sql(query).fetchall()[0][0])
  except duckdb.BinderException as e:
    print(f"Invalid query, exception: {e},\n{query}")
    return -1
  return result


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
  )


def run_snowflake_param_seach(
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
  for max_hops, extra_predicates, row_retention_probability in tqdm(
    product(
      search_params.user_input.max_hops,
      search_params.user_input.extra_predicates,
      search_params.user_input.row_retention_probability,
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
        keep_edge_prob=search_params.user_input.keep_edge_prob,
        extra_predicates=extra_predicates,
        row_retention_probability=float(row_retention_probability),
        seen_subgraphs=seen_subgraphs,
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
          "fact_table": query.fact_table,
          "max_hops": max_hops,
          "row_retention_probability": row_retention_probability,
        },
      )
    # Update the seen subgraphs with the new ones
    if search_params.user_input.unique_joins:
      seen_subgraphs = query_generator.subgraph_generator.seen_subgraphs
  df_queries = pl.DataFrame(rows)
  query_writer.write_dataframe(df_queries)
