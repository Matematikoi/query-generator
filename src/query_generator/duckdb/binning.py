from dataclasses import dataclass
from itertools import product
from typing import List

import duckdb
import polars as pl
from tqdm import tqdm

from query_generator.join_based_query_generator.snowflake import (
  generate_queries,
)
from query_generator.join_based_query_generator.utils.query_writer import (
  Writer,
)
from query_generator.utils.definitions import (
  Dataset,
  Extension,
  QueryGenerationParameters,
)


@dataclass
class SearchParameters:
  dataset: Dataset
  scale_factor: int | float
  con: duckdb.DuckDBPyConnection
  max_hops: List[int]
  extra_predicates: List[int]
  row_retention_probability: List[float]


def get_result_from_duckdb(query: str, con: duckdb.DuckDBPyConnection) -> int:
  try:
    result = int(con.sql(query).fetchall()[0][0])
  except duckdb.BinderException as e:
    print(f"Invalid query, exception: {e},\n{query}")
    return -1
  return result


def run_snowflake_param_seach(
  search_params: SearchParameters,
) -> None:
  """Run the Snowflake binning process. Binning is equiwidth binning.

  Args:
    parameters (BinningSnowflakeParameters): The parameters for
    the Snowflake binning process.

  """
  query_writer = Writer(search_params.dataset, Extension.BINNING_SNOWFLAKE)
  rows = []
  total_iterations = (
    len(search_params.max_hops)
    * len(search_params.extra_predicates)
    * len(search_params.row_retention_probability)
  )
  batch_number = 0
  for max_hops, extra_predicates, row_retention_probability in tqdm(
    product(
      search_params.max_hops,
      search_params.extra_predicates,
      search_params.row_retention_probability,
    ),
    total=total_iterations,
    desc="Progress",
  ):
    batch_number += 1
    for query in generate_queries(
      QueryGenerationParameters(
        dataset=search_params.dataset,
        max_hops=max_hops,
        max_queries_per_fact_table=10,
        max_queries_per_signature=2,
        keep_edge_prob=0.2,
        extra_predicates=extra_predicates,
        row_retention_probability=float(row_retention_probability),
      )
    ):
      selected_rows = get_result_from_duckdb(query.query, search_params.con)
      if selected_rows == -1:
        continue  # invalid query
      prefix = f"batch_{batch_number}"
      relative_path = query_writer.write_query_to_batch(batch_number, query)
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
        }
      )
  df_queries = pl.DataFrame(rows)
  query_writer.write_dataframe(df_queries)
