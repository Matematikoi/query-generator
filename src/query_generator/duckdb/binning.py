import math
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
class BinningSnowflakeParameters:
  scale_factor: int | float
  dataset: Dataset
  lower_bound: int
  upper_bound: int
  total_bins: int
  con: duckdb.DuckDBPyConnection


@dataclass
class SearchParameters:
  max_hops: List[int]
  extra_predicates: List[int]
  row_retention_probability: List[float]


def get_bin_from_value(
  value: int, bin_params: BinningSnowflakeParameters
) -> int:
  normalized_max_val = bin_params.upper_bound - bin_params.lower_bound
  normalized_value = value - bin_params.lower_bound
  bin_size = float(normalized_max_val) / float(bin_params.total_bins)
  bin = math.ceil(normalized_value / bin_size)
  if bin > bin_params.total_bins:
    bin = bin_params.total_bins + 1
  return bin


def get_result_from_duckdb(
  query: str, params: BinningSnowflakeParameters
) -> int:
  try:
    result = int(params.con.sql(query).fetchall()[0][0])
  except duckdb.BinderException as e:
    print(f"Invalid query, exception: {e},\n{query}")
    return -1
  return result


def run_snowflake_binning(
  bin_params: BinningSnowflakeParameters,
  search_params: SearchParameters,
) -> None:
  """
  Run the Snowflake binning process. Binning is equiwidth binning.

  Args:
    parameters (BinningSnowflakeParameters): The parameters for
    the Snowflake binning process.
  """
  query_writer = Writer(bin_params.dataset, Extension.BINNING_SNOWFLAKE)
  rows = []
  total_iterations = (
    len(search_params.max_hops)
    * len(search_params.extra_predicates)
    * len(search_params.row_retention_probability)
  )
  cnt = 0
  for max_hops, extra_predicates, row_retention_probability in tqdm(
    product(
      search_params.max_hops,
      search_params.extra_predicates,
      search_params.row_retention_probability,
    ),
    total=total_iterations,
    desc="Progress",
  ):
    for query in generate_queries(
      QueryGenerationParameters(
        dataset=bin_params.dataset,
        max_hops=max_hops,
        max_queries_per_fact_table=10,
        max_queries_per_signature=2,
        keep_edge_prob=0.2,
        extra_predicates=extra_predicates,
        row_retention_probability=float(row_retention_probability),
      )
    ):
      cnt += 1
      selected_rows = get_result_from_duckdb(query.query, bin_params)
      if selected_rows == -1:
        continue  # invalid query
      bin = get_bin_from_value(selected_rows, bin_params)
      prefix = f"batch_{cnt}"
      query_writer.write_query_to_bin(prefix, bin, query)
      rows.append(
        {
          "bin": bin,
          "count_star": selected_rows,
          "prefix": prefix,
          "template_number": query.template_number,
          "predicate_number": query.predicate_number,
          "fact_table": query.fact_table,
          "max_hops": max_hops,
          "row_retention_probability": row_retention_probability,
        }
      )
  df = pl.DataFrame(rows)
  query_writer.write_dataframe(df)
