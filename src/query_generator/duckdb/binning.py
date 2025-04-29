import math
from dataclasses import dataclass

import duckdb
import polars as pl

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
  params: BinningSnowflakeParameters,
) -> None:
  """
  Run the Snowflake binning process. Binning is equiwidth binning.

  Args:
    parameters (BinningSnowflakeParameters): The parameters for
    the Snowflake binning process.
  """
  query_writer = Writer(params.dataset, Extension.BINNING_SNOWFLAKE)
  # TODO: this should be a json file that I pass
  # TODO: add tqdm
  rows = []
  for max_hops in [1, 2, 4]:
    for extra_predicates in [1, 3, 5]:
      for row_retention_probability in [0.1, 0.2, 0.5, 0.8]:
        for query in generate_queries(
          QueryGenerationParameters(
            dataset=params.dataset,
            max_hops=max_hops,
            max_queries_per_fact_table=10,
            max_queries_per_signature=2,
            keep_edge_prob=0.2,
            extra_predicates=extra_predicates,
            row_retention_probability=float(row_retention_probability),
          )
        ):
          selected_rows = get_result_from_duckdb(query.query, params)
          if selected_rows == -1:
            continue  # invalid query
          bin = get_bin_from_value(selected_rows, params)
          query_writer.write_query_to_bin(bin, query)
          rows.append(
            {
              "bin": bin,
              "count_star": selected_rows,
              "fact_table": query.fact_table,
              "template_number": query.template_number,
              "predicate_number": query.predicate_number,
              "max_hops": max_hops,
              "row_retention_probability": row_retention_probability,
            }
          )
  df = pl.DataFrame(rows)
  query_writer.write_dataframe(df)
