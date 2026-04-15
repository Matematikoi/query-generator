import logging
from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Any

import polars as pl
from tqdm import tqdm

from query_generator.database_connection.query_validator_abc import (
  QueryValidator,
)
from query_generator.synthetic_queries.query_builder import (
  QueryGenerator,
)
from query_generator.synthetic_queries.utils.query_writer import Writer
from query_generator.utils.definitions import (
  BatchGeneratedQueryToWrite,
  PredicateParameters,
  SyntheticQueryGenerationParameters,
)
from query_generator.utils.params import (
  SyntheticQueriesEndpoint,
  get_toml_from_params,
)

logger = logging.getLogger(__name__)


@dataclass
class SyntheticQueriesParams:
  user_input: SyntheticQueriesEndpoint
  validator: QueryValidator


def get_total_iterations(search_params: SyntheticQueriesEndpoint) -> int:
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
    * len(search_params.keep_edge_probability)
    * len(search_params.minimum_like_support_probability)
    * len(search_params.or_probability)
  )


def generate_synthetic_queries(
  params: SyntheticQueriesParams,
) -> None:
  """Run the Snowflake binning process. Binning is equiwidth binning.

  Args:
    parameters (BinningSnowflakeParameters): The parameters for
    the Snowflake binning process.

  """
  writer = Writer(params.user_input.output_folder)
  rows: list[dict[str, str | int | float]] = []
  total_iterations = get_total_iterations(params.user_input)
  batch_number = 0
  seen_subgraphs: dict[int, bool] = {}
  for (
    max_hops,
    extra_predicates,
    row_retention_probability,
    equality_lower_bound_probability,
    keep_edge_probability,
    minimum_like_support_probability,
    or_probability,
  ) in tqdm(  # type: ignore
    product(
      params.user_input.max_hops,
      params.user_input.extra_predicates,
      params.user_input.row_retention_probability,
      params.user_input.equality_lower_bound_probability,
      params.user_input.keep_edge_probability,
      params.user_input.minimum_like_support_probability,
      params.user_input.or_probability,
    ),
    total=total_iterations,
    desc="Batch",
  ):
    logger.debug(f"Processing batch {batch_number}")
    batch_number += 1
    query_generator = QueryGenerator(
      SyntheticQueryGenerationParameters(
        dataset=params.user_input.dataset,
        max_hops=max_hops,
        max_queries_per_fact_table=params.user_input.max_signatures_per_fact_table,
        max_queries_per_signature=params.user_input.max_queries_per_signature,
        keep_edge_probability=keep_edge_probability,
        seen_subgraphs=seen_subgraphs,
        predicate_parameters=PredicateParameters(
          histogram_path=Path(params.user_input.histogram_path),
          extra_predicates=extra_predicates,
          row_retention_probability=row_retention_probability,
          operator_weights=params.user_input.operator_weights,
          equality_lower_bound_probability=equality_lower_bound_probability,
          extra_values_for_in=params.user_input.extra_values_for_in,
          minimum_like_support_probability=minimum_like_support_probability,
          or_probability=or_probability,
        ),
      )
    )
    for query in query_generator.generate_queries():
      selected_rows = params.validator.get_synthetic_query_cardinality(
        query.query
      )
      if selected_rows == -1:
        logger.error("Query generated was not valid.")
        logger.debug(f"Query generated:\n{query.query}")
        continue  # invalid query

      relative_path = writer.write_query_to_batch(
        BatchGeneratedQueryToWrite(
          batch_number=batch_number,
          fact_table=query.fact_table,
          template_number=query.template_number,
          predicate_number=query.predicate_number,
          query=query.query,
        )
      )
      # Adds query to the DataFrame
      rows.append(
        {
          "relative_path": relative_path,
          "count_star": selected_rows,
          "batch_number": batch_number,
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
          "keep_edge_probability": keep_edge_probability,
          # instead of bigint, lets do str
          "subgraph_signature": str(query.subgraph_signature),
        },
      )
    # Update the seen subgraphs with the new ones
    if params.user_input.unique_joins:
      seen_subgraphs = query_generator.subgraph_generator.seen_subgraphs
    checkpoint_queries_parquet(rows, writer)
  checkpoint_queries_parquet(rows, writer)
  logger.info(f"Total queries generated: {len(rows)}.")
  toml_params = get_toml_from_params(params.user_input)
  writer.write_toml(toml_params)


def checkpoint_queries_parquet(rows: list[Any], query_writer: Writer) -> None:
  df_queries = pl.DataFrame(rows)
  query_writer.write_dataframe(df_queries)
