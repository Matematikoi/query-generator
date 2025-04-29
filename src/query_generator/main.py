from typing import List, Optional

import typer
from typing_extensions import Annotated

from query_generator.duckdb.binning import (
  BinningSnowflakeParameters,
  SearchParameters,
  run_snowflake_binning,
)
from query_generator.duckdb.setup import setup_duckdb
from query_generator.join_based_query_generator.snowflake import (
  generate_and_write_queries,
)
from query_generator.tools.cherry_pick_binning import cherry_pick_binning
from query_generator.utils.definitions import (
  Dataset,
  QueryGenerationParameters,
)
from query_generator.utils.show_messages import show_dev_warning
from query_generator.utils.utils import validate_dir_path

app = typer.Typer(name="Query Generation")


@app.command()
def snowflake(
  dataset: Annotated[
    Dataset, typer.Option("--dataset", "-d", help="The dataset used")
  ],
  max_hops: Annotated[
    int,
    typer.Option(
      "--max-hops", "-h", help="The maximum number of hops", min=1, max=5
    ),
  ] = 3,
  max_queries_per_fact_table: Annotated[
    int,
    typer.Option(
      "--fact",
      "-f",
      help="The maximum number of queries per fact table",
      min=1,
    ),
  ] = 100,
  max_queries_per_signature: Annotated[
    int,
    typer.Option(
      "--signature",
      "-s",
      help="The maximum number of queries per signature/template",
      min=1,
    ),
  ] = 1,
  keep_edge_prob: Annotated[
    float,
    typer.Option(
      "--edge-prob",
      "-p",
      help="The probability of keeping an edge in the subgraph",
      min=0.0,
      max=1.0,
    ),
  ] = 0.2,
  row_retention_probability: Annotated[
    float,
    typer.Option(
      "--row-retention",
      "-r",
      help="The probability of keeping a row in each predicate",
      min=0.0,
      max=1.0,
    ),
  ] = 0.2,
  extra_predicates: Annotated[
    int,
    typer.Option(
      "--extra-predicates",
      "-e",
      help="The number of extra predicates to add to the query",
      min=0,
    ),
  ] = 3,
) -> None:
  """
  Generate queries using a random subgraph.
  """
  params = QueryGenerationParameters(
    dataset=dataset,
    max_hops=max_hops,
    max_queries_per_fact_table=max_queries_per_fact_table,
    max_queries_per_signature=max_queries_per_signature,
    keep_edge_prob=keep_edge_prob,
    extra_predicates=extra_predicates,
    row_retention_probability=row_retention_probability,
  )
  generate_and_write_queries(params)


@app.command()
def binning(
  dataset: Annotated[
    Dataset, typer.Option("--dataset", "-d", help="The dataset used")
  ],
  dev: Annotated[
    bool,
    typer.Option(
      "--dev",
      help="Development testing. If true then uses scale factor 1 to check.",
    ),
  ] = False,
  lower_bound: Annotated[
    int,
    typer.Option(
      "--lower-bound",
      "-l",
      help="The lower bound of the binning process",
      min=0,
    ),
  ] = 0,
  upper_bound: Annotated[
    int,
    typer.Option(
      "--upper-bound",
      "-u",
      help="The upper bound of the binning process",
      min=1,
    ),
  ] = 1_000_000,
  total_bins: Annotated[
    int,
    typer.Option(
      "--total-bins",
      "-b",
      help="The number of bins to create",
      min=10,
    ),
  ] = 200,
  max_hops_range: Annotated[
    Optional[List[int]],
    typer.Option(
      "--max-hops-range",
      "-h",
      help="The range of hops to use for the query generation",
      show_default="1, 2, 4",
    ),
  ] = None,
  extra_predicates_range: Annotated[
    Optional[List[int]],
    typer.Option(
      "--extra-predicates-range",
      "-e",
      help="The range of extra predicates to use for the query generation",
      show_default="1, 3, 5",
    ),
  ] = None,
  row_retention_probability_range: Annotated[
    Optional[List[float]],
    typer.Option(
      "--row-retention-probability-range",
      "-r",
      help="The range of row retention probabilities to use "
      "for the query generation",
      show_default="0.2, 0.3, 0.4, 0.6, 0.8, 0.9",
    ),
  ] = None,
) -> None:
  """
  This is an extension of the Snowflake algorithm.

  It makes bins from lower-bound to upper-bound and it runs
  the query on DuckDB to check that the number of rows that
  fulfill the query is bigger than the lower bound. Then it
  saves the results in bins of equidepth of size
  (upper_bound - lower_bound) / total_bins
  then it saves the query to the allocated bin.
  """
  if max_hops_range is None:
    max_hops_range = [1, 2, 4]
  if extra_predicates_range is None:
    extra_predicates_range = [1, 3, 5]
  if row_retention_probability_range is None:
    row_retention_probability_range = [0.2, 0.3, 0.4, 0.6, 0.8, 0.9]
  if lower_bound >= upper_bound:
    raise ValueError("The lower bound must be smaller than the upper bound")
  show_dev_warning(dev)
  scale_factor = 0.1 if dev else 100
  con = setup_duckdb(scale_factor, dataset)
  run_snowflake_binning(
    BinningSnowflakeParameters(
      scale_factor=scale_factor,
      dataset=dataset,
      lower_bound=lower_bound,
      upper_bound=upper_bound,
      total_bins=total_bins,
      con=con,
    ),
    SearchParameters(
      max_hops=max_hops_range,
      extra_predicates=extra_predicates_range,
      row_retention_probability=row_retention_probability_range,
    ),
  )


@app.command()
def cherry_pick(
  dataset: Annotated[
    Dataset, typer.Option("--dataset", "-d", help="The dataset used")
  ],
  folder: Annotated[
    Optional[str],
    typer.Option(
      "--folder",
      "-f",
      help="The folder where the queries are stored",
      show_default="data/generated_queries/BINNING_SNOWFLAKE/{dataset}",
    ),
  ] = None,
  queries_per_bin: Annotated[
    int,
    typer.Option(
      "--queries",
      "-q",
      help="The number of queries to be randomly picked per bin",
      min=1,
    ),
  ] = 15,
) -> None:
  """
  This function is used to cherry pick queries from the
  binning process. It randomly picks queries from the
  binning process and saves them in a folder.
  """
  if folder is None:
    folder = f"data/generated_queries/BINNING_SNOWFLAKE/{dataset.value}"
  validate_dir_path(folder)
  cherry_pick_binning(dataset, folder, queries_per_bin)


if __name__ == "__main__":
  app()
