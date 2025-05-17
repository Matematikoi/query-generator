from pathlib import Path
from typing import Annotated

import typer

from query_generator.duckdb_connection.binning import (
  SearchParameters,
  run_snowflake_param_seach,
)
from query_generator.duckdb_connection.setup import setup_duckdb
from query_generator.join_based_query_generator.snowflake import (
  generate_and_write_queries,
)
from query_generator.join_based_query_generator.utils.query_writer import (
  write_parquet,
  write_redundant_histogram_csv,
)
from query_generator.tools.cherry_pick_binning import (
  CherryPickParameters,
  cherry_pick_binning,
)
from query_generator.tools.format_queries_file_structure import (
  format_queries_file_structure,
)
from query_generator.tools.histograms import (
  make_redundant_histograms,
  query_histograms,
)
from query_generator.utils.definitions import (
  Dataset,
  Extension,
  QueryGenerationParameters,
)
from query_generator.utils.show_messages import show_dev_warning
from query_generator.utils.utils import validate_file_path

app = typer.Typer(name="Query Generation")


@app.command()
def snowflake(
  dataset: Annotated[
    Dataset,
    typer.Option("--dataset", "-d", help="The dataset used"),
  ],
  max_hops: Annotated[
    int,
    typer.Option(
      "--max-hops",
      "-h",
      help="The maximum number of hops",
      min=1,
      max=5,
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
  """Generate queries using a random subgraph."""
  params = QueryGenerationParameters(
    dataset=dataset,
    max_hops=max_hops,
    max_queries_per_fact_table=max_queries_per_fact_table,
    max_queries_per_signature=max_queries_per_signature,
    keep_edge_prob=keep_edge_prob,
    extra_predicates=extra_predicates,
    row_retention_probability=row_retention_probability,
    seen_subgraphs={},
  )
  generate_and_write_queries(params)


@app.command()
def param_search(
  dataset: Annotated[
    Dataset,
    typer.Option("--dataset", "-d", help="The dataset used"),
  ],
  *,
  dev: Annotated[
    bool,
    typer.Option(
      "--dev",
      help="Development testing. If true then uses scale factor 0.1 to check.",
    ),
  ] = False,
  unique_joins: Annotated[
    bool,
    typer.Option(
      "--unique-joins",
      "-u",
      help="If true all queries will have a unique join structure "
      "(not recommended for TPC-H)",
    ),
  ] = False,
  max_hops_range: Annotated[
    list[int] | None,
    typer.Option(
      "--max-hops-range",
      "-h",
      help="The range of hops to use for the query generation",
      show_default="1, 2, 4",
    ),
  ] = None,
  extra_predicates_range: Annotated[
    list[int] | None,
    typer.Option(
      "--extra-predicates-range",
      "-e",
      help="The range of extra predicates to use for the query generation",
      show_default="1, 2, 3, 5",
    ),
  ] = None,
  row_retention_probability_range: Annotated[
    list[float] | None,
    typer.Option(
      "--row-retention-probability-range",
      "-r",
      help="The range of row retention probabilities to use "
      "for the query generation",
      show_default="0.2, 0.3, 0.4, 0.6, 0.8, 0.85, 0.9, 1.0",
    ),
  ] = None,
) -> None:
  """This is an extension of the Snowflake algorithm.

  It runs multiple batches with different configurations of the algorithm.
  This allows us to get multiple results.
  """
  if max_hops_range is None:
    max_hops_range = [1, 2, 4]
  if extra_predicates_range is None:
    extra_predicates_range = [1, 2, 3, 5]
  if row_retention_probability_range is None:
    row_retention_probability_range = [0.2, 0.3, 0.4, 0.6, 0.8, 0.85, 0.9, 1.0]
  show_dev_warning(dev=dev)
  scale_factor = 0.1 if dev else 100
  con = setup_duckdb(dataset, scale_factor)
  run_snowflake_param_seach(
    SearchParameters(
      scale_factor=scale_factor,
      con=con,
      dataset=dataset,
      max_hops=max_hops_range,
      extra_predicates=extra_predicates_range,
      row_retention_probability=row_retention_probability_range,
      unique_joins=unique_joins,
    ),
  )


@app.command()
def cherry_pick(
  dataset: Annotated[
    Dataset,
    typer.Option("--dataset", "-d", help="The dataset used"),
  ],
  csv: Annotated[
    str | None,
    typer.Option(
      "--csv",
      "-c",
      help="The path to the batches csv",
      show_default="data/generated_queries/BINNING_SNOWFLAKE/{dataset}/{dataset}_values.csv",
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
  ] = 10,
  upper_bound: Annotated[
    int,
    typer.Option(
      "--upper-bound",
      "-u",
      help="The upper bound of the binning process",
      min=1,
    ),
  ] = 1_000_000_000,
  total_bins: Annotated[
    int,
    typer.Option(
      "--total-bins",
      "-b",
      help="The number of bins to create",
      min=10,
    ),
  ] = 1000,
  seed: Annotated[
    int,
    typer.Option(
      "--seed",
      "-s",
      help="The seed to use for the random queries selection",
      min=0,
    ),
  ] = 42,
  destination_folder: Annotated[
    str | None,
    typer.Option(
      "--destination-folder",
      "-df",
      help="The folder to save the cherry picked queries",
      show_default=f"data/generated_queries/{Extension.BINNING_CHERRY_PICKING.value}/{{dataset}}",
    ),
  ] = None,
) -> None:
  """This function is used to cherry pick queries from the
  binning process. It randomly picks queries from the
  binning process and saves them in a folder.
  """
  csv_path = (
    Path(
      f"data/generated_queries/{Extension.SNOWFLAKE_SEARCH_PARAMS.value}/{dataset.value}/{dataset.value}_batches.csv",
    )
    if csv is None
    else Path(csv)
  )
  destination_folder_path = (
    Path(
      f"data/generated_queries/{Extension.BINNING_CHERRY_PICKING.value}/{dataset.value}",
    )
    if destination_folder is None
    else Path(destination_folder)
  )
  validate_file_path(csv_path)
  cherry_pick_binning(
    CherryPickParameters(
      csv_path=csv_path,
      queries_per_bin=queries_per_bin,
      upper_bound=upper_bound,
      total_bins=total_bins,
      destination_folder=destination_folder_path,
      seed=seed,
    ),
  )


@app.command()
def format_queries(
  folder_src: Annotated[
    str,
    typer.Option(
      "--src",
      "-s",
      help="The folder to format the queries",
    ),
  ],
  folder_dst: Annotated[
    str,
    typer.Option(
      "--dst",
      "-d",
      help="The folder to save the formatted queries",
    ),
  ] = "data/generated_queries/FORMATTED_QUERIES",
) -> None:
  """Formats queries names for submission to spark

  The input folder must have the following structure:\n
  folder_src/ \n
    ├── some_name_1 \n
    │   ├── query_1.sql \n
    │   ├── query_2.sql \n
    │   └── ... \n
    ├── some_name_2 \n
    │   ├── query_1.sql \n
    │   ├── query_2.sql \n
    │   └── ... \n
    └── ... \n
  The output folder will have the following structure:\n
  folder_dst/ \n
    ├── some_name_1 \n
    │   ├── some_name_1_1.sql \n
    │   ├── some_name_1_2.sql \n
    │   └── ... \n
    ├── some_name_2 \n
    │   ├── some_name_2_1.sql \n
    │   ├── some_name_2_2.sql \n
    │   └── ... \n
    └── ... \n
  """
  src_folder_path = Path(folder_src)
  dst_folder_path = Path(folder_dst)
  format_queries_file_structure(
    src_folder_path=src_folder_path,
    dst_folder_path=dst_folder_path,
  )


@app.command()
def make_histograms(
  dataset: Annotated[
    Dataset,
    typer.Option("--dataset", "-d", help="The dataset used"),
  ],
  histogram_size: Annotated[
    int,
    typer.Option(
      "--histogram-size",
      "-h",
      help="The size of the histogram",
      min=1,
    ),
  ] = 50,
  common_values_size: Annotated[
    int,
    typer.Option(
      "--common-values-size",
      "-c",
      help="The size of the common values",
      min=1,
    ),
  ] = 10,
  destination_str: Annotated[
    str | None,
    typer.Option(
      "--path",
      "-p",
      help="The folder to save the histograms",
      show_default="data/generated_histograms/{dataset}/histogram.parquet",
    ),
  ] = None,
  *,
  dev: Annotated[
    bool,
    typer.Option(
      "--dev",
      help="Development testing. If true then uses scale factor 0.1 to check.",
    ),
  ] = False,
  include_mvc: Annotated[
    bool,
    typer.Option(
      "--exclude-mvc",
      "-e",
      help="If true then we generate most common values",
    ),
  ] = False,
) -> None:
  """This function is used to create histograms in parquet format."""
  destination_path = (
    Path(
      f"data/generated_histograms/{dataset.value}/histogram.parquet",
    )
    if destination_str is None
    else Path(destination_str)
  )
  scale_factor = 0.1 if dev else 100

  con = setup_duckdb(
    dataset,
    scale_factor,
  )
  histograms_df = query_histograms(
    dataset=dataset,
    histogram_size=histogram_size,
    common_values_size=common_values_size,
    con=con,
    include_mvc=include_mvc,
  )
  write_parquet(histograms_df, destination_path)
  # TODO(Gabriel):  http://localhost:8080/tktview/46fca17ee0
  #  Delete this code and everything that
  #  touches it [46fca17ee0ab9e46]
  redundant_histogram_df = make_redundant_histograms(
    destination_path, histogram_size
  )
  write_parquet(
    redundant_histogram_df,
    destination_path.parent / "redundant_histogram.parquet",
  )
  write_redundant_histogram_csv(
    redundant_histogram_df, destination_path.parent / "redundant_histogram.csv"
  )


if __name__ == "__main__":
  app()
