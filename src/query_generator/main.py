from pathlib import Path
from typing import Annotated

import typer

from query_generator.duckdb_connection.binning import (
  SearchParameters,
  run_snowflake_param_search,
)
from query_generator.duckdb_connection.setup import setup_duckdb
from query_generator.join_based_query_generator.snowflake import (
  generate_and_write_queries,
)
from query_generator.join_based_query_generator.utils.query_writer import (
  write_parquet,
  write_redundant_histogram_csv,
)
from query_generator.llm.complex_queries import create_complex_queries
from query_generator.tools.cherry_pick_binning import (
  CherryPickParameters,
  cherry_pick_binning,
  filter_null_and_format_job,
  filter_null_and_format_tpcds,
)
from query_generator.tools.format_queries_file_structure import (
  format_queries_file_structure,
)
from query_generator.tools.histograms import (
  make_redundant_histograms,
  query_histograms,
)
from query_generator.tools.union_queries import union_queries
from query_generator.utils.definitions import (
  Dataset,
  Extension,
  QueryGenerationParameters,
)
from query_generator.utils.params import (
  ComplexQueryGenerationParametersEndpoint,
  SearchParametersEndpoint,
  SnowflakeEndpoint,
  read_and_parse_toml,
)
from query_generator.utils.show_messages import show_dev_warning
from query_generator.utils.utils import (
  build_help_from_dataclass,
  validate_file_path,
)

app = typer.Typer(name="Query Generation", rich_markup_mode="markdown")


@app.command(help=build_help_from_dataclass(SnowflakeEndpoint))
def snowflake(
  config_path: Annotated[
    str,
    typer.Option(
      "-c",
      "--config",
      help="The path to the configuration file"
      "They can be found in the params_config/query_generation/ folder",
    ),
  ],
) -> None:
  """Generate queries using a random subgraph."""
  params_endpoint = read_and_parse_toml(Path(config_path), SnowflakeEndpoint)
  params = QueryGenerationParameters(
    dataset=params_endpoint.dataset,
    max_hops=params_endpoint.max_hops,
    max_queries_per_fact_table=params_endpoint.max_queries_per_fact_table,
    max_queries_per_signature=params_endpoint.max_queries_per_signature,
    keep_edge_probability=params_endpoint.keep_edge_probability,
    seen_subgraphs={},
    predicate_parameters=params_endpoint.predicate_parameters,
  )
  generate_and_write_queries(params)


@app.command(help=build_help_from_dataclass(SearchParametersEndpoint))
def param_search(
  config_path: Annotated[
    str,
    typer.Option(
      "-c",
      "--config",
      help="The path to the configuration file"
      "They can be found in the params_config/search_params/ folder",
    ),
  ],
) -> None:
  """This is an extension of the Snowflake algorithm.

  It runs multiple batches with different configurations of the algorithm.
  This allows us to get multiple results.
  """
  params = read_and_parse_toml(
    Path(config_path),
    SearchParametersEndpoint,
  )
  show_dev_warning(dev=params.dev)
  scale_factor = 0.1 if params.dev else 100
  con = setup_duckdb(params.dataset, scale_factor)
  run_snowflake_param_search(
    SearchParameters(
      scale_factor=scale_factor,
      con=con,
      user_input=params,
    ),
  )


@app.command()
def cherry_pick(
  dataset: Annotated[
    Dataset,
    typer.Option("--dataset", "-d", help="The dataset used"),
  ],
  parquet: Annotated[
    str | None,
    typer.Option(
      "--parquet",
      "-p",
      help="The path to the batches parquet",
      show_default="data/generated_queries/BINNING_SNOWFLAKE/{dataset}/{dataset}_values.parquet",
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
  # TODO(Gabriel): https://chiselapp.com/user/matematikoi/repository/query-generation/tktview/19a06e6eab725f84b5c74a94fda4efb5e8d43dbc
  # the input should be a toml file configuration file
  parquet_path = (
    Path(
      f"data/generated_queries/{Extension.SNOWFLAKE_SEARCH_PARAMS.value}/{dataset.value}/{dataset.value}_batches.parquet",
    )
    if parquet is None
    else Path(parquet)
  )
  destination_folder_path = (
    Path(
      f"data/generated_queries/{Extension.BINNING_CHERRY_PICKING.value}/{dataset.value}",
    )
    if destination_folder is None
    else Path(destination_folder)
  )
  validate_file_path(parquet_path)
  cherry_pick_binning(
    CherryPickParameters(
      parquet_path=parquet_path,
      queries_per_bin=queries_per_bin,
      upper_bound=upper_bound,
      total_bins=total_bins,
      destination_folder=destination_folder_path,
      seed=seed,
    ),
  )


@app.command()
def filter_null(
  parquet_path_str: Annotated[
    str,
    typer.Option(
      "--parquet",
      "-p",
      help="The path to the parquet file with queries",
    ),
  ],
  dataset: Annotated[
    Dataset,
    typer.Option(
      "--dataset",
      "-d",
      help="The dataset used",
    ),
  ],
  destination: Annotated[
    str,
    typer.Option(
      "--destination",
      "-e",
      help="The path to the destination folder",
    ),
  ],
) -> None:
  """Filters null queries and formats for traces collection

  Supports JOB and TPCDS separately since the trace collection
  works different for the two of them.
  """
  parquet_path = Path(parquet_path_str)
  destination_path = Path(destination)
  validate_file_path(parquet_path)
  if dataset == Dataset.JOB:
    filter_null_and_format_job(
      parquet_path=parquet_path,
      destination_path=destination_path,
    )
  else:
    filter_null_and_format_tpcds(
      parquet_path=parquet_path,
      destination_path=destination_path,
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
  ] = 51,
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
    destination_path.parent / "regrouped_job_hist.parquet",
  )
  write_redundant_histogram_csv(
    redundant_histogram_df, destination_path.parent / "regrouped_job_hist.csv"
  )


@app.command(
  help=build_help_from_dataclass(ComplexQueryGenerationParametersEndpoint)
)
def add_complex_queries(
  config_file: Annotated[
    str,
    typer.Option(
      "--config",
      "-c",
      help="The path to the configuration file with complex queries",
    ),
  ],
) -> None:
  """Add complex queries using LLM prompts.
  The configuration file should be a TOML file with the
  ComplexQueryGenerationParametersEndpoint structure."""
  params = read_and_parse_toml(
    Path(config_file), ComplexQueryGenerationParametersEndpoint
  )
  create_complex_queries(params)


@app.command("union-queries")
def union_queries_endpoint(
  parquet_path: Annotated[
    str,
    typer.Option(
      "--parquet",
      "-p",
      help="The path to the parquet file with queries to union",
    ),
  ],
  destination: Annotated[
    str,
    typer.Option(
      "--destination",
      "-d",
      help="The path to the destination folder for union queries",
    ),
  ],
  max_queries: Annotated[
    int,
    typer.Option(
      "--max-queries",
      "-m",
      help="The maximum number of queries to union",
      min=1,
    ),
  ] = 5,
) -> None:
  union_queries(
    Path(parquet_path),
    Path(destination),
    max_queries,
  )


if __name__ == "__main__":
  app()
