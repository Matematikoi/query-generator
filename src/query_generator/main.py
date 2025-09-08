from pathlib import Path
from typing import Annotated

import duckdb
import typer

from query_generator.duckdb_connection.setup import generate_db
from query_generator.filter.filter import filter_synthetic_queries
from query_generator.llm.llm_extension import llm_extension
from query_generator.synthetic_queries.synthetic_query_generator import (
  SyntheticQueriesParams,
  generate_synthetic_queries,
)
from query_generator.synthetic_queries.utils.query_writer import (
  write_parquet,
  write_redundant_histogram_csv,
)
from query_generator.tools.format_queries_file_structure import (
  format_queries_file_structure,
)
from query_generator.tools.histograms import (
  make_redundant_histograms,
  query_histograms,
)
from query_generator.tools.union_queries import union_queries
from query_generator.utils.params import (
  ExtensionAndLLMEndpoint,
  FilterEndpoint,
  GenerateDBEndpoint,
  HistogramEndpoint,
  SyntheticQueriesEndpoint,
  get_toml_from_params,
  read_and_parse_toml,
)
from query_generator.utils.utils import (
  build_help_from_dataclass,
)

app = typer.Typer(name="Query Generation", rich_markup_mode="markdown")


@app.command(help=build_help_from_dataclass(SyntheticQueriesEndpoint))
def synthetic_queries(
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
    SyntheticQueriesEndpoint,
  )
  con = duckdb.connect(database=params.duckdb_database, read_only=True)
  generate_synthetic_queries(
    SyntheticQueriesParams(
      con=con,
      user_input=params,
    ),
  )


@app.command("generate-db", help=build_help_from_dataclass(GenerateDBEndpoint))
def generate_db_endpoint(
  config_path: Annotated[
    str,
    typer.Option("-c", "--config", help="The path to the configuration file"),
  ],
) -> None:
  """Generates a DuckDB database with TPCDS or TPCH datasets.

  If the scale factor required is not generated, it will generate it.
  It returns a duckdb connection to the database.
  """
  params = read_and_parse_toml(
    Path(config_path),
    GenerateDBEndpoint,
  )
  generate_db(params)


@app.command("filter-synthetic", help=build_help_from_dataclass(FilterEndpoint))
def filter_synthetic_endpoint(
  config_path: Annotated[
    str,
    typer.Option(
      "-c",
      "--config",
      help="The path to the configuration file"
      "They can be found in the params_config/filter/ folder",
    ),
  ],
) -> None:
  """Filters queries based on the Count Star

  Supports two methods of filtering:
  - Filter null queries and format for traces collection (count star = 0)
  - Cherry pick queries based on binning (makes equi-width bins
  based on the parameters provided by the user and picks queries
  in each bin up to a limit)
  """
  params = read_and_parse_toml(
    Path(config_path),
    FilterEndpoint,
  )
  filter_synthetic_queries(params)


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
  config_path: Annotated[
    str,
    typer.Option(
      "-c",
      "--config",
      help="The path to the configuration file"
      "They can be found in the params_config/histograms/ folder",
    ),
  ],
) -> None:
  """This function is used to create histograms in parquet format."""
  params = read_and_parse_toml(Path(config_path), HistogramEndpoint)
  destination_path = Path(params.output_folder) / "histogram.parquet"

  con = duckdb.connect(params.database_path, read_only=True)
  histograms_df = query_histograms(
    histogram_size=params.histogram_size,
    common_values_size=params.common_values_size,
    con=con,
    include_mcv=params.include_mcv,
  )
  write_parquet(histograms_df, destination_path)
  # TODO(Gabriel):  http://localhost:8080/tktview/46fca17ee0
  #  Delete this code and everything that
  #  touches it [46fca17ee0ab9e46]
  redundant_histogram_df = make_redundant_histograms(
    destination_path, params.histogram_size
  )
  write_parquet(
    redundant_histogram_df,
    destination_path.parent / "regrouped_job_hist.parquet",
  )
  write_redundant_histogram_csv(
    redundant_histogram_df, destination_path.parent / "regrouped_job_hist.csv"
  )


@app.command(
  "extensions-and-llm", help=build_help_from_dataclass(ExtensionAndLLMEndpoint)
)
def extension_and_llm_endpoint(
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
  params = read_and_parse_toml(Path(config_file), ExtensionAndLLMEndpoint)
  if params.union_extension:
    assert params.union_params is not None
    union_queries(
      Path(params.queries_parquet),
      Path(params.destination_folder),
      params.union_params.max_queries,
      params.union_params.probability,
    )
    print("Union extension done")

  if params.llm_extension:
    llm_extension(params)
  toml_params = get_toml_from_params(params)
  (Path(params.destination_folder) / "extension_config.toml").write_text(
    toml_params
  )


if __name__ == "__main__":
  app()
