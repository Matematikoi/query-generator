import logging
from pathlib import Path
from typing import Annotated

import duckdb
import typer

from query_generator.duckdb_connection.setup import generate_db
from query_generator.extensions.fix_transform import fix_transform
from query_generator.extensions.llm_clients import (
  LLMClientFactory,
  OllamaLLMClient,
)
from query_generator.extensions.llm_extension import llm_extension
from query_generator.extensions.union_queries import union_queries
from query_generator.filter.filter import filter_synthetic_queries
from query_generator.logger import (
  default_logger,
)
from query_generator.metrics.get_metrics import get_metrics
from query_generator.synthetic_queries.synthetic_query_generator import (
  SyntheticQueriesParams,
  generate_synthetic_queries,
)
from query_generator.synthetic_queries.utils.query_writer import (
  write_parquet,
)
from query_generator.tools.histograms import (
  make_redundant_histograms,
  query_histograms,
)
from query_generator.utils.params import (
  ExtensionAndOllamaEndpoint,
  FilterEndpoint,
  FixTransformEndpoint,
  GenerateDBEndpoint,
  GetMetricsEndpoint,
  HistogramEndpoint,
  SyntheticQueriesEndpoint,
  get_toml_from_params,
  read_and_parse_toml,
)
from query_generator.utils.utils import (
  build_help_from_dataclass,
)

app = typer.Typer(name="Query Generation", rich_markup_mode="markdown")
logger = logging.getLogger(__name__)


def main() -> None:
  try:
    app(standalone_mode=False)
  except Exception:
    logger.exception("Unhandled exception during CLI execution.")
    raise


@app.command("generate-db", help=build_help_from_dataclass(GenerateDBEndpoint))
def generate_db_endpoint(
  config_path: Annotated[
    str,
    typer.Option("-c", "--config", help="The path to the configuration file"),
  ],
  *,
  debug: Annotated[
    bool,
    typer.Option(
      "-d",
      "--debug",
      help="Enable debug logging to file",
      is_flag=True,
      flag_value=True,
    ),
  ] = False,
) -> None:
  """Generates a DuckDB database with TPCDS or TPCH datasets.

  If the scale factor required is not generated, it will generate it.
  It returns a duckdb connection to the database.
  """
  params = read_and_parse_toml(
    Path(config_path),
    GenerateDBEndpoint,
  )
  default_logger(
    str(Path(params.db_path).parent),
    debug_file=debug,
    file_name="database_generation.log",
  )
  generate_db(params)


@app.command(
  "make-histograms", help=build_help_from_dataclass(HistogramEndpoint)
)
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
  *,
  debug: Annotated[
    bool,
    typer.Option(
      "-d",
      "--debug",
      help="Enable debug logging to file",
      is_flag=True,
      flag_value=True,
    ),
  ] = False,
) -> None:
  """This function is used to create histograms in parquet format."""
  params = read_and_parse_toml(Path(config_path), HistogramEndpoint)
  default_logger(
    params.output_folder, debug_file=debug, file_name="make_histograms.log"
  )
  destination_path = Path(params.output_folder) / "histogram.parquet"

  con = duckdb.connect(params.database_path, read_only=True)
  histograms_df = query_histograms(
    histogram_size=params.histogram_size,
    common_values_size=params.common_values_size,
    con=con,
    include_mcv=params.common_values_size > 0,
  )
  logger.info("Finished querying the database.")
  redundant_histogram_df = make_redundant_histograms(
    histograms_df, params.redundant_histogram_size
  )
  write_parquet(redundant_histogram_df, destination_path)
  logger.info("Parquet file saved.")


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
  *,
  debug: Annotated[
    bool,
    typer.Option(
      "-d",
      "--debug",
      help="Enable debug logging to file",
      is_flag=True,
      flag_value=True,
    ),
  ] = False,
) -> None:
  """This is an extension of the Snowflake algorithm.

  It runs multiple batches with different configurations of the algorithm.
  This allows us to get multiple results.
  """
  params = read_and_parse_toml(
    Path(config_path),
    SyntheticQueriesEndpoint,
  )
  default_logger(params.output_folder, debug_file=debug)
  con = duckdb.connect(database=params.duckdb_database, read_only=True)
  generate_synthetic_queries(
    SyntheticQueriesParams(
      con=con,
      user_input=params,
    ),
  )


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
  *,
  debug: Annotated[
    bool,
    typer.Option(
      "-d",
      "--debug",
      help="Enable debug logging to file",
      is_flag=True,
      flag_value=True,
    ),
  ] = False,
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
  default_logger(
    params.destination_folder,
    debug_file=debug,
    file_name="filter_synthetic.log",
  )
  filter_synthetic_queries(params)


@app.command(
  "extensions-with-ollama",
  help=build_help_from_dataclass(ExtensionAndOllamaEndpoint),
)
def extensions_with_ollama_endpoint(
  config_file: Annotated[
    str,
    typer.Option(
      "--config",
      "-c",
      help="The path to the configuration file with complex queries",
    ),
  ],
  *,
  debug: Annotated[
    bool,
    typer.Option(
      "-d",
      "--debug",
      help="Enable debug logging to file",
      is_flag=True,
      flag_value=True,
    ),
  ] = False,
) -> None:
  """Add complex queries using LLM prompts.
  The configuration file should be a TOML file with the
  ComplexQueryGenerationParametersEndpoint structure."""
  params = read_and_parse_toml(Path(config_file), ExtensionAndOllamaEndpoint)
  default_logger(params.destination_folder, debug_file=debug)
  cnt = 0
  if params.union_extension:
    assert params.union_params is not None
    logger.info("Starting Union extension")
    cnt += union_queries(
      Path(params.queries_parquet),
      Path(params.destination_folder),
      params.union_params.max_queries,
      params.union_params.probability,
    )
    logger.info("Union extension done")

  if params.llm_extension:
    assert params.ollama_model is not None
    assert params.llm_params is not None
    logger.info("Starting LLM extension")
    cnt += llm_extension(
      llm_params=params.llm_params,
      llm_client_factory=LLMClientFactory(
        factory=OllamaLLMClient, init_kwargs={}
      ),
      llm_config_params=params.ollama_model,
      input_queries_base_path=Path(params.queries_parquet).parent,
      destination_path=Path(params.destination_folder),
    )
    logger.info("LLM extension done")

  logger.info(f"Total extension queries generated: {cnt}.")
  toml_params = get_toml_from_params(params)
  (Path(params.destination_folder) / "extension_config.toml").write_text(
    toml_params
  )


@app.command(
  "fix-transform", help=build_help_from_dataclass(FixTransformEndpoint)
)
def add_limit_endpoint(
  config_file: Annotated[
    str,
    typer.Option(
      "--config",
      "-c",
      help="The path to the configuration file with complex queries",
    ),
  ],
  *,
  debug: Annotated[
    bool,
    typer.Option(
      "-d",
      "--debug",
      help="Enable debug logging to file",
      is_flag=True,
      flag_value=True,
    ),
  ] = False,
) -> None:
  params = read_and_parse_toml(Path(config_file), FixTransformEndpoint)
  default_logger(
    params.destination_folder, debug_file=debug, file_name="fix_transform.log"
  )
  fix_transform(params)
  get_metrics(params)
  toml_params = get_toml_from_params(params)
  (Path(params.destination_folder) / "fix_config.toml").write_text(
    toml_params
  )

@app.command("get-metrics", help=build_help_from_dataclass(GetMetricsEndpoint))
def get_metrics_endpoint(
  config_file: Annotated[
    str,
    typer.Option(
      "--config",
      "-c",
      help="The path to the configuration file with complex queries",
    ),
  ],
  *,
  debug: Annotated[
    bool,
    typer.Option(
      "-d",
      "--debug",
      help="Enable debug logging to file",
      is_flag=True,
      flag_value=True,
    ),
  ] = False,
) -> None:
  params = read_and_parse_toml(Path(config_file), GetMetricsEndpoint)
  default_logger(
    str(params.output_folder), debug_file=debug, file_name="fix_transform.log"
  )
  get_metrics(params)
  toml_params = get_toml_from_params(params)
  (Path(params.output_folder) / "metrics_config.toml").write_text(
    toml_params
  )

if __name__ == "__main__":
  main()
