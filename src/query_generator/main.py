import typer
from rich import print
from typing_extensions import Annotated

from query_generator.join_based_query_generator.snowflake import (
  run_snowflake_generator,
)
from query_generator.utils.definitions import BenchmarkType

app = typer.Typer(name="Query Generation")


@app.command()
def snowflake(
  benchmark: Annotated[
    BenchmarkType, typer.Option("--dataset", "-d", help="The dataset used")
  ],
  max_hops: Annotated[
    int,
    typer.Option(
      "--max-hops", "-h", help="The maximum number of hops", min=1, max=5
    ),
  ],
  max_queries_per_template: Annotated[
    int,
    typer.Option(
      "--queries",
      "-q",
      help="The maximum number of queries per template",
      min=1,
    ),
  ],
) -> None:
  """
  Generate queries using a random subgraph.

  """
  run_snowflake_generator(benchmark, max_hops, max_queries_per_template)


@app.command()
def duckdb_setup() -> None:
  """
  Setup DuckDB for query generation.
  """
  print("DuckDB setup is not implemented yet.")


if __name__ == "__main__":
  app()
