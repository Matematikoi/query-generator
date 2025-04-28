import typer
from rich import print
from typing_extensions import Annotated

from query_generator.join_based_query_generator.snowflake import (
  generate_and_write_queries,
)
from query_generator.utils.definitions import (
  Dataset,
  QueryGenerationParameters,
)

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
def duckdb_setup() -> None:
  """
  Setup DuckDB for query generation.
  """
  print("DuckDB setup is not implemented yet.")


if __name__ == "__main__":
  app()
