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
def binning(
  dev: Annotated[
    bool,
    typer.Option(
      "--dev",
      help="Development testing. If true then uses scale factor 1 to check.",
    ),
  ] = False,
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
  if dev:
    print(
      "Running on [red] development mode [/red]"
      "This means that results are not valid and only for testing"
      "purposes"
    )
  else:
    print(
      "Running on [green] production mode [/green]"
      "This means that results are valid and for testing"
      "purposes"
    )
  # setup_duckdb
  # loop as fuck


if __name__ == "__main__":
  app()
