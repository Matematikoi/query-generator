from enum import StrEnum
from pathlib import Path
from query_generator.utils.params import FixTransformEndpoint
from sqlglot import exp, parse_one
import polars as pl
from tqdm import tqdm
class DuckDBTraceEnum(StrEnum):
    """Rows for DuckDBTraceOuputDataFrameRow."""

    relative_path = "relative_path"
    query_folder = "query_folder"
    query_name = "query_name"
    duckdb_trace = "duckdb_trace"
    error = "error"
    trace_success = "trace_success"
    duckdb_output = "duckdb_output"


CTE_NAME = "cte_for_limit"



def wrap_query_with_limit(sql:str, limit:int) -> str:
    original: exp.Expression = parse_one(sql)

    # Build WITH cte_name AS (<original>)
    cte_alias = exp.TableAlias(this=exp.to_identifier(CTE_NAME))
    cte = exp.CTE(this=original.copy(), alias=cte_alias)
    with_clause = exp.With(expressions=[cte])

    # Build: SELECT * FROM cte_name LIMIT <limit>
    outer_select = (
        exp.select("*")
        .from_(exp.to_table(CTE_NAME))
        .limit(limit)
    )

    # Attach the WITH clause to the outer SELECT
    outer_select.set("with", with_clause)

    return outer_select.sql(pretty=True)

def add_limit(params: FixTransformEndpoint) -> None:
    """Add LIMIT to sql queries according to output size."""
    df_traces = pl.read_parquet(params.traces_parquet)
    queries_folder: Path = Path(params.queries_folder)
    destination_folder = Path(params.destination_folder)
    
    for row in tqdm(df_traces.iter_rows(named=True), total=len(df_traces)):
        query_path = queries_folder / row[DuckDBTraceEnum.relative_path]
        query = query_path.read_text()
        if len(list((row[DuckDBTraceEnum.duckdb_output]))) > params.max_output_size:
            query = wrap_query_with_limit(query, params.max_output_size)
        new_query_path = destination_folder / row[DuckDBTraceEnum.relative_path]
        new_query_path.parent.mkdir(parents=True, exist_ok=True)
        new_query_path.write_text(query)