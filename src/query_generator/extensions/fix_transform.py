from enum import StrEnum
from pathlib import Path
from query_generator.utils.params import FixTransformEndpoint
from sqlglot import exp, parse_one
import polars as pl
from tqdm import tqdm
import random
import duckdb

CTE_NAME = "cte_for_limit"

class DuckDBTraceEnum(StrEnum):
    """Rows for DuckDBTraceOuputDataFrameRow."""

    relative_path = "relative_path"
    query_folder = "query_folder"
    query_name = "query_name"
    duckdb_trace = "duckdb_trace"
    error = "error"
    trace_success = "trace_success"
    duckdb_output = "duckdb_output"
    error_group_by_sqlglot = "error_group_by_sqlglot"
class TransformationCount(StrEnum):
    COUNT = "COUNT"
    MAX  = "MAX "
    MIN  = "MIN "
    DISTINCT = "DISTINCT"

def get_duckdb_schema(conn: duckdb.DuckDBPyConnection) -> dict[str, dict[str, str]]:
    rows = conn.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('information_schema','pg_catalog','duckdb_internal')
        ORDER BY table_name, ordinal_position
    """).fetchall()

    out, ints, texts = {}, {
        'TINYINT','SMALLINT','INTEGER','BIGINT','HUGEINT',
        'UTINYINT','USMALLINT','UINTEGER','UBIGINT'
    }, {'VARCHAR','CHAR','BPCHAR','TEXT'}

    for tbl, col, typ in rows:
        t = typ.upper()
        if t in ints:
            t = 'INT'
        elif t in texts:
            t = 'TEXT'
        elif t.startswith('DECIMAL') or t.startswith('NUMERIC'):
            t = 'DECIMAL'
        out.setdefault(tbl, {})[col] = t
    return out


def wrap_query_with_limit(sql:str, limit:int) -> str:
    original: exp.Expression = parse_one(sql)

    cte_alias = exp.TableAlias(this=exp.to_identifier(CTE_NAME))
    cte = exp.CTE(this=original.copy(), alias=cte_alias)
    with_clause = exp.With(expressions=[cte])

    outer_select = (
        exp.select("*")
        .from_(exp.to_table(CTE_NAME))
        .limit(limit)
    )

    outer_select.set("with", with_clause)

    return outer_select.sql(pretty=True)

def get_only_columns_in_select(tree):
    cols = []
    select = tree.find(exp.Select)
    if select:
        for proj in select.expressions or []:
            for col in proj.find_all(exp.Column):
                col_sql = col.sql()
                if "*" not in col_sql:
                    cols.append(col_sql)
    return cols

def get_group_by_attributes(tree):
    return [i.sql() for i in tree.find(exp.Group).find_all(exp.Column)]


def get_select_attributes(tree):
    return [i.sql() for i in tree.find(exp.Select) if "*" not in i.sql()]

def retrieve_column_name(table_dot_columns):
    result = []
    for t in table_dot_columns:
        if "." in table_dot_columns:
            result.append(t.split(".")[1])
        else:
            result.append(t)
    return result

def get_subquery(tree, column):
    select_values = get_select_attributes(tree)
    for select_value in select_values:
        if column in select_value:
            return select_value
    raise KeyError("Column not found in select attributes")

def get_table_from_column(col: str, schema:dict[str:dict[str, str]]) -> str:
    search_col = col.split(".")[1] if "." in col else col
    for table_name in schema:
        if search_col.lower() in schema[table_name].keys():
            return table_name
    return None

def change_select_attribute(sql, sub_sql, new_column, old_column):
    if any(
        keyword in sub_sql.lower() for keyword in ["order by", "grouping(", " over "]
    ):
        return sql
    old_column = old_column.split(".")[1] if "." in old_column else old_column
    new_sub_sql = sub_sql.replace(old_column, new_column)
    return sql.replace(sub_sql, new_sub_sql)


def get_repeated_columns(tree):
    select_columns = get_only_columns_in_select(tree)
    group_by_columns = get_group_by_attributes(tree)
    repeated_columns = set(retrieve_column_name(select_columns)).intersection(
        set(retrieve_column_name(group_by_columns))
    )
    return list(repeated_columns)

def get_different_column(table, select_columns, group_by_columns, schema:dict[str:dict[str, str]]=None)-> str:
    for col in list(schema[table].keys())[::-1]:
        if not any(col in s for s in (select_columns + group_by_columns)):
            return col
    raise KeyError("No different column found in table")


def make_select_group_by_clause_disjoint(query:str, schema:dict[str:dict[str, str]])-> tuple[str, Exception]:
    """Disjoint the select and group by clause."""
    try:
        tree = parse_one(query)
        if tree.find(exp.Group) is not None:
            for repeated_column in get_repeated_columns(tree):
                table = get_table_from_column(repeated_column, schema)
                new_column = get_different_column(
                    table,
                    get_select_attributes(tree),
                    get_group_by_attributes(tree),
                    schema
                )
                sub_sql = get_subquery(tree, repeated_column)
                query = change_select_attribute(
                    query, sub_sql, new_column, repeated_column
                )
    except Exception as e:
        return query, e
    return query, None

def get_transformation(*,is_numeric:bool):
    possibilites = [TransformationCount.COUNT, TransformationCount.DISTINCT]
    if is_numeric:
        possibilites.append(TransformationCount.MIN)
        possibilites.append(TransformationCount.MAX)
    return random.choice(possibilites)



def replace_min_max(sql: str, schema: dict[str, dict[str, str]] = None) -> str:
    root = parse_one(sql)

    select = root.find(exp.Select)
    if not select:
        return root.sql()

    def transformer(node: exp.Expression) -> exp.Expression:
        if not isinstance(node, exp.Count):
            return node

        arg = node.this  
        if isinstance(arg, exp.Star):
            return node  

        name = (
            arg.name if isinstance(arg, exp.Column)
            else (arg.this if isinstance(arg, exp.Identifier) else None)
        )
        if not name:
            return node

        table = get_table_from_column(name, schema)
        if table is None or 'distinct' in node.sql().lower():
            return node
        is_numeric = any(keyword in schema[table][name.lower()] for keyword in ['INT', 'DECIMAL'] )
        transformation = get_transformation(is_numeric=is_numeric)
        if transformation == TransformationCount.COUNT:
            return node
        elif transformation == TransformationCount.DISTINCT:
            return  exp.Count(this=exp.Distinct(expressions=[arg.copy()]))
        elif transformation == TransformationCount.MIN:
            return exp.Min(this=arg.copy())
        elif transformation == TransformationCount.MAX:
            return exp.Max(this=arg.copy())
        
        return node

    # Apply only within SELECT's expressions
    select.set(
        "expressions",
        [proj.transform(transformer) for proj in select.expressions],
    )

    return root.sql(pretty = True)



def fix_transform(params: FixTransformEndpoint) -> None:
    """Add LIMIT to sql queries according to output size."""
    random.seed(42)
    queries_folder: Path = Path(params.queries_folder)
    destination_folder = Path(params.destination_folder)
    queries_paths = list(queries_folder.glob('**/*.sql'))
    rows = []
    con = duckdb.connect(database=params.duckdb_database, read_only=True)
    schema = get_duckdb_schema(con)
    for query_path in tqdm(queries_paths, total=len(queries_paths)):
        query = query_path.read_text()
        query, exception_group_by = make_select_group_by_clause_disjoint(query, schema)

        query = replace_min_max(query, schema)
        query = wrap_query_with_limit(query, params.max_output_size)

        new_query_path = destination_folder / query_path.relative_to(queries_folder)
        new_query_path.parent.mkdir(parents=True, exist_ok=True)
        new_query_path.write_text(query)
        rows.append({
            DuckDBTraceEnum.relative_path :str( Path(query_path).relative_to(queries_folder)),
            DuckDBTraceEnum.error_group_by_sqlglot : str(exception_group_by) if exception_group_by is not None else "",
            "original": query_path.read_text(),
            "new_query": query

        })
    df_transformation = pl.DataFrame(rows)
    df_transformation.write_parquet(destination_folder/'transformation_log.parquet')