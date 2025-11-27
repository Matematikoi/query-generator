import logging
import random
from enum import StrEnum
from pathlib import Path

import duckdb
import polars as pl
from cattrs import unstructure
from sqlglot import exp, parse_one
from sqlglot.expressions import Expression
from tqdm import tqdm

from query_generator.duckdb_connection.trace_collection import (
  DuckDBTraceOuputDataFrameRow,
  DuckDBTraceParams,
  duckdb_collect_one_trace,
)
from query_generator.utils.exceptions import (
  ColumnNotFoundError,
  NoColumnAlternativeError,
)
from query_generator.utils.params import FixTransformEndpoint

logger = logging.getLogger(__name__)
CTE_NAME = "cte_for_limit"


class TransformEnum(StrEnum):
  """Rows for DuckDBTraceOuputDataFrameRow."""

  relative_path = "relative_path"
  error_group_by_sqlglot = "error_group_by_sqlglot"
  was_transformed = "was_transformed"
  original_query = "original_query"
  new_query = "new_query"


class TransformationCount(StrEnum):
  COUNT = "COUNT"
  MAX = "MAX "
  MIN = "MIN "
  DISTINCT = "DISTINCT"


def get_duckdb_schema(duckdb_database: str) -> dict[str, dict[str, str]]:
  """Gets the schema from a given database"""
  conn = duckdb.connect(database=duckdb_database, read_only=True)
  rows = conn.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN (
                      'information_schema',
                      'pg_catalog',
                      'duckdb_internal'
                      )
        ORDER BY table_name, ordinal_position
    """).fetchall()
  conn.close()

  out, ints, texts = (
    {},
    {
      "TINYINT",
      "SMALLINT",
      "INTEGER",
      "BIGINT",
      "HUGEINT",
      "UTINYINT",
      "USMALLINT",
      "UINTEGER",
      "UBIGINT",
    },
    {"VARCHAR", "CHAR", "BPCHAR", "TEXT"},
  )

  for tbl, col, typ in rows:
    t = typ.upper()
    if t in ints:
      t = "INT"
    elif t in texts:
      t = "TEXT"
    elif t.startswith("DECIMAL") or t.startswith("NUMERIC"):
      t = "DECIMAL"
    out.setdefault(tbl, {})[col] = t
  return out


def wrap_query_with_limit(sql: str, limit: int) -> str:
  original: exp.Expression = parse_one(sql)

  cte_alias = exp.TableAlias(this=exp.to_identifier(CTE_NAME))
  cte = exp.CTE(this=original.copy(), alias=cte_alias)
  with_clause = exp.With(expressions=[cte])

  outer_select = exp.select("*").from_(exp.to_table(CTE_NAME)).limit(limit)

  outer_select.set("with", with_clause)

  return outer_select.sql(pretty=True)


def get_only_columns_in_select(tree: Expression):
  cols = []
  select = tree.find(exp.Select)
  if select:
    for proj in select.expressions or []:
      for col in proj.find_all(exp.Column):
        col_sql = col.sql()
        if "*" not in col_sql:
          cols.append(col_sql)
  return cols


def get_group_by_attributes(tree: exp.Expression):
  group_clause = tree.find(exp.Group)
  assert group_clause is not None
  columns = group_clause.find_all(exp.Column)
  assert columns is not None
  return [i.sql() for i in columns]


def get_select_attributes(tree: exp.Expression):
  select_clause = tree.find(exp.Select)
  assert select_clause is not None
  return [i.sql() for i in select_clause if "*" not in i.sql()]


def retrieve_column_name(table_dot_columns: list[str]) -> list[str]:
  result = []
  for t in table_dot_columns:
    if "." in table_dot_columns:
      result.append(t.split(".")[1])
    else:
      result.append(t)
  return result


def get_subquery(tree: exp.Expression, column: str) -> str:
  select_values = get_select_attributes(tree)
  for select_value in select_values:
    if column in select_value:
      return select_value
  raise ColumnNotFoundError(column)


def get_table_from_column(
  col: str, schema: dict[str, dict[str, str]]
) -> str | None:
  search_col = col.split(".")[1] if "." in col else col
  for table_name in schema:  # noqa: PLC0206
    if search_col.lower() in schema[table_name]:
      return table_name
  return None


def change_select_attribute(
  sql: str, sub_sql: str, new_column: str, old_column: str
) -> str:
  if any(
    keyword in sub_sql.lower()
    for keyword in ["order by", "grouping(", " over "]
  ):
    return sql
  old_column = old_column.split(".")[1] if "." in old_column else old_column
  new_sub_sql = sub_sql.replace(old_column, new_column)
  return sql.replace(sub_sql, new_sub_sql)


def get_repeated_columns(tree: exp.Expression) -> list[str]:
  select_columns = get_only_columns_in_select(tree)
  group_by_columns = get_group_by_attributes(tree)
  repeated_columns = set(retrieve_column_name(select_columns)).intersection(
    set(retrieve_column_name(group_by_columns))
  )
  return list(repeated_columns)


def get_different_column(
  table: str,
  select_columns: list[str],
  group_by_columns: list[str],
  schema: dict[str, dict[str, str]],
) -> str:
  for col in list(schema[table].keys())[::-1]:
    if not any(col in s for s in (select_columns + group_by_columns)):
      return col
  raise NoColumnAlternativeError()


def make_select_group_by_clause_disjoint(
  query: str, schema: dict[str, dict[str, str]]
) -> tuple[str, Exception | None]:
  """Disjoint the select and group by clause."""
  try:
    tree = parse_one(query)
    if tree.find(exp.Group) is not None:
      for repeated_column in get_repeated_columns(tree):
        table = get_table_from_column(repeated_column, schema)
        if table is None:
          continue
        new_column = get_different_column(
          table,
          get_select_attributes(tree),
          get_group_by_attributes(tree),
          schema,
        )
        sub_sql = get_subquery(tree, repeated_column)
        query = change_select_attribute(
          query, sub_sql, new_column, repeated_column
        )
  except Exception as e:
    logger.warning("Failed to make select and group by disjoint")
    logger.debug(f"Query that failed:\n{query}")
    return query, e
  return query, None


def get_transformation(*, is_numeric: bool) -> TransformationCount:
  possibilites = [TransformationCount.COUNT, TransformationCount.DISTINCT]
  if is_numeric:
    possibilites.append(TransformationCount.MIN)
    possibilites.append(TransformationCount.MAX)
  return random.choice(possibilites)


def replace_min_max(sql: str, schema: dict[str, dict[str, str]]) -> str:
  root = parse_one(sql)

  select = root.find(exp.Select)
  if not select:
    return root.sql()

  def transformer(node: exp.Expression) -> exp.Expression:  # noqa: PLR0911
    if not isinstance(node, exp.Count):
      return node

    arg = node.this
    if isinstance(arg, exp.Star):
      return node

    name = (
      arg.name
      if isinstance(arg, exp.Column)
      else (arg.this if isinstance(arg, exp.Identifier) else None)
    )
    if not name:
      return node

    table = get_table_from_column(name, schema)
    if table is None or "distinct" in node.sql().lower():
      return node
    is_numeric = any(
      keyword in schema[table][name.lower()] for keyword in ["INT", "DECIMAL"]
    )
    transformation = get_transformation(is_numeric=is_numeric)
    if transformation == TransformationCount.COUNT:
      return node
    if transformation == TransformationCount.DISTINCT:
      return exp.Count(this=exp.Distinct(expressions=[arg.copy()]))
    if transformation == TransformationCount.MIN:
      return exp.Min(this=arg.copy())
    if transformation == TransformationCount.MAX:
      return exp.Max(this=arg.copy())

    return node

  # Apply only within SELECT's expressions
  select.set(
    "expressions",
    [proj.transform(transformer) for proj in select.expressions],
  )

  return root.sql(pretty=True)


def get_trace_from_transform(
  query: str, query_path: Path, params: FixTransformEndpoint
) -> tuple[DuckDBTraceOuputDataFrameRow, bool]:
  trace_params = DuckDBTraceParams(
    queries_path=params.queries_folder,
    duckdb_path=params.duckdb_database,
    timeout_seconds=params.timeout_seconds,
    fetch_limit=params.max_output_size,
    output_folder=params.destination_folder,
  )

  trace = duckdb_collect_one_trace(query, query_path, trace_params)
  if trace.trace_success:
    return trace, True
  # Transformation failed, fall back to previous query
  return duckdb_collect_one_trace(
    query_path.read_text(), query_path, trace_params
  ), False


def fix_transform(params: FixTransformEndpoint) -> None:
  """Add LIMIT to sql queries according to output size."""
  random.seed(42)
  queries_folder: Path = Path(params.queries_folder)
  destination_folder = Path(params.destination_folder)
  queries_paths = list(queries_folder.glob("**/*.sql"))
  rows = []

  schema = get_duckdb_schema(params.duckdb_database)
  traces = []
  for query_path in tqdm(queries_paths, total=len(queries_paths)):  # type: ignore
    query = query_path.read_text()
    query, exception_group_by = make_select_group_by_clause_disjoint(
      query, schema
    )

    query = replace_min_max(query, schema)

    trace, transformation_success = get_trace_from_transform(
      query, query_path, params
    )
    traces.append(trace)

    if len(trace.duckdb_output) > params.max_output_size:
      query = wrap_query_with_limit(query, params.max_output_size)

    new_query_path = destination_folder / query_path.relative_to(queries_folder)
    new_query_path.parent.mkdir(parents=True, exist_ok=True)
    new_query_path.write_text(query)
    rows.append(
      {
        TransformEnum.relative_path: str(
          Path(query_path).relative_to(queries_folder)
        ),
        TransformEnum.error_group_by_sqlglot: str(exception_group_by)
        if exception_group_by is not None
        else "",
        TransformEnum.original_query: query_path.read_text(),
        TransformEnum.new_query: query,
        TransformEnum.was_transformed: transformation_success,
      }
    )
  df_traces = pl.DataFrame([unstructure(t) for t in traces])
  df_traces.write_parquet(destination_folder / "traces_duckdb.parquet")
  df_transformation = pl.DataFrame(rows)
  df_transformation.write_parquet(
    destination_folder / "transformation_log.parquet"
  )
  logger.info(f"Total queries processed: {len(queries_paths)}.")
  logger.info(
    f"Total queries succesfully transformed: {
      df_transformation.filter(pl.col(TransformEnum.was_transformed)).height
    }."
  )
  logger.info(f"Total traces collected: {df_traces.height}.")
