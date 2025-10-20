import random
import re
from pathlib import Path

import polars as pl

from query_generator.utils.exceptions import InvalidQueryError
from query_generator.utils.utils import set_seed

MINIMUM_QUERIES_TO_UNION = 2


def get_select_list(query: str) -> str:
  match = re.search(r"SELECT (.*) FROM", query)
  if match is None:
    raise InvalidQueryError(query)
  return match.group(1)


def get_list_of_columns(select_list: str) -> list[str]:
  result = list(re.findall(r"COUNT\(([^*)]+)\)", select_list))
  if len(result) < MINIMUM_QUERIES_TO_UNION:
    raise InvalidQueryError(select_list)
  return result


def rename_select_list(columns: list[str]) -> str:
  return ",".join([f"{col} AS column_{cnt}" for cnt, col in enumerate(columns)])


def get_count_select_list(size: int) -> str:
  return ",".join(["COUNT(*)"] + [f"COUNT(column_{i})" for i in range(size)])


def get_new_query(sampled_query_paths: list[Path], probability: float) -> str:
  """Generate a new query by combining sampled queries.

  Args:
      sampled_query_paths (list[Path]): List of paths to the sampled queries.
      probability (float): The probability of using UNION instead of UNION ALL.
  """
  assert probability >= 0 and probability <= 1
  queries = [path.read_text() for path in sampled_query_paths]
  base_select_list = get_select_list(queries[0])
  columns = get_list_of_columns(base_select_list)
  new_select_list = rename_select_list(columns)
  use_union_all = random.random() > probability
  union_keyword = "UNION ALL" if use_union_all else "UNION"
  new_queries = [
    re.sub(r"SELECT (.*) FROM", f"SELECT {new_select_list} FROM", query)
    for query in queries
  ]
  return f"""\
WITH union_queries AS (\
{f" {union_keyword} ".join([f"({q})" for q in new_queries])}\
) \
SELECT {get_count_select_list(len(columns))} FROM union_queries
"""


def union_extension(
  parquet_path: Path,
  destination_path: Path,
  max_queries: int,
  probability: float,
) -> None:
  """Generate union queries from a parquet file of queries.

  Args:
      parquet_path (Path): The path to the parquet file with queries.
      destination_path (Path): The path to the destination folder for union
      queries.
      max_queries (int): The maximum number of queries to union.
      probability (float): The probability of using UNION instead of UNION ALL.
  """
  set_seed()
  df_input = pl.read_parquet(parquet_path)
  cnt = 0
  rows = []
  for join_signature, df_signature in df_input.group_by(
    "subgraph_signature", maintain_order=True
  ):
    queries_relative_paths = df_signature.get_column("relative_path").to_list()
    queries_paths = [
      Path(parquet_path.parent) / i for i in queries_relative_paths
    ]

    if len(queries_paths) < MINIMUM_QUERIES_TO_UNION:
      continue

    sampled_query_paths: list[Path] = random.sample(
      queries_paths,
      k=random.randint(
        MINIMUM_QUERIES_TO_UNION, min(max_queries, len(queries_paths))
      ),
    )
    new_query = get_new_query(sampled_query_paths, probability)
    new_query_path = (
      destination_path / "union" / f"union-{join_signature[0]}.sql"
    )
    new_query_path.parent.mkdir(parents=True, exist_ok=True)
    new_query_path.write_text(new_query)
    cnt += 1
    rows.append(
      {
        "relative_path": str(new_query_path.relative_to(destination_path)),
        "used_queries": [
          str(query.relative_to(parquet_path.parent))
          for query in sampled_query_paths
        ],
      }
    )
  df_output = pl.from_dicts(
    rows,
    schema={
      "relative_path": pl.Utf8,
      "used_queries": pl.List(pl.Utf8),
    },
  )
  df_output_path = destination_path / "union_description.parquet"
  df_output.write_parquet(df_output_path)
