import random
import re
from pathlib import Path

from query_generator.utils.exceptions import InvalidQueryError
from query_generator.utils.utils import set_seed

MINIMUM_QUERIES_TO_UNION = 2


def get_new_query(sampled_query_paths: list[Path]) -> str:
  """Generate a new query by combining sampled queries."""
  queries = [path.read_text() for path in sampled_query_paths]
  match = re.search(r"SELECT (.*) FROM", queries[0])
  if match is None:
    raise InvalidQueryError(queries[0])
  base_sql_statement = match.group(1)
  new_queries = [
    re.sub(r"SELECT (.*) FROM", f"SELECT {base_sql_statement} FROM", query)
    for query in queries
  ]
  return " UNION ALL ".join(new_queries)


def union_queries(
  parquet_path: Path, destination_path: Path, max_queries: int
) -> None:
  set_seed()
  path_to_dirs = parquet_path.parent
  cnt = 0
  for dir in path_to_dirs.iterdir():
    if dir.is_dir():
      query_files = list(dir.glob("*.sql"))

      if len(query_files) < MINIMUM_QUERIES_TO_UNION:
        continue

      sampled_query_paths = random.sample(
        query_files,
        k=random.randint(
          MINIMUM_QUERIES_TO_UNION, min(max_queries, len(query_files))
        ),
      )

      new_query = get_new_query(sampled_query_paths)
      new_query_path = destination_path / "union" / f"union-{cnt}.sql"
      new_query_path.parent.mkdir(parents=True, exist_ok=True)
      new_query_path.write_text(new_query)
      cnt += 1
