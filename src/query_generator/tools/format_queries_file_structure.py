import json
from collections import defaultdict
from pathlib import Path

import polars as pl

from query_generator.utils.exceptions import OverwriteFileError


def format_queries_file_structure(
  *, src_folder_path: Path, dst_folder_path: Path
) -> None:
  src_relative_paths: list[str] = []
  dst_relative_paths: list[str] = []
  query_dict: dict[str, dict[str, str]] = defaultdict(dict)

  for src_template_folder in src_folder_path.iterdir():
    if not src_template_folder.is_dir():
      continue
    # Iterate in alphabetical order
    # to ensure the same order of queries
    files_in_alphabetical_order = sorted(
      src_template_folder.iterdir(), key=lambda f: f.name
    )
    for idx, file in enumerate(files_in_alphabetical_order):
      query = file.read_text()
      new_query_id = idx + 1
      # the code works with 1 indexing because reasons (?)
      new_path = (
        dst_folder_path
        / src_template_folder.name
        / f"{src_template_folder.name}-{new_query_id}.sql"
      )
      new_path.parent.mkdir(parents=True, exist_ok=True)

      if new_path.exists():
        raise OverwriteFileError(new_path)

      query_dict[src_template_folder.name][str(new_query_id)] = query
      new_path.write_text(query)
      src_relative_paths.append(str(file.relative_to(src_folder_path)))
      dst_relative_paths.append(str(new_path.relative_to(dst_folder_path)))
  pl.DataFrame(
    {
      "original_name": src_relative_paths,
      "new_name": dst_relative_paths,
    }
  ).with_columns(
    [
      pl.col("original_name").cast(pl.Utf8),
      pl.col("new_name").cast(pl.Utf8),
    ]
  ).write_parquet(str(dst_folder_path / "mapping.parquet"))
  query_dict_path = dst_folder_path / "queries.json"
  query_dict_path.write_text(json.dumps(query_dict, indent=2))
