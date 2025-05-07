from pathlib import Path

import polars as pl

from query_generator.utils.exceptions import OverwriteFileError


def format_queries_file_structure(
  *, src_folder_path: Path, dst_folder_path: Path
) -> None:
  src_relative_paths = []
  dst_relative_paths = []
  for subfolder in src_folder_path.iterdir():
    if not subfolder.is_dir():
      continue
    # Iterate in alphabetical order
    # to ensure the same order of queries
    files_in_alphabetical_order = sorted(
      subfolder.iterdir(), key=lambda f: f.name
    )
    for idx, file in enumerate(files_in_alphabetical_order):
      query = file.read_text()
      # the code works with 1 indexing because reasons (?)
      new_path = (
        dst_folder_path / subfolder.name / f"{subfolder.name}-{idx + 1}.sql"
      )
      new_path.parent.mkdir(parents=True, exist_ok=True)
      if new_path.exists():
        raise OverwriteFileError(new_path)
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
  ).write_csv(str(dst_folder_path / "mapping.csv"))
