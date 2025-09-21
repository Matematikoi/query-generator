from pathlib import Path

import polars as pl
from traitlets import Any

from query_generator.utils.params import get_toml_from_params


def write_to_file(file_path: Path, content: str) -> None:
  """Write content to a file, creating parent directories if needed."""
  file_path.parent.mkdir(parents=True, exist_ok=True)
  file_path.write_text(content)


def write_to_parquet(file_path: Path, df: pl.DataFrame) -> None:
  """Write a DataFrame to a parquet file, creating parent directories if needed."""
  file_path.parent.mkdir(parents=True, exist_ok=True)
  df.write_parquet(file_path)


def write_to_toml(file_path: Path, params: Any) -> None:
  toml_content = get_toml_from_params(params)
  write_to_file(file_path, toml_content)
