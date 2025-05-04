import random
from pathlib import Path


def set_seed() -> None:
  """Set the seed for random number generation."""
  seed = 80
  random.seed(seed)


def validate_dir_path(path: Path) -> None:
  """Validate if the given path is a valid directory."""
  if not isinstance(path, Path):
    raise ValueError(f"Path {path} is not a valid Path object.")
  if not path.is_file():
    raise ValueError(f"Path {path} is not a valid file.")
