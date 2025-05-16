import random
from pathlib import Path


def set_seed() -> None:
  """Set the seed for random number generation."""
  seed = 80
  random.seed(seed)


def validate_file_path(path: Path) -> None:
  """Validate if the given path is a valid file."""
  if not path.is_file():
    raise FileNotFoundError(path)
  if not path.exists():
    raise FileNotFoundError(path)
  if path.is_dir():
    raise IsADirectoryError(path)
