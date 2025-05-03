import os
import random


def set_seed() -> None:
  seed = 80
  random.seed(seed)


def validate_dir_path(path: str) -> None:
  """Validate if the given path is a valid directory."""
  if not os.path.isdir(path):
    raise ValueError(f"Path {path} is not a valid directory.")
  if not os.path.exists(path):
    raise ValueError(f"Path {path} does not exist.")
