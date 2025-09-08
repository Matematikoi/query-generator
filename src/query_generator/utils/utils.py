import inspect
import random
import re
from dataclasses import MISSING, fields
from pathlib import Path
from typing import Any, get_type_hints


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


def no_rewrap(s: str) -> str:
  """Preserve formatting of a string in markdown."""
  # Preserve formatting for all paragraphs
  s = s.strip()
  return "\b\n" + s.replace("\n\n", "\n\n\b\n")


def markdown_hard_breaks(s: str) -> str:
  """add two spaces at EOL for every single newline"""
  return re.sub(r"(?<!\n)\n(?!\n)", "  \n", s)


def build_help_from_dataclass(cls: Any) -> str:
  """Adds markdown documentation for typer CLI from dataclass."""
  doc = markdown_hard_breaks(inspect.getdoc(cls) or "")
  hints = get_type_hints(cls)
  lines = [doc, "", "Parameters summary:"]
  for field in fields(cls):
    type = hints.get(field.name, field.type)
    type_name = getattr(type, "__name__", str(type))
    if field.default is MISSING:
      lines.append(f"- {field.name} ({type_name}, required)")
    else:
      lines.append(f"- {field.name} ({type_name}, default={field.default})")
  return no_rewrap("\n".join(lines))
