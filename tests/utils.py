from datetime import datetime

from query_generator.utils.definitions import Dataset
from pathlib import Path


def is_float(s: str) -> bool:
  try:
    float(s)
    return True
  except ValueError:
    return False


def is_int(s: str) -> bool:
  try:
    int(s)
    return True
  except ValueError:
    return False


def is_date(s: str) -> bool:
  try:
    datetime.strptime(s, "%Y-%m-%d")
    return True
  except ValueError:
    return False


def get_precomputed_histograms(dataset: Dataset):
  base_path = Path(__file__).parent.parent
  match dataset:
    case Dataset.TPCDS:
      return base_path / "data/histograms/histogram_tpcds.parquet"
    case Dataset.TPCH:
      return base_path / "data/histograms/histogram_tpch.parquet"
    case Dataset.JOB:
      return base_path / "data/histograms/histogram_job.parquet"
