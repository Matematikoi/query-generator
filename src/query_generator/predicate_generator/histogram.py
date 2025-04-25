import math
import random
from dataclasses import dataclass
from typing import Iterator, List, Tuple

import pandas as pd

from query_generator.utils.definitions import Dataset


class PredicateGenerator:
  @dataclass
  class Predicate:
    table: str
    column: str
    min_value: float | int
    max_value: float | int

  def __init__(self, dataset: Dataset):
    self.dataset = dataset
    self.histogram: pd.DataFrame = self.read_histogram()

  def read_histogram(self) -> pd.DataFrame:
    """
    Read the histogram data for the specified dataset.
    Args:
        dataset: The dataset type (TPCH or TPCDS).
    Returns:
        pd.DataFrame: DataFrame containing the histogram data.
    """

    if self.dataset == Dataset.TPCH:
      path = "data/histograms/raw_tpch_hist.csv"
    elif self.dataset == Dataset.TPCDS:
      path = "data/histograms/raw_tpcds_hist.csv"
    else:
      raise ValueError(f"Unsupported dataset histogram: {self.dataset}")
    # Remove rows with empty bins or that are dates
    df = pd.read_csv(path)

    df = df[(df["bins"] != "[]") & (df["dtype"] != "date")]
    return df

  def get_random_predicates(
    self,
    tables: List[str],
    num_predicates: int,
    row_retention_probability: float = 0.2,
  ) -> Iterator["PredicateGenerator.Predicate"]:
    """
    Generate random predicates based on the histogram data.
    Args:
        tables (str): List of tables to select predicates from.
        num_predicates (int): Number of predicates to generate.
        row_retention_probability (float): Probability of retaining rows.
    Returns:
        List[PredicateGenerator.Predicate]: List of generated predicates.
    """
    selected_tables_histogram = self.histogram[
      self.histogram["table"].isin(tables)
    ]

    for _, row in selected_tables_histogram.sample(num_predicates).iterrows():
      table = row["table"]
      column = row["column"]
      bins = row["bins"]
      min_value, max_value = self._get_min_max_from_bins(
        bins, row_retention_probability
      )
      predicate = PredicateGenerator.Predicate(
        table=table, column=column, min_value=min_value, max_value=max_value
      )
      yield predicate

  def _get_min_max_from_bins(
    self, bins: str, row_retention_probability: float
  ) -> Tuple[float | int, float | int]:
    """
    Convert the bins string representation to a tuple of min and max values.
    Args:
        bins (str): String representation of bins.
        row_retention_probability (float): Probability of retaining rows.
    Returns:
        tuple: Tuple containing min and max values.
    """
    number_array: List[int | float] = eval(bins)
    subrange_length = math.ceil(row_retention_probability * len(number_array))
    start_index = random.randint(0, len(number_array) - subrange_length)

    min_value = number_array[start_index]
    max_value = number_array[
      min(start_index + subrange_length, len(number_array) - 1)
    ]
    return min_value, max_value
