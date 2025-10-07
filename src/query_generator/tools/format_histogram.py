from typing import Any

import polars as pl

from query_generator.tools.histograms import (
  HistogramColumns,
  MostCommonValuesColumns,
)


def get_mcv_as_str(mcv: dict[Any, str]) -> str:
  result = [
    f"{item[MostCommonValuesColumns.VALUE.value]} "
    f"(occurences:{item[MostCommonValuesColumns.COUNT.value]})"
    for item in mcv
  ]
  return f"[{', '.join(result)}]"


def get_histogram_as_str(histogram: pl.DataFrame) -> str:
  result = []
  for row in histogram.iter_rows(named=True):
    result.append(
      f"Table: {row[HistogramColumns.TABLE.value]},"
      f" attribute: {row[HistogramColumns.COLUMN.value]},"
      f" type: {row[HistogramColumns.DTYPE.value]},"
      f" distinct count: {row[HistogramColumns.DISTINCT_COUNT.value]},"
      f" table size: {row[HistogramColumns.TABLE_SIZE.value]},"
      # f" most common values: "
      # f"{get_mcv_as_str(row[HistogramColumns.MOST_COMMON_VALUES.value])}"
    )
  return "\n\n".join(result)
