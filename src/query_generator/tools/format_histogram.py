from typing import Any

import polars as pl

from query_generator.tools.histograms import (
  HistogramColumns,
  MostCommonValuesColumns,
)


def get_mcv_as_str(mcv: dict[Any, str]) -> str:
  result = [
    f"{item[MostCommonValuesColumns.VALUE]} "
    f"(occurences:{item[MostCommonValuesColumns.COUNT]})"
    for item in mcv
  ]
  return f"[{', '.join(result)}]"


def get_histogram_as_str(histogram: pl.DataFrame) -> str:
  result = [
    f"Table: {row[HistogramColumns.TABLE]},"
    f" attribute: {row[HistogramColumns.COLUMN]},"
    f" type: {row[HistogramColumns.DTYPE]},"
    f" distinct count: {row[HistogramColumns.DISTINCT_COUNT]},"
    f" table size: {row[HistogramColumns.TABLE_SIZE]},"
    # f" most common values: "
    # f"{get_mcv_as_str(row[HistogramColumns.MOST_COMMON_VALUES.value])}"
    for row in histogram.iter_rows(named=True)
  ]
  return "\n\n".join(result)
