from pathlib import Path

MINIMUM_SIZE_OF_HISTOGRAMS = 2


class InvalidHistogramError(Exception):
  def __init__(self, size: int) -> None:
    super().__init__(
      f"Histogram has size {size} < "
      f"{MINIMUM_SIZE_OF_HISTOGRAMS} that is the"
      "minimum size of a valid histogram"
    )


class GraphExploredError(Exception):
  def __init__(self, attempts: int) -> None:
    super().__init__(f"Graph has been explored {attempts} times.")


class TableNotFoundError(Exception):
  def __init__(self, table_name: str) -> None:
    super().__init__(f"Table {table_name} not found in schema.")


class DuplicateEdgesError(Exception):
  def __init__(self, table: str) -> None:
    super().__init__(f"Duplicate edges found for table {table}.")


class UnkwonDatasetError(Exception):
  def __init__(self, dataset: str) -> None:
    super().__init__(f"Unknown dataset: {dataset}")


class MissingScaleFactorError(Exception):
  def __init__(self, dataset: str) -> None:
    super().__init__(
      f"Scale factor is required for dataset {dataset} but not provided."
    )


class InvalidForeignKeyError(Exception):
  def __init__(self, table: str, column: str) -> None:
    super().__init__(
      "Invalid foreign key reference in table {table} for column {column}",
    )


class InvalidUpperBoundError(Exception):
  def __init__(self, lower_bound: int, upper_bound: int) -> None:
    super().__init__(
      f"The lower bound {lower_bound} "
      f"is greater than the upper bound {upper_bound}",
    )


class PartiallySupportedDatasetError(Exception):
  def __init__(self, dataset: str) -> None:
    super().__init__(f"This dataset is only partially supported: {dataset}.")


class OverwriteFileError(Exception):
  def __init__(self, file_path: Path) -> None:
    super().__init__(f"File {str(file_path)} already exists.")


class InvalidHistogramTypeError(Exception):
  def __init__(self, type: str) -> None:
    super().__init__(
      f"Unsupported type {type} for histogram parsing. "
      "Please check the histogram data."
    )
