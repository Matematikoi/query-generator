from pathlib import Path


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


class OverwriteFileError(Exception):
  def __init__(self, file_path: Path) -> None:
    super().__init__(f"File {str(file_path)} already exists.")
