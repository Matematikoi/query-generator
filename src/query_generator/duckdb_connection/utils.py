from dataclasses import dataclass

import duckdb


@dataclass
class RawDuckDBTableDescription:
  """Class to describe a table in DuckDB.

  Represents the schema of a table as returned by the `DESCRIBE`
  command in DuckDB.
  Each instance corresponds to a single column in the table.

  Attributes:
      column_name (str): The name of the column.
      column_type (str): The data type of the column
      (e.g., INTEGER, VARCHAR, DATE).
      null (str): Indicates whether the column allows NULL values
      ("YES" or "NO").
      key (str): Specifies if the column is part of a key
      (e.g., "PRIMARY KEY", "NULL").
      default (str): The default value for the column, if any.
      extra (str): Additional information about the column,
      such as constraints.
  """

  column_name: str
  column_type: str
  null: str
  key: str
  default: str
  extra: str


@dataclass
class RawDuckDBHistograms:
  bin: str
  count: int
  """Class to represent a histogram bin in DuckDB.
  Attributes:
      bin (str): The bin value, which is a string representation
      of the range of values in the bin. The format is 
      "x <= value" for the first bin and "lower_bound < x <= upper_bound"
      for subsequent bins.
      count (int): The count of occurrences in the bin.
  """


@dataclass
class RawDuckDBMostCommonValues:
  value: str
  count: int
  """Class to represent the most common values in a column.
  Attributes:
      value (str): The most common value in the column.
      although it can be of any type, it is represented as a string.
      This is because DuckDB returns all values as strings.
      The value can be a number, date, or string.
      count (int): The count of occurrences of the value.
  """


def get_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
  """Retrieve the list of tables in the database.

  Args:
      con (duckdb.DuckDBPyConnection): The connection to the database.

  Returns:
      list[str]: A list of table names in the database.
  """
  tables = con.execute("show tables;").fetchall()
  return [table[0] for table in tables]


def get_columns(
  con: duckdb.DuckDBPyConnection, table: str
) -> list[RawDuckDBTableDescription]:
  """Retrieve the list of columns for a specific table.

  Args:
      con (duckdb.DuckDBPyConnection): The connection to the database.
      table (str): The name of the table.

  Returns:
      list[str]: A list of column names in the specified table.
  """
  columns = con.execute(f"DESCRIBE {table};").fetchall()
  return [
    RawDuckDBTableDescription(
      column_name=column[0],
      column_type=column[1],
      null=column[2],
      key=column[3],
      default=column[4],
      extra=column[5],
    )
    for column in columns
  ]


def get_equi_height_histogram(
  con: duckdb.DuckDBPyConnection, table: str, column: str, bin_count: int
) -> list[RawDuckDBHistograms]:
  query = f"""
    SELECT bin, count
    FROM histogram(
    '{table}',
    {column},
    bin_count := {bin_count},
    technique := 'equi-height'
  );
  """
  data = con.execute(query).fetchall()
  return [RawDuckDBHistograms(bin=d[0], count=d[1]) for d in data]


def get_distinct_count(
  con: duckdb.DuckDBPyConnection, table: str, column: str
) -> int:
  data: int = con.execute(f"""
    SELECT COUNT(DISTINCT {column}) FROM {table};
  """).fetchall()[0][0]
  return data


def get_frequent_non_null_values(
  con: duckdb.DuckDBPyConnection, table: str, column: str, limit: int
) -> list[RawDuckDBMostCommonValues]:
  data = con.execute(f"""
    SELECT {column}, COUNT(*) as count
    FROM {table}
    WHERE {column} IS NOT NULL
    GROUP BY {column}
    ORDER BY count DESC
    LIMIT {limit};
  """).fetchall()
  return [RawDuckDBMostCommonValues(value=d[0], count=d[1]) for d in data]
