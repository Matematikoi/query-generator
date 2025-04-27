from enum import Enum


class Extension(Enum):
  SNOWFLAKE = "SNOWFLAKE"
  START_JOIN = "START_JOIN"


class Dataset(Enum):
  TPCDS = "TPCDS"
  TPCH = "TPCH"
