from enum import Enum


# TODO: this really should be a dataset
class BenchmarkType(Enum):
  TPCDS = "TPCDS"
  TPCH = "TPCH"
