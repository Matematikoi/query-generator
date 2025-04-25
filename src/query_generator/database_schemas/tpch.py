from typing import Any, Dict, List, Tuple


def get_tpch_table_info() -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
  # using all the numerical columns from the TPC-DS schema (like JOB)
  tables: Dict[str, Dict[str, Any]] = {
    "customer": {
      "alias": "c",
      "columns": {
        "c_acctbal": {"max": 9999.99, "min": -999.99},
        "c_custkey": {"max": 15000000, "min": 1},
        "c_nationkey": {"max": 24, "min": 0},
      },
      "foreign_keys": [
        {
          "column": "c_nationkey",
          "ref_table": "nation",
          "ref_column": "n_nationkey",
        }
      ],
    },
    "lineitem": {
      "alias": "l",
      "columns": {
        # "l_commitdate": {"max": "1998-10-31", "min": "1992-01-31"},
        "l_discount": {"max": 0.1, "min": 0.0},
        "l_extendedprice": {"max": 104948.5, "min": 900.05},
        "l_linenumber": {"max": 7, "min": 1},
        "l_orderkey": {"max": 600000000, "min": 1},
        "l_partkey": {"max": 20000000, "min": 1},
        "l_quantity": {"max": 50.0, "min": 1.0},
        # "l_receiptdate": {"max": "1998-12-31", "min": "1992-01-03"},
        # "l_shipdate": {"max": "1998-12-01", "min": "1992-01-02"},
        "l_suppkey": {"max": 1000000, "min": 1},
        "l_tax": {"max": 0.08, "min": 0.0},
      },
      "foreign_keys": [
        {
          "column": "l_partkey",
          "ref_table": "partsupp",
          "ref_column": "ps_partkey",
        },
        # TODO: This is a composite key.
        # {
        #     "column": "l_suppkey",
        #     "ref_table": "partsupp",
        #     "ref_column": "ps_suppkey",
        # },
        {
          "column": "l_orderkey",
          "ref_table": "orders",
          "ref_column": "o_orderkey",
        },
      ],
    },
    "nation": {
      "alias": "n",
      "columns": {
        "n_nationkey": {"max": 24, "min": 0},
        "n_regionkey": {"max": 4, "min": 0},
      },
      "foreign_keys": [
        {
          "column": "n_regionkey",
          "ref_table": "region",
          "ref_column": "r_regionkey",
        }
      ],
    },
    "orders": {
      "alias": "o",
      "columns": {
        "o_custkey": {"max": 14999999, "min": 1},
        # "o_orderdate": {"max": "1998-08-02", "min": "1992-01-01"},
        "o_orderkey": {"max": 600000000, "min": 1},
        "o_shippriority": {"max": 0, "min": 0},
        "o_totalprice": {"max": 591036.15, "min": 811.73},
      },
      "foreign_keys": [
        {
          "column": "o_custkey",
          "ref_table": "customer",
          "ref_column": "c_custkey",
        }
      ],
    },
    "part": {
      "alias": "p",
      "columns": {
        "p_partkey": {"max": 20000000, "min": 1},
        "p_retailprice": {"max": 2098.99, "min": 900.01},
        "p_size": {"max": 50, "min": 1},
      },
      "foreign_keys": [],
    },
    "partsupp": {
      "alias": "ps",
      "columns": {
        "ps_availqty": {"max": 9999, "min": 1},
        "ps_partkey": {"max": 20000000, "min": 1},
        "ps_suppkey": {"max": 1000000, "min": 1},
        "ps_supplycost": {"max": 1000.0, "min": 1.0},
      },
      "foreign_keys": [
        {
          "column": "ps_partkey",
          "ref_table": "part",
          "ref_column": "p_partkey",
        },
        {
          "column": "ps_suppkey",
          "ref_table": "supplier",
          "ref_column": "s_suppkey",
        },
      ],
    },
    "region": {
      "alias": "r",
      "columns": {"r_regionkey": {"max": 4, "min": 0}},
      "foreign_keys": [],
    },
    "supplier": {
      "alias": "s",
      "columns": {
        "s_acctbal": {"max": 9999.98, "min": -999.99},
        "s_nationkey": {"max": 24, "min": 0},
        "s_suppkey": {"max": 1000000, "min": 1},
      },
      "foreign_keys": [
        {
          "column": "s_nationkey",
          "ref_table": "nation",
          "ref_column": "n_nationkey",
        }
      ],
    },
  }

  # Validate foreign keys
  for table_name, table_info in tables.items():
    for fk in table_info["foreign_keys"]:
      ref_table = fk["ref_table"]
      if fk["column"] not in tables[table_name]["columns"]:
        raise ValueError(
          f"Table {table_name} has foreign key column "
          f"{fk['column']} that does not exist"
        )
      if ref_table not in tables:
        raise ValueError(
          f"Table {table_name} has foreign key "
          f"to non-existing table {ref_table}"
        )
      if fk["ref_column"] not in tables[ref_table]["columns"]:
        raise ValueError(
          f"Table {table_name} has foreign key to column {fk['ref_column']} "
          f"that does not exist in table {ref_table}"
        )

  fact_tables = ["lineitem", "orders", "partsupp"]

  return tables, fact_tables
