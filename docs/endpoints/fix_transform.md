# Attributes

- `queries_folder` (str): The folder containing the sql queries to
    which the LIMIT will be added.
- `destination_folder` (str): The folder to save the formatted queries.

## Engine

- `engine.database_path` (str): Path to the database. A `.duckdb` file when
  `validator_engine = "duckdb"`; a directory of Parquet tables
  (`database_path/table_name/*.parquet`) when `validator_engine = "pyspark"`.
- `engine.validator_engine` (str): `"duckdb"` (default) or `"pyspark"`.
- `engine.spark_config` (dict, optional): Extra Spark configuration keys passed
  directly to `SparkSession.builder.config()`. Example:
  `"spark.sql.shuffle.partitions" = "4"`.
- `max_output_size` (int): The maximum output size for the queries. Queries
with an output tuple size greater than this value will have a LIMIT added.
If the value is 0, no limit will be imposed.    
- `timeout_seconds` (float): The maximum amount of seconds the query is 
allowed to run. Queries beyond this threshold will not be "valid" queries.
- `filter_empty_set` (bool): Whether to filter out queries that return
an empty set. If set to true, only queries that return at least one
tuple will be kept. By default is set to False.
- `make_select_group_by_disjoint` (bool): Whether to make the select clause
attributes disjoint from the group by clause attributes. DuckDB mode only.
By default is set to False.
- `make_count_statement_diverse` (bool): Whether to change the COUNT statements
to other aggregate functions or COUNT variants. DuckDB mode only.
By default is set to False.
- `max_memory_gb` (int): The maximum amount of memory in gigabytes that
duckdb is allowed to use while running the queries. DuckDB mode only.
By default is set to 5.

Since the limit on queries will be imposed based on the output of the queries,
the queries need to be run to collect their output sizes.
We do another pass of query running to collect the final traces, written to
`traces.parquet` in the destination folder. Raw trace files are kept in
`DUCKDB_TRACES/` (DuckDB mode) or `SPARK_TRACES/` (Spark mode).

# Spark Catalog

In Spark mode, a Hive metastore is created at
`engine.database_path/.spark/catalog/` on first run and reused on subsequent
runs. The setup registers every non-hidden subdirectory of `database_path` as a
Parquet table and computes CBO statistics for all columns.

# Transformations

There are three transformation being done currently:
1. Change the select clause to have disjoint attributes with the 
group by clause.
1. Change the `COUNT()` statements to one of the following:
    1. `MIN` (only for numerical attributes)
    1. `MAX` (only for numerical attributes)
    1. `COUNT( DISTINCT )`
    1. `COUNT`
1. Add a limit to the query if the output of it is over the user defined 
threshold.
