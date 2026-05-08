# Attributes

This endpoint provides metrics about the generated queries once the
traces are collected with the `fix-transform` endpoint (DuckDB) or the
`spark-traces` endpoint (PySpark).

- `input_parquet` (str): The path to the input Parquet file containing
  execution traces — `duckdb_traces.parquet` for DuckDB,
  `spark_traces.parquet` for PySpark.
- `output_folder` (str): The folder where the metrics will be saved.
- `template_occurrence_limit` (dict[str, int]): Optional cap on how many
  queries to process per template key; e.g., if a template appears 500
  times and the limit is 100, only 100 instances are processed.
- `group_by_templates` (dict[str, str]): Optional mapping of template
  group names to glob patterns for grouping templates in the metrics.
- `x_axis_limits` (dict[str, list[float]]): Optional histogram x-axis
  limits per metric name. Each value must be `[min, max]` and is applied
  when plotting the histogram for that metric.
- `y_axis_limits` (dict[str, list[float]]): Optional histogram y-axis
  limits per metric name. Each value must be `[min, max]` and is applied
  when plotting the histogram for that metric.

# Engine

- `validator_engine` (str): `"duckdb"` (default) or `"pyspark"`. Controls
  which trace format is parsed. Use `"duckdb"` when traces were collected by
  the DuckDB mode of `fix-transform`, and `"pyspark"` when traces were
  collected by the Spark mode.

# Metrics

- `latency_duckdb`: the execution time in seconds it takes for the query
to finish inside DuckDB

- `cumulative_cardinality_duckdb`: how many rows were produced by physical
operators.

- `cumulative_rows_scanned_duckdb`: how many rows were read by physical
operators.

- `cardinality_over_rows_scanned`: ratio defined as,
`cumulative_cardinality`/`cumulative_rows_scanned`

- `query_plan_size`: the total number of nodes in the **physical
query operator plan graph**, i.e., the number of physical operators in
the execution plan.

- `query_plan_length`: the length of the **longest path** in the
physical query operator plan graph, measuring the maximum operator
dependency depth.

- `query_size_bytes`: the size of the SQL query string in bytes, used as
a proxy for syntactic query complexity. Available for all engines.

- `query_size_tokens`: the number of lexical tokens in the SQL query.
Available for all engines.

- `output_cardinality`: the number of rows produced by the root operator, i.e.,
the final result size of the query.

- `query_keywords`: the set of SQL keywords appearing in the query (e.g.,
`JOIN`, `GROUP BY`, `HAVING`, `OVER`, `WITH`), used as an approximated
proxy for predicate and operator usage. Available for all engines.

- `operator_distribution`: a histogram of physical operator types appearing
in the **physical query operator plan graph** (e.g., `TABLE_SCAN`, `FILTER`,
`HASH_JOIN`, `AGGREGATE`, `WINDOW`), describing what types of execution
work the query performs.

- `function_classification`: classifies SQL functions and operators found in
each query using sqlglot. Each record contains `category`, `subcategory`,
`name` (the sqlglot class name, e.g. `Add`, `Abs`, `Like`; for anonymous
functions the SQL function name is used instead), and `expression` (the raw
SQL text). Categories include:
  - **scalar**: string, datetime, numeric, null_handling, type_conversion,
    regex, json, array, map_struct, hash_crypto, session_system, arithmetic,
    bitwise, comparison, pattern_matching, struct_access, logical, distance,
    range
  - **agg**: core, statistical, ordered_set, collection, approximate
  - **window**: ranking, navigation, distribution, aggregate
  - **conditional**: case, if
  - **table_valued**: e.g., UNNEST

  Binary operators (`+`, `-`, `*`, `/`, `=`, `<>`, `&`, `|`, `LIKE`, etc.)
  and unary operators (`-`, `~`, `NOT`) are also classified.

# Spark Metrics

Available when `validator_engine = "pyspark"`:

- `wall_time_ms`: wall-clock time in milliseconds from query submission to
  completion, as recorded in the Spark event log.

- `number_of_stages`: number of Spark stages executed for the query.

- `number_of_joins`: count of all join nodes in the physical plan tree
  (any node whose name contains `"Join"`, e.g. `BroadcastHashJoin`,
  `SortMergeJoin`).

- `shuffle_bytes_written`: total bytes written across all shuffle
  exchanges, summed over all nodes in the physical plan.

- `number_of_physical_operators`: count of non-infrastructure nodes in
  the physical plan. Infrastructure nodes (e.g. `AdaptiveSparkPlan`,
  `WholeStageCodegen`, `InputAdapter`) are excluded; `Scan parquet *`
  variants are normalised to `Scan parquet`.

- `operator_distribution`: per-operator counts as a list of
  `{operator, count}` structs, sorted descending by count. Infrastructure
  nodes are excluded and scan names are normalised (same rules as above).
  Stored as `List(Struct)` for direct Polars querying without JSON parsing.

- `parsed_spark_trace`: the parsed Spark event log as a compact JSON
  string, replacing the raw event log to reduce parquet size.

- `query_size_bytes`, `query_size_tokens`, `query_keywords`: same
  SQL-text metrics as DuckDB (see above), derived from the executed query
  string.
