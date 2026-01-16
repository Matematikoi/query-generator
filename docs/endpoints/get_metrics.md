# Attributes

This endpoint provides metrics about the generated queries once the
traces are collected with the `fix-transform` endpoint

- `input_parquet` (str): The path to the input Parquet file which
was output by the `fix-transform` endpoint. Called duckdb_traces.parquet
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
a proxy for syntactic query complexity.

- `query_size_tokens`: the number of lexical tokens in the SQL query.

- `output_cardinality`: the number of rows produced by the root operator, i.e.,
the final result size of the query.

- `query_keywords`: the set of SQL keywords appearing in the query (e.g.,
`JOIN`, `GROUP BY`, `HAVING`, `OVER`, `WITH`), used as an approximated
proxy for predicate and operator usage.

- `operator_distribution`: a histogram of physical operator types appearing
in the **physical query operator plan graph** (e.g., `TABLE_SCAN`, `FILTER`,
`HASH_JOIN`, `AGGREGATE`, `WINDOW`), describing what types of execution
work the query performs.
