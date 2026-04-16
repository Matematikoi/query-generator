# Adding a New Engine

This guide walks through every change needed to support a new SQL execution engine
(e.g., Trino, BigQuery, Flink) in the query generator pipeline.

The pipeline has four integration points, covered in order below:

1. [Database generator](#1-database-generator) — produce the data format the engine reads
2. [Histogram generation](#2-histogram-generation) — column statistics used for predicate generation
3. [Synthetic query generation](#3-synthetic-query-generation) — validate generated queries against the engine
4. [LLM extension — validation](#4-llm-extension--validation) — validate LLM-produced queries against the engine
5. [LLM extension — prompts, schema, and functions](#5-llm-extension--prompts-schema-and-functions) — guide the LLM with engine-specific context

Trace collection and metrics are not covered here; they operate on files already
written to disk and do not need engine-specific changes.

---

## 1. Database Generator

The `generate-db` endpoint creates a DuckDB database and, optionally, exports the
tables to Parquet. The Parquet export is what allows engines that cannot read DuckDB
files (e.g., PySpark) to consume the same data.

### What to change

**`src/query_generator/duckdb_connection/setup.py`**

If your engine requires a data format other than DuckDB or Parquet, add an export
function here alongside `export_duckdb_con_to_parquet`. The function receives a live
`duckdb.DuckDBPyConnection` and a destination path, and writes the tables to whatever
format your engine needs.

If Parquet is sufficient (as it is for PySpark, Trino, BigQuery, etc.) no code change
is required — just set `parquet_path` in the config.

### Config changes — `params_config/generate_db/`

| Parameter | When to set |
|-----------|-------------|
| `dataset` | Always — selects the benchmark schema (e.g., `"TPCDS"`) |
| `scale_factor` | Always |
| `db_path` | Always — path for the DuckDB file (needed for the histogram step) |
| `parquet_path` | Set this when your engine reads Parquet |

**DuckDB-only (`tpcds_dev.toml`):**
```toml
dataset = "TPCDS"
scale_factor = 0.1
db_path = "tmp/database_TPCDS_0.1.duckdb"
```

**With Parquet export (`tpcds_spark_dev.toml`):**
```toml
dataset = "TPCDS"
scale_factor = 0.1
db_path = "tmp/database_TPCDS_0.1.duckdb"
parquet_path = "tmp/database_parquet/TPCDS_0.1"
```

For a new engine that reads Parquet, create
`params_config/generate_db/tpcds_myengine_dev.toml` following the second example.
The DuckDB file is still required because the histogram step always uses DuckDB.

---

## 2. Histogram Generation

The `make-histograms` endpoint computes per-column statistics from a DuckDB database
and writes them to a Parquet file. The synthetic query generator reads this file to
build predicates that avoid empty results.

**The histogram step always runs against DuckDB.** Even when your validation engine
is PySpark or any other system, run `make-histograms` against the DuckDB database
produced in step 1. No engine-specific config is needed:

```bash
pixi run main make-histograms -c params_config/histogram/tpcds_dev.toml
```

### If you cannot load your data into DuckDB

In rare cases the data cannot be loaded into DuckDB at all. If that happens, you
must produce the histogram Parquet file manually. The file must have one row per
(table, column) pair with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `table` | `str` | Table name |
| `column` | `str` | Column name |
| `histogram` | `list[str]` | Equi-height histogram upper-bound values as strings (e.g. `["10", "20", "30"]`) |
| `distinct_count` | `int` | Number of distinct non-null values |
| `dtype` | `str` | DuckDB type string: `INTEGER`, `BIGINT`, `VARCHAR`, `DATE`, `DECIMAL`, etc. |
| `table_size` | `int` | Total row count of the table |
| `null_count` | `int` | Number of null values in the column |
| `sample_size` | `int` | Number of rows used to compute the statistics |
| `common_substrings` | `list[struct] \| null` | String columns only. Each struct: `{substring: str, support: int, support_probability: float}`. Required for `LIKE` / `NOT LIKE` predicates; set to `null` for non-string columns. |

The following columns are only present when `include_mcv = true` (i.e. `common_values_size > 0`):

| Column | Type | Description |
|--------|------|-------------|
| `most_common_values` | `list[struct]` | Each struct: `{value: str, count: int}` |
| `histogram-mcv` | `list[str]` | Same format as `histogram` but computed on values that are not in `most_common_values` |

We strongly recommend using the `make-histograms` endpoint instead of constructing
this file by hand. Computing accurate equi-height histograms and common-substring
candidates is non-trivial, and any schema mismatch will cause silent failures during
predicate generation.

---

## 3. Synthetic Query Generation

The `synthetic-queries` endpoint generates join queries and validates each one against
a live database connection before accepting it. The validator is abstracted behind
`QueryValidator` so any engine can be plugged in.

### Step 3a — Implement `QueryValidator`

Create a new file in `src/query_generator/database_connection/`, e.g.,
`myengine_validation.py`, and implement the abstract base class:

```python
# src/query_generator/database_connection/query_validator_abc.py
class QueryValidator(ABC):
    def is_query_valid(self, query: str) -> tuple[bool, Exception | None]: ...
    def get_query_output_size(self, query: str) -> tuple[int | None, bool]: ...
    def get_synthetic_query_cardinality(self, query: str) -> int: ...
```

- `is_query_valid` — returns `(True, None)` if the query executes without error,
  `(False, exception)` otherwise.
- `get_query_output_size` — returns `(row_count, timed_out)`. `row_count` may be
  `None` if the query fails.
- `get_synthetic_query_cardinality` — wraps the query in `SELECT COUNT(*) FROM (...)`,
  runs it on a persistent connection, and returns the integer count. Returns `-1` on
  error or timeout.

Look at `duckdb_validation.py` and `pyspark_validation.py` for reference
implementations. Both use a subprocess-per-query pattern for isolation and a
persistent connection for the cardinality method.

### Step 3b — Register the engine

**`src/query_generator/utils/definitions.py`** — add a value to `ValidatorEngine`:

```python
class ValidatorEngine(StrEnum):
    DUCKDB = "duckdb"
    PYSPARK = "pyspark"
    MYENGINE = "myengine"   # add this
```

**`src/query_generator/database_connection/factory.py`** — wire the new value to your
class:

```python
from query_generator.database_connection.myengine_validation import MyEngineQueryValidator

def build_query_validator(...) -> QueryValidator:
    if validator_engine == ValidatorEngine.DUCKDB:
        return DuckDBQueryExecutor(database_path, validation_timeout_seconds)
    if validator_engine == ValidatorEngine.PYSPARK:
        return PySparkQueryValidator(database_path, validation_timeout_seconds)
    if validator_engine == ValidatorEngine.MYENGINE:
        return MyEngineQueryValidator(database_path, validation_timeout_seconds)
    msg = f"Unknown validator engine: {validator_engine}"
    raise ValueError(msg)
```

### Config changes — `params_config/synthetic_generation/`

| Parameter | DuckDB | PySpark / new engine |
|-----------|--------|---------------------|
| `validation_database_path` | path to `.duckdb` file | path to Parquet directory (or engine-specific path) |
| `validator_engine` | `"duckdb"` (default) | `"pyspark"` / `"myengine"` |

**DuckDB (`tpcds_dev.toml`):**
```toml
dataset = "TPCDS"
validation_database_path = "tmp/database_TPCDS_0.1.duckdb"
histogram_path = "tmp/histograms/histogram.parquet"
output_folder = "tmp/synthetic_queries"
...
```

**New engine (`tpcds_myengine_dev.toml`):**
```toml
dataset = "TPCDS"
validation_database_path = "tmp/database_parquet/TPCDS_0.1"
validator_engine = "myengine"
histogram_path = "tmp/histograms/histogram.parquet"
output_folder = "tmp/synthetic_queries_myengine"
...
```

`histogram_path` always points to the file produced by the DuckDB-based histogram
run. All other parameters (`max_hops`, `operator_weights`, etc.) are engine-agnostic.

---

## 4. LLM Extension — Validation

The `extensions-online` and `extensions-batch` endpoints take the synthetic queries,
send them to an LLM for augmentation, then validate the LLM output. Validation uses
the same `QueryValidator` abstraction as the synthetic step.

### What to change

No new code is needed beyond what was done in step 3. The factory
`build_query_validator` is reused here.

### Config changes — `params_config/extensions_online/`

The engine is specified under `[llm_params.engine_params]`:

| Parameter | Description |
|-----------|-------------|
| `database_path` | Path to the database the validator will query |
| `validator_engine` | `"duckdb"`, `"pyspark"`, or your new engine name |
| `validation_timeout_seconds` | Per-query timeout (default 20 s) |

**DuckDB (`tpcds_dev.toml`):**
```toml
[llm_params.engine_params]
database_path = "tmp/database_TPCDS_0.1.duckdb"
validator_engine = "duckdb"
prompts_path = "params_config/prompts/basic_prompt.toml"
schema_path = "params_config/schemas/dev.txt"
```

**New engine (`tpcds_myengine_dev.toml`):**
```toml
[llm_params.engine_params]
database_path = "tmp/database_parquet/TPCDS_0.1"
validator_engine = "myengine"
prompts_path = "params_config/prompts/myengine_prompt.toml"
schema_path = "params_config/schemas/tpcds_myengine.txt"
function_examples_path = "params_config/functions/myengine_functions.toml"
number_of_function_examples = 5
```

---

## 5. LLM Extension — Prompts, Schema, and Functions

The LLM needs engine-specific context to generate valid queries. Three files feed
this context.

### 5a — Schema file (`params_config/schemas/`)

A plain-text description of the tables and columns the LLM will work with. It is
injected wherever `{schema}` appears in the base prompt. The format is free — plain
SQL `CREATE TABLE` statements, Spark schema notation, or prose are all fine.

Create `params_config/schemas/tpcds_myengine.txt` with whatever notation best
communicates the schema to the model you are using.

Reference: `params_config/schemas/tpcds_spark.txt` uses Spark schema notation;
`params_config/schemas/tpcds.txt` uses `CREATE TABLE` SQL.

### 5b — Prompts file (`params_config/prompts/`)

A TOML file with two sections:

**`base_prompt`** — injected at the start of every message. Use it to set the
context (engine identity, output format requirements, engine-specific syntax rules).
Use `{schema}` to embed the schema file contents. Always instruct the model to return
queries wrapped in ` ```sql ``` ` blocks because the parser expects that format.

**`[weighted_prompts.*]`** — one entry per query-modification task. Each entry has a
`prompt` string and a `weight` float. The pipeline samples one prompt per LLM call,
weighted by these values. Weights do not need to sum to 1.

Create `params_config/prompts/myengine_prompt.toml`:

```toml
base_prompt = """
You are a database expert writing queries for MyEngine on the TPCDS dataset.
Return only the query in ```sql ``` markdown format.

The schema is:
{schema}

## MyEngine-specific rules
... (dialect rules, known gotchas, concrete bad/good examples)
"""

[weighted_prompts.group_by_1]
prompt = "Add a GROUP BY clause with 1 key to this query."
weight = 10

[weighted_prompts.window_function]
prompt = "Add a window function to this query."
weight = 10

[weighted_prompts.outer_join]
prompt = "Change one join to an outer join while keeping all predicates."
weight = 10
```

Reference: `params_config/prompts/spark_prompt.toml` has a detailed base prompt with
10 named SparkSQL syntax rules and concrete bad/good examples for each — this level
of detail significantly reduces LLM validation failures.

### 5c — Function examples file (`params_config/functions/`)

A TOML file that catalogues SQL functions the LLM should use. The pipeline samples
`number_of_function_examples` entries at prompt time and appends them to the user
message, spreading function coverage across calls.

Each entry belongs to a `[functions.<category>.<subcategory>]` table and has a
`name` and an `example` field (a short SQL snippet demonstrating the function).

Create `params_config/functions/myengine_functions.toml` with functions that are
available and idiomatic in your engine. If your engine is SQL-standard-compatible you
can start from `params_config/functions/standard_sql_functions.toml` and remove or
replace entries that behave differently.

This parameter is optional (`function_examples_path` defaults to `None`). Omit it
when prototyping and add it once the basic pipeline is working.

---

## End-to-end example

After creating the files above, a full toy run for a new engine looks like:

```bash
# 1. Generate the DuckDB database and export to Parquet
pixi run main generate-db -c params_config/generate_db/tpcds_myengine_dev.toml

# 2. Build histograms (always against DuckDB — no engine-specific config needed)
pixi run main make-histograms -c params_config/histogram/tpcds_dev.toml

# 3. Generate synthetic queries validated against the new engine
pixi run main synthetic-queries -c params_config/synthetic_generation/tpcds_myengine_dev.toml

# 4. Filter empty-result queries
pixi run main filter-synthetic -c params_config/filter/filter_tpcds_myengine_dev.toml

# 5. LLM augmentation with engine-specific prompts and validation
pixi run main extensions-online -c params_config/extensions_online/tpcds_myengine_dev.toml
```

Steps 3–5 use your `MyEngineQueryValidator` for validation; step 2 always uses DuckDB.

---

## Checklist

- [ ] `ValidatorEngine` enum updated in `src/query_generator/utils/definitions.py`
- [ ] `MyEngineQueryValidator` implemented in `src/query_generator/database_connection/myengine_validation.py`
- [ ] Factory updated in `src/query_generator/database_connection/factory.py`
- [ ] `params_config/generate_db/tpcds_myengine_dev.toml` created (with `parquet_path` if needed)
- [ ] `params_config/synthetic_generation/tpcds_myengine_dev.toml` created (with `validator_engine = "myengine"`)
- [ ] `params_config/filter/filter_tpcds_myengine_dev.toml` created
- [ ] `params_config/extensions_online/tpcds_myengine_dev.toml` created
- [ ] `params_config/schemas/tpcds_myengine.txt` created
- [ ] `params_config/prompts/myengine_prompt.toml` created
- [ ] (Optional) `params_config/functions/myengine_functions.toml` created
