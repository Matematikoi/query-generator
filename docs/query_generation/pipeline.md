
~~~pikchr
S: circle "start" fit
arrow
MDB: box "generate-db" fit 
arrow
P: box "synthetic-queries" fit
arrow 
F1: box "filter-synthetic" fit 
arrow
LLM: box "extensions-and-llm" fit
arrow
circle "dataset" "ready" fit




H: box "make-histograms" fit at (P + (0,-2))
arrow dashed from S.s to H.nw "for non" aligned "precomputed DB" aligned
F: file "histogram" at ( P + (1.5,-1.2)) fit
arrow from H.n to F.s "generates" aligned ""
arrow <- from P.s to F.n "uses" aligned ""
spline from S.n  up 0.5 then right until even with P.nw then to P.nw dashed ->
text "If database is not supported" at (MDB + (0,0.74)) "" "for generation" 

DB: cylinder "DuckDB Database" fit at (P + (0,-1))
arrow from P.s to DB.n "run" aligned "queries" aligned
arrow from MDB.s to DB.nw "generates" aligned "DuckDB database" aligned 
arrow from H.n to DB.s  "queries" aligned ""
spline <- from DB.ne right 0.5 then to LLM.sw dashed "Syntax Check" aligned


PARQUET: file "cardinalities" fit at (P + (0.5,1.3))
arrow from P.n to PARQUET.sw "generates" aligned "parquet" aligned
arrow  from PARQUET.se  to F1.n "uses" aligned "cardinalities" aligned
~~~



# Example Full run
We follow a small example for explaining this steps. We invite the reader
to run `pixi run main --help` to get documentation of the existing endpoints
and to run `pixi run main {endpoint} --help` to get documentation of each
endpoint.
1. Generate TPC-DS
1. Make histograms of the generated database in step 1
1. Generate queries with the database and the histograms
of the previous steps.
1. Filter the generated synthetic queries.
1. Augment them using llm and unions.

**Generate TPCDS**
We choose the `generate-db` endpoint to generate the data, and we pass
the `params_config/generate_db/tpcds_dev.toml` configuration to
generate a small TPCDS of scale factor 0.1. The toml contains all the 
information of input. If you want to generate a different size of TPC-DS
you can just change the toml scale factor and run the same command.

```bash
pixi run main generate-db -c params_config/generate_db/tpcds_dev.toml
```
The toml used was:

```toml
dataset = "TPCDS"
scale_factor = 0.1
db_path = "tmp/database_TPCDS_0.1.duckdb"
```

**Make histograms**
Same as before we run
```bash
pixi run main make-histograms -c params_config/make_histograms/tpcds_dev.toml
```

with the toml file:

```toml
output_folder = "tmp/histograms/"
database_path = "tmp/database_TPCDS_0.1.duckdb"
histogram_size = 51
common_values_size = 10
include_mcv = true
```

**Make Synthetic Queries**
We now run the query generation with 
```bash
pixi run main synthetic-queries -c params_config/synthetic_queries/tpcds_dev.toml
```

```toml
duckdb_database = "tmp/database_TPCDS_0.1.duckdb"
dataset = "TPCDS"
output_folder = "tmp/synthetic_queries"
max_hops = [1]
extra_predicates = [5]
row_retention_probability = [0.2, 0.9]
unique_joins = true
max_queries_per_fact_table = 1
max_queries_per_signature = 2
keep_edge_probability = [0.2]
equality_lower_bound_probability = [0,0.1]
extra_values_for_in = 3

[operator_weights]
operator_in = 1
operator_range = 3
operator_equal = 3
```

**Filtering the queries**
To filter the queries we use 
```bash
pixi run main filter-synthetic -c params_config/filter_synthetic/filter_tpcds_dev.toml
```
This will also split the queries by their query signature.

```toml
input_parquet = "tmp/synthetic_queries/output.parquet"
destination_folder = "tmp/filtered_queries"
filter_null  = true
cherry_pick = false
```


**Extension and LLM**

TODO


