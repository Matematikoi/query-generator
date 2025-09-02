# File structure

The structure of the files is similar to:
```
📦query-generator
 ┣ 📂data
 ┃ ┣ 📂duckdb
 ┃ ┃ ┗ 📂TPCDS
 ┃ ┃ ┃ ┗ 📜0.1.db
 ┃ ┣ 📂generated_queries
 ┃ ┃ ┗ 📂SNOWFLAKE_SEARCH_PARAMS
 ┃ ┃ ┃ ┗ 📂TPCDS
 ┃ ┗ 📂histograms
 ┃ ┃ ┣ 📜histogram_job.parquet
 ┃ ┃ ┣ 📜histogram_tpcds.parquet
 ┃ ┃ ┣ 📜histogram_tpch.parquet
 ┣ 📂docs
 ┣ 📂params_config
 ┃ ┣ 📂complex_queries
 ┃ ┃ ┣ 📜tpcds.toml
 ┃ ┃ ┣ 📜tpcds_dev.toml
 ┃ ┣ 📂search_params
 ┃ ┃ ┣ 📜job.toml
 ┃ ┃ ┣ 📜job_dev.toml
 ┃ ┃ ┣ 📜tpcds.toml
 ┃ ┃ ┗ 📜tpcds_dev.toml
 ┃ ┗ 📂snowflake
 ┃ ┃ ┗ 📜tpcds.toml
 ┣ 📂src
 ┣ 📂tests
 ┣ 📜CONTRIBUTING.md
 ┣ 📜README.md
 ┗ 📜pyproject.toml
```
- The `docs` folder contains the documentation files.
- The `src` folder contains the source code for the generator
- The `test` folder contains the tests made to the code for 
quality assurance.
- The `pyproject.toml` contains the information pixi needs to
install the libraries and run the project.

## Data folder
Includes
1. The databases that are generated. They are under `duckdb`
1. The generated queries by default.
1. The precomputed histograms for popular databases

# Params Config folder
It contains the input files for the most relevant query generation like
TPC-DS 100. When the file has a `_dev` in it, it means that the files 
