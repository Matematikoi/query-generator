# Index
1. [Installation](#installation-of-requirements)
1. [Pipeline](#pipeline)
1. [Authors](#authors-and-contact)


# Installation of requirements

We use [pixi](https://pixi.sh/latest/) for managing the python environment.
For running the LLM augmentation we use [ollama](https://ollama.com/)

You can install pixi in Linux and Mac by running:

```bash
curl -fsSL https://pixi.sh/install.sh | sh
```

You can check pixi was succesfully installed by running:

```bash
pixi run main --help
```

which will give you a list of our endpoints along with some documentation


To install ollama you can run 

```bash
curl -fsSL https://ollama.com/install.sh | sh
```

For Mac you can use [the installer](https://ollama.com/download/mac).

## Pixi cheatsheet
If you want to run the formatter and the test make sure you are in the
dev environment and run it with 

```bash
pixi shell -e dev
pixi run lint
```
If all went well there are no errors on the formatting or the tests.
# Pipeline
![image](https://matematikoi.github.io/org/images/pipeline_query_generation.png)

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

## Summary
Just in case you just want the commands to run the examples:

```bash
pixi run main generate-db -c params_config/generate_db/tpcds_dev.toml
pixi run main make-histograms -c params_config/make_histograms/tpcds_dev.toml
pixi run main synthetic-queries -c params_config/synthetic_queries/tpcds_dev.toml
pixi run main filter-synthetic -c params_config/filter_synthetic/filter_tpcds_dev.toml
pixi run main extensions-and-llm -c params_config/extensions_and_llms/tpcds_dev.toml
```

## **Generate TPCDS**

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

## **Make histograms**

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

## **Make Synthetic Queries**

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

## **Filtering the queries**

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


## **Extension and LLM**

Finally we can do extensions for extra relational algebra operators.
This extension takes as input the filtered queries. To run 

```bash
pixi run main extensions-and-llm -c params_config/extensions_and_llms/tpcds_dev.toml
```
This will generate the union and llm extension, the provided toml is:

```toml
llm_extension = true
union_extension = true
database_path = "data/duckdb/TPCDS/0.db"
queries_parquet = "tmp/filtered_queries/filtered.parquet"
destination_folder = "tmp/extended_queries"

[union_params]
max_queries = 5
probability = 0.7

[llm_params]
retry = 1
total_queries = 5
llm_model = "deepseek-r1:1.5b"
llm_base_prompt = """
    You are writing queries for a markdown text using \
    the format:sql for correct formatting in markdown. the schema
    is: ...
    """

[llm_params.llm_prompts.self_join]
prompt = "write this query with a self join"
weight = 30

[llm_params.llm_prompts.outer_join]
prompt = "write this query with an outer join"
weight = 30
```
# Authors and contact
This project was made by Gabriel Lozano under the supervision of Yanlei Diao
and Guillaum Lachaud at École Polytechnique.
You may contact the main collaborator via email 
[gabriel.lozano@lix.polytechnique.fr](mailto:gabriel.lozano@lix.polytechnique.fr)

