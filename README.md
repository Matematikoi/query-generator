# Index
1. [Installation](#installation-of-requirements)
1. [Commands for a small working example](#commands-for-a-small-working-example)
1. [Full TPCDS query generation](#full-tpcds-run)
1. [Authors](#authors-and-contact)


# Installation of required tools

We use [pixi](https://pixi.sh/latest/) for managing the python environment.
For running the LLM augmentation we use 
[Ollama](https://ollama.com/) as a self-managed service

**Install pixi.** You can install pixi in Linux and Mac by running:

```bash
curl -fsSL https://pixi.sh/install.sh | sh
```

You can check pixi was succesfully installed by running:

```bash
pixi run main --help
```

which will give you a list of our endpoints along with some documentation

**Install Ollama.**  To install ollama you can run 

```bash
curl -fsSL https://ollama.com/install.sh | sh
```

For Mac you can use [the installer](https://ollama.com/download/mac).

# Pipeline
(outdated).
![image](https://matematikoi.github.io/org/images/pipeline_query_generation.png)


# Small working example

We provide a small example for explaining the main steps of database and 
query generation: 
1. Generate Database (only TPCDS and TPCH are currently supported).
1. Make histograms (column statistics) of the generated database in step 1.
1. Generate join queries using the database and the histograms of the previous steps.
1. Filter the generated join queries to  
	- remove queries with empty results;
	- optionally, bin the queries based on the output cardinality and subsample in each bin.  
1. Augment the join queries using LLMs and unions.
1. Post-process the queries and collect the traces:
	- fixing LLM issues (e.g., the attributes used in Group By-Aggregation);
	- adding LIMIT 100 if the query returns too many rows. 
	- collect traces using DuckDB.
1. Get metrics about the generated queries.

We invite the user to run `pixi run main --help` to get documentation of 
all of the existing endpoints. 

The user can then run `pixi run main {endpoint} --help` to get documentation 
of each endpoint.

## Commands for a small working example
We next illustrate the commands to run a small example pipeline.

For Ollama we use the 
model `llama3:latest` ollama model, which means that you should run
`ollama pull llama3:latest` before running the `extensions-with-ollama` endpoint.


```bash
pixi run main generate-db -c params_config/generate_db/tpcds_dev.toml
pixi run main make-histograms -c params_config/histogram/tpcds_dev.toml
pixi run main synthetic-queries -c params_config/synthetic_generation/tpcds_dev.toml
pixi run main filter-synthetic -c params_config/filter/filter_tpcds_dev.toml
pixi run main extensions-with-ollama -c params_config/extensions_with_ollama/tpcds_dev.toml
pixi run main fix-transform -c params_config/fix_transform/tpcds_ollama_dev.toml
pixi run main get-metrics -c params_config/get_metrics/ollama_tpcds_dev.toml
```

You can access the documentation of each endpoint by running
`pixi run main {endpoint} --help`

Or by accessing the repective doc:

- [`generate-db`](./docs/endpoints/generate_db.md)
- [`make-histograms`](./docs/endpoints/histogram.md)
- [`synthetic-queries`](./docs/endpoints/synthetic_generation.md)
- [`filter-synthetic`](./docs/endpoints/filter.md)
- [`extensions-with-ollama`](./docs/endpoints/extensions_with_ollama.md)
- [`extensions-with-bedrock`](./docs/endpoints/extensions_with_bedrock.md)
- [`fix-transform`](./docs/endpoints/fix_transform.md)
- [`get-metrics`](./docs/endpoints/get_metrics.md)

For debugging, every command also accepts a `--debug` flag that raises the
log file level to debug. For example, after running `generate-db`, you can run
the histogram task with debug logging enabled:
`pixi run main make-histograms -c params_config/histogram/tpcds_dev.toml --debug`

# Full TPCDS run

```bash
# generate the tpcds 100 dataset
pixi run main generate-db -c params_config/generate_db/tpcds.toml
# make histograms for TPCDS 100
pixi run main make-histograms -c params_config/histogram/tpcds.toml
# make synthetic queries
pixi run main synthetic-queries -c params_config/synthetic_generation/tpcds.toml
# filter the synthetic queries
pixi run main filter-synthetic -c params_config/filter/filter_tpcds.toml
# Generate a test dataset with scale factor 0.1 to check LLM-generated queries.
pixi run main generate-db -c params_config/generate_db/tpcds_test.toml
# ollama augmentation and union
pixi run main extensions-with-ollama -c params_config/extensions_with_ollama/tpcds_llama4.toml
# Transformation to the queries
pixi run main fix-transform -c params_config/fix_transform/tpcds_ollama.toml
# Get metrics about the generated queries
pixi run main get-metrics -c params_config/get_metrics/ollama_tpcds.toml
```
# TPCH toy example

```bash
pixi run main generate-db -c params_config/generate_db/tpch_dev.toml
pixi run main make-histograms -c params_config/histogram/tpch_dev.toml
pixi run main filter-synthetic -c params_config/filter/filter_tpch_dev.toml
pixi run main extensions-with-ollama -c params_config/extensions_with_ollama/tpch_dev.toml
pixi run main fix-transform -c params_config/fix_transform/tpch_ollama_dev.toml
```


# Authors and contact
This project was made by Gabriel Lozano under the supervision of Yanlei Diao
and Guillaum Lachaud at École Polytechnique.
You may contact the main collaborator via email 
[gabriel.lozano@lix.polytechnique.fr](mailto:gabriel.lozano@lix.polytechnique.fr)
