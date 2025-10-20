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

# Pipeline
![image](https://matematikoi.github.io/org/images/pipeline_query_generation.png)

# Example Full run

We provide a small example for explaining the main steps of database and query generation: 
1. Generate Database (only TPCDS and TPCH are currently supported)
1. Make histograms (column statistics) of the generated database in step 1
1. Generate join queries using the database and the histograms of the previous steps.
1. Filter the generated join queries to  
	1. remove queries with empty results;
	1. optionally, bin the queries based on the output cardinality and subsample in each bin.  
1. Augment the join queries using LLMs and unions.
1. OPTIONAL. Post-process the queries by 
	1. fixing LLM issues (e.g., for Group By-Aggregation)
	1. adding LIMIT 100 if the query returns too many rows 

We invite the user to run `pixi run main --help` to get documentation of all of the existing endpoints. 

The user can then run `pixi run main {endpoint} --help` to get documentation 
of each endpoint.

## Commands for small working example
In case that you just want the commands to run the examples, you only need to 
install pixi, and ollama to run all of these commands. For ollama we use the 
model `llama3:latest` ollama model, which means that you should run
`ollama pull llama3:latest` before running the `extensions-with-ollama` endpoint.

## Summary
In case that you just want the commands to run the examples:

```bash
pixi run main generate-db -c params_config/generate_db/tpcds_dev.toml
pixi run main make-histograms -c params_config/histogram/tpcds_dev.toml
pixi run main synthetic-queries -c params_config/synthetic_generation/tpcds_dev.toml
pixi run main filter-synthetic -c params_config/filter/filter_tpcds_dev.toml
pixi run main extensions-with-ollama -c params_config/extension_and_ollama/tpcds_dev.toml
pixi run main fix-transform -c params_config/fix_transform/tpcds_dev.toml
```

## **Generate database**

Currently only TPC-DS and TPC-H are being supported.

We choose the `generate-db` endpoint to generate the data, and we pass
the `params_config/generate_db/tpcds_dev.toml` configuration to
generate a small TPCDS of scale factor 0.1. The toml contains all the 
information of input. If you want to generate a different size of TPC-DS
you can change the toml scale factor and run the same command.

```bash
pixi run main generate-db -c params_config/generate_db/tpcds_dev.toml
```
[For more details
check the full documentation here](./docs/endpoints/generate_db.md)

This will generate a database in the `./tmp/` folder
## **Make histograms**

Same as before we run
```bash
pixi run main make-histograms -c params_config/make_histograms/tpcds_dev.toml
```
[For more details
check the full documentation here](./docs/endpoints/histogram.md)

This will generate column statistics in the `./tmp/histograms` folder.

## **Make Synthetic Queries**

We now run the query generation with 
```bash
pixi run main synthetic-queries -c params_config/synthetic_queries/tpcds_dev.toml
```
[For more details
check the full documentation here](./docs/endpoints/synthetic_generation.md)

This will generate the synthetic queries in the `./tmp/synthetic_queries` 
folder. It would use the database and statistics created in the previous steps.
Feel free to check the `toml` file `params_config/synthetic_queries/tpcds_dev.toml`
for more details.
## **Filtering the queries**

To filter the queries we use 
```bash
pixi run main filter-synthetic -c params_config/filter_synthetic/filter_tpcds_dev.toml
```
[For more details
check the full documentation here](./docs/endpoints/filter.md)

The result will filter the queries according to two methods:
1. Filter empty queries (no tuples returned)
1. Sample according to the tuple output size using equi-width bins. 

For more details please run `pixi run main filter-synthetic --help`

## **Extension and Ollama**

We can do extensions for extra relational algebra operators.
This extension takes as input the filtered queries. 
[For more details
check the full documentation here](./docs/endpoints/extension_and_ollama.md)

```bash
pixi run main extensions-with-ollama -c params_config/extension_and_ollama/tpcds_dev.toml
```
This will generate the union and ollama extension.

To run this toml you will need to have OLLAMA installed. The example 
uses the `llama3:latest` ollama model, which means that you should run
`ollama pull llama3:latest` before running this command, otherwise the endpoint
will fail since it won't find the model in your machine.

For more details please run `pixi run main extensions-with-ollama --help`
## **Fix Transform**
We also provide a post-processing with sqlglot to adjust queries created
with the LLMs. In the example run:
```bash
pixi run main fix-transform -c params_config/fix_transform/tpcds_dev.toml
```
[For more details
check the full documentation here](./docs/endpoints/.md)

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
# generate an empty dataset for the LLM
pixi run main generate-db -c params_config/generate_db/tpcds_empty.toml
# ollama augmentation and union
pixi run main extensions-with-ollama -c params_config/extension_and_ollama/tpcds_llama4.toml
# final transformation to the queries
pixi run main fix-transform -c params_config/fix_transform/tpcds.toml
```

# Authors and contact
This project was made by Gabriel Lozano under the supervision of Yanlei Diao
and Guillaum Lachaud at École Polytechnique.
You may contact the main collaborator via email 
[gabriel.lozano@lix.polytechnique.fr](mailto:gabriel.lozano@lix.polytechnique.fr)

