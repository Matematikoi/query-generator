# Query generation

This is a query generation project. We take a database and generate
queries on that database. 
The queries generated can be used as input data for machine learning pipelines
involving query optimization, cardinality estimation or latency prediction.

The queries are generated based on foreign keys, and stats collected from the
database itself.

# Important links
1. [Project structure](/file?name=docs/miscellaneous/data_structure.md&ci=docs)
1. [Pipeline with examples](/file?name=docs/query_generation/pipeline.md&ci=docs)
1. [Snowflake Algorithm](/wiki?name=Snowflake)
1. [LLM augmentation](/wiki?name=LLM%20augmentation)

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

# Running the generator

We support various ways of running queries. 
To see our supported pipelines see [the query-generation](/file?name=docs/query_generation/pipeline.md&ci=docs)
pipelines available.

# Authors and contact
This project was made by Gabriel Lozano under the supervision of Yanlei Diao
and Guillaum Lachaud at École Polytechnique.
You may contact the main collaborator via email 
[gabriel.lozano@lix.polytechnique.fr](mailto:gabriel.lozano@lix.polytechnique.fr)


