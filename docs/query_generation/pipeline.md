# Index

1. [Snowflake](#snowflake) - Our simplest method
2. [Running multiple Snowflake](#searchparams) the `search-params` endpoint
1. [Cherry pick](#cherrypick) diverse data.
1. [Filter null](#filternull).
1. [LLM extension](#llm)
1. [Union queries](#union)

<a name="snowflake"></a>
# Snowflake

Our simplest method of generating queries is based solely on stats.

For an in depth documentation check the [snowflake wiki](/wiki?name=Snowflake).

You would only need to run:

```bash
pixi run main snowflake -c params_config/snowflake/tpcds.toml
```

To get the files. The files will be saved in
`./data/generated_queries/SNOWFLAKE/TPCDS`

If you wish to use any other dataset you would have to compute
the statistics using the `make-histograms` endpoint beforehand.

~~~pikchr
S: circle "Start" fit
arrow
P: box "snowflake" fit
arrow
circle "dataset" "ready" fit



H: box "make-histograms" fit at (P + (0,-2))
arrow from S.s to H.nw "optional" aligned ""
F: file "histogram" at ( P + (0,-1)) fit
arrow from H.n to F.s "generates" aligned ""
arrow from P.s to F.n "uses" aligned ""
~~~
<a name="searchparams"></a>
# Parameter searching - Running and validating multiple Snowflake

Parameter searching iterates over snowflake by running the queries on
a database to get the queries cardinalities. This generates a
file with the information of cardinalities. Following endpoints
can use this file to sample a different set from the generated
queries from `param-search`.


Param search works by running multiple parameters of the
`snowflake` endpoints. Thus running `search-params` is equivalent
to running multiple `snowflake` with different parameters and
running and saving the results with a database.

~~~pikchr
S: circle "Start" fit
arrow
P: box "param-search" fit
arrow
circle "dataset" "ready" fit



H: box "make-histograms" fit at (P + (0,-2))
arrow from S.s to H.nw "optional" aligned ""
F: file "histogram" at ( P + (0,-1)) fit
arrow from H.n to F.s "generates" aligned ""
arrow from P.s to F.n "uses" aligned ""


DB: cylinder "DuckDB Database" fit at (P + (0,1))
arrow from P.n to DB.s "run" aligned "queries" aligned


PARQUET: file "cardinalities" fit at (P + (1.5,1))
arrow from P.ne to PARQUET.sw "generates" aligned "parquet" aligned
~~~

<a name="cherrypick"></a>
# Cherry pick

Cherry pick is only a filter to the [`search-params`](#searchparams)
endpoint to sample queries according to the cardinality.

~~~pikchr
S: circle "Start" fit
arrow
P: box "param-search" fit
arrow color red
F1: box "cherry-pick" fit color red
arrow
circle "dataset" "ready" fit




H: box "make-histograms" fit at (P + (0,-2))
arrow from S.s to H.nw "optional" aligned ""
F: file "histogram" at ( P + (0,-1)) fit
arrow from H.n to F.s "generates" aligned ""
arrow from P.s to F.n "uses" aligned ""


DB: cylinder "DuckDB Database" fit at (P + (0,1))
arrow from P.n to DB.s "run" aligned "queries" aligned


PARQUET: file "cardinalities" fit at (P + (1.5,1))
arrow from P.ne to PARQUET.sw "generates" aligned "parquet" aligned
arrow  from PARQUET.s  to F1.n
line  to PARQUET.s  from F1.n "input" aligned "for filter" aligned
~~~

Using Cherry-pick there is first a stage of running the queries with `search-params`
and then we use the cardinalities of the answer set to sample from equi-width
bins.


<a name="filternull"></a>
# Filter null

Filter null is a filter to the [`search-params`](#searchparams)
endpoint to sample queries according to the cardinality.

Filter null deletes the queries that have an empty query set.

~~~pikchr
S: circle "Start" fit
arrow
P: box "param-search" fit
arrow color red
F1: box "filter-null" fit color red
arrow
circle "dataset" "ready" fit




H: box "make-histograms" fit at (P + (0,-2))
arrow from S.s to H.nw "optional" aligned ""
F: file "histogram" at ( P + (0,-1)) fit
arrow from H.n to F.s "generates" aligned ""
arrow from P.s to F.n "uses" aligned ""


DB: cylinder "DuckDB Database" fit at (P + (0,1))
arrow from P.n to DB.s "run" aligned "queries" aligned


PARQUET: file "cardinalities" fit at (P + (1.5,1))
arrow from P.ne to PARQUET.sw "generates" aligned "parquet" aligned
arrow  from PARQUET.s  to F1.n
line  to PARQUET.s  from F1.n "input" aligned "for filter" aligned
~~~

<a name = "llm"></a>
# LLM pipeline

Once we have a set of queries we want to augment using LLMs we can use
the LLM endpoint `add-complex-queries`.

For a more in detail description [see the LLM wiki](/wiki?name=LLM%20augmentation)

~~~pikchr
S: circle "Start" fit
arrow
P: box "param-search" fit
arrow
D: diamond "Choose" "filter" fit
F1: box "cherry-pick" fit at (D + (2,1))
arrow  from D.e to F1.w
F2: box "filter-null" fit at (D + (2,0))
arrow from D.e to F2.w
F3: box "no filter" fit at (D + (2,-1))
arrow from D.e to F3.w

LLM: box "add-complex-queries" fit at (F2 + (2,0))
arrow
circle "dataset" "ready"


arrow from F1.e to LLM.w
arrow from F2.e to LLM.w
arrow from F3.e to LLM.w



H: box "make-histograms" fit at (P + (0,-2))
arrow from S.s to H.nw "optional" aligned ""
F: file "histogram" at ( P + (0,-1)) fit
arrow from H.n to F.s "generates" aligned ""
arrow from P.s to F.n "uses" aligned ""


DB: cylinder "DuckDB Database" fit at (P + (0,1))
arrow from P.n to DB.s "run" aligned "queries" aligned


PARQUET: file "cardinalities" fit at (P + (1.5,1))
arrow from P.ne to PARQUET.sw "generates" aligned "parquet" aligned
arrow dashed from PARQUET.e to F1.w "input" ""
arrow dashed from PARQUET.e to F2.w "input               " aligned ""
~~~


<a name = "union"></a>
# Union pipeline
Union works after the filters. Generating a union dataset.

~~~pikchr
S: circle "Start" fit
arrow
P: box "param-search" fit
arrow
D: diamond "Choose" "filter" fit
F1: box "cherry-pick" fit at (D + (2,1))
arrow  from D.e to F1.w
F2: box "filter-null" fit at (D + (2,0))
arrow from D.e to F2.w
F3: box "no filter" fit at (D + (2,-1))
arrow from D.e to F3.w

LLM: box "union-queries" fit at (F2 + (2,0))
arrow
circle "dataset" "ready"


arrow from F1.e to LLM.w
arrow from F2.e to LLM.w
arrow from F3.e to LLM.w



H: box "make-histograms" fit at (P + (0,-2))
arrow from S.s to H.nw "optional" aligned ""
F: file "histogram" at ( P + (0,-1)) fit
arrow from H.n to F.s "generates" aligned ""
arrow from P.s to F.n "uses" aligned ""


DB: cylinder "DuckDB Database" fit at (P + (0,1))
arrow from P.n to DB.s "run" aligned "queries" aligned


PARQUET: file "cardinalities" fit at (P + (1.5,1))
arrow from P.ne to PARQUET.sw "generates" aligned "parquet" aligned
arrow dashed from PARQUET.e to F1.w "input" ""
arrow dashed from PARQUET.e to F2.w "input               " aligned ""
~~~
