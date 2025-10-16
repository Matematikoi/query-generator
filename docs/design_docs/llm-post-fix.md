# General Problem

We would like to be able to fix a set of queries with LLMs but the current infrastructure doesn’t allow us to do any patch to the query-sets.

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
// <!-- ~~~ -->

Current infrastructure

## Specific Problem

We have spent a lot of resource analyzing and studying the LLM1 5k  but we have some issues with queries that require fixing. So we want to make small "Patch" that will fix this queries.


## Limitations of current `extensions-and-llm` endpoint
We are using that endpoint to do union and llm. So adding a post filter
fix will make it messy.

## Functions that can be reused:
This functions don't need rewriting:
1. query_llm
1. get_random_queries
1. extract_sql
1. validate_query_dukdb



## Requirement
We want our new endpoint to fix more than one problem at a time. Current 
problem is the following:

~~~pikchr
circle "start" fit
arrow
D1 : diamond "Does it have" "a group by?" fit
arrow "yes" above
B: box "fix the" "group by" fit
arrow
END: circle "end" fit
spline from D1.s down then right until even with END.s \
then up to END.s 
text "No" at B + (0,-1.1)
~~~

But we might have an issue of multiple output condition.

~~~pikchr
circle "start" fit
arrow
D1 : diamond "condition" fit
arrow "yes" above
B: box "fix 1" fit
arrow
END: circle "end" fit
B2: box at (B+(0,-1)) "Fix 2" fit
arrow from D1.s to B2.w "no" above aligned
arrow from B2.e to END.w
~~~
We can also have nested conditions like

~~~pikchr
circle "start" fit
arrow
D1 : diamond "condition 1" fit
arrow "yes" above
B: diamond "condition 2" fit
arrow
box "fix 1" fit
arrow
END: circle "end" fit
B2: box at (B+(0,-1)) "Fix 2" fit
arrow from D1.s to B2.w "no" above aligned
arrow from B2.e to END.w
~~~

Thus our solution should be able to have some sort of solution
for "multiple output conditions" and "nested conditions". 

Since we are dealing with LLMs we can see that nested conditions can be 
expressed with a concatenations, so we can send an LLM the nested condition
of cond1 and cond2 as : "If cond1 and cond2". The multiple output condition
can be explained with priority. We want some conditions to pass after another,
if the condition applies no other condition will apply.

## Proposed solution
Our solution is based on using priority to add conditions to the queries.

```toml
database_path = "path/"
queries_parquet = "path/"
base_prompt = "base promp...."

[prompts.prompt_1]
priority = 0
condition = "Some condition for the LLM"
fix= "Fix to apply if condition is fulfilled"


[prompts.prompt_2]
priority = 1
condition = "Another condition for the LLM"
fix= "Fix to apply if condition is fulfilled"

```

Arguments:
1. database_path
2. queries_parquet 
3. base_prompt
4. prompts
    1. priority
    1. condition
    1. fix


We might have to omit `queries_parquet` for the purpose of the run for fixing 
LLM1. 

# New architecture

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
FIX: box "fix-llm" fit
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
spline <- from DB.ne right 0.5 \
then to (LLM.w+(-0.25,-0.25)) to LLM.sw dashed "Syntax Check" aligned
spline dashed from FIX.s down 0.2 then to (LLM.w+(-0.25,-0.25))

PARQUET: file "cardinalities" fit at (P + (0.5,1.3))
arrow from P.n to PARQUET.sw "generates" aligned "parquet" aligned
arrow  from PARQUET.se  to F1.n "uses" aligned "cardinalities" aligned
~~~

# Endpoint diagram
For every query we have we run the following function:


~~~pikchr center
define aa {
    arrow 0.2
}



circle "start" fit
aa
L1: circle "" fit
aa
box "Get highest priority" "prompt" fit
aa
box "Query LLM" "for condition" fit
aa
D: diamond "Is condition" "fulfilled?" fit
arrow "Yes" above
box "Do the fix" fit
aa
box "extract query from" "LLM output" fit
aa
L2: diamond "is fixed?" fit
aa
circle "end" fit

spline -> from L2.s then down 0.7 then left until even with L1\
then to L1.s
text at (D + (0,-1.1)) "Loop for all conditions"

arrow from D.n up then right until even with L2 then down to L2.n
text "No" at (D+(1,1.1))
~~~