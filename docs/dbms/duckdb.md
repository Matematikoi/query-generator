# Use of DuckDB
Index

1. [Synthetic Queries](#synthetic-queries)
1. [DuckDB usage](#duckdb-usage)

# Synthetic queries
We use pypika to construct our synthetic queries. Pypika works as a query
builder and it allows to change between SQL dialects easily. The file in
resposible for the query building is 
[the query builder](../../src/query_generator/synthetic_queries/query_builder.py).
We use the `QueryBuilderPypika` methods `generate_query_from_subgraph` and
`add_predicates` to generate the base query from the join structure and then
add statistics-based predicates.


### Pypika
You can read more about pypika 
[in the pypika docs.](https://pypika.readthedocs.io/en/latest/)
Pypika allows to write queries in a more functional approach:
```python
q = Query.from_('customers').select('id', 'fname', 'lname', 'phone')
```
and get the sql code associated with it:
```python
q.get_sql()
```

Currently we use the `OracleQuery` dialect, because it works for both DuckDB
and Spark. More dialects like `POSTGRESQL` and `REDSHIFT` are available
in the [Pypika module](https://pypika.readthedocs.io/en/latest/api/pypika.enums.html?highlight=oracle#pypika.enums.Dialects)

The only place where we would need to change to add more dialects is in 
the [query builder file](../../src/query_generator/synthetic_queries/query_builder.py)
where we select oracle. This can easily turn into a knob.
```
    query = OracleQuery().select(fn.Count("*"))
```

# DuckDB usage
DuckDB is used in three main parts:
1. To calculate the stats.
2. When generating the LLM queries to validate they can run in a small sample database.
3. When collecting the traces that are later on plotted.
