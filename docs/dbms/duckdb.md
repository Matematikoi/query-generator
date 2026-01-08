# Use of DuckDB
Index

1. [Synthetic Queries](#synthetic-queries)
1. [DuckDB usage](#duckdb-usage)
1. [Hardcoded Schema](#hardcoded-schema)


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
```python
    query = OracleQuery().select(fn.Count("*"))
```

# DuckDB usage
DuckDB is used in three main parts:
1. Generate the Database.
1. To calculate the stats.
2. When generating the LLM queries to validate they can run in a small sample database.
3. When collecting the traces that are later on plotted.

## Generating the data 
We currently only generate data for TPC-DS and TPC-H. The generation method 
uses DuckDB libraries to generate the data. The data is saved in a DuckDB 
database. 

## Calculating the stats 
To calculate the stats we use for generating synthetic data we use a duckdb 
database that we query to get the stats we need.

## Generating LLM data 
When we generate an LLM query we do a sanity check to make sure that the
query is runnable by the DuckDB query engine, thus we run the query in 
a small dataset.

## Trace collection - Fix transform
We use duckdb to collect traces after we do LLM augmentation.



# Hardcoded schema
There are three hardcoded schemas that are used in the 
[get schemas file](../../src/query_generator/database_schemas/schemas.py).
These schemas are: TPC-DS, TPC-H, and JOB. The format is a messy and 
redundant, one example for TPC-DS is:
```json
"call_center": {
  "alias": "cc",
  "columns": {
    "cc_call_center_sk": {"max": 30, "min": 1},
    "cc_company": {"max": 6, "min": 1},
    "cc_division": {"max": 6, "min": 1},
    "cc_employees": {"max": 69113, "min": 3180},
    "cc_gmt_offset": {"max": -5.0, "min": -6.0},
    "cc_mkt_id": {"max": 6, "min": 1},
    "cc_open_date_sk": {"max": 2451120, "min": 2450812},
    "cc_rec_end_date": {"max": "2001-12-31", "min": "2000-01-01"},
    "cc_rec_start_date": {"max": "2002-01-01", "min": "1998-01-01"},
    "cc_sq_ft": {"max": 43081086, "min": 531060},
    "cc_tax_percentage": {"max": 0.12, "min": 0.0},
  },
  "foreign_keys": [],
},
"catalog_page": {
  "alias": "cp",
  "columns": {
    "cp_catalog_number": {"max": 109, "min": 1},
    "cp_catalog_page_number": {"max": 188, "min": 1},
    "cp_catalog_page_sk": {"max": 20400, "min": 1},
    "cp_end_date_sk": {"max": 2453186, "min": 2450844},
    "cp_start_date_sk": {"max": 2453005, "min": 2450815},
  },
  "foreign_keys": [],
}
```

A new format is required. Only the synthetic query generator uses this schema.
