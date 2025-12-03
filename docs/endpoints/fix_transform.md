# Attributes

- `queries_folder` (str): The folder containing the sql queries to
    which the LIMIT will be added.
- `destination_folder` (str): The folder to save the formatted queries.
- `duckdb_database` (str): Path to the duckdb database to validate queries.
It will also do a trace collection.
- `max_output_size` (int): The maximum output size for the queries. Queries
with an output tuple size greater than this value will have a LIMIT added.
If the value is 0, no limit will be imposed.    
- `timeout_seconds` (float): The maximum amount of seconds the query is 
allowed to run. Queries beyond this threshold will not be "valid" queries.
- `filter_empty_set` (bool): Whether to filter out queries that return
an empty set. If set to true, only queries that return at least one
tuple will be kept. By default is set to False.
- `make_select_group_by_disjoint` (bool): Whether to make the select clause
attributes disjoint from the group by clause attributes. By default
is set to False.
- `make_count_statement_diverse` (bool): Whether to change the COUNT statements
to other aggregate functions or COUNT variants. By default is set to False.
- `max_memory_gb` (int): The maximum amount of memory in gigabytes that
duckdb is allowed to use while running the queries. By default is set to 5.

Since the limit on queries will be imposed based on the output of the queries,
the queries need to be run to collect their output sizes.
We do another pass of query running to collect the final traces and leave them
in the DUCKDB_TRACES folder.

# Transformations

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
