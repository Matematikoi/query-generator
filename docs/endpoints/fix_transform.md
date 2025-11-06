# Attributes

- `queries_folder` (str): The folder containing the sql queries to
    which the LIMIT will be added.
- `destination_folder` (str): The folder to save the formatted queries.
- `duckdb_database` (str): Path to the duckdb database to validate queries.
It will also do a trace collection.
- `max_output_size` (int): The maximum output size for the queries. Queries
    with an output tuple size greater than this value will have a LIMIT added.
- `timeout_seconds` (float): The maximum amount of seconds the query is 
allowed to run. Queries beyond this threshold will not be "valid" queries.

Since the limit on queries will be imposed based on the output of the queries,
the queries need to be run. Additionally to collecting the queries 

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