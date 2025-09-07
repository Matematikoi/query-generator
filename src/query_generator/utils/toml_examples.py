TOML_EXAMPLE = {
  "llm_augmentation": '''\
llm = false
retry = 1
llm_model = "llama4:16x17b"
llm_base_prompt = """
    You are a database expert. And you are writing \
    queries to test your current database with the TPCDS \
    dataset with challenging and diverse queries. You are \
    currently re-writing new queries that should not be \
    trivial or equivalent to the input query.\
    When answering, only answer with the query in markdown,\
    for example:\
    ```sql select * from table```
    """
queries_path = "/path/to/synthetic/queries"
total_queries = 10000
seed = 424
dataset = "TPCDS"
destination_folder = "/path/to/destination"

[llm_prompts.self_join]
prompt = """Your task is modify this query to add \
    a self-join while keeping the predicates"""
weight = 10

[llm_prompts.outer_join]
prompt = """Your task is to modify one join to add \
    an outer-join while keeping the predicates"""
weight = 10
''',
  "synthetic_generation": """\
dataset = "JOB"
duckdb_database = "path/to/duckdb.db"
output_folder = "path/to/destination/"
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
""",
  "filter": """\
input_parquet = "/path/to/file.parquet"
destination_folder = "/path/to/destination/"
filter_null = true
cherry_pick = true
[cherry_pick_config]
queries_per_bin = 100
upper_bound = 1000000
total_bins = 100
  """,
  "generate_db": """\
dataset = "TPCDS"
scale_factor = 0.1
db_path = "path/to/duckdb.db"
""",
}
