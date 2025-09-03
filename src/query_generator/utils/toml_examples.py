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
  "snowflake": """\
dataset = "TPCDS"
max_hops = 3
max_queries_per_fact_table = 100
max_queries_per_signature = 1
keep_edge_probability = 0.2
[predicate_parameters]
row_retention_probability = 0.2
equality_lower_bound_probability = 0.01
extra_values_for_in = 3
extra_predicates = 3
[predicate_parameters.operator_weights]
operator_in = 1
operator_range = 3
operator_equal = 3
""",
}
