from enum import StrEnum


class EndpointName(StrEnum):
  EXTENSION_AND_OLLAMA = "extension_and_ollama"
  SYNTHETIC_GENERATION = "synthetic_generation"
  FILTER = "filter"
  GENERATE_DB = "generate_db"
  HISTOGRAM = "histogram"
  FIX_TRANSFORM = "fix_transform"


TOML_EXAMPLE = {
  EndpointName.EXTENSION_AND_OLLAMA: '''\
llm_extension = true
union_extension = true
queries_parquet = "tmp/filtered_queries/filtered.parquet"
destination_folder = "tmp/extended_queries"

[union_params]
max_queries = 5
probability = 0.7

[llm_params]
database_path = "data/duckdb/TPCDS/0.db"
retry = 1
total_queries = 5
llm_model = "deepseek-r1:1.5b"
llm_base_prompt = """
    You are writing queries for a markdown text using \
    the format:```sql for correct formatting in markdown

    your task is to write the given sql query again but with modificiation
    surrounding it with ```sql Select from....```
    """

[llm_params.llm_prompts.self_join]
prompt = "Add a self join to the query"
weight = 30

[llm_params.llm_prompts.outer_join]
prompt = "Add an outer join to the query"
weight = 30
''',
  EndpointName.SYNTHETIC_GENERATION: """\
dataset = "JOB"
duckdb_database = "path/to/duckdb.db"
output_folder = "path/to/destination/"
max_hops = [1]
extra_predicates = [5]
row_retention_probability = [0.2, 0.9]
unique_joins = true
max_signatures_per_fact_table = 1
max_queries_per_signature = 2
keep_edge_probability = [0.2]
equality_lower_bound_probability = [0,0.1]
extra_values_for_in = 3
histogram_path = "path/to/histogram"

[operator_weights]
operator_in = 1
operator_range = 3
operator_equal = 3
""",
  EndpointName.FILTER: """\
input_parquet = "/path/to/file.parquet"
destination_folder = "/path/to/destination/"
filter_null = true
cherry_pick = true
[cherry_pick_config]
queries_per_bin = 100
upper_bound = 1000000
total_bins = 100
  """,
  EndpointName.GENERATE_DB: """\
dataset = "TPCDS"
scale_factor = 0.1
db_path = "path/to/duckdb.db"
""",
  EndpointName.HISTOGRAM: """\
output_folder = "path/to/destination/"
database_path = "path/to/duckdb.db"
histogram_size = 51
common_values_size = 10
""",
  "fix_transform": """\
destination_folder = "path/to/destination/"
max_output_size = 1000
queries_folder = "path/to/queries/"
timeout_seconds = 10
duckdb_database  = "path/to/duckdb_database.duckdb"
""",
}
