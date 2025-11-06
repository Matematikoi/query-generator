from enum import StrEnum


class EndpointName(StrEnum):
  EXTENSIONS_WITH_OLLAMA = "extensions_with_ollama"
  SYNTHETIC_GENERATION = "synthetic_generation"
  FILTER = "filter"
  GENERATE_DB = "generate_db"
  HISTOGRAM = "histogram"
  FIX_TRANSFORM = "fix_transform"
  PROMPTS = "prompts"

TOML_EXAMPLE: dict[EndpointName, str] = {
  EndpointName.EXTENSIONS_WITH_OLLAMA: """\
llm_extension = true
union_extension = true
queries_parquet = "tmp/filtered_queries/filtered.parquet"
destination_folder = "tmp/extended_queries"
ollama_model = "deepseek-r1:1.5b"

[union_params]
max_queries = 5
probability = 0.7

[llm_params]
database_path = "data/duckdb/TPCDS/0.db"
retry = 1
total_queries = 5
prompts_path = "params_config/prompts/basic_prompt.toml"
schema_path = "params_config/schemas/dev.txt"
""",
  EndpointName.PROMPTS:"""
base_prompt = "use the {{schema}} keyword to append the schema"

[weighted_prompts.prompt_1]
prompt = "some instruction"
weight = 30

[weighted_prompts.outer_join]
prompt = "Another instruction"
weight = 2

""",
  EndpointName.SYNTHETIC_GENERATION: """\
dataset = "TPCDS"
duckdb_database = "path/to/duckdb.db"
output_folder = "path/to/destination/"
histogram_path = "path/to/histogram.parquet"

unique_joins = true
max_signatures_per_fact_table = 1
max_queries_per_signature = 2

max_hops = [1]
keep_edge_probability = [0.2]

extra_predicates = [5]
row_retention_probability = [0.2, 0.9]
equality_lower_bound_probability = [0,0.1]
extra_values_for_in = 3

[operator_weights]
operator_in = 1
operator_range = 3
operator_equal = 3
""",
  EndpointName.FILTER: """\
input_parquet = "/path/to/file.parquet"
destination_folder = "/path/to/destination/"
empty_set = true
stratified_sampling = true
[stratified_sampling_config]
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
database_path = "path/to/duckdb.db"
output_folder = "path/to/destination/"
histogram_size = 51
common_values_size = 10
""",
  EndpointName.FIX_TRANSFORM: """\
queries_folder = "path/to/queries/"
duckdb_database  = "path/to/duckdb_database.duckdb"
destination_folder = "path/to/destination/"
max_output_size = 1000
timeout_seconds = 10
""",
}
