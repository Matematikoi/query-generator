# Attributes

- `llm_extension` (bool): Whether to use the LLM batch extension.
- `union_extension` (bool): Whether to use the union extension.
- `queries_parquet` (str): The path to the parquet file generated in the
`synthetic-queries` step or in the `filter-synthetic` step.
- `destination_folder` (str): The folder to save the generated complex queries.
- `union_params` (UnionParams): The params used for the union generation. See
details below.
- `llm_params` (LLMParams | None): The parameters for the LLM. See below for
details.

## Attributes Union params
Same as `extensions-online`. We generate at most one union query per unique
join structure.

- `max_queries` (int): The maximum number of queries to union. Default is 5.
- `probability` (float): The probability of using UNION instead of UNION ALL.
Default is 0.5.

## Attributes llm_params
This endpoint uses the OpenAI Batch API to process all queries in bulk
instead of one at a time. This gives a 50% cost reduction compared to the
online endpoint. Only the `openai` provider is supported for batch mode.

- `provider` (str): Must be `"openai"` for the batch endpoint.
- `model` (str): The OpenAI model name (e.g., `"gpt-4o-mini"`).
- `batch_size` (int): Number of queries per OpenAI batch submission.
  Default is 100. OpenAI has a limit of 50,000 requests per batch.
- `batch_poll_interval_seconds` (float): Seconds to sleep between polls
  when waiting for a batch to complete. Default is 30.0.
- `database_path` (str): The path to the DuckDB database file. Used to
  validate generated queries.
- `total_queries` (int): The total number of queries to process.
- `retry` (int): Number of retry rounds for failed queries. Each retry
  round re-submits all failed queries as a new batch with the DuckDB
  error appended to the conversation.
- `schema_path` (str): Path to the schema file used in prompts.
- `prompts_path` (str): Path to the TOML file containing prompts.
- `duckdb_timeout_seconds` (float): Timeout for DuckDB validation.
  Default is 5 seconds.

## How it works

1. Sample `total_queries` synthetic queries from the input parquet.
2. Build LLM prompts for all queries (same prompt logic as `extensions-online`).
3. Chunk queries into groups of `batch_size` and submit each chunk as an
   OpenAI batch.
4. Poll each batch until completion, then download and parse results.
5. Validate each generated SQL query against DuckDB.
6. Failed queries are re-submitted in subsequent retry rounds with the
   DuckDB error message appended to the conversation.
7. Results are saved incrementally as parquet files.

## Output

Same structure as `extensions-online`:
- SQL files organized by extension type under the destination folder.
- `llm_extension.parquet`: Summary of valid generated queries.
- `logs.parquet`: Full log including conversations and errors.
