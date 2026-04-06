# Attributes

- `llm_extension` (bool): Whether to use the LLM extension.
- `union_extension` (bool): Whether to use the union extension.
- `queries_parquet` (str): The path to the parquet file generated in the
`synthetic-queries` step or in the `filter-synthetic` step.
- `destination_folder` (str): The folder to save the generated complex queries.
- `union_params` (UnionParams): The params used for the union generation. See
details below
- `llm_params` (LLMParams | None): The parameters for the LLM. See below for
details.

## Attributes Union params
We generate at most one union query per unique join structure. If there
are not at least two queries with the same join structure, then no union
query would be produced for that join structure.

- `max_queries` (int): The maximum number of queries to union in one union
query. Default is 5.
- `probability` (float): The probability of using UNION instead of UNION ALL.
Default is 0.5.

## Attributes llm_params
This Attributes are mandatory when the `llm_extension` is true.
For each prompt that the model passes to the LLM model, it will also randomly
pick one synthetic query to modify. Thus the prompts are about modifying
a query and not creating it from scratch.

- `provider` (str): The LLM provider to use. Supported values: `"ollama"`,
`"openai"`, `"openai-flex"`, `"bedrock"`. Defaults to `"ollama"`.
`"openai-flex"` uses OpenAI's flex service tier (`service_tier="flex"`),
which provides batch-level pricing (50% discount) through the standard
Chat Completions API — no file uploads or polling required.
`"bedrock"` uses AWS Bedrock to call Claude models via the Anthropic SDK.
Requires `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
environment variables.
- `model` (str): The model name to use for the LLM. When using the ollama
provider, you can see a list of available models in
[https://ollama.com/library](https://ollama.com/library). This parameter
is mandatory when `llm_extension` is set to true.
- `validator_engine` (str): The query validation engine to use. Supported values:
`"duckdb"` (default) or `"pyspark"`. When `"pyspark"`, `database_path` must
point to a parquet directory with structure `database_path/table_name/data.parquet`
(as produced by `generate-db` with `parquet_path`).
- `database_path` (str): The path to the database used for query validation.
When `validator_engine` is `"duckdb"`, this should be a `.duckdb` or `.db`
duckdb database file. When `validator_engine` is `"pyspark"`, this should be a
parquet directory (as produced by `generate-db` with `parquet_path`).
- `total_queries` (int): The total number of queries to process with LLM. This
is not the total number of queries produced since some cases may fail to
generate a valid query.
- `retry` (int): The number of times to retry generating a query if it fails.
This means that everytime a prompt has an error, we take the DBMS error
and send it back to the LLM for them to fix. Common errors are having
a wrong column name, or syntax errors.
- `schema_path` (str): The path to the schema used. Used to add it into 
the basic prompts mention in the `prompts_path`. The file can be any
plain file, like a txt.
- `prompts_path` (str): The path to the toml file that contains the prompts.
the details on the toml file are below.
- `duckdb_timeout_seconds` (float): The timeout time for query validation
with the selected validator engine. By default is 20 seconds.
- `function_examples_path` (str | None): Optional path to a TOML file
containing SQL function examples (e.g.,
`params_config/functions/standard_sql_functions.toml`). Default is None.
See [Function examples in prompts](#function-examples-in-prompts) below
for details.
- `number_of_function_examples` (int): The number of function examples to
sample and include in each prompt. Only used when `function_examples_path`
is set. Default is 5.

## Attributes prompts
The file selected in the `prompts_path` is also a toml file that has
the following structure.

When using the ollama provider, be sure to have the LLM model loaded
in ollama. This means that you have run `ollama pull {model_name}` and
that `ollama run {model_name}` is working.

- `llm_base_prompt` (str): The base prompt to use for the LLM. This means
that all queries will have this prompt injected at the start of the initial
message. Use it to send information about the schema. This endpoint 
parses queries when send in markdown format, so we also specify to 
surround the queries by markdown notation (```sql ```). 
The `{schema}` keyword is used to append the schema mentioned in 
the `schema_path`. 
- `weighted_prompts` (dict[str, ComplexQueryLLMPrompt]): A dictionary of
additional prompts to use for the LLM.
This dictionary maps any operations e.g. `group_by` to a prompt
configuration. You need to specify two attributes for each prompt,
a `prompt` that will be used and a `weight` that defines the
probability of using that prompt.
The weights do not need to sum up to 1, they will be normalized automatically.
The higher the weight, the more likely the prompt will be used.
After each prompt a synthetic query will be added to be used as the base
query for the LLM model.
    - `prompt` (str): The prompt to use in combination with the base prompt and
    the example synthetic query
    - `weight` (float): The weight of the prompt. It assigns a probability to
    select this prompt over the others. The values don't have to add up
    to 1 since they will be normalized. 

## Function examples in prompts

Without function examples, the synthetic queries produced by the pipeline
tend to cover only a narrow set of SQL functions—mostly basic aggregates
and arithmetic. Functional-coverage analysis shows that the synthetic-only
stage covers as few as 8 out of 188 catalogued functions, and even after
LLM augmentation the coverage varies widely depending on the model used.
The root cause is that the LLM has no signal about *which* functions to
introduce; it defaults to the ones it sees most often in training data.

To address this, `function_examples_path` points to a TOML file that
catalogues SQL functions organized by category and subcategory (window,
aggregate, scalar, conditional, etc.), each with a concrete example query.
At prompt time, the pipeline randomly samples `number_of_function_examples`
entries from this file and includes them in the user message. The prompt
places the synthetic query first as reference, then the task instruction,
then the function examples close to the output. The resulting prompt
looks like:

```text
This is the query you will be modifying. You can base yourself upon it:

<synthetic query>

<weighted prompt text>

Add the following SQL functions to the modified query. Skip any that are impossible to fit:

- Function: CumeDist (window.distribution)
  Example:
  ```sql
  SELECT ss_sales_price, CUME_DIST() OVER (ORDER BY ss_sales_price) AS cume_dist FROM store_sales LIMIT 5
  ```

- Function: Corr (agg.statistical)
  Example:
  ```sql
  SELECT CORR(ss_sales_price, ss_quantity) AS price_qty_corr FROM store_sales LIMIT 5
  ```

Return only the modified query in ```sql ``` format.
```

Because the sampling is random and per-prompt, successive runs naturally
spread coverage across the full function taxonomy. The provided
`standard_sql_functions.toml` contains 130+ Spark-safe function examples
across categories such as `window.ranking`, `agg.statistical`,
`scalar.string`, `scalar.datetime`, and `conditional.case`, so setting
this parameter helps the workload cover functions that would otherwise
rarely appear.

# Output

The queries for union will be saved under the `./union/` folder.
The rest of the queries will be saved according to the names
given to each `llm_prompt`. 

We also output some additional files for debugging, including the 
`logs.parquet` which includes all the dialog with the LLM that 
occurred during the query generation process. There is an `error`
columns in this `logs.parquet` destined to show the last error found when
trying to run the query; a `None` object will most likely mean that 
the parser didn't found a SQL query in the LLM output. This is common
in small LLM that don't understand they need to surround the text with 
triple quotes.
A summarized version for the queries that were valid is included in 
`llm_extension.parquet`. A log of the union queries is also generated
if the endpoint is used under the `union_description.parquet`.

## Bedrock Anthropic Provider Setup

To use the Bedrock Anthropic provider, follow these steps:

### 1. Enable Claude model access
Go to the [AWS Bedrock console](https://console.aws.amazon.com/bedrock)
and request access to the Anthropic Claude models in your target region.

### 2. Create IAM user credentials
Create an IAM user with programmatic access. Go to **IAM > Users >
Security credentials > Create access key**.

### 3. Set environment variables
```bash
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
```
