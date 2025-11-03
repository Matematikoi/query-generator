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
- `llm_model` (str): The model to use for the LLM from ollama. You can
see a list of available models in 
[https://ollama.com/library](https://ollama.com/library). This parameter
is only mandatory if the `llm_extension` is set to true.

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

- `database_path` (str): The path to the DuckDB database file. Used to confirm
query validity.
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

## Attributes prompts
The file selected in the `prompts_path` is also a toml file that has
the following structure.

Before being able to run this model be sure to have the LLM model loaded
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
