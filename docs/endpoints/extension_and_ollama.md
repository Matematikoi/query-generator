# Attributes:
- llm_extension (bool): Whether to use the LLM extension.
- union_extension (bool): Whether to use the union extension.
- queries_parquet (str): The path to the parquet file generated in the
`synthetic-queries` step or in the `filter-synthetic` step.
- destination_folder (str): The folder to save the generated complex queries.
- union_params (UnionParams): The params used for the union generation. See
details below
- llm_params (LLMParams | None): The parameters for the LLM. See below for
details.


## Attributes Union params:
- max_queries (int): The maximum number of queries to union. Default is 5.
- probability (float): The probability of using UNION instead of UNION ALL.
Default is 0.5.

## Attributes llm params:
For each prompt that the model passes to the LLM model, it will also randomly
pick one synthetic query to modify. Thus the prompts are about modifying 
a query and not creating it from scratch.

Before being able to run this model be sure to have the LLM model loaded
in ollama. This means that you have run `ollama pull {{model_name}}` and
that `ollama run {{model_name}}` is working.

- database_path (str): The path to the DuckDB database file. Used to confirm
query validity.
- llm_base_prompt (str): The base prompt to use for the LLM. This means
that all queries will have this prompt injected at the start of the initial
message. Use it to send information about the schema. This endpoint 
parses queries when send in markdown format, so we also specify to 
surround the queries by markdown notation (```sql ```).
- llm_model (str): The model to use for the LLM from ollama. You can
see a list of available models in 
[https://ollama.com/library](https://ollama.com/library)
- total_queries (int): The total number of queries to process with LLM. This
is not the total number of queries produced since some cases may fail to
generate a valid query.
- retry (int): The number of times to retry generating a query if it fails.
This means that everytime a prompt has an error, we take the DBMS error
and send it back to the LLM for them to fix. Common errors are having
a wrong column name, or syntax errors.
- llm_prompts (dict[str, ComplexQueryLLMPrompt]): A dictionary of
    additional prompts to use for the LLM.
    This dictionary maps any operations e.g. `group_by` to a prompt
    configuration. You need to specify two attributes for each prompt,
    a `prompt` that will be used and a `weight` that defines the
    probability of using that prompt.
    The weights do not need to sum up to 1, they will be normalized
    automatically.
    The higher the weight, the more likely the prompt will be used.
    After each prompt a synthetic query will be added to be used as the base
    query for the LLM model.
    - prompt (str): The prompt to use in combination with the base prompt and
    the example synthetic query
    - weight (float): The weight of the prompt. It assigns a probability to
    select this prompt over the others. The values don't have to add up
    to 1 since they will be normalized. 

examples of toml files can be found in:
`params_config/complex_queries/*toml`