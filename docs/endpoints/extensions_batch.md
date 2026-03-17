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
This endpoint uses batch APIs to process all queries in bulk instead of one
at a time. This gives a 50% cost reduction compared to the online endpoint.
Both `openai` and `bedrock` providers are supported for batch mode.

- `provider` (str): `"openai"` or `"bedrock"` for the batch endpoint.
- `model` (str): The model name (e.g., `"gpt-4o-mini"` for OpenAI or
  `"anthropic.claude-sonnet-4-20250514-v1:0"` for Bedrock).
- `batch_size` (int): Number of queries per batch submission.
  Default is 100. OpenAI has a limit of 50,000 requests per batch.
- `batch_poll_interval_seconds` (float): Seconds to sleep between polls
  when waiting for a batch to complete. Default is 30.0.
- `database_path` (str): The path to the DuckDB database file. Used to
  validate generated queries.
- `total_queries` (int): The total number of queries to process.
  OpenAI requires a minimum of 100 requests per batch.
- `retry` (int): Number of retry rounds for failed queries. Each retry
  round re-submits all failed queries as a new batch with the DuckDB
  error appended to the conversation.
- `schema_path` (str): Path to the schema file used in prompts.
- `prompts_path` (str): Path to the TOML file containing prompts.
- `duckdb_timeout_seconds` (float): Timeout for DuckDB validation.
  Default is 5 seconds.

### Bedrock-only fields

These fields are required when `provider = "bedrock"`:

- `s3_input_uri` (str): S3 URI for uploading batch input JSONL
  (e.g., `"s3://my-bucket/batch-input/"`).
- `s3_output_uri` (str): S3 URI where Bedrock writes output JSONL
  (e.g., `"s3://my-bucket/batch-output/"`).
- `bedrock_role_arn` (str): IAM role ARN that Bedrock assumes to
  read/write S3 (e.g., `"arn:aws:iam::123456789012:role/BedrockBatchRole"`).
- `aws_region` (str): AWS region for Bedrock and S3
  (e.g., `"us-east-1"`).

## How it works

1. Sample `total_queries` synthetic queries from the input parquet.
2. Build LLM prompts for all queries (same prompt logic as `extensions-online`).
3. Chunk queries into groups of `batch_size` and submit each chunk as a
   batch (OpenAI Batch API or Bedrock model invocation job).
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

## Bedrock Provider Setup

To use the Bedrock provider, follow these steps:

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

### 4. Create S3 bucket
Create an S3 bucket in the same region as Bedrock for batch input/output:
```bash
aws s3 mb s3://my-batch-bucket --region us-east-1
```

### 5. Create IAM role for Bedrock
Create an IAM role that Bedrock can assume, with permissions to read/write
your S3 bucket. The trust policy should allow `bedrock.amazonaws.com`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"Service": "bedrock.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }
  ]
}
```

Attach a policy granting S3 access:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::my-batch-bucket",
        "arn:aws:s3:::my-batch-bucket/*"
      ]
    }
  ]
}
```

### 6. Configure TOML
```toml
[llm_params]
provider = "bedrock"
model = "anthropic.claude-sonnet-4-20250514-v1:0"
s3_input_uri = "s3://my-batch-bucket/batch-input/"
s3_output_uri = "s3://my-batch-bucket/batch-output/"
bedrock_role_arn = "arn:aws:iam::123456789012:role/BedrockBatchRole"
aws_region = "us-east-1"
```
