"""Tests for BedrockBatchClient with mocked boto3."""

import json
from unittest.mock import MagicMock, patch

from query_generator.extensions.llm_clients import (
  BatchRequest,
  BedrockBatchClient,
  _parse_s3_uri,
)


def _make_requests(count: int = 2) -> list[BatchRequest]:
  return [
    BatchRequest(
      custom_id=f"req-{i}",
      messages=[
        {"role": "system", "content": "You are a SQL expert."},
        {"role": "user", "content": f"Write query {i}"},
      ],
      model="anthropic.claude-sonnet-4-20250514-v1:0",
    )
    for i in range(count)
  ]


def test_parse_s3_uri() -> None:
  bucket, key = _parse_s3_uri("s3://my-bucket/some/prefix/")
  assert bucket == "my-bucket"
  assert key == "some/prefix/"


def test_parse_s3_uri_no_key() -> None:
  bucket, key = _parse_s3_uri("s3://my-bucket")
  assert bucket == "my-bucket"
  assert key == ""


@patch("query_generator.extensions.llm_clients.boto3")
def test_submit_batch(mock_boto3: MagicMock) -> None:
  """Verifies JSONL format, S3 upload, and job creation."""
  mock_s3 = MagicMock()
  mock_bedrock = MagicMock()
  mock_boto3.client.side_effect = (
    lambda svc, **kw: mock_s3 if svc == "s3" else mock_bedrock
  )
  mock_bedrock.create_model_invocation_job.return_value = {
    "jobArn": "arn:aws:bedrock:us-east-1:123:job/abc"
  }

  client = BedrockBatchClient(
    s3_input_uri="s3://my-bucket/input/",
    s3_output_uri="s3://my-bucket/output/",
    role_arn="arn:aws:iam::123:role/Role",
    region="us-east-1",
    model="anthropic.claude-sonnet-4-20250514-v1:0",
  )
  requests = _make_requests(2)
  job_arn = client.submit_batch(requests)

  assert job_arn == "arn:aws:bedrock:us-east-1:123:job/abc"
  mock_s3.put_object.assert_called_once()
  put_call = mock_s3.put_object.call_args
  body = put_call.kwargs["Body"].decode()
  lines = [json.loads(l) for l in body.strip().split("\n")]
  assert len(lines) == 2
  assert lines[0]["recordId"] == "req-0"
  assert "anthropic_version" in lines[0]["modelInput"]
  assert lines[0]["modelInput"]["system"] == "You are a SQL expert."
  # System message should not be in messages list
  for msg in lines[0]["modelInput"]["messages"]:
    assert msg["role"] != "system"

  mock_bedrock.create_model_invocation_job.assert_called_once()


@patch("query_generator.extensions.llm_clients.boto3")
def test_poll_batch_completed(mock_boto3: MagicMock) -> None:
  """Mocks InProgress -> Completed transition."""
  mock_s3 = MagicMock()
  mock_bedrock = MagicMock()
  mock_boto3.client.side_effect = (
    lambda svc, **kw: mock_s3 if svc == "s3" else mock_bedrock
  )

  mock_bedrock.get_model_invocation_job.side_effect = [
    {
      "status": "InProgress",
      "outputDataConfig": {
        "s3OutputDataConfig": {"s3Uri": "s3://my-bucket/output/job-123/"}
      },
    },
    {
      "status": "Completed",
      "outputDataConfig": {
        "s3OutputDataConfig": {"s3Uri": "s3://my-bucket/output/job-123/"}
      },
    },
  ]

  client = BedrockBatchClient(
    s3_input_uri="s3://my-bucket/input/",
    s3_output_uri="s3://my-bucket/output/",
    role_arn="arn:aws:iam::123:role/Role",
    region="us-east-1",
    model="anthropic.claude-sonnet-4-20250514-v1:0",
  )
  status = client.poll_batch("job-arn-123", poll_interval=0.01)
  assert status == "completed"


@patch("query_generator.extensions.llm_clients.boto3")
def test_poll_batch_stopped(mock_boto3: MagicMock) -> None:
  """Verifies Stopped maps to cancelled."""
  mock_s3 = MagicMock()
  mock_bedrock = MagicMock()
  mock_boto3.client.side_effect = (
    lambda svc, **kw: mock_s3 if svc == "s3" else mock_bedrock
  )

  mock_bedrock.get_model_invocation_job.return_value = {
    "status": "Stopped",
    "outputDataConfig": {
      "s3OutputDataConfig": {"s3Uri": "s3://my-bucket/output/job-123/"}
    },
  }

  client = BedrockBatchClient(
    s3_input_uri="s3://my-bucket/input/",
    s3_output_uri="s3://my-bucket/output/",
    role_arn="arn:aws:iam::123:role/Role",
    region="us-east-1",
    model="anthropic.claude-sonnet-4-20250514-v1:0",
  )
  status = client.poll_batch("job-arn-123", poll_interval=0.01)
  assert status == "cancelled"


@patch("query_generator.extensions.llm_clients.boto3")
def test_download_results(mock_boto3: MagicMock) -> None:
  """Mocks S3 download with Bedrock output format."""
  mock_s3 = MagicMock()
  mock_bedrock = MagicMock()
  mock_boto3.client.side_effect = (
    lambda svc, **kw: mock_s3 if svc == "s3" else mock_bedrock
  )

  output_lines = [
    json.dumps(
      {
        "recordId": "req-0",
        "modelOutput": {
          "content": [{"text": "SELECT 1"}],
        },
      }
    ),
    json.dumps(
      {
        "recordId": "req-1",
        "modelOutput": {
          "content": [{"text": "SELECT 2"}],
        },
      }
    ),
  ]
  output_body = "\n".join(output_lines) + "\n"

  mock_paginator = MagicMock()
  mock_s3.get_paginator.return_value = mock_paginator
  mock_paginator.paginate.return_value = [
    {
      "Contents": [
        {"Key": "output/job-123/results.jsonl.out"},
      ]
    }
  ]
  mock_s3.get_object.return_value = {
    "Body": MagicMock(read=lambda: output_body.encode())
  }

  client = BedrockBatchClient(
    s3_input_uri="s3://my-bucket/input/",
    s3_output_uri="s3://my-bucket/output/",
    role_arn="arn:aws:iam::123:role/Role",
    region="us-east-1",
    model="anthropic.claude-sonnet-4-20250514-v1:0",
  )
  # Simulate that poll_batch was called and stored the output URI
  client._job_output_uri["job-arn"] = "s3://my-bucket/output/job-123/"

  results = client.download_results("job-arn")
  assert len(results) == 2
  assert results[0].custom_id == "req-0"
  assert results[0].content == "SELECT 1"
  assert results[0].error is None
  assert results[1].custom_id == "req-1"
  assert results[1].content == "SELECT 2"


@patch("query_generator.extensions.llm_clients.boto3")
def test_download_results_with_error(mock_boto3: MagicMock) -> None:
  """Verifies error records are parsed correctly."""
  mock_s3 = MagicMock()
  mock_bedrock = MagicMock()
  mock_boto3.client.side_effect = (
    lambda svc, **kw: mock_s3 if svc == "s3" else mock_bedrock
  )

  output_lines = [
    json.dumps(
      {
        "recordId": "req-0",
        "error": {"message": "Model throttled"},
      }
    ),
  ]
  output_body = "\n".join(output_lines) + "\n"

  mock_paginator = MagicMock()
  mock_s3.get_paginator.return_value = mock_paginator
  mock_paginator.paginate.return_value = [
    {
      "Contents": [
        {"Key": "output/job-123/results.jsonl.out"},
      ]
    }
  ]
  mock_s3.get_object.return_value = {
    "Body": MagicMock(read=lambda: output_body.encode())
  }

  client = BedrockBatchClient(
    s3_input_uri="s3://my-bucket/input/",
    s3_output_uri="s3://my-bucket/output/",
    role_arn="arn:aws:iam::123:role/Role",
    region="us-east-1",
    model="anthropic.claude-sonnet-4-20250514-v1:0",
  )
  client._job_output_uri["job-arn"] = "s3://my-bucket/output/job-123/"

  results = client.download_results("job-arn")
  assert len(results) == 1
  assert results[0].content is None
  assert results[0].error is not None
