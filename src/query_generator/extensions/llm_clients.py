"""LLM clients file."""

import io
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, Protocol, TypeVar, cast

import boto3
from ollama import Client
from openai import OpenAI

logger = logging.getLogger(__name__)


@dataclass
class BatchRequest:
  """A single request to be included in a batch."""

  custom_id: str
  messages: list[dict[str, str]]
  model: str


@dataclass
class BatchResult:
  """A single result from a batch."""

  custom_id: str
  content: str | None
  error: str | None


class BatchClient(Protocol):
  """Protocol for batch LLM clients."""

  def submit_batch(self, requests: list[BatchRequest]) -> str: ...
  def poll_batch(self, batch_id: str, poll_interval: float) -> str: ...
  def download_results(self, batch_id: str) -> list[BatchResult]: ...


LLM_Message = list[dict[str, str]]


class LLMClient(Protocol):
  """Protocol for LLM Clients"""

  def query(self, messages: LLM_Message, llm_config_params: Any) -> None:
    """Queries an LLM with messages.

    Should also update the messages param to the llm response."""
    return

  def get_logs(self) -> dict[str, Any]:
    return {}


LLMClientType = TypeVar("LLMClientType", bound="LLMClient")


@dataclass
class LLMClientFactory(Generic[LLMClientType]):
  factory: Callable[..., LLMClientType]
  init_kwargs: dict[str, Any]

  def build(self) -> LLMClientType:
    return self.factory(**self.init_kwargs)


class OllamaLLMClient:
  """Wrapper for Ollama Client"""

  def __init__(self):
    self.initialization_timestamp = datetime.now()
    self.messages_timestamps = []
    self.client = Client()
    self.eval_count = []
    self.prompt_eval_count = []

  def query(self, messages: LLM_Message, llm_config_params: str) -> None:
    """Send a single request to the LLM and return its response."""
    self.messages_timestamps.append(datetime.now())
    response = self.client.chat(
      model=llm_config_params, messages=messages, stream=False
    )
    self.eval_count.append(response.eval_count)
    self.prompt_eval_count.append(response.prompt_eval_count)
    response_str = response.message.content
    if not response_str:
      messages.append(
        {"role": "assistant", "content": "I can't help you with that"}
      )
    else:
      messages.append({"role": "assistant", "content": response_str})

  def get_logs(self) -> dict[str, Any]:
    return {
      "client_creation_timestamp": self.initialization_timestamp,
      "messages_timestamps": self.messages_timestamps,
      "eval_count": self.eval_count,
      "prompt_eval_count": self.prompt_eval_count,
      "total_tokens": sum(self.eval_count) + sum(self.prompt_eval_count),
    }


class OpenAILLMClient:
  """Wrapper for OpenAI Client."""

  def __init__(self):
    self.initialization_timestamp = datetime.now()
    self.messages_timestamps: list[datetime] = []
    self.client = OpenAI()
    self.eval_count: list[int] = []
    self.prompt_eval_count: list[int] = []

  def query(self, messages: LLM_Message, llm_config_params: str) -> None:
    """Send a single request to the OpenAI API and return its response."""
    self.messages_timestamps.append(datetime.now())
    logger.info("Sending request to OpenAI model %s", llm_config_params)
    response = self.client.chat.completions.create(
      model=llm_config_params,
      messages=cast(Any, messages),
    )
    usage = response.usage
    self.eval_count.append(usage.completion_tokens if usage else 0)
    self.prompt_eval_count.append(usage.prompt_tokens if usage else 0)
    logger.debug(
      "OpenAI response: %d completion tokens, %d prompt tokens",
      self.eval_count[-1],
      self.prompt_eval_count[-1],
    )
    content = response.choices[0].message.content if response.choices else None
    if not content:
      messages.append(
        {"role": "assistant", "content": "I can't help you with that"}
      )
    else:
      messages.append({"role": "assistant", "content": content})

  def get_logs(self) -> dict[str, Any]:
    return {
      "client_creation_timestamp": self.initialization_timestamp,
      "messages_timestamps": self.messages_timestamps,
      "eval_count": self.eval_count,
      "prompt_eval_count": self.prompt_eval_count,
      "total_tokens": sum(self.eval_count) + sum(self.prompt_eval_count),
    }


class OpenAIBatchClient:
  """Client for submitting and retrieving OpenAI Batch API requests."""

  def __init__(self) -> None:
    self.client = OpenAI()

  def submit_batch(self, requests: list[BatchRequest]) -> str:
    """Upload JSONL and create a batch. Returns the batch ID."""
    lines: list[str] = []
    for req in requests:
      line = {
        "custom_id": req.custom_id,
        "method": "POST",
        "url": "/v1/chat/completions",
        "body": {"model": req.model, "messages": req.messages},
      }
      lines.append(json.dumps(line))
    jsonl_bytes = ("\n".join(lines) + "\n").encode()

    uploaded = self.client.files.create(
      file=io.BytesIO(jsonl_bytes), purpose="batch"
    )
    batch = self.client.batches.create(
      input_file_id=uploaded.id,
      endpoint="/v1/chat/completions",
      completion_window="24h",
    )
    logger.info("Created batch %s with %d requests.", batch.id, len(requests))
    return batch.id

  def poll_batch(self, batch_id: str, poll_interval: float) -> str:
    """Poll until the batch completes or fails. Returns final status."""
    while True:
      batch = self.client.batches.retrieve(batch_id)
      status = batch.status
      logger.info("Batch %s status: %s", batch_id, status)
      if status in ("completed", "failed", "expired", "cancelled"):
        return status
      time.sleep(poll_interval)

  def download_results(self, batch_id: str) -> list[BatchResult]:
    """Download and parse results from a completed batch."""
    batch = self.client.batches.retrieve(batch_id)
    if not batch.output_file_id:
      logger.warning("Batch %s has no output file.", batch_id)
      return []
    content = self.client.files.content(batch.output_file_id).text
    results: list[BatchResult] = []
    for line in content.strip().split("\n"):
      if not line:
        continue
      obj = json.loads(line)
      custom_id = obj["custom_id"]
      error = obj.get("error")
      if error:
        results.append(
          BatchResult(custom_id=custom_id, content=None, error=str(error))
        )
        continue
      response_body = obj.get("response", {}).get("body", {})
      choices = response_body.get("choices", [])
      text = choices[0]["message"]["content"] if choices else None
      results.append(BatchResult(custom_id=custom_id, content=text, error=None))
    return results


def _parse_s3_uri(uri: str) -> tuple[str, str]:
  """Parse 's3://bucket/key' into (bucket, key)."""
  if not uri.startswith("s3://"):
    msg = f"Invalid S3 URI: {uri}"
    raise ValueError(msg)
  without_scheme = uri[len("s3://") :]
  bucket, _, key = without_scheme.partition("/")
  return bucket, key


class BedrockBatchClient:
  """Client for batch inference via AWS Bedrock."""

  def __init__(
    self,
    s3_input_uri: str,
    s3_output_uri: str,
    role_arn: str,
    region: str,
    model: str,
  ) -> None:
    self.s3_input_uri = s3_input_uri
    self.s3_output_uri = s3_output_uri
    self.role_arn = role_arn
    self.model = model
    self.s3_client = boto3.client("s3", region_name=region)
    self.bedrock_client = boto3.client("bedrock", region_name=region)
    self._job_output_uri: dict[str, str] = {}

  def submit_batch(self, requests: list[BatchRequest]) -> str:
    """Build Bedrock JSONL, upload to S3, create invocation job."""
    lines: list[str] = []
    for req in requests:
      line = {
        "recordId": req.custom_id,
        "modelInput": {
          "anthropic_version": "bedrock-2023-05-31",
          "max_tokens": 4096,
          "messages": [m for m in req.messages if m["role"] != "system"],
          "system": next(
            (m["content"] for m in req.messages if m["role"] == "system"),
            "",
          ),
        },
      }
      lines.append(json.dumps(line))
    jsonl_bytes = ("\n".join(lines) + "\n").encode()

    input_bucket, input_key = _parse_s3_uri(self.s3_input_uri)
    timestamp = int(time.time())
    file_key = f"{input_key}batch_{timestamp}.jsonl"
    self.s3_client.put_object(
      Bucket=input_bucket,
      Key=file_key,
      Body=jsonl_bytes,
    )
    input_s3_uri = f"s3://{input_bucket}/{file_key}"

    response = self.bedrock_client.create_model_invocation_job(
      modelId=self.model,
      roleArn=self.role_arn,
      jobName=f"batch-{timestamp}",
      inputDataConfig={
        "s3InputDataConfig": {"s3Uri": input_s3_uri},
      },
      outputDataConfig={
        "s3OutputDataConfig": {"s3Uri": self.s3_output_uri},
      },
    )
    job_arn = response["jobArn"]
    logger.info(
      "Created Bedrock batch job %s with %d requests.",
      job_arn,
      len(requests),
    )
    return job_arn

  def poll_batch(self, batch_id: str, poll_interval: float) -> str:
    """Poll Bedrock job until terminal state. Returns lowercase status."""
    status_map = {
      "Completed": "completed",
      "Failed": "failed",
      "Stopped": "cancelled",
      "Stopping": "cancelled",
    }
    terminal = set(status_map.keys())
    while True:
      response = self.bedrock_client.get_model_invocation_job(
        jobIdentifier=batch_id
      )
      status = response["status"]
      logger.info("Bedrock job %s status: %s", batch_id, status)
      if status in terminal:
        if status == "Completed":
          self._job_output_uri[batch_id] = response["outputDataConfig"][
            "s3OutputDataConfig"
          ]["s3Uri"]
        return status_map[status]
      time.sleep(poll_interval)

  def download_results(self, batch_id: str) -> list[BatchResult]:
    """Download and parse results from a completed Bedrock job."""
    output_uri = self._job_output_uri.get(batch_id)
    if not output_uri:
      logger.warning("No output URI for job %s", batch_id)
      return []

    bucket, prefix = _parse_s3_uri(output_uri)
    paginator = self.s3_client.get_paginator("list_objects_v2")
    results: list[BatchResult] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
      for obj in page.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".jsonl.out"):
          continue
        body = (
          self.s3_client.get_object(Bucket=bucket, Key=key)["Body"]
          .read()
          .decode()
        )
        for raw_line in body.strip().split("\n"):
          if not raw_line:
            continue
          record = json.loads(raw_line)
          record_id = record["recordId"]
          model_output = record.get("modelOutput")
          if model_output is None:
            error = record.get("error", "Unknown error")
            results.append(
              BatchResult(
                custom_id=record_id,
                content=None,
                error=str(error),
              )
            )
            continue
          content_blocks = model_output.get("content", [])
          text = content_blocks[0]["text"] if content_blocks else None
          results.append(
            BatchResult(
              custom_id=record_id,
              content=text,
              error=None,
            )
          )
    return results


@dataclass
class BedrockConfig:
  """Configuration for Bedrock batch client."""

  s3_input_uri: str
  s3_output_uri: str
  role_arn: str
  region: str
  model: str


def get_batch_client(
  provider: str,
  bedrock_config: BedrockConfig | None = None,
) -> BatchClient:
  """Return the appropriate batch client for the provider."""
  if provider == "bedrock":
    assert bedrock_config is not None, (
      "bedrock_config required for bedrock provider"
    )
    return BedrockBatchClient(
      s3_input_uri=bedrock_config.s3_input_uri,
      s3_output_uri=bedrock_config.s3_output_uri,
      role_arn=bedrock_config.role_arn,
      region=bedrock_config.region,
      model=bedrock_config.model,
    )
  return OpenAIBatchClient()


def get_llm_client_factory(provider: str) -> LLMClientFactory:
  """Return the appropriate LLMClientFactory for the given provider."""
  if provider == "openai":
    return LLMClientFactory(factory=OpenAILLMClient, init_kwargs={})
  return LLMClientFactory(factory=OllamaLLMClient, init_kwargs={})
