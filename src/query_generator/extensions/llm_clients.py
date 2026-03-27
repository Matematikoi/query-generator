"""LLM clients file."""

import io
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, Literal, Protocol, TypeVar, cast

import anthropic
from anthropic import AnthropicBedrock
from ollama import Client
from openai import APIError, OpenAI

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
ServiceTier = Literal["auto", "default", "flex", "priority", "scale"]


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

  def __init__(self, service_tier: ServiceTier = "auto"):
    self.initialization_timestamp = datetime.now()
    self.messages_timestamps: list[datetime] = []
    self.client = OpenAI()
    self.service_tier = service_tier
    self.eval_count: list[int] = []
    self.prompt_eval_count: list[int] = []
    self.cached_tokens: list[int] = []

  def query(self, messages: LLM_Message, llm_config_params: str) -> None:
    """Send a single request to the OpenAI API and return its response."""
    self.messages_timestamps.append(datetime.now())
    logger.info("Sending request to OpenAI model %s", llm_config_params)
    while True:
      try:
        response = self.client.chat.completions.create(
          model=llm_config_params,
          messages=cast(Any, messages),
          service_tier=self.service_tier,
        )
        break
      except APIError:
        logger.warning("OpenAI API error, sleeping 5min before retry")
        time.sleep(300)
    usage = response.usage
    self.eval_count.append(usage.completion_tokens if usage else 0)
    self.prompt_eval_count.append(usage.prompt_tokens if usage else 0)
    cached = 0
    if usage and usage.prompt_tokens_details:
      cached = usage.prompt_tokens_details.cached_tokens or 0
    self.cached_tokens.append(cached)
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
      "cached_tokens": self.cached_tokens,
      "total_tokens": sum(self.eval_count) + sum(self.prompt_eval_count),
    }


class AnthropicBedrockLLMClient:
  """Wrapper for Anthropic Bedrock Client."""

  def __init__(self):
    self.initialization_timestamp = datetime.now()
    self.messages_timestamps: list[datetime] = []
    self.client = AnthropicBedrock()
    self.eval_count: list[int] = []
    self.prompt_eval_count: list[int] = []
    self.cache_read_input_tokens: list[int] = []
    self.cache_creation_input_tokens: list[int] = []

  def query(self, messages: LLM_Message, llm_config_params: str) -> None:
    """Send a single request to the Bedrock API and return its response."""
    self.messages_timestamps.append(datetime.now())

    system_parts: list[str] = []
    non_system: list[dict[str, str]] = []
    for msg in messages:
      if msg["role"] == "system":
        system_parts.append(msg["content"])
      else:
        non_system.append(msg)

    logger.info("Sending request to Bedrock model %s", llm_config_params)
    while True:
      try:
        kwargs: dict[str, Any] = {
          "model": llm_config_params,
          "messages": non_system,
          "max_tokens": 4096,
        }
        if system_parts:
          kwargs["system"] = [
            {
              "type": "text",
              "text": "\n".join(system_parts),
              "cache_control": {"type": "ephemeral"},
            }
          ]
        response = self.client.messages.create(**kwargs)
        break
      except anthropic.RateLimitError:
        logger.warning("Rate limited by Bedrock, sleeping 10min before retry")
        time.sleep(600)

    usage = response.usage
    self.eval_count.append(usage.output_tokens)
    self.prompt_eval_count.append(usage.input_tokens)
    self.cache_read_input_tokens.append(
      getattr(usage, "cache_read_input_tokens", 0) or 0
    )
    self.cache_creation_input_tokens.append(
      getattr(usage, "cache_creation_input_tokens", 0) or 0
    )
    logger.debug(
      "Bedrock response: %d output tokens, %d input tokens",
      self.eval_count[-1],
      self.prompt_eval_count[-1],
    )
    content = response.content[0].text if response.content else None
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
      "cache_read_input_tokens": self.cache_read_input_tokens,
      "cache_creation_input_tokens": self.cache_creation_input_tokens,
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


def get_llm_client_factory(provider: str) -> LLMClientFactory:
  """Return the appropriate LLMClientFactory for the given provider."""
  if provider == "openai":
    return LLMClientFactory(factory=OpenAILLMClient, init_kwargs={})
  if provider == "openai-flex":
    return LLMClientFactory(
      factory=OpenAILLMClient, init_kwargs={"service_tier": "flex"}
    )
  if provider == "bedrock":
    return LLMClientFactory(factory=AnthropicBedrockLLMClient, init_kwargs={})
  return LLMClientFactory(factory=OllamaLLMClient, init_kwargs={})
