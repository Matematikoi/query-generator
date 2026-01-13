"""LLM clients file."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, Protocol, TypeVar

from ollama import Client

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
