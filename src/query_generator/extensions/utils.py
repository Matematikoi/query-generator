from typing import Any, Protocol

from ollama import Client

LLM_Message = list[dict[str, str]]


class LLMClient(Protocol):
  """Protocol for LLM Clients"""

  def query(self, messages: LLM_Message, llm_config_params: Any) -> None:
    """Queries an LLM with messages.

    Should also update the messages param to the llm response."""
    return


class OllamaLLMClient:
  """Wrapper for Ollama Client"""

  def __init__(self):
    self.client = Client()

  def query(self, messages: LLM_Message, llm_config_params: str) -> None:
    """Send a single request to the LLM and return its response."""
    response = self.client.chat(
      model=llm_config_params, messages=messages, stream=False
    )
    response_str = response.message.content
    if not response_str:
      messages.append(
        {"role": "assistant", "content": "I can't help you with that"}
      )
    else:
      messages.append({"role": "assistant", "content": response_str})
