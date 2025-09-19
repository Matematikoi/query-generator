import re

from duckdb import DuckDBPyConnection
from ollama import Client

LLM_Message = list[dict[str, str]]


def query_llm(client: Client, messages: LLM_Message, model: str) -> None:
  """Send a single request to the LLM and return its response."""
  response = client.chat(model=model, messages=messages, stream=False)
  response_str = response.message.content
  if not response_str:
    messages.append(
      {"role": "assistant", "content": "I can't help you with that"}
    )
  else:
    messages.append({"role": "assistant", "content": response_str})


def extract_sql(llm_text: str) -> str:
  if "<think>" in llm_text:
    _, _, text = llm_text.partition("</think>")
  else:
    text = llm_text
  matches = re.findall(r"```sql\s*(.*?)\s*```", text, re.DOTALL)
  return matches[-1].strip() if matches else ""


def validate_query_duckdb(
  con: DuckDBPyConnection, query: str
) -> tuple[bool, Exception]:
  try:
    con.sql(query).fetchone()
  except Exception as e:
    return False, e
  else:
    return True, Exception("no exception found")
