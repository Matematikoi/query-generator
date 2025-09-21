import re

from duckdb import DuckDBPyConnection
import duckdb
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


def add_retry_query_to_messages(
  messages: LLM_Message, exception: Exception
) -> None:
  messages.append(
    {
      "role": "user",
      "content": f"""
      Fix this error with the query you provided:
      {str(exception)}
    """,
    }
  )


def get_text_after_think(llm_text: str) -> str:
  """Extract the text after the last </think> tag."""
  if "</think>" in llm_text:
    return llm_text.rsplit("</think>", 1)[-1].strip()
  return llm_text.strip()


def validate_and_retry_query_with_llm(
  messages: LLM_Message,
  llm_client: Client,
  database_path: str,
  max_retries: int,
  llm_model: str,
) -> str:
  con = duckdb.connect(database=database_path, read_only=True)
  current_exeption = Exception("no query was found")
  valid_query = False
  new_query = ""
  retries = 0
  while not valid_query and retries < max_retries:
    retries += 1
    llm_extracted_query = extract_sql(messages[-1]["content"])
    try:
      con.execute(llm_extracted_query)
      valid_query = True
      new_query = llm_extracted_query
    except Exception as e:
      current_exeption = e
      add_retry_query_to_messages(messages, current_exeption)
      query_llm(llm_client, messages, llm_model)
  con.close()
  return new_query if valid_query else ""
