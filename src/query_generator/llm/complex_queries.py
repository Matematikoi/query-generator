import random
import re
from pathlib import Path

from duckdb import DuckDBPyConnection
from ollama import Client
from tqdm import tqdm

from query_generator.duckdb_connection.setup import setup_duckdb
from query_generator.utils.params import (
  ComplexQueryGenerationParametersEndpoint,
)
from collections import defaultdict



def query_llm(client: Client, prompt: str, model: str) -> str:
  """Send a single request to the LLM and return its response."""
  response = client.chat(
    model=model, messages=[{"role": "user", "content": prompt}], stream=False
  )
  response_str = response.message.content
  if not response_str:
    return ""
  return response_str


def get_random_queries(
  params: ComplexQueryGenerationParametersEndpoint,
) -> list[str]:
  sql_files = list(Path(params.queries_path).rglob("*.sql"))
  random_query_paths = random.sample(sql_files, params.total_queries)
  return [p.read_text() for p in random_query_paths]


def get_random_prompt(
  params: ComplexQueryGenerationParametersEndpoint, query: str
) -> tuple[str, str]:
  extension_types = list(params.llm_prompts.keys())
  weights = [params.llm_prompts[e].weight for e in extension_types]
  extension_type = random.choices(extension_types, weights=weights)[0]

  return (
    extension_type,
    f"""
  {params.llm_base_prompt}
  {params.llm_prompts[extension_type].prompt}
  {query}
  """,
  )


def extract_sql(llm_text: str) -> str:
  _, _, tail = llm_text.partition("</think>")
  m = re.search(r"```sql\s*(.*?)\s*```", tail, re.DOTALL | re.IGNORECASE)
  return m.group(1).strip() if m else ""


def validate_query_duckdb(con: DuckDBPyConnection, query: str) -> bool:
  try:
    con.sql(query).fetchone()
  except Exception:
    return False
  else:
    return True


def create_complex_queries(
  params: ComplexQueryGenerationParametersEndpoint,
) -> None:
  llm_client = Client()
  random.seed(params.seed)
  con = setup_duckdb(params.dataset, 0)
  destination_path = Path(params.destination_folder)
  query_counter: dict[str,int] = defaultdict(int)
  for query in tqdm(get_random_queries(params)):
    extension_type, prompt = get_random_prompt(params, query)
    llm_response = query_llm(llm_client, prompt, params.llm_model)
    llm_extracted_query = extract_sql(llm_response)
    valid_query = validate_query_duckdb(con, llm_extracted_query)

    if valid_query:
      query_counter[extension_type] = 1 + query_counter[extension_type]
      new_path = destination_path/ extension_type/ f"{query_counter[extension_type]}.sql"
      new_path.parent.mkdir(parents=True, exist_ok=True)
      new_path.write_text(llm_extracted_query)
    
