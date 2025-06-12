from ollama import Client
from tqdm import tqdm

from query_generator.utils.params import (
  ComplexQueryGenerationParametersEndpoint,
)


def query_llm(client: Client, prompt: str, model: str) -> str:
  """Send a single request to the LLM and return its response."""
  response = client.chat(
    model=model, messages=[{"role": "user", "content": prompt}], stream=False
  )
  response_str = response.message.content
  if not response_str:
    return ""
  return response_str


def create_complex_queries(
  params: ComplexQueryGenerationParametersEndpoint,
) -> None:
  client = Client()

  countries = [
    "Brazil",
    "Colombia",
    "Peru",
    "Argentina",
    "chile",
    "Belgium",
    "france",
    "USA",
    "Canda",
    "Mexico",
    "Denmark",
    "Germany",
    "Italy",
    "Iceland",
    # **Added 40 more countries:**
    "Afghanistan",
    "Albania",
    "Algeria",
    "Andorra",
    "Angola",
    "Antigua and Barbuda",
    "Armenia",
    "Australia",
    "Austria",
    "Azerbaijan",
    "Bahamas",
    "Bahrain",
    "Bangladesh",
    "Barbados",
    "Belarus",
    "Bhutan",
    "Bolivia",
    "Bosnia and Herzegovina",
    "Botswana",
    "Brunei",
    "Bulgaria",
    "Burkina Faso",
    "Burundi",
    "Cambodia",
    "Cameroon",
    "Canada",
    "Cape Verde",
    "Central African Republic",
    "Chad",
    "China",
    "Costa Rica",
    "Croatia",
    "Cuba",
    "Cyprus",
    "Czech Republic",
    "Dominica",
    "Dominican Republic",
    "Ecuador",
    "Egypt",
    "El Salvador",
    "Estonia",
    "Ethiopia",
    "Fiji",
    "Finland",
    "Gabon",
    "Gambia",
    "Georgia",
    "Ghana",
    "Greece",
    "Guatemala",
    "Guyana",
    "Haiti",
  ]
  prompts = [f"What is the capital city of {country}?" for country in countries]

  print(params)
  for prompt in tqdm(prompts):
    response = query_llm(client, prompt, params.llm_model)
    print(f"Prompt: {prompt}\nResponse: {response}\n")
