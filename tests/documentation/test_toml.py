import tomllib
from pathlib import Path

from cattrs import structure

from query_generator.utils.params import (
  ExtensionAndOllamaEndpoint,
  FilterEndpoint,
  FixTransformEndpoint,
  GenerateDBEndpoint,
  HistogramEndpoint,
  LLMPrompts,
  SyntheticQueriesEndpoint,
  read_and_parse_toml,
)
from query_generator.utils.toml_examples import TOML_EXAMPLE, EndpointName

mapping = {
  EndpointName.EXTENSIONS_WITH_OLLAMA: ExtensionAndOllamaEndpoint,
  EndpointName.SYNTHETIC_GENERATION: SyntheticQueriesEndpoint,
  EndpointName.FILTER: FilterEndpoint,
  EndpointName.GENERATE_DB: GenerateDBEndpoint,
  EndpointName.HISTOGRAM: HistogramEndpoint,
  EndpointName.FIX_TRANSFORM: FixTransformEndpoint,
  EndpointName.PROMPTS: LLMPrompts,
}


def test_toml():
  """All toml used should be mapped and validated"""
  for key, toml_raw in TOML_EXAMPLE.items():
    toml_dict = tomllib.loads(toml_raw)
    structure(toml_dict, mapping[key])


def test_toml_files():
  """Test the example toml files provided"""
  base_path = Path(__file__).parent.parent.parent
  for endpoint_name in EndpointName:
    docs_path = (base_path / "params_config" / endpoint_name).glob("*toml")
    assert len(list(docs_path)) > 0
    # parse the docs and throw no errors in the process
    for doc in docs_path:
      params = read_and_parse_toml(doc.read_text(), mapping[endpoint_name])
      assert params is not None


def test_llm_params():
  """test llm params are being correctly parsed"""
  tpcds_dev = Path("params_config/extensions_with_ollama/tpcds_dev.toml")
  params = read_and_parse_toml(tpcds_dev, ExtensionAndOllamaEndpoint)
  base_prompt_expected: str = """
    You are writing queries in markdown notation.
    Use the format ```sql SELECT... ``` to ensure proper markdown formatting.

    your only task is to write the given sql query again but
surrounding it with ```sql Select from....```
"""
  assert (
    params.llm_params.prompts.base_prompt.strip()
    == base_prompt_expected.strip()
  )
