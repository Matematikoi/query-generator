from pathlib import Path
import tomllib

from cattrs import structure
from query_generator.utils.params import (
  FixTransformEndpoint,
  GenerateDBEndpoint,
  HistogramEndpoint,
  ExtensionAndOllamaEndpoint,
  FilterEndpoint,
  SyntheticQueriesEndpoint,
  read_and_parse_toml,
)
from query_generator.utils.toml_examples import TOML_EXAMPLE, EndpointName

mapping = {
  EndpointName.EXTENSION_AND_OLLAMA: ExtensionAndOllamaEndpoint,
  EndpointName.SYNTHETIC_GENERATION: SyntheticQueriesEndpoint,
  EndpointName.FILTER: FilterEndpoint,
  EndpointName.GENERATE_DB: GenerateDBEndpoint,
  EndpointName.HISTOGRAM: HistogramEndpoint,
  EndpointName.FIX_TRANSFORM: FixTransformEndpoint,
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
    print(endpoint_name)
    docs_path = (base_path / "params_config" / endpoint_name).glob("*toml")
    print(base_path / "params_config" / endpoint_name)
    assert len(list(docs_path)) > 0
    # parse the docs and throw no errors in the process
    for doc in docs_path:
      params = read_and_parse_toml(doc.read_text(), mapping[endpoint_name])
      assert params is not None
