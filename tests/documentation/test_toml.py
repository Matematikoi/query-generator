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


def test_toml():
  """All toml used should be mapped and validated"""
  mapping = {
    EndpointName.EXTENSION_AND_OLLAMA: ExtensionAndOllamaEndpoint,
    EndpointName.SYNTHETIC_GENERATION: SyntheticQueriesEndpoint,
    EndpointName.FILTER: FilterEndpoint,
    EndpointName.GENERATE_DB: GenerateDBEndpoint,
    EndpointName.HISTOGRAM: HistogramEndpoint,
    EndpointName.FIX_TRANSFORM: FixTransformEndpoint,
  }
  for key, toml_raw in TOML_EXAMPLE.items():
    toml_dict = tomllib.loads(toml_raw)
    structure(toml_dict, mapping[key])
