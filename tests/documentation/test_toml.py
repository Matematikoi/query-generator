import tomllib

from cattrs import structure
from query_generator.utils.params import (
  GenerateDBEndpoint,
  LLMExtensionEndpoint,
  FilterEndpoint,
  SyntheticQueriesEndpoint,
  read_and_parse_toml,
)
from query_generator.utils.toml_examples import TOML_EXAMPLE


def test_toml():
  """All toml used should be mapped and validated"""
  mapping = {
    "llm_augmentation": LLMExtensionEndpoint,
    "synthetic_generation": SyntheticQueriesEndpoint,
    "filter": FilterEndpoint,
    "generate_db": GenerateDBEndpoint,
  }
  for key, toml_raw in TOML_EXAMPLE.items():
    toml_dict = tomllib.loads(toml_raw)
    structure(toml_dict, mapping[key])
