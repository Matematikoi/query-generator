import tomllib

from cattrs import structure
from query_generator.utils.params import (
  LLMExtensionEndpoint,
  FilterEndpoint,
  SyntheticQueriesEndpoint,
  SnowflakeEndpoint,
  read_and_parse_toml,
)
from query_generator.utils.toml_examples import TOML_EXAMPLE


def test_toml():
  """All toml used should be mapped and validated"""
  mapping = {
    "llm_augmentation": LLMExtensionEndpoint,
    "snowflake": SnowflakeEndpoint,
    "synthetic_generation": SyntheticQueriesEndpoint,
    "filter": FilterEndpoint,
  }
  for key, toml_raw in TOML_EXAMPLE.items():
    toml_dict = tomllib.loads(toml_raw)
    structure(toml_dict, mapping[key])
