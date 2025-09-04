import tomllib

from cattrs import structure
from query_generator.utils.params import (
  ComplexQueryGenerationParametersEndpoint,
  FilterEndpoint,
  SearchParametersEndpoint,
  SnowflakeEndpoint,
  read_and_parse_toml,
)
from query_generator.utils.toml_examples import TOML_EXAMPLE


def test_toml():
  """All toml used should be mapped and validated"""
  mapping = {
    "llm_augmentation": ComplexQueryGenerationParametersEndpoint,
    "snowflake": SnowflakeEndpoint,
    "synthetic_generation": SearchParametersEndpoint,
    "filter": FilterEndpoint,
  }
  for key, toml_raw in TOML_EXAMPLE.items():
    toml_dict = tomllib.loads(toml_raw)
    structure(toml_dict, mapping[key])
