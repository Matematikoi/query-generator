from pathlib import Path
import pytest

from query_generator.utils.params import (
  ExtensionAndLLMEndpoint,
  SyntheticQueriesEndpoint,
  read_and_parse_toml,
)

base_path = Path(__file__).parent.parent.parent


def test_param_search():
  """
  Test that the example toml provided for the param-search
  endpoint are valid
  """

  file_path = base_path / f"params_config/synthetic_queries/"
  for file in Path(file_path).glob("*.toml"):
    file_path = base_path / f"params_config/synthetic_queries/{file.name}"
    params = read_and_parse_toml(
      Path(file_path),
      SyntheticQueriesEndpoint,
    )
    assert params is not None


def test_add_complex_queries():
  file_path = base_path / f"params_config/extensions_and_llm"
  for file in Path(file_path).glob("*.toml"):
    file_path = base_path / f"params_config/extensions_and_llm/{file.name}"
    params = read_and_parse_toml(
      Path(file_path),
      ExtensionAndLLMEndpoint,
    )
    assert params is not None
