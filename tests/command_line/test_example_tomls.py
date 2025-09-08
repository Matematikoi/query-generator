from pathlib import Path
import pytest

from query_generator.utils.params import (
  ExtensionAndLLMEndpoint,
  FilterEndpoint,
  GenerateDBEndpoint,
  HistogramEndpoint,
  SyntheticQueriesEndpoint,
  read_and_parse_toml,
)

base_path = Path(__file__).parent.parent.parent


def test_synthetic_queries():
  """
  Test that the example toml provided for the param-search
  endpoint are valid
  """

  file_path = base_path / f"params_config/synthetic_queries/"
  for file in Path(file_path).glob("*.toml"):
    params = read_and_parse_toml(
      file,
      SyntheticQueriesEndpoint,
    )
    assert params is not None


def test_extensions_and_llm():
  file_path = base_path / f"params_config/extensions_and_llm"
  for file in Path(file_path).glob("*.toml"):
    params = read_and_parse_toml(
      file,
      ExtensionAndLLMEndpoint,
    )
    assert params is not None


def test_generate_db_toml():
  file_path = base_path / f"params_config/generate_db"
  for file in Path(file_path).glob("*.toml"):
    params = read_and_parse_toml(
      file,
      GenerateDBEndpoint,
    )
    assert params is not None


def test_make_histograms():
  file_path = base_path / f"params_config/make_histograms"
  for file in Path(file_path).glob("*.toml"):
    params = read_and_parse_toml(
      file,
      HistogramEndpoint,
    )
    assert params is not None


def test_filter_synthetic():
  file_path = base_path / f"params_config/filter_synthetic"
  for file in Path(file_path).glob("*.toml"):
    params = read_and_parse_toml(
      file,
      FilterEndpoint,
    )
    assert params is not None
