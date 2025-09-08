from pathlib import Path
import pytest

from query_generator.utils.params import (
  ExtensionAndLLMEndpoint,
  SyntheticQueriesEndpoint,
  read_and_parse_toml,
)

base_path = Path(__file__).parent.parent.parent


@pytest.mark.parametrize(
  "file_name", ["tpcds_dev.toml", "tpcds.toml", "job.toml", "job_dev.toml"]
)
def test_param_search(file_name):
  """
  Test that the example toml provided for the param-search
  endpoint are valid
  """

  file_path = base_path / f"params_config/synthetic_queries/{file_name}"
  params = read_and_parse_toml(
    Path(file_path),
    SyntheticQueriesEndpoint,
  )
  assert params is not None


@pytest.mark.parametrize(
  "file_name",
  [
    "tpcds_dev.toml",
    "tpcds.toml",
    "tpcds_devstral.toml",
    "tpcds_gemma.toml",
    "tpcds_llama4.toml",
    "tpcds_qwen.toml",
  ],
)
def test_add_complex_queries(file_name):
  file_path = base_path / f"params_config/complex_queries/{file_name}"
  params = read_and_parse_toml(
    Path(file_path),
    ExtensionAndLLMEndpoint,
  )
  assert params is not None
