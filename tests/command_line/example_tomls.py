from pathlib import Path
import pytest

from query_generator.utils.params import (
  SearchParametersEndpoint,
  read_and_parse_toml,
)

base_path = Path(__file__).parent.parent


@pytest.mark.parametrize("file_name", ["tpcds_dev.toml", "tpcds.toml"])
def test_param_search(file_name):
  """
  Test that the example toml provided for the param-search
  endpoint are valid
  """

  file_path = base_path / f"params_config/search_params/{file_name}"
  params = read_and_parse_toml(
    Path(file_path),
    SearchParametersEndpoint,
  )
