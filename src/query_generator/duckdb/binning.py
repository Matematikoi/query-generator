from dataclasses import dataclass

from query_generator.utils.definitions import Dataset


@dataclass
class BinningSnoflakeParameters:
  scale_factor: int
  dataset: Dataset
  lower_bound: int
  upper_bound: int
  total_bins: int


def run_snowflake_binning(
  parameters: BinningSnoflakeParameters,
) -> None:
  """
  Run the Snowflake binning process. Binning is equiwidth binning.

  Args:
    parameters (BinningSnoflakeParameters): The parameters for
    the Snowflake binning process.
  """
