from dataclasses import dataclass

from query_generator.utils.definitions import Dataset


@dataclass
class BinningSnoflakeParameters:
  scale_factor: int
  dataset: Dataset
