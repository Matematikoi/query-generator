class DuckDBHistogram:
  def __init__(self, bins: list[str], count: list[int]) -> None:
    self.bins = bins
    self.count = count
    self.get_lower_upper_bounds()

  def get_lower_upper_bounds(self) -> None:
    pass
