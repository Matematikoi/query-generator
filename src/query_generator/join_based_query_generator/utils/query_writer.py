import os


class QueryWriter:
  def __init__(self, output_dir: str) -> None:
    self.output_dir = output_dir
    if not os.path.exists(self.output_dir):
      os.makedirs(self.output_dir)

  def write_query(self, query: str, file_name: str) -> None:
    """
    Write the generated queries to a file.
    Args:
        queries (List[str]): List of SQL queries.
        file_name (str): Name of the output file.
    """
    with open(os.path.join(self.output_dir, file_name), "w") as f:
      f.write(query)
