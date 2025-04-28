import os

from query_generator.utils.definitions import Dataset, Extension


class QueryWriter:
  def __init__(self, dataset: Dataset, extension: Extension) -> None:
    self.extension = extension
    self.dataset = dataset

  def write_query(
    self, query: str, template_number: int, predicate_number: int
  ) -> None:
    """
    Write the generated queries to a file.
    Args:
        queries (List[str]): List of SQL queries.
        file_name (str): Name of the output file.
    """
    folder = "data/generated_queries/"
    f"{self.extension}/{self.dataset}/{template_number}"
    if not os.path.exists(folder):
      os.makedirs(folder)
    file_name = f"/{template_number}_{predicate_number}.sql"
    with open(os.path.join(folder, file_name), "w") as f:
      f.write(query)
