import os

from query_generator.utils.definitions import (
  Dataset,
  Extension,
  GeneratedQueryFeatures,
)


class QueryWriter:
  def __init__(self, dataset: Dataset, extension: Extension) -> None:
    self.extension = extension
    self.dataset = dataset

  def write_query(self, query: GeneratedQueryFeatures) -> None:
    """
    Write the generated queries to a file.
    Args:
        queries (List[str]): List of SQL queries.
        file_name (str): Name of the output file.
    """
    folder = (
      "data/generated_queries/"
      f"{self.extension.value}/{self.dataset.value}/{query.fact_table}_{query.template_number}"
    )
    if not os.path.exists(folder):
      os.makedirs(folder)
    file_name = f"{query.template_number}_{query.predicate_number}.sql"
    with open(os.path.join(folder, file_name), "w") as f:
      f.write(query.query)
