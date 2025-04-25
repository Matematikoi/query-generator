import copy
from dataclasses import dataclass
from typing import Any, Dict, List


class ForeignKeyGraph:
  """
  Class to represent a foreign key graph.
  It is used to store the foreign key relationships between tables.
  """

  @dataclass
  class Node:
    """
    Class to represent a node in the foreign key graph.
    A node is a table in the database.
    """

    name: str

  @dataclass
  class Edge:
    """
    Class to represent an edge in the foreign key graph.
    An edge is a foreign key relationship between two tables.
    """

    table: "ForeignKeyGraph.Node"
    column: str
    reference_table: "ForeignKeyGraph.Node"
    reference_column: str
    id: int

  def __init__(self, tables_schema: Dict[str, Dict[str, Any]]) -> None:
    """
    Initialize the foreign key graph.

    Args:
        tables_schema (Dict[str, Dict[str, Any]]):
        Dictionary containing the schema of the tables.
    """
    self.tables = List(tables_schema.keys())
    self.table_to_index = {name: i for i, name in enumerate(self.tables)}
    self.graph: List[List[ForeignKeyGraph.Edge]] = [[] for _ in self.tables]

    self.populate_graph(copy.deepcopy(tables_schema))

  def populate_graph(self, tables_schema: Dict[str, Dict[str, Any]]) -> None:
    """
    Populate the foreign key graph with edges based on the schema.
    This method iterates through each table and its foreign keys,
    creating edges to the referenced tables.
    """
    # in order to have the same id for edges we sort them
    # by table name and column name
    self.tables.sort()
    for table in self.tables:
      tables_schema[table]["foreign_keys"].sort(
        key=lambda x: (x["ref_table"], x["column"], x["ref_column"])
      )

    edge_id = 0
    for table in self.tables:
      for fk in tables_schema[table]["foreign_keys"]:
        reference_table = fk["ref_table"]
        if reference_table not in self.tables:
          raise ValueError(
            f"Reference table {reference_table} not found in schema."
          )

        edge = ForeignKeyGraph.Edge(
          table=ForeignKeyGraph.Node(name=table),
          column=fk["column"],
          reference_table=ForeignKeyGraph.Node(name=reference_table),
          reference_column=fk["ref_column"],
          id=edge_id,
        )
        edge_id += 1

        self.graph[self.table_to_index[table]].append(edge)

  def is_leaf(self, table: str) -> bool:
    """
    Check if a table is a leaf node in the graph.

    Args:
        table (str): Table name.

    Returns:
        bool: True if the table is a leaf node, False otherwise.
    """
    return len(self.graph[self.table_to_index[table]]) == 0

  def get_edges(self, table: str) -> List["ForeignKeyGraph.Edge"]:
    """
    Get the edges (foreign key relationships) for a given table.

    Args:
        table (str): Table name.

    Returns:
        List[ForeignKeyGraph.Edge]: List of edges for the table.
    """
    if table not in self.tables:
      raise ValueError(f"Table {table} not found in schema.")

    edges = self.graph[self.table_to_index[table]]
    edges_ids = [edge.id for edge in edges]
    # Check for duplicate edges
    if len(edges_ids) != len(set(edges_ids)):
      raise ValueError(f"Duplicate edges found for table {table}.")

    return edges

  def get_subgraph_signature(self, edges: List["ForeignKeyGraph.Edge"]) -> int:
    """
    Get a signature of the edges for a given table.
    The signature is defined as a bitwise OR of the edge IDs.

    Args:
        edges (List[ForeignKeyGraph.Edge]): List of edges for the table.

    Returns:
        int: signature of the edges. It is a bitwise OR of the edge IDs.
    """
    signature = 0
    for edge in edges:
      signature |= self.get_edge_signature(edge)
    return signature

  def get_edge_signature(self, edge: "ForeignKeyGraph.Edge") -> int:
    """
    Get a signature of the edge for a given table.
    The signature is defined as a bitwise OR of the edge ID.

    Args:
        edge (ForeignKeyGraph.Edge): Edge for the table.

    Returns:
        int: signature of the edge. It is a bitwise OR of the edge ID.
    """
    return 1 << edge.id
