import random
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass

from query_generator.data_structures.foreign_key_graph import ForeignKeyGraph
from query_generator.utils.exceptions import GraphExploredError

MAX_ATTEMPTS_FOR_NEW_SUBGRAPH = 1000


class SubGraphGenerator:
  def __init__(
    self,
    graph: ForeignKeyGraph,
    keep_edge_prob: float,
    max_hops: int,
    seen_subgraphs: dict[int, bool],
  ) -> None:
    self.hops = max_hops
    self.keep_edge_prob = keep_edge_prob
    self.graph = graph
    self.seen_subgraphs: dict[int, bool] = seen_subgraphs

  def get_random_subgraph(self, fact_table: str) -> list[ForeignKeyGraph.Edge]:
    """Starting from the fact table, for each edge of the current table we
    decide based on the keep_edge_probability whether to keep the edge or not.

    We repeat this process up until the maximum number of hops.
    """

    @dataclass
    class JoinDepthNode:
      table: str
      depth: int

    queue: deque[JoinDepthNode] = deque()
    queue.append(JoinDepthNode(fact_table, 0))
    edges_subgraph = []

    while queue:
      current_node = queue.popleft()
      if current_node.depth >= self.hops:
        continue

      current_edges = self.graph.get_edges(current_node.table)
      for current_edge in current_edges:
        if random.random() < self.keep_edge_prob:
          edges_subgraph.append(current_edge)
          queue.append(
            JoinDepthNode(
              current_edge.reference_table.name,
              current_node.depth + 1,
            ),
          )

    return edges_subgraph

  def get_unseen_random_subgraph(
    self,
    fact_table: str,
  ) -> list[ForeignKeyGraph.Edge]:
    """Generate a random subgraph starting from the fact table.

    Args:
        fact_table (str): Name of the fact table.

    Returns:
        List[ForeignKeyGraph.Edge]: List of edges in the generated subgraph.

    """
    attempts = 0
    while True:
      attempts += 1
      if attempts > MAX_ATTEMPTS_FOR_NEW_SUBGRAPH:
        raise GraphExploredError(attempts)
      edges = self.get_random_subgraph(fact_table)
      edges_signature = self.graph.get_subgraph_signature(edges)
      if len(edges) == 0:
        continue  # no edges found, retry
      if edges_signature not in self.seen_subgraphs:
        self.seen_subgraphs[edges_signature] = True
        return edges

  def generate_subgraph(
    self,
    fact_table: str,
    max_signatures_per_fact_table: int,
  ) -> Iterator[list[ForeignKeyGraph.Edge]]:
    # TODO(GABRIEL): http://localhost:8080/tktview/5cfb15b1aa88be40c2d1ae7f5bb521c478d0dad0
    # Add logger
    #  communicate with the user the total number of signatures
    # or add a debug mode
    for _ in range(max_signatures_per_fact_table):
      try:
        yield self.get_unseen_random_subgraph(fact_table)
      except GraphExploredError:
        # The exception is failing to find a new subgraph
        # after multiple attempts
        break
