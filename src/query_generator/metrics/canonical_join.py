"""Canonical join form extraction from DuckDB trace graphs."""

import networkx as nx


def is_join(op: str) -> bool:
  """Return True for JOIN-family and structural binary operators."""
  return op.endswith("JOIN") or op in {
    "CTE",
    "CROSS_PRODUCT",
    "UNION",
    "RECURSIVE_CTE",
  }


def is_scan(op: str) -> bool:
  """Return True for SCAN-family and leaf-equivalent operators."""
  return "SCAN" in op or op in {"EMPTY_RESULT", "HASH_GROUP_BY"}


def summarize_plan(graph: nx.DiGraph) -> nx.DiGraph:
  """Collapse a DuckDB trace graph to its JOIN/SCAN skeleton.

  Skips unary operators. Keeps ROOT, JOIN-family nodes, and SCAN leaves.
  The summarized graph preserves node 0 as ROOT.

  Args:
      graph: Full DuckDB trace graph from DuckDBTraceParser.trace_graph.

  Returns:
      Summarized nx.DiGraph with only structural nodes.

  Raises:
      ValueError: If the graph violates expected structural invariants.
      KeyError: If node 0 is missing or has no operator_type.

  """
  root_id = 0
  result: nx.DiGraph = nx.DiGraph()
  next_id = [0]

  def add_node(attrs: dict) -> int:
    nid = next_id[0]
    next_id[0] += 1
    result.add_node(nid, **attrs)
    return nid

  def collapse(u: int) -> int:
    op = graph.nodes[u]["operator_type"]
    children = list(graph.successors(u))

    if len(children) == 0:
      if not is_scan(op):
        msg = (
          f"Leaf node {u} has operator {op!r}, expected a SCAN-family operator."
        )
        raise ValueError(msg)
      return add_node(dict(graph.nodes[u]))

    if len(children) > 1:
      if not is_join(op):
        msg = (
          f"Node {u} has {len(children)} children "
          f"but operator {op!r} is not a JOIN."
        )
        raise ValueError(msg)
      j = add_node(dict(graph.nodes[u]))
      for child in children:
        result.add_edge(j, collapse(child))
      return j

    # Unary: skip and recurse
    return collapse(children[0])

  root_attrs = dict(graph.nodes[root_id])
  if root_attrs.get("operator_type") != "ROOT":
    actual = root_attrs.get("operator_type")
    msg = f"Node 0 has operator_type {actual!r}, expected 'ROOT'."
    raise ValueError(msg)

  root_h = add_node(root_attrs)
  root_children = list(graph.successors(root_id))
  if len(root_children) != 1:
    msg = f"ROOT node must have exactly 1 child, got {len(root_children)}."
    raise ValueError(msg)

  result.add_edge(root_h, collapse(root_children[0]))
  return result


def _node_label(graph: nx.DiGraph, u: int) -> str:
  node = graph.nodes[u]
  op = node["operator_type"]
  if op == "ROOT":
    return "ROOT"
  if op == "UNION":
    return "UNION"
  if is_join(op):
    return "JOIN"
  if is_scan(op):
    return node.get("table") or op
  return op


def _dfs(graph: nx.DiGraph, u: int) -> str:
  label = _node_label(graph, u)
  children = list(graph.successors(u))
  if not children:
    return label
  child_forms = sorted(_dfs(graph, v) for v in children)
  return label + "(" + ",".join(child_forms) + ")"


def map_to_canonical(graph: nx.DiGraph) -> str:
  """Convert a summarized plan graph to a canonical join form string.

  Children are sorted at each node so join order does not affect the result.
  Unknown operators (neither join nor scan) use their operator type string.

  Args:
      graph: Summarized graph from summarize_plan.

  Returns:
      Canonical string such as "ROOT(JOIN(a,JOIN(b,c)))".

  """
  return _dfs(graph, 0)


def get_canonical_join_form_fn(graph: nx.DiGraph) -> str | None:
  """Return the canonical join form string, or None if extraction fails.

  Args:
      graph: Full DuckDB trace graph from DuckDBTraceParser.trace_graph.

  Returns:
      Canonical form string, or None on any error.

  """
  try:
    summarized = summarize_plan(graph)
    return map_to_canonical(summarized)
  except Exception:
    return None
