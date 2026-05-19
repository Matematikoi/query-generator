"""Tests for canonical join form extraction."""

import networkx as nx
import pytest

from query_generator.metrics.canonical_join import (
  get_canonical_join_form_fn,
  map_to_canonical,
  summarize_plan,
)


def make_join_graph() -> nx.DiGraph:
  """Two-table HASH_JOIN with unary ops in between.

  ROOT -> PROJECTION -> FILTER -> HASH_JOIN -> TABLE_SCAN (table=a)
                                            -> TABLE_SCAN (table=b)
  """
  G = nx.DiGraph()
  G.add_node(
    0, operator_type="ROOT", output_cardinality=10, estimated_cardinality=None
  )
  G.add_node(
    1,
    operator_type="PROJECTION",
    output_cardinality=10,
    estimated_cardinality=None,
  )
  G.add_node(
    2, operator_type="FILTER", output_cardinality=10, estimated_cardinality=None
  )
  G.add_node(
    3,
    operator_type="HASH_JOIN",
    output_cardinality=10,
    estimated_cardinality=None,
  )
  G.add_node(
    4,
    operator_type="TABLE_SCAN",
    output_cardinality=100,
    estimated_cardinality=None,
    table="a",
  )
  G.add_node(
    5,
    operator_type="TABLE_SCAN",
    output_cardinality=200,
    estimated_cardinality=None,
    table="b",
  )
  G.add_edges_from([(0, 1), (1, 2), (2, 3), (3, 4), (3, 5)])
  return G


def test_summarize_plan_skips_unary_operators():
  G = make_join_graph()
  H = summarize_plan(G)
  # Only ROOT, HASH_JOIN, and two TABLE_SCANs should remain
  operator_types = [d["operator_type"] for _, d in H.nodes(data=True)]
  assert "PROJECTION" not in operator_types
  assert "FILTER" not in operator_types
  assert H.number_of_nodes() == 4


def test_summarize_plan_preserves_structure():
  G = make_join_graph()
  H = summarize_plan(G)
  # ROOT -> JOIN -> two leaves
  assert H.number_of_edges() == 3


def test_summarize_plan_raises_on_empty_graph():
  G = nx.DiGraph()
  with pytest.raises(Exception):
    summarize_plan(G)


def test_map_to_canonical_two_table_join():
  G = make_join_graph()
  H = summarize_plan(G)
  result = map_to_canonical(H)
  assert result == "ROOT(JOIN(a,b))"


def test_map_to_canonical_is_sorted():
  """Children are sorted so join order does not affect canonical form."""
  G = nx.DiGraph()
  G.add_node(
    0, operator_type="ROOT", output_cardinality=0, estimated_cardinality=None
  )
  G.add_node(
    1,
    operator_type="HASH_JOIN",
    output_cardinality=0,
    estimated_cardinality=None,
  )
  G.add_node(
    2,
    operator_type="TABLE_SCAN",
    output_cardinality=0,
    estimated_cardinality=None,
    table="z",
  )
  G.add_node(
    3,
    operator_type="TABLE_SCAN",
    output_cardinality=0,
    estimated_cardinality=None,
    table="a",
  )
  G.add_edges_from([(0, 1), (1, 2), (1, 3)])
  H = summarize_plan(G)
  result = map_to_canonical(H)
  assert result == "ROOT(JOIN(a,z))"


def test_get_canonical_join_form_returns_none_on_invalid_graph():
  G = nx.DiGraph()  # empty graph — no ROOT node
  result = get_canonical_join_form_fn(G)
  assert result is None


def test_get_canonical_join_form_happy_path():
  G = make_join_graph()
  result = get_canonical_join_form_fn(G)
  assert result == "ROOT(JOIN(a,b))"
