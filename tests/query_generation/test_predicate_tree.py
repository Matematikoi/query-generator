"""Tests for the _build_predicate_tree function in query_builder."""

from unittest.mock import patch

from pypika import Field

from query_generator.synthetic_queries.query_builder import (
  _build_predicate_tree,
)


def make_criterion(name: str, value: int):
  """Build a simple equality criterion for testing."""
  return Field(name) == value


class TestBuildPredicateTree:
  def test_empty_list_returns_none(self):
    result = _build_predicate_tree([], or_probability=0.0)
    assert result is None

  def test_single_criterion_returned_as_is(self):
    c = make_criterion("a", 1)
    result = _build_predicate_tree([c], or_probability=0.0)
    assert result is c

  def test_all_and_with_zero_or_probability(self):
    criteria = [
      make_criterion(name, i) for i, name in enumerate(["a", "b", "c", "d"])
    ]
    result = _build_predicate_tree(criteria, or_probability=0.0)
    sql = str(result)
    assert " OR " not in sql
    assert " AND " in sql

  def test_all_or_with_one_or_probability(self):
    criteria = [
      make_criterion(name, i) for i, name in enumerate(["a", "b", "c", "d"])
    ]
    result = _build_predicate_tree(criteria, or_probability=1.0)
    sql = str(result)
    assert " AND " not in sql
    assert " OR " in sql

  def test_two_criteria_and(self):
    criteria = [make_criterion("x", 1), make_criterion("y", 2)]
    result = _build_predicate_tree(criteria, or_probability=0.0)
    sql = str(result)
    assert "x" in sql
    assert "y" in sql
    assert " AND " in sql
    assert " OR " not in sql

  def test_two_criteria_or(self):
    criteria = [make_criterion("x", 1), make_criterion("y", 2)]
    result = _build_predicate_tree(criteria, or_probability=1.0)
    sql = str(result)
    assert "x" in sql
    assert "y" in sql
    assert " OR " in sql
    assert " AND " not in sql

  def test_all_criteria_appear_in_result(self):
    names = ["col_a", "col_b", "col_c", "col_d", "col_e"]
    criteria = [make_criterion(name, i) for i, name in enumerate(names)]
    result = _build_predicate_tree(criteria, or_probability=0.5)
    sql = str(result)
    for name in names:
      assert name in sql

  def test_mixed_or_inner_and_outer(self):
    """First merge OR, second merge AND → outer AND with bracketed OR leaf."""
    # criteria: a==0, b==1, c==2
    criteria = [
      make_criterion(name, i) for i, name in enumerate(["a", "b", "c"])
    ]
    # sample always picks indices [0, 1]
    # random() = 0.3 (< 0.5) on first merge  → OR:  (a==0) | (b==1)
    # random() = 0.7 (>= 0.5) on second merge → AND: (c==2) & ((a==0)|(b==1))
    with (
      patch(
        "query_generator.synthetic_queries.query_builder.random.sample",
        return_value=[0, 1],
      ),
      patch(
        "query_generator.synthetic_queries.query_builder.random.random",
        side_effect=[0.3, 0.7],
      ),
    ):
      result = _build_predicate_tree(criteria, or_probability=0.5)
    assert str(result) == '"c"=2 AND ("a"=0 OR "b"=1)'

  def test_mixed_and_inner_or_outer(self):
    """First merge AND, second merge OR → outer OR with bracketed AND leaf."""
    # criteria: a==0, b==1, c==2
    criteria = [
      make_criterion(name, i) for i, name in enumerate(["a", "b", "c"])
    ]
    # random() = 0.7 (>= 0.5) on first merge  → AND: (a==0) & (b==1)
    # random() = 0.3 (< 0.5) on second merge  → OR:  (c==2) | ((a==0)&(b==1))
    with (
      patch(
        "query_generator.synthetic_queries.query_builder.random.sample",
        return_value=[0, 1],
      ),
      patch(
        "query_generator.synthetic_queries.query_builder.random.random",
        side_effect=[0.7, 0.3],
      ),
    ):
      result = _build_predicate_tree(criteria, or_probability=0.5)
    assert str(result) == '"c"=2 OR ("a"=0 AND "b"=1)'
