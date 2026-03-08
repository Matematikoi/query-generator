import duckdb
import pytest
from commonstrings import PyCommon_multiple_strings

from query_generator.duckdb_connection.utils import DuckDBColumnInfo
from query_generator.tools.histograms import (
  CandidateEntry,
  filter_substrings_by_length_support_monotonicity,
  get_common_substrings,
  get_top_substrings_per_length,
)


def make_tree(strings: list[str]) -> PyCommon_multiple_strings:
  tree = PyCommon_multiple_strings()
  tree.from_strings(strings)
  return tree


# --- filter_substrings_by_length_support_monotonicity ---


def test_monotonicity_empty():
  assert filter_substrings_by_length_support_monotonicity([]) == []


def test_monotonicity_single():
  candidates = [CandidateEntry(support=5, substring="hello")]
  result = filter_substrings_by_length_support_monotonicity(candidates)
  assert result == candidates


def test_monotonicity_all_kept():
  # Longer strings have lower support — all pass
  candidates = [
    CandidateEntry(support=10, substring="hi"),
    CandidateEntry(support=8, substring="hey"),
    CandidateEntry(support=5, substring="hello"),
    CandidateEntry(support=2, substring="howdy"),
  ]
  result = filter_substrings_by_length_support_monotonicity(candidates)
  assert set(c.substring for c in result) == {"hi", "hey", "hello", "howdy"}


def test_monotonicity_filters_dominated():
  # "hello" (len=5, support=10) dominates "hi" (len=2, support=5)
  # because "hello" is longer AND has strictly higher support
  candidates = [
    CandidateEntry(support=10, substring="hello"),
    CandidateEntry(support=5, substring="hi"),
  ]
  result = filter_substrings_by_length_support_monotonicity(candidates)
  substrings = {c.substring for c in result}
  assert "hello" in substrings
  assert "hi" not in substrings


def test_monotonicity_equal_support_across_lengths_kept():
  # Same support for different lengths — shorter should still be kept
  # (condition is strict: filter only if strictly longer has strictly higher)
  candidates = [
    CandidateEntry(support=5, substring="hi"),
    CandidateEntry(support=5, substring="hello"),
  ]
  result = filter_substrings_by_length_support_monotonicity(candidates)
  assert len(result) == 2


def test_monotonicity_same_length_all_kept():
  # Same length — neither dominates the other regardless of support
  candidates = [
    CandidateEntry(support=10, substring="cat"),
    CandidateEntry(support=3, substring="dog"),
  ]
  result = filter_substrings_by_length_support_monotonicity(candidates)
  assert len(result) == 2


def test_monotonicity_longer_higher_support_filters_shorter():
  # "hello" (len=5, support=10) filters "hi" (len=2, support=2)
  # "hello" is longer and has higher support, so "hi" is filtered
  candidates = [
    CandidateEntry(support=2, substring="hi"),
    CandidateEntry(support=10, substring="hello"),
  ]
  result = filter_substrings_by_length_support_monotonicity(candidates)
  substrings = {c.substring for c in result}
  assert "hello" in substrings
  assert "hi" not in substrings


# --- get_top_substrings_per_length ---


def test_top_substrings_high_support_returns_empty():
  # min_support_count higher than the number of strings in the tree → no results
  tree = make_tree(["hello"] * 10)
  result = get_top_substrings_per_length(
    tree, min_support_count=1000, max_per_length=5
  )
  assert result == []


def test_top_substrings_respects_max_per_length():
  strings = ["color"] * 20
  tree = make_tree(strings)
  result = get_top_substrings_per_length(
    tree, min_support_count=1, max_per_length=2
  )
  lengths = [len(c.substring) for c in result]
  for length in set(lengths):
    assert lengths.count(length) <= 2


def test_top_substrings_sorted_by_support_desc():
  strings = ["abcde"] * 10 + ["abcdf"] * 5
  tree = make_tree(strings)
  result = get_top_substrings_per_length(
    tree, min_support_count=1, max_per_length=10
  )
  length_groups: dict[int, list[int]] = {}
  for c in result:
    length_groups.setdefault(len(c.substring), []).append(c.support)
  for supports in length_groups.values():
    assert supports == sorted(supports, reverse=True)


def test_top_substrings_stops_early():
  # A tree with strings of length 3 only — lengths > 3 should yield nothing and stop
  strings = ["cat"] * 10
  tree = make_tree(strings)
  result = get_top_substrings_per_length(
    tree, min_support_count=1, max_per_length=100
  )
  assert all(len(c.substring) <= 3 for c in result)


# --- get_common_substrings ---


@pytest.fixture
def str_column_info() -> DuckDBColumnInfo:
  con = duckdb.connect()
  con.execute("CREATE TABLE t (s VARCHAR)")
  # 100 rows: 60x "turquoise", 30x "midnight blue", 10x "lavender"
  con.execute("INSERT INTO t SELECT 'turquoise' FROM range(60)")
  con.execute("INSERT INTO t SELECT 'midnight blue' FROM range(30)")
  con.execute("INSERT INTO t SELECT 'lavender' FROM range(10)")
  return DuckDBColumnInfo(con=con, table="t", column="s")


def test_common_substrings_returns_list(str_column_info):
  result = get_common_substrings(
    str_column_info,
    sample_size=100,
    support_probability_threshold=0.05,
    max_substrings_per_length=10,
  )
  assert isinstance(result, list)


def test_common_substrings_fields(str_column_info):
  result = get_common_substrings(
    str_column_info,
    sample_size=100,
    support_probability_threshold=0.05,
    max_substrings_per_length=10,
  )
  for entry in result:
    assert "substring" in entry
    assert "support" in entry
    assert "support_probability" in entry
    assert isinstance(entry["substring"], str)
    assert isinstance(entry["support"], int)
    assert 0.0 <= entry["support_probability"] <= 1.0


def test_common_substrings_threshold_filters(str_column_info):
  # With a high threshold, only high-support substrings should remain
  result = get_common_substrings(
    str_column_info,
    sample_size=100,
    support_probability_threshold=0.5,
    max_substrings_per_length=10,
  )
  for entry in result:
    assert entry["support_probability"] >= 0.5


def test_common_substrings_monotonicity(str_column_info):
  result = get_common_substrings(
    str_column_info,
    sample_size=100,
    support_probability_threshold=0.05,
    max_substrings_per_length=10,
  )
  # No longer string should have strictly higher support than a shorter one
  for i, a in enumerate(result):
    for b in result[i + 1 :]:
      if len(a["substring"]) > len(b["substring"]):
        assert a["support"] <= b["support"]


def test_common_substrings_empty_column():
  con = duckdb.connect()
  con.execute("CREATE TABLE t (s VARCHAR)")
  col = DuckDBColumnInfo(con=con, table="t", column="s")
  result = get_common_substrings(
    col,
    sample_size=100,
    support_probability_threshold=0.05,
    max_substrings_per_length=10,
  )
  assert result == []
