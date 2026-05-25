from pathlib import Path

from query_generator.synthetic_queries.query_builder import (
  QueryBuilderPypika,
  load_schema,
)
from query_generator.synthetic_queries.predicate_generator import (
  HistogramDataType,
  PredicateRange,
)
from query_generator.utils.definitions import (
  Dataset,
  PredicateParameters,
)
from tests.utils import get_precomputed_histograms


def test_add_range_supports_all_histogram_types():
  schema = load_schema(
    Path(__file__).parent.parent.parent / "schemas" / "tpch.json"
  )
  query_builder = QueryBuilderPypika(
    None,
    schema.tables,
    PredicateParameters(
      histogram_path=get_precomputed_histograms(Dataset.TPCH),
      extra_predicates=None,
      row_retention_probability=0.2,
      operator_weights=None,
      equality_lower_bound_probability=None,
      extra_values_for_in=None,
      minimum_like_support_probability=None,
    ),
  )
  for dtype in HistogramDataType:
    query_builder._build_criterion_range(
      PredicateRange(
        table="lineitem",
        column="foo",
        min_value=2020,
        max_value=2020,
        dtype=dtype,
      ),
    )
