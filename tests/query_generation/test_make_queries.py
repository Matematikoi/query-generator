from unittest import mock

import pytest


from query_generator.database_schemas.schemas import get_schema
from query_generator.synthetic_queries.query_builder import (
  QueryBuilder,
)
from query_generator.synthetic_queries.predicate_generator import (
  HistogramDataType,
  PredicateRange,
)
from query_generator.utils.definitions import (
  Dataset,
  PredicateOperatorProbability,
  PredicateParameters,
  SyntheticQueryGenerationParameters,
)
from query_generator.utils.exceptions import UnkownDatasetError
from pypika import OracleQuery
from pypika import functions as fn


def test_add_range_supports_all_histogram_types():
  tables_schema, _ = get_schema(Dataset.TPCH)
  query_builder = QueryBuilder(
    None,
    tables_schema,
    Dataset.TPCH,
    PredicateParameters(
      histogram_path=None,
      extra_predicates=None,
      row_retention_probability=0.2,
      operator_weights=None,
      equality_lower_bound_probability=None,
      extra_values_for_in=None,
    ),
  )
  for dtype in HistogramDataType:
    query_builder._add_range(
      OracleQuery()
      .from_(query_builder.table_to_pypika_table["lineitem"])
      .select(fn.Count("*")),
      PredicateRange(
        table="lineitem",
        column="foo",
        min_value=2020,
        max_value=2020,
        dtype=dtype,
      ),
    )
