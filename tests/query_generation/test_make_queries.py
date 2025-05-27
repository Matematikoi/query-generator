from unittest import mock

import pytest


from query_generator.database_schemas.schemas import get_schema
from query_generator.join_based_query_generator.snowflake import (
  QueryBuilder,
  generate_and_write_queries,
)
from query_generator.predicate_generator.predicate_generator import (
  HistogramDataType,
  PredicateRange,
)
from query_generator.utils.definitions import (
  Dataset,
  PredicateOperatorProbability,
  QueryGenerationParameters,
)
from query_generator.utils.exceptions import UnkownDatasetError
from pypika import OracleQuery
from pypika import functions as fn


def test_tpch_query_generation():
  with mock.patch(
    "query_generator.join_based_query_generator.snowflake.Writer.write_query",
  ) as mock_writer:
    generate_and_write_queries(
      QueryGenerationParameters(
        dataset=Dataset.TPCDS,
        max_hops=1,
        max_queries_per_fact_table=1,
        max_queries_per_signature=1,
        keep_edge_probability=0.2,
        row_retention_probability=0.2,
        extra_predicates=1,
        seen_subgraphs={},
        operator_weights=PredicateOperatorProbability(
          operator_in=0.4,
          operator_equal=0.4,
          operator_range=0.2,
        ),
      )
    )

    assert mock_writer.call_count > 5


def test_tpcds_query_generation():
  with mock.patch(
    "query_generator.join_based_query_generator.snowflake.Writer.write_query",
  ) as mock_writer:
    generate_and_write_queries(
      QueryGenerationParameters(
        dataset=Dataset.TPCDS,
        max_hops=1,
        max_queries_per_fact_table=1,
        max_queries_per_signature=1,
        keep_edge_probability=0.2,
        row_retention_probability=0.2,
        extra_predicates=1,
        seen_subgraphs={},
        operator_weights=PredicateOperatorProbability(
          operator_in=0.4,
          operator_equal=0.4,
          operator_range=0.2,
        ),
      ),
    )

    assert mock_writer.call_count > 5


def test_non_implemented_dataset():
  with mock.patch(
    "query_generator.join_based_query_generator.snowflake.Writer.write_query",
  ) as mock_writer:
    with pytest.raises(UnkownDatasetError):
      generate_and_write_queries(
        QueryGenerationParameters(
          dataset="non_implemented_dataset",
          max_hops=1,
          max_queries_per_fact_table=1,
          max_queries_per_signature=1,
          keep_edge_probability=0.2,
          row_retention_probability=0.2,
          extra_predicates=1,
          seen_subgraphs={},
          operator_weights=PredicateOperatorProbability(
            operator_in=0.4,
            operator_equal=0.4,
            operator_range=0.2,
          ),
        ),
      )
    assert mock_writer.call_count == 0


def test_add_rage_supports_all_histogram_types():
  tables_schema, _ = get_schema(Dataset.TPCH)
  query_builder = QueryBuilder(None, tables_schema, Dataset.TPCH)
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
