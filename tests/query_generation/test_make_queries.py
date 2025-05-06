from unittest import mock

import pytest

from query_generator.join_based_query_generator.snowflake import (
  generate_and_write_queries,
)
from query_generator.utils.definitions import (
  Dataset,
  QueryGenerationParameters,
)
from query_generator.utils.exceptions import UnkwonDatasetError


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
        keep_edge_prob=0.2,
        row_retention_probability=0.2,
        extra_predicates=1,
        seen_subgraphs={},
      ),
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
        keep_edge_prob=0.2,
        row_retention_probability=0.2,
        extra_predicates=1,
        seen_subgraphs={},
      ),
    )

    assert mock_writer.call_count > 5


def test_non_implemented_dataset():
  with mock.patch(
    "query_generator.join_based_query_generator.snowflake.Writer.write_query",
  ) as mock_writer:
    with pytest.raises(UnkwonDatasetError):
      generate_and_write_queries(
        QueryGenerationParameters(
          dataset="non_implemented_dataset",
          max_hops=1,
          max_queries_per_fact_table=1,
          max_queries_per_signature=1,
          keep_edge_prob=0.2,
          row_retention_probability=0.2,
          extra_predicates=1,
          seen_subgraphs={},
        ),
      )
    assert mock_writer.call_count == 0
