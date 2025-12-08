"""Parsing of DuckDB Traces."""

import json
import logging
from collections import deque
from enum import StrEnum
from typing import Any, NotRequired, TypedDict, get_type_hints
import sqlparse

import networkx as nx
import polars as pl

logger = logging.getLogger(__name__)


class DuckDBPhysicalOperators(StrEnum):
    """List of all duckdb physical operators."""

    TABLE_SCAN = "TABLE_SCAN"
    DUMMY_SCAN = "DUMMY_SCAN"
    CHUNK_SCAN = "CHUNK_SCAN"
    COLUMN_DATA_SCAN = "COLUMN_DATA_SCAN"
    DELIM_SCAN = "DELIM_SCAN"
    ORDER_BY = "ORDER_BY"
    LIMIT = "LIMIT"
    LIMIT_PERCENT = "LIMIT_PERCENT"
    STREAMING_LIMIT = "STREAMING_LIMIT"
    RESERVOIR_SAMPLE = "RESERVOIR_SAMPLE"
    STREAMING_SAMPLE = "STREAMING_SAMPLE"
    TOP_N = "TOP_N"
    WINDOW = "WINDOW"
    STREAMING_WINDOW = "STREAMING_WINDOW"
    UNNEST = "UNNEST"
    UNGROUPED_AGGREGATE = "UNGROUPED_AGGREGATE"
    HASH_GROUP_BY = "HASH_GROUP_BY"
    PERFECT_HASH_GROUP_BY = "PERFECT_HASH_GROUP_BY"
    PARTITIONED_AGGREGATE = "PARTITIONED_AGGREGATE"
    FILTER = "FILTER"
    PROJECTION = "PROJECTION"
    COPY_TO_FILE = "COPY_TO_FILE"
    BATCH_COPY_TO_FILE = "BATCH_COPY_TO_FILE"
    LEFT_DELIM_JOIN = "LEFT_DELIM_JOIN"
    RIGHT_DELIM_JOIN = "RIGHT_DELIM_JOIN"
    BLOCKWISE_NL_JOIN = "BLOCKWISE_NL_JOIN"
    NESTED_LOOP_JOIN = "NESTED_LOOP_JOIN"
    HASH_JOIN = "HASH_JOIN"
    PIECEWISE_MERGE_JOIN = "PIECEWISE_MERGE_JOIN"
    IE_JOIN = "IE_JOIN"
    ASOF_JOIN = "ASOF_JOIN"
    CROSS_PRODUCT = "CROSS_PRODUCT"
    POSITIONAL_JOIN = "POSITIONAL_JOIN"
    POSITIONAL_SCAN = "POSITIONAL_SCAN"
    UNION = "UNION"
    INSERT = "INSERT"
    BATCH_INSERT = "BATCH_INSERT"
    DELETE_OPERATOR = "DELETE_OPERATOR"
    UPDATE = "UPDATE"
    MERGE_INTO = "MERGE_INTO"
    EMPTY_RESULT = "EMPTY_RESULT"
    CREATE_TABLE = "CREATE_TABLE"
    CREATE_TABLE_AS = "CREATE_TABLE_AS"
    BATCH_CREATE_TABLE_AS = "BATCH_CREATE_TABLE_AS"
    CREATE_INDEX = "CREATE_INDEX"
    EXPLAIN = "EXPLAIN"
    EXPLAIN_ANALYZE = "EXPLAIN_ANALYZE"
    EXECUTE = "EXECUTE"
    VACUUM = "VACUUM"
    RECURSIVE_CTE = "RECURSIVE_CTE"
    RECURSIVE_KEY_CTE = "RECURSIVE_KEY_CTE"
    CTE = "CTE"
    RECURSIVE_CTE_SCAN = "RECURSIVE_CTE_SCAN"
    RECURSIVE_RECURRING_CTE_SCAN = "RECURSIVE_RECURRING_CTE_SCAN"
    CTE_SCAN = "CTE_SCAN"
    EXPRESSION_SCAN = "EXPRESSION_SCAN"
    ALTER = "ALTER"
    CREATE_SEQUENCE = "CREATE_SEQUENCE"
    CREATE_VIEW = "CREATE_VIEW"
    CREATE_SCHEMA = "CREATE_SCHEMA"
    CREATE_MACRO = "CREATE_MACRO"
    CREATE_SECRET = "CREATE_SECRET"
    DROP = "DROP"
    PRAGMA = "PRAGMA"
    TRANSACTION = "TRANSACTION"
    PREPARE = "PREPARE"
    EXPORT = "EXPORT"
    SET = "SET"
    SET_VARIABLE = "SET_VARIABLE"
    RESET = "RESET"
    LOAD = "LOAD"
    INOUT_FUNCTION = "INOUT_FUNCTION"
    CREATE_TYPE = "CREATE_TYPE"
    ATTACH = "ATTACH"
    DETACH = "DETACH"
    RESULT_COLLECTOR = "RESULT_COLLECTOR"
    EXTENSION = "EXTENSION"
    PIVOT = "PIVOT"
    COPY_DATABASE = "COPY_DATABASE"
    VERIFY_VECTOR = "VERIFY_VECTOR"
    UPDATE_EXTENSIONS = "UPDATE_EXTENSIONS"
    ROOT = (
        "ROOT"  # placeholder for the root of the plan; not an actual operator
    )

    @staticmethod
    def get_all_operators() -> list[str]:
        """Get all operator types as a list of strings."""
        return [operator.value for operator in DuckDBPhysicalOperators]

    @staticmethod
    def get_empty_operator_dict() -> dict["DuckDBPhysicalOperators", int]:
        """Get a dict with all operators initialized to zero."""
        return dict.fromkeys(DuckDBPhysicalOperators, 0)


ParsedTraceExtraInfoDuckdb = TypedDict(
    "ParsedTraceExtraInfoDuckdb",
    {"Estimated Cardinality": NotRequired[str]},
)


class DuckDBTraceNode(TypedDict):
    """Attributes for each node in the duckdb trace graph.

    Attributes:
        output_cardinality: The output cardinality of the operator.
        operator_type: DuckDB physical operator type.

    """

    output_cardinality: int
    operator_type: str  # Maps to physical operators.


ParsedTraceExtraInfoDuckdb = TypedDict(
    "ParsedTraceExtraInfoDuckdb",
    {"Estimated Cardinality": NotRequired[str]},
)


class ParsedDuckDBTraceChildren(TypedDict):
    """Children of the parsed trace.

    Attributes:
        result_set_size: The size in bytes of the result.
        operator_type: The physical operator type.

    """

    result_set_size: int
    operator_name: str
    cpu_time: float
    extra_info: ParsedTraceExtraInfoDuckdb
    cumulative_cardinality: int
    operator_type: str
    operator_cardinality: int
    cumulative_rows_scanned: int
    operator_timing: float
    children: list["ParsedDuckDBTraceChildren"]


class ParsedDuckDBTraceRoot(TypedDict):
    """Head of the parsed trace."""

    rows_returned: int
    latency: float
    result_set_size: int
    query_name: str
    blocked_thread_time: float
    system_peak_buffer_memory: int
    cpu_time: float
    cumulative_cardinality: int
    cumulative_rows_scanned: int
    children: list[ParsedDuckDBTraceChildren]


class DuckDBMetrics(TypedDict):
    """Collect the individual metrics from duckdb."""

    latency_duckdb: float
    cumulative_cardinality_duckdb: int
    cumulative_rows_scanned_duckdb: int
    query_plan_size: int
    query_plan_length: int
    query_size_bytes: int
    query_size_tokens: int
    output_cardinality: int
    query_keywords: list[str]
    operator_distribution: dict[DuckDBPhysicalOperators, int]


def get_attributes_root_node(trace: ParsedDuckDBTraceRoot) -> DuckDBTraceNode:
    """Get the attributes for the root node."""
    return {
        "output_cardinality": trace["rows_returned"],
        "operator_type": DuckDBPhysicalOperators.ROOT.value,
    }


def add_node(
    trace_graph: nx.DiGraph, node_id: int, attributes: DuckDBTraceNode
):
    """Add a node to the trace graph with given attributes."""
    trace_graph.add_node(node_id, **attributes)


def get_attributes_children_node(
    trace: ParsedDuckDBTraceChildren,
) -> DuckDBTraceNode:
    """Get the attributes for a children node.

    Gets output cardinality and operator type.
    """
    return {
        "output_cardinality": trace["operator_cardinality"],
        "operator_type": trace["operator_type"],
    }


def trace_to_graph(trace: ParsedDuckDBTraceRoot) -> nx.DiGraph:
    """Convert the parsed trace into a directed graph."""
    trace_graph = nx.DiGraph()
    add_node(trace_graph, 0, get_attributes_root_node(trace))
    cnt = 1
    queue: deque[tuple[ParsedDuckDBTraceChildren, int]] = deque(
        [(i, 0) for i in trace["children"]]
    )

    while len(queue) > 0:
        node, parent = queue.popleft()
        queue.extend([(i, cnt) for i in node["children"]])
        add_node(trace_graph, cnt, get_attributes_children_node(node))
        cnt += 1
        trace_graph.add_edge(parent, cnt - 1)
    return trace_graph


class DuckDBTraceParser:
    """Parser for duckdb traces."""

    def __init__(self, raw_trace: str) -> None:
        """Initialize the duckdb trace parser.

        Args:
            raw_trace: The raw trace as a JSON string.

        """
        self.raw_trace = raw_trace
        self.trace: ParsedDuckDBTraceRoot = json.loads(raw_trace)
        self.trace_graph = trace_to_graph(self.trace)
        self.parsed_query = self.get_parsed_query()

    def get_parsed_query(self) -> list[sqlparse.sql.Token]:
        """Get the parsed query using sqlparse."""
        parsed_statements = sqlparse.parse(self.get_raw_query())
        if len(parsed_statements) == 0:
            return []
        return list(parsed_statements[0].flatten())

    def get_latency(self) -> float:
        """Get the latency of the query."""
        return self.trace["latency"]

    def get_number_of_nodes(self) -> int:
        """Get the number of nodes in the trace graph."""
        return self.trace_graph.number_of_nodes()

    def get_longest_path_length(self) -> int:
        """Get the longest path length in the trace graph."""
        return nx.algorithms.dag.dag_longest_path_length(self.trace_graph)

    def get_cumulative_cardinality(self) -> int:
        """Get the cumulative cardinality from the trace."""
        return self.trace["cumulative_cardinality"]

    def get_rows_scanned(self) -> int:
        """Get the cumulative rows scanned from the trace."""
        return self.trace["cumulative_rows_scanned"]

    def get_operator_types(self) -> dict[DuckDBPhysicalOperators, int]:
        """Get the operator types from the trace graph."""
        counts: dict[DuckDBPhysicalOperators, int] = (
            DuckDBPhysicalOperators.get_empty_operator_dict()
        )
        for n, data in self.trace_graph.nodes(data=True):
            op = data.get("operator_type")
            if op is None:
                logger.error(f"Node {n} has no operator_type")
                continue
            # Validate operator type
            operator = DuckDBPhysicalOperators(op)
            counts[operator] += 1
        return counts

    def get_raw_query(self) -> str:
        """Get the raw query from the trace."""
        return self.trace["query_name"]

    def get_query_size_bytes(self) -> int:
        """Get the query size in bytes."""
        raw_query = self.get_raw_query()
        if raw_query is None:
            return 0
        return len(raw_query.encode("utf-8"))

    def get_query_size_tokens(self) -> int:
        """Get the query size in tokens."""
        cnt = 0
        for token in self.parsed_query:
            if not token.is_whitespace and not token.is_newline:
                cnt += 1
        return cnt

    def get_query_keywords(self) -> list[str]:
        """Get the query keywords."""
        return [ t.normalized for t in self.parsed_query if t.is_keyword ]

    def get_output_cardinality(self) -> int:
        """Get the output cardinality from the trace."""
        return self.trace["rows_returned"]

    def get_metrics(self) -> DuckDBMetrics:
        """Get the metrics from the trace."""
        return {
            "latency_duckdb": self.get_latency(),
            "cumulative_cardinality_duckdb": self.get_cumulative_cardinality(),
            "cumulative_rows_scanned_duckdb": self.get_rows_scanned(),
            "query_plan_size": self.get_number_of_nodes(),
            "query_plan_length": self.get_longest_path_length(),
            "query_size_bytes": self.get_query_size_bytes(),
            "query_size_tokens": self.get_query_size_tokens(),
            "output_cardinality": self.get_output_cardinality(),
            "query_keywords": self.get_query_keywords(),
            "operator_distribution": self.get_operator_types(),
        }

    @staticmethod
    def get_metrics_from_raw_trace(
        raw_trace: str,
    ) -> DuckDBMetrics:
        """Get the metrics from the raw trace.

        Args:
            raw_trace: The raw trace as a JSON string.

        Returns:
            The duckdb metrics as a typed dict. Ideal for polars operations.

        """
        parser = DuckDBTraceParser(raw_trace)
        return parser.get_metrics()

    @staticmethod
    def get_metrics_polars_struct() -> pl.datatypes.Struct:
        """Get the polars struct for the duckdb metrics."""
        return polars_struct_from_typeddict(DuckDBMetrics)


def python_type_to_polars(
    python_type: type,
) -> type[pl.DataType] | pl.datatypes.Struct | pl.List:
    """Map native python types to polars."""
    mapping: dict[
        type[Any], type[pl.DataType] | pl.datatypes.Struct | pl.List
    ] = {
        int: pl.Int64,
        float: pl.Float64,
        str: pl.String,
        bool: pl.Boolean,
        dict[DuckDBPhysicalOperators, int]: pl.Struct(
            dict.fromkeys(DuckDBPhysicalOperators.get_all_operators(), pl.Int64)
        ),
        list[str]: pl.List(pl.String),
    }
    return mapping[python_type]


def polars_struct_from_typeddict(typed_dict: type[Any]) -> pl.Struct:
    """Get polars struct from a typed dict."""
    fields = []
    for name, hint in get_type_hints(typed_dict).items():
        fields.append(pl.Field(name, python_type_to_polars(hint)))
    return pl.Struct(fields)
