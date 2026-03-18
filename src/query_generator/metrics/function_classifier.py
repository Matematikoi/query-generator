"""SQL function classification using sqlglot."""

import logging
from enum import StrEnum
from typing import TypedDict

import sqlglot
import sqlglot.expressions as exp

logger = logging.getLogger(__name__)


class FunctionRecordFields(StrEnum):
  CATEGORY = "category"
  SUBCATEGORY = "subcategory"
  NAME = "name"
  EXPRESSION = "expression"


class FunctionRecord(TypedDict):
  """A classified SQL function record."""

  category: str
  subcategory: str
  name: str
  expression: str


# ---------------------------------------------------------------------------
# Taxonomy lookup tables — maps sqlglot expression types to subcategories.
# Any type not listed falls through to "other".
# ---------------------------------------------------------------------------

SCALAR_SUBCATEGORY: dict[type, str] = {
  # string
  exp.Concat: "string",
  exp.ConcatWs: "string",
  exp.Upper: "string",
  exp.Lower: "string",
  exp.Substring: "string",
  exp.Left: "string",
  exp.Right: "string",
  exp.Length: "string",
  exp.Trim: "string",
  exp.Replace: "string",
  exp.Initcap: "string",
  exp.Pad: "string",
  exp.Space: "string",
  exp.Repeat: "string",
  exp.Reverse: "string",
  exp.Overlay: "string",
  exp.Split: "string",
  exp.SplitPart: "string",
  exp.StartsWith: "string",
  exp.EndsWith: "string",
  exp.Chr: "string",
  exp.Ascii: "string",
  exp.Soundex: "string",
  exp.Levenshtein: "string",
  exp.Encode: "string",
  exp.Decode: "string",
  exp.Translate: "string",
  exp.Stuff: "string",
  exp.BitLength: "string",
  exp.IsAscii: "string",
  # datetime
  exp.DateAdd: "datetime",
  exp.DateDiff: "datetime",
  exp.DateTrunc: "datetime",
  exp.DateSub: "datetime",
  exp.DateBin: "datetime",
  exp.Extract: "datetime",
  exp.CurrentDate: "datetime",
  exp.CurrentTime: "datetime",
  exp.CurrentTimestamp: "datetime",
  exp.Year: "datetime",
  exp.Month: "datetime",
  exp.Day: "datetime",
  exp.Hour: "datetime",
  exp.Minute: "datetime",
  exp.Second: "datetime",
  exp.Quarter: "datetime",
  exp.Week: "datetime",
  exp.DayOfWeek: "datetime",
  exp.DayOfMonth: "datetime",
  exp.DayOfYear: "datetime",
  exp.LastDay: "datetime",
  exp.AddMonths: "datetime",
  exp.MonthsBetween: "datetime",
  exp.TimestampAdd: "datetime",
  exp.TimestampDiff: "datetime",
  exp.TimestampSub: "datetime",
  exp.TimestampTrunc: "datetime",
  exp.TimeAdd: "datetime",
  exp.TimeDiff: "datetime",
  exp.TimeSub: "datetime",
  exp.TimeTrunc: "datetime",
  exp.DatetimeAdd: "datetime",
  exp.DatetimeDiff: "datetime",
  exp.DatetimeSub: "datetime",
  exp.DatetimeTrunc: "datetime",
  exp.UnixSeconds: "datetime",
  exp.UnixMillis: "datetime",
  exp.StrToDate: "datetime",
  exp.StrToTime: "datetime",
  # numeric
  exp.Abs: "numeric",
  exp.Round: "numeric",
  exp.Floor: "numeric",
  exp.Ceil: "numeric",
  exp.Sqrt: "numeric",
  exp.Pow: "numeric",
  exp.Ln: "numeric",
  exp.Log: "numeric",
  exp.Exp: "numeric",
  exp.Sign: "numeric",
  exp.Pi: "numeric",
  exp.Rand: "numeric",
  exp.Randn: "numeric",
  exp.Greatest: "numeric",
  exp.Least: "numeric",
  exp.Degrees: "numeric",
  exp.Radians: "numeric",
  exp.Sin: "numeric",
  exp.Cos: "numeric",
  exp.Tan: "numeric",
  exp.Asin: "numeric",
  exp.Acos: "numeric",
  exp.Atan: "numeric",
  exp.Atan2: "numeric",
  exp.Cbrt: "numeric",
  exp.IsNan: "numeric",
  exp.IsInf: "numeric",
  # null handling
  exp.Coalesce: "null_handling",
  exp.Nullif: "null_handling",
  exp.EqualNull: "null_handling",
  exp.IsNullValue: "null_handling",
  exp.Nvl2: "null_handling",
  # type conversion
  exp.Cast: "type_conversion",
  exp.TryCast: "type_conversion",
  exp.Convert: "type_conversion",
  exp.Typeof: "type_conversion",
  exp.Hex: "type_conversion",
  exp.Unhex: "type_conversion",
  # regex
  exp.RegexpReplace: "regex",
  exp.RegexpExtract: "regex",
  exp.RegexpExtractAll: "regex",
  exp.RegexpLike: "regex",
  exp.RegexpILike: "regex",
  exp.RegexpCount: "regex",
  exp.RegexpInstr: "regex",
  exp.RegexpSplit: "regex",
  exp.RegexpFullMatch: "regex",
  # json
  exp.JSONExtract: "json",
  exp.JSONExtractScalar: "json",
  exp.JSONObject: "json",
  exp.JSONArray: "json",
  exp.JSONExists: "json",
  exp.JSONFormat: "json",
  exp.JSONKeys: "json",
  exp.JSONType: "json",
  exp.JSONBExtract: "json",
  exp.JSONBExtractScalar: "json",
  exp.JSONBContains: "json",
  exp.ParseJSON: "json",
  # array
  exp.Array: "array",
  exp.Flatten: "array",
  exp.ArraySize: "array",
  exp.ArrayContains: "array",
  exp.ArrayContainsAll: "array",
  exp.ArrayAppend: "array",
  exp.ArrayPrepend: "array",
  exp.ArrayConcat: "array",
  exp.ArraySort: "array",
  exp.ArrayReverse: "array",
  exp.ArrayRemove: "array",
  exp.ArraySlice: "array",
  exp.ArrayFilter: "array",
  exp.ArrayAll: "array",
  exp.ArrayAny: "array",
  exp.ArrayToString: "array",
  exp.ArrayFirst: "array",
  exp.ArrayLast: "array",
  # map / struct
  exp.Map: "map_struct",
  exp.Struct: "map_struct",
  exp.StructExtract: "map_struct",
  exp.MapKeys: "map_struct",
  exp.MapSize: "map_struct",
  # hash / crypto
  exp.MD5: "hash_crypto",
  exp.SHA: "hash_crypto",
  exp.SHA2: "hash_crypto",
  exp.FarmFingerprint: "hash_crypto",
  # session / system
  exp.CurrentUser: "session_system",
  exp.Uuid: "session_system",
  exp.CurrentSchema: "session_system",
}

AGG_SUBCATEGORY: dict[type, str] = {
  # core
  exp.Count: "core",
  exp.CountIf: "core",
  exp.Sum: "core",
  exp.Avg: "core",
  exp.Min: "core",
  exp.Max: "core",
  exp.First: "core",
  exp.Last: "core",
  exp.AnyValue: "core",
  # statistical
  exp.Stddev: "statistical",
  exp.StddevPop: "statistical",
  exp.StddevSamp: "statistical",
  exp.Variance: "statistical",
  exp.VariancePop: "statistical",
  exp.Corr: "statistical",
  exp.CovarPop: "statistical",
  exp.CovarSamp: "statistical",
  exp.Kurtosis: "statistical",
  exp.Skewness: "statistical",
  # ordered_set
  exp.PercentileCont: "ordered_set",
  exp.PercentileDisc: "ordered_set",
  exp.Median: "ordered_set",
  exp.Mode: "ordered_set",
  exp.Quantile: "ordered_set",
  # collection
  exp.ArrayAgg: "collection",
  exp.ArrayConcatAgg: "collection",
  exp.GroupConcat: "collection",
  exp.JSONArrayAgg: "collection",
  exp.JSONObjectAgg: "collection",
  exp.ObjectAgg: "collection",
  # approximate
  exp.ApproxDistinct: "approximate",
  exp.Hll: "approximate",
}


BINARY_SUBCATEGORY: dict[type, str] = {
  # arithmetic
  exp.Add: "scalar.arithmetic",
  exp.Sub: "scalar.arithmetic",
  exp.Mul: "scalar.arithmetic",
  exp.Div: "scalar.arithmetic",
  exp.IntDiv: "scalar.arithmetic",
  exp.Mod: "scalar.arithmetic",
  # string
  exp.DPipe: "scalar.string",
  # bitwise
  exp.BitwiseAnd: "scalar.bitwise",
  exp.BitwiseOr: "scalar.bitwise",
  exp.BitwiseXor: "scalar.bitwise",
  exp.BitwiseLeftShift: "scalar.bitwise",
  exp.BitwiseRightShift: "scalar.bitwise",
  # comparison
  exp.EQ: "scalar.comparison",
  exp.NEQ: "scalar.comparison",
  exp.GT: "scalar.comparison",
  exp.GTE: "scalar.comparison",
  exp.LT: "scalar.comparison",
  exp.LTE: "scalar.comparison",
  exp.NullSafeEQ: "scalar.comparison",
  exp.NullSafeNEQ: "scalar.comparison",
  exp.Is: "scalar.comparison",
  # pattern matching
  exp.Like: "scalar.pattern_matching",
  exp.ILike: "scalar.pattern_matching",
  exp.Glob: "scalar.pattern_matching",
  exp.SimilarTo: "scalar.pattern_matching",
  exp.Match: "scalar.pattern_matching",
  # other binary
  exp.Dot: "scalar.struct_access",
  exp.Escape: "scalar.pattern_matching",
  exp.Adjacent: "scalar.range",
  exp.Distance: "scalar.distance",
  exp.Overlaps: "scalar.range",
  exp.ExtendsLeft: "scalar.range",
  exp.ExtendsRight: "scalar.range",
  exp.PropertyEQ: "scalar.struct_access",
  # postgres JSON
  exp.JSONBContainsAllTopKeys: "scalar.json",
  exp.JSONBContainsAnyTopKeys: "scalar.json",
  exp.JSONBDeleteAtPath: "scalar.json",
}

UNARY_SUBCATEGORY: dict[type, str] = {
  exp.Neg: "scalar.arithmetic",
  exp.BitwiseNot: "scalar.bitwise",
  exp.Not: "scalar.logical",
}


ANONYMOUS_NAME_CLASSIFICATION: dict[str, str] = {
  "JSON_ARRAY": "scalar.json",
  "JSON_EXISTS": "scalar.json",
  "JSON_ARRAYAGG": "agg.collection",
  "TO_JSON": "scalar.json",
  "JARO_WINKLER_SIMILARITY": "scalar.string",
  "STRPTIME": "scalar.datetime",
  "REGEXP_MATCHES": "scalar.regex",
  "STRING_SPLIT_REGEX": "scalar.regex",
  "JSON_EXTRACT_STRING": "scalar.json",
  "JSON": "scalar.json",
  "SHA256": "scalar.hash_crypto",
  "CURRENT_SETTING": "scalar.session_system",
}


def _window_fn_ids(window_nodes: list) -> set[int]:
  """Return object IDs of funcs inside a Window OVER."""
  return {id(w.this) for w in window_nodes if w.this is not None}


def _classify_window_function(node: exp.Expression) -> str:
  """Classify a function that appears inside an OVER clause."""
  match node:
    case exp.Rank() | exp.DenseRank() | exp.RowNumber() | exp.Ntile():
      return "window.ranking"
    case (
      exp.Lag()
      | exp.Lead()
      | exp.FirstValue()
      | exp.LastValue()
      | exp.NthValue()
    ):
      return "window.navigation"
    case exp.CumeDist() | exp.PercentRank():
      return "window.distribution"
    case _:
      return "window.aggregate"


def _classify_agg_or_scalar(
  node: exp.Expression,
) -> str:
  """Classify a Func node as agg.* or scalar.*."""
  if isinstance(node, exp.Anonymous | exp.AnonymousAggFunc):
    name_upper = node.name.upper() if hasattr(node, "name") else ""
    if name_upper in ANONYMOUS_NAME_CLASSIFICATION:
      return ANONYMOUS_NAME_CLASSIFICATION[name_upper]
    if isinstance(node, exp.AnonymousAggFunc):
      return "agg.anonymous"
    return "scalar.anonymous"
  if isinstance(node, exp.AggFunc):
    sub = AGG_SUBCATEGORY.get(type(node), "other")
    return f"agg.{sub}"
  sub = SCALAR_SUBCATEGORY.get(type(node), "other")
  return f"scalar.{sub}"


def _classify_operator(node: exp.Expression) -> str | None:
  """Classify Binary/Unary operator nodes. Returns None if not applicable."""
  if isinstance(node, exp.Binary) and not isinstance(node, exp.Connector):
    return BINARY_SUBCATEGORY.get(type(node))
  if isinstance(node, exp.Unary) and not isinstance(node, exp.Paren):
    return UNARY_SUBCATEGORY.get(type(node))
  return None


def _classify_function(
  node: exp.Expression,
  fn_ids: set[int],
) -> str:
  """Return a 'category.subcategory' label for a node.

  Returns 'other' for connectors and unrecognised nodes.
  """
  if isinstance(node, exp.Case | exp.If):
    return (
      "conditional.case" if isinstance(node, exp.Case) else "conditional.if"
    )
  op = _classify_operator(node)
  if op:
    return op
  if not isinstance(node, exp.Func) or isinstance(node, exp.Connector):
    return "other"
  if isinstance(node, exp.UDTF):
    return "table_valued"
  if id(node) in fn_ids:
    return _classify_window_function(node)
  return _classify_agg_or_scalar(node)


def parse_sql_functions(
  sql_text: str,
) -> list[FunctionRecord]:
  """Parse SQL and return classified records per function.

  Returns a list of dicts with category, subcategory,
  expression. Entries classified as 'other' are excluded.
  Returns [] on parse errors.
  """
  try:
    tree = sqlglot.parse_one(sql_text, error_level=sqlglot.ErrorLevel.IGNORE)
    if tree is None:
      return []

    window_nodes = list(tree.find_all(exp.Window))
    fn_ids = _window_fn_ids(window_nodes)

    rows: list[FunctionRecord] = []
    seen: set[int] = set()

    _node_types: tuple[type[exp.Expression], ...] = (
      exp.Func,
      exp.Case,
      exp.If,
      exp.Binary,
      exp.Neg,
      exp.BitwiseNot,
      exp.Not,
    )
    for node in tree.find_all(*_node_types):
      nid = id(node)
      if nid in seen or isinstance(node, exp.Connector):
        continue
      seen.add(nid)

      full = _classify_function(node, fn_ids)
      if full == "other":
        continue

      cat, _, subcat = full.partition(".")
      name = (
        node.name
        if isinstance(node, exp.Anonymous | exp.AnonymousAggFunc)
        else type(node).__name__
      )
      record: FunctionRecord = {
        "category": cat,
        "subcategory": subcat or cat,
        "name": name,
        "expression": node.sql(dialect="duckdb"),
      }
      rows.append(record)
  except Exception:
    logger.warning(
      "Failed to parse SQL functions: %.80s",
      sql_text,
      exc_info=True,
    )
    return []
  else:
    return rows
