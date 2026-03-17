#set page(numbering: "1")
#set text(size: 11pt)
#set par(justify: true)

= Report on DuckDB and Spark Function Compatibility

This report analyzes the function inventory in
`params_config/functions/minimal_example.toml` and answers a practical question:
which examples are already portable to Spark, which ones are only DuckDB-flavored
surface syntax, and which ones are truly DuckDB-specific.

The short conclusion is:

#table(
  columns: (2.2fr, 1fr, 4.8fr),
  inset: 6pt,
  stroke: .4pt,
  [Metric], [Value], [Comment],
  [Examples in the file], [`188`], [Total labeled examples in `minimal_example.toml`.],
  [Fully compatible with Spark], [`134`], [Portable under the current Spark SQL docs baseline.],
  [Shared semantics, different syntax], [`52`], [Spark can express the idea, but not with the DuckDB syntax used here.],
  [DuckDB-specific], [`2`], [The current Spark docs do not expose a documented builtin counterpart of similar directness.],
)

The main insight is that the semantic gap is smaller than the syntax gap. Most of
the non-portable examples are not "Spark cannot do this"; they are "Spark can do
this, but not with DuckDB's spelling, literals, helper functions, or table-valued
syntax."

== General Problem

The current query inventory was assembled from DuckDB-oriented examples. That
creates two portability buckets that need different handling:

#table(
  columns: (1.7fr, 5.3fr),
  inset: 6pt,
  stroke: .4pt,
  [Risk], [Explanation],
  [Shared semantics, different syntax], [Spark supports the operation, but not the DuckDB spelling, literal syntax, helper function, operator, or table-valued form used in the example.],
  [Feature gap], [The example uses functionality for which the current Spark docs do not expose a builtin counterpart of similar directness.],
)

This distinction matters. If the goal is a Spark-compatible query generator, then
"exact same semantics, different spelling" should be handled differently from a
true engine capability gap.

== Taxonomy

This report keeps the taxonomy already used by the extractor. The taxonomy is based
on the `sqlglot` expression class hierarchy, with logical connectors such as
`AND`, `OR`, and `XOR` excluded because they are operators rather than ordinary
function calls.

#table(
  columns: 4,
  inset: 6pt,
  stroke: .4pt,
  [Top-level category], [Detection rule], [Example], [Examples in file],
  [`window.*`], [Function node appears as the `.this` child of a `Window`, i.e. inside `OVER (...)`], [`RANK() OVER (...)`], [14],
  [`agg.*`], [`AggFunc`, but not inside `OVER (...)`], [`COUNT(*)`, `AVG(x)`, `PERCENTILE_CONT(0.5)`], [26],
  [`scalar.*`], [`Func`, excluding aggregate, window, and conditional nodes], [`UPPER(name)`, `ROUND(x, 2)`, `COALESCE(a, b)`], [145],
  [`conditional`], [`Case` or `If`], [`CASE WHEN ... END`, `IF(cond, a, b)`], [2],
  [`table_valued`], [`UDTF` or lateral row generator], [`UNNEST(...)`], [1],
)

The actual subcategory distribution in `minimal_example.toml` is:

#table(
  columns: (2.7fr, .8fr, 2.7fr, .8fr),
  inset: 6pt,
  stroke: .4pt,
  [Subcategory], [Count], [Subcategory], [Count],
  [`window.ranking`], [4], [`scalar.string`], [25],
  [`window.navigation`], [5], [`scalar.datetime`], [22],
  [`window.distribution`], [2], [`scalar.numeric`], [26],
  [`window.aggregate`], [3], [`scalar.null_handling`], [2],
  [`agg.core`], [9], [`scalar.type_conversion`], [5],
  [`agg.statistical`], [10], [`scalar.regex`], [5],
  [`agg.ordered_set`], [5], [`scalar.json`], [8],
  [`agg.collection`], [1], [`scalar.array`], [14],
  [`agg.approximate`], [1], [`scalar.map_struct`], [5],
  [ ], [ ], [`scalar.hash_crypto`], [2],
  [ ], [ ], [`scalar.session_system`], [4],
  [ ], [ ], [`scalar.arithmetic`], [7],
  [ ], [ ], [`scalar.bitwise`], [6],
  [ ], [ ], [`scalar.comparison`], [9],
  [ ], [ ], [`scalar.pattern_matching`], [3],
  [ ], [ ], [`scalar.struct_access`], [1],
  [ ], [ ], [`scalar.logical`], [1],
  [ ], [ ], [`conditional.case`], [1],
  [ ], [ ], [`conditional.if`], [1],
  [ ], [ ], [`table_valued.table_valued`], [1],
)

== Method

The report uses two baselines:

#table(
  columns: (1.6fr, 5.4fr),
  inset: 6pt,
  stroke: .4pt,
  [Baseline], [Definition],
  [DuckDB execution baseline], [Every SQL snippet in the TOML file was executed against DuckDB `1.4.2` using the real TPC-DS database `tmp/database_TPCDS_0.1.duckdb` in read-only mode.],
  [Spark compatibility baseline], [Compatibility was checked against the current official Spark SQL function documentation on `spark.apache.org`, with attention to functions whose availability has changed across releases.],
)

Compatibility is evaluated on the SQL snippet as written, not only on the label.
That matters because a few labels use the nearest working DuckDB spelling for the
intended operation, and the compatibility judgment follows the executable SQL.
Examples:

#table(
  columns: (1.4fr, 5.6fr),
  inset: 6pt,
  stroke: .4pt,
  [Label], [Executable DuckDB formulation],
  [`Soundex`], [`JARO_WINKLER_SIMILARITY(...)`],
  [`DateSub`], [`DATE_ADD(..., INTERVAL '-7 days')`],
  [`ParseJSON`], [`JSON(...)`],
  [`SHA2`], [`SHA256(...)`],
)

== Compatibility Summary

The counts in this section refer to the current `188` examples in the file.

#table(
  columns: 3,
  inset: 6pt,
  stroke: .4pt,
  [Bucket], [Count], [Meaning],
  [Fully compatible], [134], [The example is already compatible with Spark under the current docs baseline, or differs only in harmless type aliases such as `VARCHAR`.],
  [Shared semantics, different syntax], [52], [Spark supports the same idea, but the example uses DuckDB-specific literals, function names, operators, or generator syntax.],
  [DuckDB-specific], [2], [The example uses functionality for which Spark does not expose a documented builtin counterpart of similar directness.],
)

Two observations are more important than the raw counts:

#table(
  columns: (1fr, 6fr),
  inset: 6pt,
  stroke: .4pt,
  [Observation], [Interpretation],
  [`1`], [`54` examples are not Spark-ready as written, but `52` of them are still rewrite candidates rather than feature gaps.],
  [`2`], [Nested-type handling dominates the rewrite work: `14` array examples, `6` map/struct-access examples, and `1` table-valued row-expansion example.],
  [`3`], [The true feature-gap bucket is still small relative to the syntax-rewrite bucket.],
)

== Fully Compatible Functions

These `134` examples are the lowest-risk starting point for a cross-engine query
generator.

#table(
  columns: (2fr, .8fr, 6.2fr),
  inset: 6pt,
  stroke: .4pt,
  [Subcategory], [Count], [Functions],
  [`window.ranking`], [4], [`DenseRank`, `Ntile`, `Rank`, `RowNumber`],
  [`window.navigation`], [5], [`FirstValue`, `Lag`, `LastValue`, `Lead`, `NthValue`],
  [`window.distribution`], [2], [`CumeDist`, `PercentRank`],
  [`window.aggregate`], [3], [`AvgOver`, `CountOver`, `SumOver`],
  [`agg.core`], [7], [`AnyValue`, `Avg`, `Count`, `CountIf`, `Max`, `Min`, `Sum`],
  [`agg.statistical`], [10], [`Corr`, `CovarPop`, `CovarSamp`, `Kurtosis`, `Skewness`, `Stddev`, `StddevPop`, `StddevSamp`, `Variance`, `VariancePop`],
  [`agg.ordered_set`], [4], [`Median`, `Mode`, `PercentileCont`, `PercentileDisc`],
  [`agg.collection`], [1], [`ArrayAgg`],
  [`agg.approximate`], [1], [`ApproxDistinct`],
  [`scalar.string`], [20], [`Ascii`, `Chr`, `Concat`, `ConcatWs`, `Left`, `Length`, `Levenshtein`, `Lower`, `Pad`, `Repeat`, `Replace`, `Reverse`, `Right`, `Space`, `Split`, `SplitPart`, `Substring`, `Translate`, `Trim`, `Upper`],
  [`scalar.datetime`], [16], [`CurrentDate`, `CurrentTime`, `CurrentTimestamp`, `DateTrunc`, `Day`, `DayOfMonth`, `DayOfWeek`, `DayOfYear`, `Extract`, `Hour`, `LastDay`, `Minute`, `Month`, `Quarter`, `Second`, `Year`],
  [`scalar.numeric`], [25], [`Abs`, `Acos`, `Asin`, `Atan`, `Atan2`, `Cbrt`, `Ceil`, `Cos`, `Degrees`, `Exp`, `Floor`, `Greatest`, `IsNan`, `Least`, `Ln`, `Log`, `Pi`, `Pow`, `Radians`, `Rand`, `Round`, `Sign`, `Sin`, `Sqrt`, `Tan`],
  [`scalar.null_handling`], [2], [`Coalesce`, `Nullif`],
  [`scalar.type_conversion`], [4], [`Cast`, `Hex`, `Typeof`, `Unhex`],
  [`scalar.regex`], [2], [`RegexpExtract`, `RegexpReplace`],
  [`scalar.hash_crypto`], [1], [`MD5`],
  [`scalar.session_system`], [2], [`CurrentSchema`, `CurrentUser`],
  [`scalar.arithmetic`], [6], [`Add`, `Div`, `Mod`, `Mul`, `Neg`, `Sub`],
  [`scalar.bitwise`], [6], [`BitwiseAnd`, `BitwiseLeftShift`, `BitwiseNot`, `BitwiseOr`, `BitwiseRightShift`, `BitwiseXor`],
  [`scalar.comparison`], [8], [`EQ`, `GT`, `GTE`, `Is`, `LT`, `LTE`, `NEQ`, `NullSafeNEQ`],
  [`scalar.pattern_matching`], [2], [`ILike`, `Like`],
  [`scalar.logical`], [1], [`Not`],
  [`conditional.case`], [1], [`Case`],
  [`conditional.if`], [1], [`If`],
)

Version note: a few items in this bucket are Spark-version sensitive, especially
`CurrentTime`, `PercentileCont`, `PercentileDisc`, and `Mode`.
They are fully compatible under the current Spark docs baseline, but some older
Spark releases require rewrites or alternative names.

== Shared Semantics, Different Syntax

These `52` examples are the main portability problem. They are rewrite candidates
rather than semantic dead ends.

#table(
  columns: (1.4fr, 2.3fr, 2.4fr, 3fr),
  inset: 6pt,
  stroke: .4pt,
  [Rewrite class], [DuckDB examples], [Spark-side counterpart], [Comment],
  [Ordered aggregates], [`First`, `Last`], [`first_value` / `last_value`, or an order-aware aggregate rewrite], [The DuckDB example uses ordered aggregate syntax inside the function call.],
  [Percentiles], [`Quantile`], [`percentile(...)` or `percentile_approx(...)`], [The semantic intent is the same, but the surface API differs.],
  [Date arithmetic], [`DateAdd`, `DateDiff`, `DateSub`, `Week`], [`dateadd`, `date_diff`, `date_sub`, `weekofyear`], [Naming, argument order, and interval spelling differ across engines.],
  [Date parsing], [`StrToDate`, `StrToTime`], [`to_date(...)`, `to_timestamp(...)`, or casts], [Spark exposes the same intent through different parsing helpers.],
  [String predicates and encoding], [`StartsWith`, `EndsWith`, `Encode`, `Decode`], [`startswith`, `endswith`, `encode(str, charset)`, `decode(bin, charset)`], [Spark requires different spellings and, for encoding, explicit charset arguments.],
  [Soft type conversion], [`TryCast`], [the Spark try-to family, depending on target type], [Spark docs do not expose a generic `try_cast` builtin with DuckDB's spelling.],
  [Regex helpers], [`RegexpCount`, `RegexpLike`, `RegexpSplit`], [`regexp_count(...)`, `regexp_like(...)`, `split(...)`], [Spark supports the same family of operations through different function names.],
  [JSON helpers], [`JSONExtract`, `JSONExtractScalar`, `JSONObject`, `JSONArray`, `JSONExists`, `JSONKeys`, `JSONType`, `ParseJSON`], [`get_json_object(...)`, `parse_json(...)`, `to_json(...)`, `json_object_keys(...)`, or related rewrites], [JSON support exists in both engines, but the constructor and path-function surface differs substantially.],
  [Arrays and lists], [`Array`, `ArraySize`, `ArrayContains`, `ArrayAppend`, `ArrayPrepend`, `ArrayConcat`, `ArraySort`, `ArrayReverse`, `ArrayRemove`, `ArraySlice`, `ArrayFilter`, `ArrayToString`, `ArrayFirst`, `ArrayLast`], [`array(...)`, `array_size`, `array_contains`, `array_append`, `array_prepend`, `concat`, `array_sort`, `reverse`, `filter`, `slice`, `array_join`, `element_at`], [Most of the mismatch comes from DuckDB list literals and `LIST_*` helper names.],
  [Struct and map helpers], [`Struct`, `StructExtract`, `MapKeys`, `MapSize`, `MapContainsKey`, `Dot`], [`named_struct`, `struct`, `map`, `map_keys`, `cardinality`, `element_at`, and dot access], [DuckDB struct and map literal syntax differs from Spark's constructors.],
  [Session/system helpers], [`Uuid`, `CurrentTimezone`], [`uuid()`, `current_timezone()`], [Same intent, different function names.],
  [Hash and crypto], [`SHA2`], [`sha2(expr, bits)`], [DuckDB uses `SHA256(...)` in the executable example while Spark documents the `sha2` family.],
  [Operators and predicates], [`IntDiv`, `NullSafeEQ`, `SimilarTo`], [`DIV`, `<=>`, `RLIKE`], [These are mainly operator-level syntax mismatches.],
  [Table-valued expansion], [`Unnest`], [`explode` or `inline`], [Same row-expansion idea, different table-valued function name.],
)

The most important sub-result in this bucket is that nested types and JSON create
the largest rewrite clusters:

#table(
  columns: (2.2fr, 4.8fr),
  inset: 6pt,
  stroke: .4pt,
  [Concentration area], [Finding],
  [Arrays], [All `14` array examples are rewrite candidates.],
  [JSON], [All `8` JSON examples are rewrite candidates.],
  [Maps, structs, and struct access], [All `6` examples in this area are rewrite candidates.],
  [Table-valued expansion], [The single table-valued example `Unnest` is also a rewrite candidate.],
)

So the portability problem is concentrated rather than evenly spread across the
taxonomy.

== DuckDB-Specific Cases

If "DuckDB-specific" means "the exact spelling is DuckDB-specific," then the list
is long: `LIST_*`, `CURRENT_SETTING(...)`, `GEN_RANDOM_UUID()`, `[1, 2, 3]`,
`{'a': 1}`, `UNNEST`, and `//` are all DuckDB-flavored spellings.

If "DuckDB-specific" means "Spark lacks a documented builtin counterpart with
similar directness," the list is much smaller. Under that semantic definition,
there are two clear examples in the current file.

#table(
  columns: (1.4fr, 2.2fr, 4.4fr),
  inset: 6pt,
  stroke: .4pt,
  [Example label], [DuckDB-side function], [Why it stays DuckDB-specific under the current Spark docs baseline],
  [`Soundex`], [`JARO_WINKLER_SIMILARITY(...)`], [Spark documents `soundex(...)` but does not expose a documented Jaro-Winkler similarity builtin in the current SQL function docs.],
  [`IsInf`], [`ISINF(...)`], [Spark documents `isnan(...)` but not a matching `isinf(...)` builtin.],
)

This is why the report separates "shared semantics, different syntax" from "true
feature gap." Conflating them would overstate the portability problem.

== Recommendations

For the query generator, the practical next step is to treat compatibility as a
three-stage pipeline:

#table(
  columns: (1fr, 6fr),
  inset: 6pt,
  stroke: .4pt,
  [Step], [Recommendation],
  [`1`], [Keep the `134` fully compatible examples as the immediate cross-engine core.],
  [`2`], [Add a rewrite layer for the `52` syntax-different examples, especially arrays, JSON, maps, structs, `UNNEST`, and date helpers.],
  [`3`], [Keep the `2` DuckDB-specific examples behind capability flags or exclude them from engine-neutral prompt seeds.],
)

If the long-term goal is engine-neutral generation, the most useful normalization
rules are:

#table(
  columns: (2fr, 5fr),
  inset: 6pt,
  stroke: .4pt,
  [Normalization area], [Why it matters],
  [Nested-type literals and helpers], [This is the largest cluster of syntax rewrites in the file.],
  [JSON constructors and extractors], [The two engines support similar JSON operations through noticeably different function families.],
  [Date arithmetic and parsing], [Date subtraction, week extraction, and parsing helpers all require dialect-aware rewrites.],
  [Row generators], [`UNNEST` and `explode` express the same operation through different table-valued syntax.],
  [Session/system helpers], [UUID and timezone access are portable concepts but not portable spellings.],
)

== Sources

#table(
  columns: (2.4fr, 4.6fr),
  inset: 6pt,
  stroke: .4pt,
  [Source], [Link],
  [DuckDB list functions], [#link("https://duckdb.org/docs/stable/sql/functions/list")[duckdb.org list docs]],
  [DuckDB struct functions], [#link("https://duckdb.org/docs/stable/sql/functions/struct.html")[duckdb.org struct docs]],
  [DuckDB text functions], [#link("https://duckdb.org/docs/stable/sql/functions/text")[duckdb.org text docs]],
  [DuckDB date functions], [#link("https://duckdb.org/docs/stable/sql/functions/date")[duckdb.org date docs]],
  [DuckDB utility functions], [#link("https://duckdb.org/docs/stable/sql/functions/utility")[duckdb.org utility docs]],
  [DuckDB regular expression functions], [#link("https://duckdb.org/docs/stable/sql/functions/regular_expressions")[duckdb.org regex docs]],
  [DuckDB JSON functions], [#link("https://duckdb.org/docs/stable/data/json/json_functions")[duckdb.org JSON docs]],
  [Spark SQL built-in functions], [#link("https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html")[spark built-in functions]],
  [Spark SQL function API index], [#link("https://spark.apache.org/docs/latest/api/sql/index.html")[spark SQL API index]],
  [Spark SQL operators reference], [#link("https://spark.apache.org/docs/latest/sql-ref-operators.html")[spark operators reference]],
)
