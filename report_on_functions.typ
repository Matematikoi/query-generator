#set page(numbering: "1")
#set text(size: 11pt)
#set par(justify: true)

= Report on DuckDB and Spark Function Compatibility

This report analyzes the function inventory in
`params_config/functions/minimal_example.toml` and answers a practical question:
which examples are already portable to Spark, which ones are only DuckDB-flavored
surface syntax, which ones are truly DuckDB-specific, and which ones are simply
wrong for current DuckDB.

The short conclusion is:

#table(
  columns: (2.2fr, 1fr, 4.8fr),
  inset: 6pt,
  stroke: .4pt,
  [Metric], [Value], [Comment],
  [Examples in the file], [`193`], [Total labeled examples in `minimal_example.toml`.],
  [DuckDB-valid examples], [`182`], [Executed successfully in DuckDB `1.4.2` against the real TPC-DS schema in `tmp/database_TPCDS_0.1.duckdb`.],
  [Invalid in DuckDB], [`11`], [These examples do not run as written and should be fixed before portability analysis.],
  [Fully compatible with Spark], [`137`], [Portable under the current Spark SQL docs baseline.],
  [Shared semantics, different syntax], [`44`], [Spark can express the idea, but not with the DuckDB syntax used here.],
  [DuckDB-specific], [`1`], [Semantic feature gap rather than syntax mismatch.],
)

The main insight is that the semantic gap is smaller than the syntax gap. Most of
the non-portable examples are not "Spark cannot do this"; they are "Spark can do
this, but not with DuckDB's spelling, literals, helper functions, or table-valued
syntax."

== General Problem

The current query inventory was assembled from DuckDB-oriented examples. That
creates two distinct portability risks:

#table(
  columns: (1.7fr, 5.3fr),
  inset: 6pt,
  stroke: .4pt,
  [Risk], [Explanation],
  [DuckDB feature, Spark-incompatible syntax], [The example uses a real DuckDB feature, but Spark does not accept the DuckDB spelling, literal syntax, helper function, or table-valued form as written.],
  [Invalid DuckDB example], [The example does not match current DuckDB builtins or signatures, because the name was borrowed from another dialect or the label was treated as if it were a real builtin.],
)

This distinction matters. If the goal is a Spark-compatible query generator, then
"exact same semantics, different spelling" should be handled differently from
"feature does not exist in Spark" and differently again from "example is invalid in
DuckDB and should be fixed first."

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
  [`agg.*`], [`AggFunc`, but not inside `OVER (...)`], [`COUNT(*)`, `AVG(x)`, `PERCENTILE_CONT(0.5)`], [29],
  [`scalar.*`], [`Func`, excluding aggregate, window, and conditional nodes], [`UPPER(name)`, `ROUND(x, 2)`, `COALESCE(a, b)`], [147],
  [`conditional`], [`Case` or `If`], [`CASE WHEN ... END`, `IF(cond, a, b)`], [2],
  [`table_valued`], [`UDTF` or lateral row generator], [`UNNEST(...)`], [1],
)

The actual subcategory distribution in `minimal_example.toml` is:

#table(
  columns: (2.7fr, .8fr, 2.7fr, .8fr),
  inset: 6pt,
  stroke: .4pt,
  [Subcategory], [Count], [Subcategory], [Count],
  [`window.ranking`], [4], [`scalar.string`], [27],
  [`window.navigation`], [5], [`scalar.datetime`], [22],
  [`window.distribution`], [2], [`scalar.numeric`], [26],
  [`window.aggregate`], [3], [`scalar.null_handling`], [2],
  [`agg.core`], [9], [`scalar.type_conversion`], [5],
  [`agg.statistical`], [10], [`scalar.regex`], [5],
  [`agg.ordered_set`], [5], [`scalar.json`], [8],
  [`agg.collection`], [4], [`scalar.array`], [14],
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
That matters because some labels in the file are conceptual shortcuts rather than
literal function calls. Examples:

#table(
  columns: (1.4fr, 5.6fr),
  inset: 6pt,
  stroke: .4pt,
  [Label], [How the example is actually implemented],
  [`Initcap`], [`UPPER`, `SUBSTR`, and `LOWER`.],
  [`Overlay`], [Concatenation and substringing rather than `OVERLAY(...)`.],
  [`RegexpCount`], [`LEN(REGEXP_EXTRACT_ALL(...))`.],
  [`Space`], [`REPEAT(' ', 5)`.],
)

== Compatibility Summary

#table(
  columns: 3,
  inset: 6pt,
  stroke: .4pt,
  [Bucket], [Count], [Meaning],
  [Fully compatible], [137], [The example is already compatible with Spark under the current docs baseline, or differs only in harmless type aliases such as `VARCHAR`.],
  [Shared semantics, different syntax], [44], [Spark supports the same idea, but the example uses DuckDB-specific literals, function names, operators, or generator syntax.],
  [DuckDB-specific], [1], [The example uses functionality for which Spark does not expose a documented builtin counterpart of similar directness.],
  [Wrong in DuckDB], [11], [The example does not run in current DuckDB and should be corrected before any cross-engine comparison.],
)

Two observations are more important than the raw counts:

#table(
  columns: (1fr, 6fr),
  inset: 6pt,
  stroke: .4pt,
  [Observation], [Interpretation],
  [`1`], [`45` examples are not Spark-ready as written, but `44` of those are still conceptually available in Spark.],
  [`2`], [The true feature-gap bucket is tiny. Most of the friction comes from syntax rather than capability.],
)

== Fully Compatible Functions

These `137` examples are the lowest-risk starting point for a cross-engine query
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
  [`agg.collection`], [2], [`ArrayAgg`, `GroupConcat`],
  [`agg.approximate`], [1], [`ApproxDistinct`],
  [`scalar.string`], [22], [`Ascii`, `Chr`, `Concat`, `ConcatWs`, `Initcap`, `Left`, `Length`, `Levenshtein`, `Lower`, `Overlay`, `Pad`, `Repeat`, `Replace`, `Reverse`, `Right`, `Space`, `Split`, `SplitPart`, `Substring`, `Translate`, `Trim`, `Upper`],
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
`CurrentTime`, `GroupConcat`, `PercentileCont`, `PercentileDisc`, and `Mode`.
They are fully compatible under the current Spark docs baseline, but some older
Spark releases require rewrites or alternative names.

== Shared Semantics, Different Syntax

These `44` entries are the main portability problem. They are not semantic dead
ends; they mostly need a dialect-aware rewrite layer.

#table(
  columns: (1.4fr, 2.3fr, 2.4fr, 3fr),
  inset: 6pt,
  stroke: .4pt,
  [Rewrite class], [DuckDB examples], [Spark-side counterpart], [Comment],
  [Ordered aggregates], [`First`, `Last`], [`first_value` / `last_value`, or an order-aware aggregate rewrite], [The DuckDB example uses ordered aggregate syntax inside the function call.],
  [Percentiles], [`Quantile`], [`percentile(...)` or `percentile_approx(...)`], [The semantic intent is the same, but the surface API differs.],
  [Date helpers], [`DateAdd`, `DateDiff`, `Week`], [`dateadd`, `date_diff`, `weekofyear`], [Naming and argument order differ across engines.],
  [String predicates and encoding], [`StartsWith`, `EndsWith`, `Encode`, `Decode`], [`startswith`, `endswith`, `encode(str, charset)`, `decode(bin, charset)`], [Spark requires different spellings and, for encoding, explicit charset arguments.],
  [Soft type conversion], [`TryCast`], [the Spark try-to family, depending on target type], [Spark docs do not expose a generic `try_cast` builtin with DuckDB's spelling.],
  [Regex counting], [`RegexpCount`], [`regexp_count(...)`], [The DuckDB example computes the count indirectly via extraction.],
  [JSON helpers], [`JSONExtract`, `JSONObject`, `JSONArray`, `JSONExists`, `JSONKeys`, `JSONType`], [`get_json_object`, `to_json(...)` , `json_object_keys(...)`, null checks, or `typeof(parse_json(...))`], [JSON support exists in both engines, but the constructor and path-function surface differs substantially.],
  [Arrays and lists], [`Array`, `ArraySize`, `ArrayContains`, `ArrayAppend`, `ArrayPrepend`, `ArrayConcat`, `ArraySort`, `ArrayReverse`, `ArrayRemove`, `ArraySlice`, `ArrayFilter`, `ArrayToString`, `ArrayFirst`, `ArrayLast`], [`array(...)`, `array_size`, `array_contains`, `array_append`, `array_prepend`, `concat`, `array_sort`, `reverse`, `filter`, `slice`, `array_join`, `element_at`], [Most of the mismatch comes from DuckDB list literals and `LIST_*` helper names.],
  [Struct and map helpers], [`Struct`, `StructExtract`, `MapKeys`, `MapSize`, `MapContainsKey`, `Dot`], [`named_struct`, `struct`, `map`, `map_keys`, `cardinality`, `element_at`, and dot access], [DuckDB struct and map literal syntax differs from Spark's constructors.],
  [Session/system helpers], [`Uuid`, `CurrentTimezone`], [`uuid()`, `current_timezone()`], [Same intent, different function names.],
  [Operators and predicates], [`IntDiv`, `NullSafeEQ`, `SimilarTo`], [`DIV`, `<=>`, `RLIKE`], [These are mainly operator-level syntax mismatches.],
  [Table-valued expansion], [`Unnest`], [`explode` or `inline`], [Same row-expansion idea, different table-valued function name.],
)

The most important sub-result in this bucket is that nested-type handling is the
largest dialect gap:

#table(
  columns: (2.2fr, 4.8fr),
  inset: 6pt,
  stroke: .4pt,
  [Concentration area], [Finding],
  [Arrays], [All `14` array examples are rewrite candidates.],
  [Maps and structs], [All `5` map/struct examples are rewrite candidates.],
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
there is only one clear example in this file.

#table(
  columns: (1.8fr, 5.2fr),
  inset: 6pt,
  stroke: .4pt,
  [Interpretation], [Result],
  [Syntactic DuckDB-specificity], [Many examples match this looser definition, including `LIST_*`, `CURRENT_SETTING(...)`, `GEN_RANDOM_UUID()`, literal list and struct syntax, `UNNEST`, and `//`.],
  [Semantic DuckDB-specificity], [Only `IsInf` is a clear example in this file. DuckDB exposes `ISINF(...)`, while current Spark docs expose `isnan(...)` but not a matching `isinf(...)` builtin.],
)

This is why the report separates "shared semantics, different syntax" from "true
feature gap." Conflating them would overstate the portability problem.

== Wrong in DuckDB

These `11` entries are invalid in current DuckDB and should be fixed in the source
file before they are used as canonical examples.

#table(
  columns: (1.3fr, 2fr, 2.2fr, 2.1fr),
  inset: 6pt,
  stroke: .4pt,
  [Example], [DuckDB 1.4.2 result], [DuckDB-compatible replacement], [Spark note],
  [`ArrayConcatAgg`], [`array_concat_agg` does not exist], [
    `flatten(array_agg(arr))`
    #linebreak()
    or
    #linebreak()
    `list_reduce(array_agg(arr), ...)`
  ], [
    `flatten(collect_list(arr))`
    #linebreak()
    is a likely Spark rewrite.
  ],
  [`JSONArrayAgg`], [`json_arrayagg` does not exist], [`to_json(array_agg(x))`], [`to_json(collect_list(x))` or a similar rewrite.],
  [`Soundex`], [`soundex` does not exist], [No builtin DuckDB replacement in the tested baseline.], [Spark documents `soundex(...)`.],
  [`DateSub`], [Wrong `date_sub` signature for this call shape], [`d_date - INTERVAL 7 DAY`], [Spark uses `date_sub(d_date, 7)`.],
  [`StrToDate`], [`str_to_date` does not exist], [`strptime(...)::DATE`], [Spark uses `to_date(...)`.],
  [`StrToTime`], [`str_to_time` does not exist], [`CAST(strptime(...) AS TIME)`], [Spark time parsing is also version-sensitive.],
  [`RegexpLike`], [`regexp_like` does not exist], [`regexp_matches(...)`], [Spark documents `regexp_like(...)`.],
  [`RegexpSplit`], [`regexp_split` does not exist], [`string_split_regex(...)`], [Spark typically uses `split(...)`.],
  [`JSONExtractScalar`], [`json_extract_scalar` does not exist], [
    `json_extract_string(...)`
    #linebreak()
    or
    #linebreak()
    `->>`
  ], [Spark typically uses `get_json_object(...)`.],
  [`ParseJSON`], [`parse_json` does not exist], [`json(...)`], [Recent Spark docs do document `parse_json(...)`.],
  [`SHA2`], [`sha2` does not exist], [`sha256(...)`], [Spark documents `sha2(expr, bits)`.],
)

This section is important because it shows that the TOML file is not only a
DuckDB-vs-Spark issue. It also contains a small amount of intra-DuckDB cleanup
work.

== Recommendations

For the query generator, the practical next step is to treat compatibility as a
four-stage pipeline:

#table(
  columns: (1fr, 6fr),
  inset: 6pt,
  stroke: .4pt,
  [Step], [Recommendation],
  [`1`], [Keep the `137` fully compatible examples as the immediate cross-engine core.],
  [`2`], [Add a rewrite layer for the `44` syntax-different examples, especially arrays, maps, structs, `UNNEST`, and date arithmetic.],
  [`3`], [Keep `IsInf` behind an engine capability flag.],
  [`4`], [Fix the `11` invalid DuckDB examples before using them as seed prompts, regression fixtures, or documentation examples.],
)

If the long-term goal is engine-neutral generation, the most useful normalization
rules are:

#table(
  columns: (2fr, 5fr),
  inset: 6pt,
  stroke: .4pt,
  [Normalization area], [Why it matters],
  [Nested-type literals and helpers], [This is the largest cluster of syntax rewrites in the file.],
  [Row generators], [`UNNEST` and `explode` express the same operation through different table-valued syntax.],
  [Date arithmetic and null-safe equality], [These mismatches recur in common analytic queries and should be normalized centrally.],
  [JSON constructors and extractors], [The two engines support similar operations through noticeably different function families.],
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
