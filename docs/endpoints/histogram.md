# Attributes

- database_path (str): The path to the DuckDB database to use for generating histograms.
- output_folder (str): The folder to save the generated histogram parquet file.
- histogram_size (int): The number of bins to use for the histogram. Default is 51.
- common_values_size (int): The number of common values to include. Default is 10.
If the value of `common_values_size` is 0 then no MCV will be calculated.
- sample_size (int): Maximum sampled rows per table used by histogram-related queries.
Effective sample size is `min(sample_size, table_size)`. Default is 100000.
- support_probability_threshold_for_substrings (float): Minimum occurrence probability
for a substring to be considered as a LIKE predicate candidate. A substring must appear
in at least `threshold * sample_size` rows to be included. Default is 0.05.
- max_substrings_per_length (int): Maximum number of substring candidates to keep per
substring length. Among candidates of the same length, those with the highest support
are prioritised. Default is 100.

# Output

The output is a single `.parquet` file that contains statistics for each column in the
input database.

- If `common_values_size > 0`, a column `most_common_values` will include said
information and `histogram-mcv` will be the histogram computed without those values.
- For string columns, a column `common_substrings` contains a list of LIKE predicate
candidates. Each entry is a struct with:
  - `substring` (str): the candidate substring.
  - `support` (int): number of sampled rows containing the substring.
  - `support_probability` (float): `support / sample_size`.

  Candidates are filtered to ensure that no longer substring has strictly higher support
  than a shorter one (length-support monotonicity).
