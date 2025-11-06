# Attributes

- database_path (str): The path to the DuckDB database to use for generating histograms.
- output_folder (str): The folder to save the generated histogram parquet
file.
- histogram_size (int): The number of bins to use for the histogram.
Default is 51.
- common_values_size (int): The number of common values to include. 
Default is 10. If the value of the `common_values_size` is 0 then, no
MCV will be calculated. 

# Ouput

The output is a single `.parquet` file that contains statistics for
each column in the input database. If the `common_values_size` was
greater than 0, a column `most_common_values` will include said
information and a histogram `histogram-mcv` will be the histogram
without the `most_common_values`.