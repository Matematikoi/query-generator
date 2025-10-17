



# Attributes
- output_folder (str): The folder to save the generated histogram parquet
    file.
- database_path (str): The path to the DuckDB database to use for generating
    histograms.
- histogram_size (int): The number of bins to use for the histogram.
    Default is 51.
- common_values_size (int): The number of common values to include. 
Default is 10. If the value of the `common_values_size` is 0 then, no
MCV will be calculated. 