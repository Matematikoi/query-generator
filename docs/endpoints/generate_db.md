# Attributes

We only need three parameters for the generation of the dataset.
- dataset (Dataset): The dataset to be used (TPCDS, TPCH).
- db_path (str): The path where the DuckDB database will be stored.
- scale_factor (int | float): The scale factor for the dataset.
- parquet_path (str): Optional path for saving the database in Parquet format.

Currently only TPC-DS and TPC-H are available for data generation.

