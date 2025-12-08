# Attributes
This endpoint provides metrics about the generated queries once the
traces are collected with the `fix-transform` endpoint

- `input_parquet` (str): The path to the input Parquet file which
was output by the `fix-transform` endpoint. Called duckdb_traces.parquet
- `output_folder` (str): The folder where the metrics will be saved.
