import logging
from pathlib import Path

import polars as pl

from query_generator.utils.params import (
  FilterEndpoint,
  StratifiedSamplingBase,
  get_toml_from_params,
)

logger = logging.getLogger(__name__)


def make_bins(
  batch_df: pl.DataFrame,
  upper_bound: int,
  total_bins: int,
) -> pl.DataFrame:
  bin_size = float(upper_bound) / float(total_bins)
  return batch_df.with_columns(
    (
      (pl.col("count_star") / bin_size)
      .ceil()
      .cast(pl.Int64)
      .clip(upper_bound=total_bins + 1)
    ).alias("bin"),
  )


def cherry_pick_filter(
  df_input: pl.DataFrame,
  params: StratifiedSamplingBase | None,
  *,
  apply_filter: bool,
) -> pl.DataFrame:
  logger.debug("Applying Cherry Pick Filter")
  if not apply_filter:
    return df_input
  assert params is not None, "params must be provided if apply_filter is True"
  dfs_sampled_array: list[pl.DataFrame] = []
  bins_df = make_bins(df_input, params.upper_bound, params.total_bins)
  for bin in bins_df["bin"].unique():
    bin_df = bins_df.filter(pl.col("bin") == bin)
    sample_df = bin_df.sample(
      n=min(params.queries_per_bin, len(bin_df)),
      shuffle=True,
      seed=params.seed,
      with_replacement=False,
    ).with_columns(pl.lit(bin).alias("bin"))
    dfs_sampled_array.append(sample_df)
  return pl.concat(dfs_sampled_array)


def emtpy_set_filter(
  df_input: pl.DataFrame, *, apply_filter: bool
) -> pl.DataFrame:
  logger.debug("Applying Empty Set Filter.")
  if not apply_filter:
    return df_input
  return df_input.filter(pl.col("count_star") > 0)


def filter_dataframe(
  df_input: pl.DataFrame, params: FilterEndpoint
) -> pl.DataFrame:
  return df_input.pipe(emtpy_set_filter, apply_filter=params.empty_set).pipe(
    cherry_pick_filter,
    params=params.stratified_sampling_config,
    apply_filter=params.stratified_sampling,
  )


def filter_synthetic_queries(params: FilterEndpoint) -> None:
  df_input = pl.read_parquet(params.input_parquet)
  df_filtered = filter_dataframe(df_input, params).rename(
    {"relative_path": "old_path"}
  )
  new_paths = []
  for row in df_filtered.iter_rows(named=True):
    new_path = (
      Path(params.destination_folder)
      / f"{row['subgraph_signature']}"
      / f"{row['fact_table']}_{row['template_number']}_"
      f"{row['predicate_number']}_{row['batch_number']}.sql"
    )
    old_path = Path(params.input_parquet).parent / row["old_path"]
    new_path.parent.mkdir(parents=True, exist_ok=True)
    new_path.write_text(old_path.read_text())
    new_paths.append(str(new_path.relative_to(params.destination_folder)))

  # Write parquet and params
  logger.info(f"Filtered queries from {len(df_input)} to {len(df_filtered)}.")
  df_filtered = df_filtered.with_columns(pl.Series("relative_path", new_paths))
  df_filtered_output = Path(params.destination_folder) / "filtered.parquet"
  df_filtered.write_parquet(df_filtered_output)
  params_toml = get_toml_from_params(params)
  (Path(params.destination_folder) / "filter_params.toml").write_text(
    params_toml, encoding="utf-8"
  )
