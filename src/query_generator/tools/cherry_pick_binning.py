from dataclasses import dataclass
from pathlib import Path

import polars as pl


def make_bins_in_csv(
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


@dataclass
class CherryPickParameters:
  csv_path: Path
  queries_per_bin: int
  upper_bound: int
  total_bins: int
  destination_folder: Path
  seed: int


def cherry_pick_binning(
  params: CherryPickParameters,
) -> None:
  batch_df = pl.read_csv(params.csv_path)
  bins_df = make_bins_in_csv(batch_df, params.upper_bound, params.total_bins)
  for bin in bins_df["bin"].unique():
    bin_df = bins_df.filter(pl.col("bin") == bin)
    sample_df = bin_df.sample(
      n=min(params.queries_per_bin, len(bin_df)),
      shuffle=True,
      seed=params.seed,
      with_replacement=False,
    )
    for path in sample_df.select("relative_path", "prefix").iter_rows():
      new_path = (
        params.destination_folder
        / f"bin_{bin}"
        / f"{path[1]}_{path[0].split('/')[-1]}"
      )
      old_path = params.csv_path.parent / path[0]
      new_path.parent.mkdir(parents=True, exist_ok=True)
      new_path.write_text(old_path.read_text())
