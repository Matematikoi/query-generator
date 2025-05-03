import math
from pathlib import Path

import polars as pl

from query_generator.utils.definitions import Dataset


def get_bin_from_value(value: int, upper_bound: int, total_bins: int) -> int:
  bin_size = float(upper_bound) / float(total_bins)
  bin = math.ceil(value / bin_size)
  if bin > total_bins:
    bin = total_bins + 1
  return bin


# TODO: test with the function above
def make_bins_in_csv(
  batch_df: pl.DataFrame, upper_bound: int, total_bins: int
) -> pl.DataFrame:
  bin_size = float(upper_bound) / float(total_bins)
  return batch_df.with_columns(
    (
      (pl.col("count_star") / bin_size)
      .ceil()
      .cast(pl.Int64)
      .clip(upper_bound=total_bins + 1)
    ).alias("bin")
  )


def cherry_pick_binning(
  dataset: Dataset,
  csv: Path,
  queries_per_bin: int,
  upper_bound: int,
  total_bins: int,
  seed: int,
  destination_folder: Path,
) -> None:
  batch_df = pl.read_csv(csv)
  bins_df = make_bins_in_csv(batch_df, upper_bound, total_bins)
  for bin in bins_df["bin"].unique():
    bin_df = bins_df.filter(pl.col("bin") == bin)
    sample_df = bin_df.sample(
      n=min(queries_per_bin, len(bin_df)),
      shuffle=True,
      seed=seed,
      with_replacement=False,
    )
    for path in sample_df.select("relative_path", "prefix").iter_rows():
      new_path = (
        destination_folder
        / f"bin_{bin}"
        / f"{path[1]}_{path[0].split('/')[-1]}"
      )
      old_path = csv.parent / path[0]
      new_path.parent.mkdir(parents=True, exist_ok=True)
      new_path.write_text(old_path.read_text())
