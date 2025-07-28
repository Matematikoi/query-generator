import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import polars as pl


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


@dataclass
class CherryPickParameters:
  parquet_path: Path
  queries_per_bin: int
  upper_bound: int
  total_bins: int
  destination_folder: Path
  seed: int


def cherry_pick_binning(
  params: CherryPickParameters,
) -> None:
  batch_df = pl.read_parquet(params.parquet_path)
  dfs_sampled_array: list[pl.DataFrame] = []
  bins_df = make_bins(batch_df, params.upper_bound, params.total_bins)
  for bin in bins_df["bin"].unique():
    bin_df = bins_df.filter(pl.col("bin") == bin)
    sample_df = bin_df.sample(
      n=min(params.queries_per_bin, len(bin_df)),
      shuffle=True,
      seed=params.seed,
      with_replacement=False,
    )
    dfs_sampled_array.append(sample_df)
    for relative_path in sample_df.select("relative_path").iter_rows():
      new_path = (
        params.destination_folder
        / f"bin_{bin}"
        / f"{relative_path[0].replace('/', '_')}"
      )
      old_path = params.parquet_path.parent / relative_path[0]
      new_path.parent.mkdir(parents=True, exist_ok=True)
      new_path.write_text(old_path.read_text())

  pl.concat(dfs_sampled_array).write_parquet(
    params.destination_folder / "cherry_picked.parquet"
  )


def filter_null_and_format_job(
  csv_path: Path,
  destination_path: Path,
) -> None:
  count_star_df = pl.read_parquet(csv_path).filter(pl.col("count_star") > 0)
  unique_joins_df = count_star_df.unique("subgraph_signature")
  query_dict: dict[int, str] = {}
  cnt = 0
  for row in unique_joins_df.sort("subgraph_signature").iter_rows(named=True):
    unique_join_df = count_star_df.filter(
      pl.col("subgraph_signature") == row["subgraph_signature"]
    )
    for query_row in unique_join_df.sort("relative_path").iter_rows(named=True):
      cnt += 1
      new_path = destination_path / f"snowflake_{cnt}.sql"
      old_path = csv_path.parent / query_row["relative_path"]
      query_dict[cnt] = old_path.read_text()
      new_path.parent.mkdir(parents=True, exist_ok=True)
      new_path.write_text(old_path.read_text())
  query_dict_path = destination_path / "queries.json"
  query_dict_path.write_text(json.dumps(query_dict, indent=2))


def filter_null_and_format_tpcds(
  parquet_path: Path,
  destination_path: Path,
) -> None:
  count_star_df = pl.read_parquet(
    parquet_path,
  ).filter(
    (pl.col("count_star") > 0)
    & (
      (
        pl.col("predicates_range")
        + pl.col("predicates_in_values")
        + pl.col("predicates_equality")
      )
      > 1
    )
  )
  template_dict: dict[str, int] = {}
  query_id_dict: dict[str, int] = {}
  unique_joins_df = count_star_df.unique("subgraph_signature")
  query_dict: dict[str, dict[str, str]] = defaultdict(dict)
  for cnt, row in enumerate(
    unique_joins_df.sort("subgraph_signature").iter_rows(named=True)
  ):
    unique_join_df = count_star_df.filter(
      pl.col("subgraph_signature") == row["subgraph_signature"]
    )
    for idx, query_row in enumerate(
      unique_join_df.sort("relative_path").iter_rows(named=True)
    ):
      template_dict[query_row["relative_path"]] = cnt
      query_id_dict[query_row["relative_path"]] = idx
      new_path = destination_path / f"{cnt}/{cnt}-{idx}.sql"
      old_path = parquet_path.parent / query_row["relative_path"]
      query_dict[f"{cnt}"][str(idx)] = old_path.read_text()
      new_path.parent.mkdir(parents=True, exist_ok=True)
      new_path.write_text(old_path.read_text())
  query_dict_path = destination_path / "queries.json"
  query_dict_path.write_text(json.dumps(query_dict, indent=2))

  count_star_df = count_star_df.with_columns(
    pl.col("relative_path").replace(template_dict).alias("template_id"),
    pl.col("relative_path").replace(query_id_dict).alias("query_id"),
  )
  count_star_df.write_parquet(destination_path / "count_star.parquet")
