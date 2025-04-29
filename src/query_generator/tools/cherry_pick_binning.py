import os
import random
import shutil

from query_generator.utils.definitions import Dataset


def cherry_pick_binning(
  dataset: Dataset, path: str, queries_per_bin: int
) -> None:
  new_folder = f"data/generated_queries/CHERRY_PICKED/{dataset.value}"
  for folder in os.listdir(path):
    folder_path = os.path.join(path, folder)
    if not os.path.isdir(folder_path):
      continue

    files = [
      f
      for f in os.listdir(folder_path)
      if f.endswith(".sql") and os.path.isfile(os.path.join(folder_path, f))
    ]
    selected_files = random.sample(files, k=min(queries_per_bin, len(files)))

    # Copy files to new folder
    for file in selected_files:
      src_file = os.path.join(folder_path, file)
      dst_folder = os.path.join(new_folder, folder)
      dst_file = os.path.join(dst_folder, file)

      os.makedirs(dst_folder, exist_ok=True)
      shutil.copy(src_file, dst_file)
