import pandas as pd
from pathlib import Path
from tqdm import tqdm
import os
import warnings

# Suppress SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)

# ==== CONFIG ====
# DATA_FILES = [
#     Path("data/raw/Orig_accepted_2007-18.csv"),
#     Path("data/raw/Orig_rejected_2007-18.csv"),
# ]

DATA_FILES = [
    Path("data/sample/sample_accepted.csv"),
    Path("data/sample/sample_rejected.csv"),
]

CHUNKSIZE = 50_000


def explore_and_clean(file_path, chunksize=CHUNKSIZE):
    file_path = Path(file_path)
    temp_file = file_path.parent / f"_temp_{file_path.name}"

    print(f"\n📂 File: {file_path.name}")
    total_rows = sum(1 for _ in open(file_path, encoding="utf-8")) - 1
    print(f"🔹 Total rows: {total_rows:,}")
    print(f"🚀 Cleaning in chunks of {chunksize:,} rows...\n")

    if temp_file.exists():
        temp_file.unlink()

    chunk_iter = pd.read_csv(file_path, chunksize=chunksize, low_memory=False)

    with tqdm(total=total_rows, unit="rows", ncols=100, bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} rows ({percentage:3.0f}%)") as pbar:
        for i, chunk in enumerate(chunk_iter):
            # Drop columns that are entirely NaN
            chunk = chunk.dropna(axis=1, how="all")

            # Fill numeric columns
            for col in chunk.select_dtypes(include="number"):
                chunk.loc[:, col] = chunk[col].fillna(0)

            # Fill string/object columns
            for col in chunk.select_dtypes(include="object"):
                chunk.loc[:, col] = chunk[col].fillna("Unknown")

            # Write chunk to temp file
            if i == 0:
                chunk.to_csv(temp_file, index=False, mode="w")
            else:
                chunk.to_csv(temp_file, index=False, header=False, mode="a")

            pbar.update(len(chunk))

    os.replace(temp_file, file_path)
    print(f"✅ Done: {file_path.name}")


if __name__ == "__main__":
    for file_path in DATA_FILES:
        if file_path.exists():
            explore_and_clean(file_path)
        else:
            print(f"⚠️ Missing file: {file_path}")
