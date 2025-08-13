# check.py
import pandas as pd
import os
from pathlib import Path
from collections import Counter
from tqdm import tqdm
import numpy as np

# ==== CONFIG ====
DATASETS = [
    # (full_file, sample_file)
    (
        Path("data/Original/cleaned/accepted_cleaned.csv"),
        Path("data/sample/accepted_cleaned_sample.csv")
    ),
    (
        Path("data/Original/cleaned/rejected_cleaned.csv"),
        Path("data/sample/rejected_cleaned_sample.csv")
    )
]

CHUNKSIZE = 200_000  # adjust based on memory

# ==== FUNCTIONS ====

def file_info(file_path):
    """Return file size in MB and first few lines."""
    size_mb = os.path.getsize(file_path) / (1024 ** 2)
    return round(size_mb, 2)

def analyze_file(file_path, chunksize=CHUNKSIZE):
    """Memory-safe analysis of a large CSV."""
    print(f"\n📂 Analyzing: {file_path}")
    print(f"File size: {file_info(file_path)} MB")

    total_rows = 0
    col_names = None
    dtype_counts = Counter()
    missing_counts = Counter()
    numeric_stats = {}
    categorical_values = {}
    
    for chunk in pd.read_csv(file_path, chunksize=chunksize, low_memory=False):
        if col_names is None:
            col_names = list(chunk.columns)
            print(f"Columns ({len(col_names)}): {col_names}")
            # Prepare stats containers
            numeric_stats = {
                col: {
                    "count": 0, "sum": 0.0, "sum_sq": 0.0,
                    "min": float("inf"), "max": float("-inf")
                }
                for col in chunk.select_dtypes(include=[np.number]).columns
            }
            categorical_values = {
                col: Counter()
                for col in chunk.select_dtypes(exclude=[np.number]).columns
            }

        total_rows += len(chunk)

        # Data types
        for col, dtype in chunk.dtypes.items():
            dtype_counts[str(dtype)] += 1

        # Missing counts
        for col in col_names:
            missing_counts[col] += chunk[col].isna().sum()

        # Numeric stats
        for col in numeric_stats:
            vals = chunk[col].dropna().to_numpy()
            if len(vals) > 0:
                numeric_stats[col]["count"] += len(vals)
                numeric_stats[col]["sum"] += vals.sum()
                numeric_stats[col]["sum_sq"] += (vals ** 2).sum()
                numeric_stats[col]["min"] = min(numeric_stats[col]["min"], vals.min())
                numeric_stats[col]["max"] = max(numeric_stats[col]["max"], vals.max())

        # Categorical value counts
        for col in categorical_values:
            top_vals = chunk[col].dropna().astype(str)
            categorical_values[col].update(top_vals)

    print(f"Total rows: {total_rows:,}")

    # Missing values summary
    print("\n🔍 Missing values per column:")
    for col in col_names:
        miss = missing_counts[col]
        pct = (miss / total_rows) * 100
        print(f"  {col}: {miss:,} missing ({pct:.2f}%)")

    # Numeric stats summary
    print("\n📊 Numeric column stats:")
    for col, stats in numeric_stats.items():
        if stats["count"] > 0:
            mean = stats["sum"] / stats["count"]
            variance = (stats["sum_sq"] / stats["count"]) - (mean ** 2)
            std_dev = variance ** 0.5 if variance > 0 else 0
            print(f"  {col}: min={stats['min']}, max={stats['max']}, mean={mean:.2f}, std={std_dev:.2f}")

    # Categorical top values
    print("\n🏷 Top values for categorical columns:")
    for col, counter in categorical_values.items():
        top5 = counter.most_common(5)
        print(f"  {col}: {top5}")

def compare_full_and_sample(full_path, sample_path):
    """Compare full dataset and sample dataset."""
    if not full_path.exists() or not sample_path.exists():
        print(f"❌ Missing file: {full_path} or {sample_path}")
        return
    
    print(f"\n🔄 Comparing {full_path.name} vs {sample_path.name}")
    full_cols = pd.read_csv(full_path, nrows=1).columns.tolist()
    sample_cols = pd.read_csv(sample_path, nrows=1).columns.tolist()

    if full_cols != sample_cols:
        print("⚠️ Column mismatch!")
        print(f"Full:   {full_cols}")
        print(f"Sample: {sample_cols}")
    else:
        print("✅ Columns match")

    # Compare distribution of first numeric & categorical column
    full_sample = pd.read_csv(full_path, nrows=200_000)
    sample_sample = pd.read_csv(sample_path, nrows=200_000)

    num_cols = full_sample.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = full_sample.select_dtypes(exclude=[np.number]).columns.tolist()

    if num_cols:
        col = num_cols[0]
        print(f"\nNumeric distribution check for '{col}':")
        print(f"  Full:   mean={full_sample[col].mean():.2f}, std={full_sample[col].std():.2f}")
        print(f"  Sample: mean={sample_sample[col].mean():.2f}, std={sample_sample[col].std():.2f}")

    if cat_cols:
        col = cat_cols[0]
        print(f"\nCategorical top-5 check for '{col}':")
        print(f"  Full:   {full_sample[col].value_counts().head(5).to_dict()}")
        print(f"  Sample: {sample_sample[col].value_counts().head(5).to_dict()}")

if __name__ == "__main__":
    for full, sample in DATASETS:
        analyze_file(full)
        analyze_file(sample)
        compare_full_and_sample(full, sample)
