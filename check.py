import pandas as pd
from pathlib import Path

# ==== CONFIG ====
DATA_DIRS = [
    Path("data/Original/cleaned"),     # for original cleaned files
    Path("data/sample"),  # for sample files
]

FILES = [
    "accepted_cleaned.csv",
    "rejected_cleaned.csv",
    "accepted_cleaned_sample.csv",
    "rejected_cleaned_sample.csv"
]

SAMPLE_SIZE = 5  # rows to preview

def check_file(file_path):
    print(f"\n{'='*80}")
    print(f"📂 Checking file: {file_path}")
    
    if not file_path.exists():
        print("❌ File missing!")
        return
    
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return
    
    print(f"✅ Loaded successfully: {df.shape[0]:,} rows × {df.shape[1]:,} columns")

    print("\n🔹 Columns:")
    print(df.columns.tolist())

    print("\n🔹 Data Types Overview:")
    print(df.dtypes.value_counts())

    print("\n🔹 Top 10 Columns by Missing Values:")
    print(df.isnull().sum().sort_values(ascending=False).head(10))

    print(f"\n🔹 Random {min(SAMPLE_SIZE, len(df))} Rows:")
    print(df.sample(min(SAMPLE_SIZE, len(df))).to_string(index=False))

if __name__ == "__main__":
    for data_dir in DATA_DIRS:
        for file_name in FILES:
            file_path = data_dir / file_name
            check_file(file_path)
