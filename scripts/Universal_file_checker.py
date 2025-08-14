import pandas as pd
import numpy as np
import os

# ==============================
# ⚙ CONFIG
# ==============================
FILE_PATH = './data/Original/cleaned/rejected_cleaned.csv'  # change as needed
CHUNK_SIZE = 100_000  # rows per chunk when reading large files
SAMPLE_SIZE = 5       # sample rows for preview

# ==============================
# 📦 File Info
# ==============================
if not os.path.exists(FILE_PATH):
    raise FileNotFoundError(f"File not found: {FILE_PATH}")

file_size_mb = os.path.getsize(FILE_PATH) / (1024 * 1024)
print(f"📂 File: {FILE_PATH}")
print(f"💾 Size: {file_size_mb:.2f} MB\n")

# ==============================
# 🧠 Read in Chunks
# ==============================
total_rows = 0
chunk_count = 0
columns = None
dtypes = {}
null_counts = None
numeric_cols = []
sample_data = []

for chunk in pd.read_csv(FILE_PATH, chunksize=CHUNK_SIZE, low_memory=False):
    chunk_count += 1
    total_rows += len(chunk)

    # Store columns
    if columns is None:
        columns = chunk.columns.tolist()

    # Detect numeric columns only once
    if not numeric_cols:
        numeric_cols = chunk.select_dtypes(include=[np.number]).columns.tolist()

    # Count nulls
    if null_counts is None:
        null_counts = chunk.isnull().sum()
    else:
        null_counts += chunk.isnull().sum()

    # Track dtypes (first occurrence)
    for col in chunk.columns:
        if col not in dtypes:
            dtypes[col] = str(chunk[col].dtype)

    # Collect random samples for preview
    sample_data.append(chunk.sample(min(SAMPLE_SIZE, len(chunk)), random_state=42))

# Combine samples from chunks
sample_df = pd.concat(sample_data).sample(min(SAMPLE_SIZE, len(pd.concat(sample_data))), random_state=42)

# ==============================
# 📊 Display Summary
# ==============================
print(f"📊 Shape of dataset: ({total_rows}, {len(columns)})")
print(f"📄 Columns: {columns}\n")

print("🕳️ Missing Values:")
print(null_counts)

print("\n🧬 Data Types:")
print(dtypes)

print("\n🔍 Data Preview:")
print(sample_df)

# ==============================
# 📈 Numeric Summary (using sample for speed)
# ==============================
if numeric_cols:
    try:
        # Read only numeric cols in chunks to get summary stats without full load
        stats = pd.read_csv(FILE_PATH, usecols=numeric_cols)
        print("\n📊 Numeric Summary:")
        print(stats.describe())
    except MemoryError:
        print("\n⚠️ Numeric summary skipped due to memory limits.")
else:
    print("\nℹ️ No numeric columns detected.")
