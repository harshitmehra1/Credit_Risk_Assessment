import pandas as pd
import os

RAW_DIR = os.path.join("data", "Original", "cleaned")
SAMPLE_DIR = os.path.join("data", "sample")
os.makedirs(SAMPLE_DIR, exist_ok=True)

files = [
    "accepted_cleaned.csv",
    "rejected_cleaned.csv"
]

TARGET_SIZE_MB = 50  # target sample size in MB

def create_random_sample(input_file, output_file, target_mb):
    file_size_mb = os.path.getsize(input_file) / (1024 ** 2)
    fraction = min(1.0, target_mb / file_size_mb)  # fraction of rows to sample

    print(f"\n📄 {os.path.basename(input_file)} — taking ~{fraction:.2%} of rows to reach {target_mb} MB")

    reader = pd.read_csv(input_file, chunksize=200_000, low_memory=False)
    sampled_chunks = []
    for chunk in reader:
        sampled_chunks.append(chunk.sample(frac=fraction, random_state=42))
    sample_df = pd.concat(sampled_chunks)

    sample_df.to_csv(output_file, index=False)
    final_size = os.path.getsize(output_file) / (1024 ** 2)
    print(f"✅ Saved {os.path.basename(output_file)} — {len(sample_df):,} rows ({final_size:.2f} MB)")

if __name__ == "__main__":
    for file in files:
        raw_path = os.path.join(RAW_DIR, file)
        sample_path = os.path.join(SAMPLE_DIR, file.replace(".csv", "_sample.csv"))
        create_random_sample(raw_path, sample_path, TARGET_SIZE_MB)
