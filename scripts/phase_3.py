import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm import tqdm
import os
import gc

# =====================
# CONFIG
# =====================
INPUT_FILE = "./data/merged_loans.csv"   # Your 3.3 GB file
OUTPUT_FILE = "./data/features.csv"
CHUNKSIZE = 50000                      # Adjust based on RAM (try 50k or 100k)
TEXT_COLUMNS = ["customer_notes", "loan_purpose", "employment_title"]  # Example textual columns
NUMERIC_COLUMNS = ["loan_amount", "interest_rate", "annual_income", "credit_score"]  # Example numeric columns

# =====================
# Step 1: Get Scalers/Vectorizers fitted
# =====================
print("🔹 Step 1/3: Fitting transformers on sample data...")
sample_df = pd.read_csv(INPUT_FILE, nrows=200000)  # Small sample to fit transformers

# Fit scaler for numeric columns
scaler = StandardScaler()
scaler.fit(sample_df[NUMERIC_COLUMNS])

# Fit TF-IDF for each text column separately (keeps things more interpretable)
vectorizers = {}
for col in TEXT_COLUMNS:
    vectorizer = TfidfVectorizer(max_features=50)  # limit features for memory
    vectorizer.fit(sample_df[col].astype(str).fillna(""))
    vectorizers[col] = vectorizer

del sample_df
gc.collect()

# =====================
# Step 2: Process file in chunks
# =====================
print("🔹 Step 2/3: Processing file in chunks...")
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)  # Start fresh

chunk_iter = pd.read_csv(INPUT_FILE, chunksize=CHUNKSIZE)
total_chunks = sum(1 for _ in pd.read_csv(INPUT_FILE, chunksize=CHUNKSIZE))  # For tqdm total
chunk_iter = pd.read_csv(INPUT_FILE, chunksize=CHUNKSIZE)  # re-create after counting

for chunk in tqdm(chunk_iter, total=total_chunks, desc="Processing Chunks"):
    # Process numeric columns
    numeric_scaled = scaler.transform(chunk[NUMERIC_COLUMNS])
    numeric_df = pd.DataFrame(numeric_scaled, columns=[f"{col}_scaled" for col in NUMERIC_COLUMNS])

    # Process text columns
    text_features_list = []
    for col in TEXT_COLUMNS:
        vec = vectorizers[col]
        tfidf_matrix = vec.transform(chunk[col].astype(str).fillna(""))
        tfidf_df = pd.DataFrame(tfidf_matrix.toarray(), columns=[f"{col}_tfidf_{i}" for i in range(tfidf_matrix.shape[1])])
        text_features_list.append(tfidf_df)

    # Combine all
    final_chunk = pd.concat([chunk.reset_index(drop=True), numeric_df] + text_features_list, axis=1)

    # Save chunk to file
    final_chunk.to_csv(OUTPUT_FILE, mode="a", header=not os.path.exists(OUTPUT_FILE), index=False)

    del chunk, numeric_df, text_features_list, final_chunk
    gc.collect()

# =====================
# Step 3: Done
# =====================
print(f"✅ Phase 3 feature engineering completed. Saved to: {OUTPUT_FILE}")
