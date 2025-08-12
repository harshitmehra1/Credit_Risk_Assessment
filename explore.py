import pandas as pd
from tqdm import tqdm

# ==== CONFIG ====
INPUT_FILE = "./data/raw/Orig_accepted_2007-18.csv"
OUTPUT_FILE = "./data/raw/accepted_cleaned.csv"
CHUNKSIZE = 50_000  # Adjust if memory allows

# Columns to keep (from our filtered list)
KEEP_COLS = [
    "loan_amnt", "funded_amnt", "term", "int_rate", "installment", "purpose",
    "emp_length", "home_ownership", "annual_inc", "verification_status",
    "dti", "delinq_2yrs", "earliest_cr_line", "fico_range_low", "fico_range_high",
    "inq_last_6mths", "open_acc", "pub_rec", "revol_bal", "revol_util",
    "total_acc", "issue_d", "zip_code", "addr_state", "loan_status"
]

# ==== PROCESS FILE IN CHUNKS ====
reader = pd.read_csv(INPUT_FILE, chunksize=CHUNKSIZE, low_memory=False)

with pd.ExcelWriter(OUTPUT_FILE) if OUTPUT_FILE.endswith(".xlsx") else open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f_out:
    first_chunk = True
    
    for chunk in tqdm(reader, desc="Processing chunks"):
        # Keep only selected columns
        chunk = chunk[KEEP_COLS]

        # Write to file
        if first_chunk:
            chunk.to_csv(f_out, index=False)
            first_chunk = False
        else:
            chunk.to_csv(f_out, index=False, header=False)

print(f"✅ Cleaned file saved to {OUTPUT_FILE}")
