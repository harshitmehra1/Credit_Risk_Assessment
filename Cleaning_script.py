import pandas as pd
from tqdm import tqdm
from pathlib import Path
import os
import warnings

# Suppress SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)

CHUNKSIZE = 50_000

# ==== FILE CONFIGS ====
FILES_CONFIG = [
    {
        "input": Path("data/Original/accepted_2007_to_2018Q4.csv"),
        "output": Path("data/Original/cleaned/accepted_cleaned.csv"),
        "keep_cols": [
            "loan_amnt", "funded_amnt", "term", "int_rate", "installment", "purpose",
            "emp_length", "home_ownership", "annual_inc", "verification_status",
            "dti", "delinq_2yrs", "earliest_cr_line", "fico_range_low", "fico_range_high",
            "inq_last_6mths", "open_acc", "pub_rec", "revol_bal", "revol_util",
            "total_acc", "issue_d", "zip_code", "addr_state", "loan_status"
        ]
    },
    {
        "input": Path("data/Original/rejected_2007_to_2018Q4.csv"),
        "output": Path("data/Original/cleaned/rejected_cleaned.csv"),
        "keep_cols": [
            "Amount Requested", 
            "Application Date", 
            "Loan Title", 
            "Risk_Score",
            "Debt-To-Income Ratio", 
            "Zip Code", 
            "State", 
            "Employment Length", 
            "Policy Code"
        ]
    }
]

def filter_and_clean_csv(input_file, output_file, keep_cols, chunksize):
    if not input_file.exists():
        print(f"❌ File not found: {input_file}")
        return
    
    temp_file = output_file.parent / f"_temp_{output_file.name}"
    if temp_file.exists():
        temp_file.unlink()

    total_rows = sum(1 for _ in open(input_file, encoding="utf-8")) - 1
    print(f"\n📂 File: {input_file.name}")
    print(f"🔹 Total rows: {total_rows:,}")
    print(f"🚀 Processing in chunks of {chunksize:,} rows...\n")

    reader = pd.read_csv(input_file, usecols=keep_cols, chunksize=chunksize, low_memory=False)

    with tqdm(total=total_rows, unit="rows", ncols=100, 
              bar_format="{l_bar}{bar} | {n_fmt}/{total_fmt} rows ({percentage:3.0f}%)") as pbar:
        for i, chunk in enumerate(reader):
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

    os.replace(temp_file, output_file)
    print(f"✅ Done: {output_file.name}")


if __name__ == "__main__":
    for cfg in FILES_CONFIG:
        filter_and_clean_csv(cfg["input"], cfg["output"], cfg["keep_cols"], CHUNKSIZE)
