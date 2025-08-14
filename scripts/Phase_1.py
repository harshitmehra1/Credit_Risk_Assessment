

import pandas as pd

# Load CSVs (update with your actual file paths)
accepted_df = pd.read_csv("./data/Original/cleaned/accepted_cleaned.csv")
rejected_df = pd.read_csv("./data/Original/cleaned/rejected_cleaned.csv")



# ---------------------------
# 1. Basic info for each file
# ---------------------------
print("=== ACCEPTED LOANS ===")
print(f"Shape: {accepted_df.shape}")
print("\nColumn Data Types:")
print(accepted_df.dtypes)
print("\nMissing Values:")
print(accepted_df.isnull().sum())
print("\nSample Rows:")
print(accepted_df.head(2))

print("\n=== REJECTED LOANS ===")
print(f"Shape: {rejected_df.shape}")
print("\nColumn Data Types:")
print(rejected_df.dtypes)
print("\nMissing Values:")
print(rejected_df.isnull().sum())
print("\nSample Rows:")
print(rejected_df.head(2))

# ---------------------------
# 2. Summary of numeric columns
# ---------------------------
print("\nNumeric Summary - ACCEPTED LOANS:")
print(accepted_df.describe())

print("\nNumeric Summary - REJECTED LOANS:")
print(rejected_df.describe())

# ---------------------------
# 3. Summary of categorical columns
# ---------------------------
def categorical_summary(df):
    cat_cols = df.select_dtypes(include=['object']).columns
    summary = {}
    for col in cat_cols:
        summary[col] = df[col].value_counts().head(5)
    return summary

print("\nTop Categories - ACCEPTED LOANS:")
print(categorical_summary(accepted_df))

print("\nTop Categories - REJECTED LOANS:")
print(categorical_summary(rejected_df))

# ---------------------------
# 4. Common columns between both datasets
# ---------------------------
common_cols = set(accepted_df.columns).intersection(set(rejected_df.columns))
print("\nCommon Columns in both datasets:")
print(common_cols)
