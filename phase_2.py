import pandas as pd

# Load datasets
accepted_df = pd.read_csv("./data/Original/cleaned/accepted_cleaned.csv")
rejected_df = pd.read_csv("./data/Original/cleaned/rejected_cleaned.csv")

# Add a status column
accepted_df["status"] = "accepted"
rejected_df["status"] = "rejected"

# Add loan_status column to rejected loans (they don’t have it)
rejected_df["loan_status"] = None

# Get all columns from both datasets
all_columns = list(set(accepted_df.columns) | set(rejected_df.columns))

# Reindex both DataFrames to have the same columns (missing columns will be filled with NaN)
accepted_df = accepted_df.reindex(columns=all_columns)
rejected_df = rejected_df.reindex(columns=all_columns)

# Merge into one dataset
merged_df = pd.concat([accepted_df, rejected_df], ignore_index=True)

# Save merged dataset
merged_df.to_csv("merged_loans.csv", index=False)

print(f"Merged dataset shape: {merged_df.shape}")
print(merged_df["status"].value_counts())
