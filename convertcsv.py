import pandas as pd
import json

# Load the transactions JSON file
with open("transactions.json") as file:
    data = json.load(file)

# Create a DataFrame from the JSON data
df = pd.json_normalize(data)

# Extract all unique keys from the JSON data
all_keys = set()
for entity in data:
    all_keys.update(entity.keys())

# Create columns for each unique key in the DataFrame
for key in all_keys:
    if key not in df.columns:
        df[key] = None

# Data Cleaning
# 1. Remove rows with missing or null values in the 'category' column
df = df.dropna(subset=['category'])

# 2. Remove duplicate rows based on 'transaction_id'
df = df.drop_duplicates(subset=['transaction_id'])

# 3. Standardize category names (strip any leading/trailing spaces and capitalize each word)
df['category'] = df['category'].str.strip().str.title()

# 4. Ensure that 'price' and 'quantity' are valid (positive values)
df = df[df['price'] > 0]
df = df[df['quantity'] > 0]

# 5. Ensure the 'timestamp' column is in a consistent datetime format
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')  # Invalid timestamps will be NaT
df = df.dropna(subset=['timestamp'])  # Drop rows with invalid timestamps

# 6. Handle invalid numeric values in 'price' and 'quantity'
df['price'] = pd.to_numeric(df['price'], errors='coerce')
df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

# Remove rows with NaN values in 'price' or 'quantity' after coercion
df = df.dropna(subset=['price', 'quantity'])

# Save the cleaned DataFrame to a new CSV file
df.to_csv('transactionss.csv', index=False)

# Optionally print the first few rows of the cleaned data to verify
print(df.head())