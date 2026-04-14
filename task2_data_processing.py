#!/usr/bin/env python3
"""
TrendPulse Task 2 - Data Cleaning 
Load messy JSON from Task 1 -> Clean -> Save CSV
Took me 2hrs figuring out pandas drop_duplicates lol
Written by John Doe - Dec 2024
"""

import pandas as pd
import json
import os
from datetime import datetime

print("TrendPulse Task 2 - Cleaning the data!")
print("=" * 50)

def find_json_file():
    """Find the trends JSON file in data/ folder."""
    data_dir = "data"
    if not os.path.exists(data_dir):
        print("ERROR: data/ folder not found!")
        print("Run task1_data_collection.py first!")
        exit(1)
    
    # Look for trends_YYYYMMDD.json
    for file in os.listdir(data_dir):
        if file.startswith("trends_") and file.endswith(".json"):
            filepath = os.path.join(data_dir, file)
            print("Found:", file)
            return filepath
    
    print("No trends_*.json file found!")
    exit(1)

def load_json_to_df(filepath):
    """Load JSON file into pandas DataFrame."""
    print("Loading JSON...")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    print("Loaded", len(df), "stories from", os.path.basename(filepath))
    return df

def clean_dataframe(df):
    """Apply all cleaning steps with progress prints."""
    print("Cleaning data...")
    original_count = len(df)
    
    # 1. Remove duplicates by post_id
    print("Total rows:", len(df))
    df = df.drop_duplicates(subset=['post_id'], keep='first')
    print("After duplicates:", len(df))
    
    # 2. Drop rows missing critical fields
    before_nulls = len(df)
    df = df.dropna(subset=['post_id', 'title', 'score'])
    print("After nulls:", len(df), f"({before_nulls - len(df)} removed)")
    
    # 3. Convert score and num_comments to integers
    df['score'] = pd.to_numeric(df['score'], errors='coerce').fillna(0).astype(int)
    df['num_comments'] = pd.to_numeric(df['num_comments'], errors='coerce').fillna(0).astype(int)
    
    # 4. Remove low quality (score < 5)
    before_low = len(df)
    df = df[df['score'] >= 5]
    print("After low scores:", len(df), f"({before_low - len(df)} removed)")
    
    # 5. Strip whitespace from titles
    df['title'] = df['title'].str.strip()
    
    # Final count
    print("Cleaning complete!")
    print("Started:", original_count, "-> Final:", len(df), "rows")
    
    return df

def save_csv(df):
    """Save cleaned data to CSV and print summary."""
    output_file = "data/trends_clean.csv"
    
    # Ensure data/ exists
    os.makedirs("data", exist_ok=True)
    
    # Save CSV
    df.to_csv(output_file, index=False)
    print("Saved", len(df), "rows to", output_file)
    
    # Category summary
    print("Stories per category:")
    category_counts = df['category'].value_counts()
    for cat, count in category_counts.items():
        print(f"  {cat:<15} {count:>3}")
    
    # Quick data preview
    print("Data preview:")
    print(df[['title', 'category', 'score', 'num_comments']].head(3).to_string(index=False))

def main():
    """Main workflow."""
    # Step 1: Load JSON
    json_file = find_json_file()
    df = load_json_to_df(json_file)
    
    # Step 2: Clean
    df_clean = clean_dataframe(df)
    
    # Step 3: Save & summarize
    save_csv(df_clean)
    
    print("Task 2 COMPLETE! Ready for Task 3!")
    print("Next: Pandas/NumPy analysis on trends_clean.csv")

if __name__ == "__main__":
    main()
