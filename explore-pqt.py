import pandas as pd
from datetime import datetime

print("=== STARTING AIS DATA EXPLORATION ===")
print("Loading parquet file from ais_data_20240911.parquet...")
df = pd.read_parquet("ais_data_20240911_cleaned.parquet")

# Basic information about the DataFrame
print("\n=== BASIC DATAFRAME INFORMATION ===")
print(f"DataFrame shape: {df.shape}")  # Shows (rows, columns)
print("\nDetailed DataFrame information:")
print(df.info())  # Overview of columns, data types and non-null values
print("\nStatistical summary of numeric columns:")
print(df.describe())  # Statistical summary of numeric columns

# View the data
print("\n=== DATA PREVIEW ===")
print("First 5 rows of data:")
print(df.head())  # First 5 rows
print("\nLast 5 rows of data:")
print(df.tail())  # Last 5 rows

# Column information
print("\n=== COLUMN DETAILS ===")
print("Column names:")
print(df.columns)  # List column names
print("\nData types of each column:")
print(df.dtypes)  # Data types of each column

print("\n=== DATA EXPLORATION COMPLETE ===")
print(f"Total records analyzed: {len(df)}")
print(f"Script execution completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Check for vessel name columns
print("\n=== SEARCHING FOR VESSEL NAMES ===")

# Common vessel name column possibilities
name_columns = [col for col in df.columns if 'name' in col.lower() or 'vessel' in col.lower() or 'ship' in col.lower()]
if name_columns:
    print(f"Potential vessel name columns found: {name_columns}")
    for col in name_columns:
        print(f"\nSample values from '{col}':")
        print(df[col].value_counts().head(10))
else:
    print("No obvious vessel name columns found.")
    
    # Try to examine all string columns for potential vessel names
    print("\nExamining string columns for potential vessel names:")
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        sample_values = df[col].dropna().unique()[:5]  # First 5 unique non-null values
        print(f"\nColumn '{col}' sample values: {sample_values}")
        
print("\nTo extract vessel names from message type 5, which typically contains vessel names:")
if 'message_id' in df.columns and 5 in df['message_id'].unique():
    type5_msgs = df[df['message_id'] == 5]
    if 'shipname' in type5_msgs.columns:
        print("\nFound 'shipname' in message type 5:")
        print(type5_msgs['shipname'].value_counts().head(20))
    else:
        print("\nNo 'shipname' column found in message type 5 data.")
else:
    print("\nNo message type 5 (static and voyage data) found in the dataset.")