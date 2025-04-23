import pandas as pd
import sys  # To exit gracefully on error

# --- Configuration ---
# Set the path to your Parquet file
PARQUET_FILE_PATH = 'university_papers_data.parquet'

# --- Read the Parquet File ---
print(f"Attempting to read Parquet file: {PARQUET_FILE_PATH}")
try:
    # Use pandas read_parquet function
    # Specify the engine if needed (usually auto-detected, but 'pyarrow' is explicit)
    df = pd.read_parquet(PARQUET_FILE_PATH, engine='pyarrow')
    print("Successfully loaded Parquet file into DataFrame.")

except FileNotFoundError:
    print(f"Error: File not found at '{PARQUET_FILE_PATH}'")
    print("Please ensure the file exists and the path is correct.")
    sys.exit(1)  # Exit the script with an error code
except ImportError:
    print("Error: Missing dependency. Please install pandas and pyarrow:")
    print("pip install pandas pyarrow")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred while reading the file: {e}")
    sys.exit(1)


# --- Inspect the DataFrame ---
if not df.empty:
    print("\n--- DataFrame Inspection ---")

    # 1. Dimensions (Rows, Columns)
    print(f"\n1. Shape (rows, columns): {df.shape}")

    # 2. First 5 Rows
    print("\n2. First 5 rows (head):")
    # pd.set_option('display.max_columns', None) # Uncomment to show all columns if wide
    print(df.head())
    # pd.reset_option('display.max_columns') # Uncomment to reset display option

    # 3. Column Names
    print("\n3. Column names:")
    print(list(df.columns))

    # 4. Data Types and Non-Null Counts
    print("\n4. Data types and non-null info:")
    # This is very useful to see if data types were inferred correctly and find missing values
    df.info(verbose=True, show_counts=True)

    # 5. Summary Statistics (for numerical columns)
    print("\n5. Summary statistics (numerical columns):")
    # Includes count, mean, std, min, max, percentiles
    # Use .describe(include='all') to include stats for object/string columns too
    print(df.describe())

    # 6. Check for Missing Values (count per column)
    print("\n6. Count of missing values per column:")
    missing_values = df.isnull().sum()
    # Only show columns with missing values
    print(missing_values[missing_values > 0])

    # 7. Unique Value Counts for a specific column (Example: university_key)
    if 'university_key' in df.columns:
        print("\n7. Value counts for 'university_key':")
        print(df['university_key'].value_counts())
    else:
        print("\n7. 'university_key' column not found.")

    # 8. Example: Accessing data for a specific paper (e.g., the first one)
    print("\n8. Data for the first paper (index 0):")
    try:
        # Convert the first row to a dictionary for easier viewing
        first_paper_data = df.iloc[0].to_dict()
        # Print selected fields for brevity
        print(f"  - OpenAlex ID: {first_paper_data.get('openalex_id', 'N/A')}")
        # Preview title
        print(f"  - Title: {first_paper_data.get('title', 'N/A')[:80]}...")
        print(
            f"  - University: {first_paper_data.get('university_key', 'N/A')}")
        print(f"  - Cited By: {first_paper_data.get('cited_by_count', 'N/A')}")
        # You could print the whole dictionary if needed: print(first_paper_data)
    except IndexError:
        print("  DataFrame seems empty, cannot access index 0.")
    # 9. Count of retracted papers
    if 'is_retracted' in df.columns:
        # True is treated as 1, False as 0
        num_retracted = df['is_retracted'].sum()
        print(f"\n9. Number of retracted papers: {num_retracted}")
    else:
        print("\n9. 'is_retracted' column not found.")

      # 10. Count of retracted papers
    if 'is_paratext' in df.columns:
        # True is treated as 1, False as 0
        num_paratext = df['is_paratext'].sum()
        print(f"\n10. Number of paratext papers: {num_paratext}")
    else:
        print("\n10. 'is_paratext' column not found.")

    # 11. Display one retracted paper
    if 'is_retracted' in df.columns and any(df['is_retracted']):
        retracted_paper = df[df['is_retracted'] == True].iloc[0].to_dict()
        print("\n11. Example of a retracted paper:")
        print(f"  - OpenAlex ID: {retracted_paper.get('openalex_id', 'N/A')}")
        print(f"  - Title: {retracted_paper.get('title', 'N/A')[:80]}...")
        print(
            f"  - University: {retracted_paper.get('university_key', 'N/A')}")
        print(f"  - Cited By: {retracted_paper.get('cited_by_count', 'N/A')}")
    else:
        print("\n11. No retracted papers found or 'is_retracted' column missing.")
    # 12. Display one paratext paper
    if 'is_paratext' in df.columns and any(df['is_paratext']):
        paratext_paper = df[df['is_paratext'] == True].iloc[0].to_dict()
        print("\n12. Example of a paratext paper:")
        print(f"  - OpenAlex ID: {paratext_paper.get('openalex_id', 'N/A')}")
        print(f"  - University: {paratext_paper.get('university_key', 'N/A')}")
        print(f"  - Cited By: {paratext_paper.get('cited_by_count', 'N/A')}")
        print(f"  - Language: {paratext_paper.get('language', 'N/A')}")
    else:
        print("\n12. No paratext papers found or 'is_paratext' column missing.")
else:
    print("\nThe DataFrame loaded from the Parquet file is empty.")

print("\n--- Inspection Complete ---")
