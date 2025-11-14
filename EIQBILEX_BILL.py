import polars as pl
from pathlib import Path
import sys

def process_billfiles_parquet(input_dir: Path, output_path: Path) -> None:
    """
    Process all MST parquet billfiles and combine into single parquet.
    Assumes input files are already in parquet format with decoded data.
    """
    
    print("="*80)
    print("EIQBILEX - Combining Parquet Bill Files")
    print("="*80)
    
    # Define input file pattern (parquet files)
    input_files = [
        input_dir / f"RBP2.B033.MST{i:02d}.BILLFILE.MIS.parquet"
        for i in range(1, 11)
    ]
    
    # Check if files exist
    existing_files = [f for f in input_files if f.exists()]
    
    if not existing_files:
        print(f"ERROR: No input parquet files found in {input_dir}")
        print(f"Expected files like: RBP2.B033.MST01.BILLFILE.MIS.parquet")
        sys.exit(1)
    
    print(f"Found {len(existing_files)} parquet input files")
    print("-" * 80)
    
    # Read and combine all parquet files
    dataframes = []
    for file_path in existing_files:
        try:
            df = pl.read_parquet(file_path)
            print(f"✓ Loaded {len(df):,} records from {file_path.name}")
            
            if len(df) > 0:
                dataframes.append(df)
        except Exception as e:
            print(f"✗ ERROR processing {file_path.name}: {e}")
            sys.exit(1)
    
    # Combine all dataframes
    if not dataframes:
        print("ERROR: No data to process")
        sys.exit(1)
    
    print("-" * 80)
    print("Combining all files...")
    combined_df = pl.concat(dataframes)
    
    print(f"Total records: {len(combined_df):,}")
    
    # Get data statistics
    print("\nData Statistics:")
    print(f"  Unique Accounts: {combined_df['BILL_ACCT_NO'].n_unique():,}")
    print(f"  Date Range: {combined_df['BILL_DUE_DATE'].min()} to {combined_df['BILL_DUE_DATE'].max()}")
    
    if 'BILL_TOT_BILLED' in combined_df.columns:
        total_billed = combined_df['BILL_TOT_BILLED'].sum()
        print(f"  Total Billed: {total_billed:,.2f}")
    
    # Write to output parquet
    combined_df.write_parquet(output_path)
    
    output_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"\n✓ Output written to: {output_path}")
    print(f"  File size: {output_size_mb:.2f} MB")
    
    # Show sample
    print("\nSample output (first 5 rows):")
    print(combined_df.head(5))
    
    print("="*80)
    print("Processing completed successfully")
    print("="*80)

if __name__ == "__main__":
    # Set your input directory (where the 10 parquet files are)
    input_directory = Path(".")  # Current directory
    
    # Set your output file
    output_file = Path("STG_LN_BILL.parquet")
    
    # Process parquet files
    process_billfiles_parquet(input_directory, output_file)
