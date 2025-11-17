import polars as pl
from pathlib import Path
import sys

def process_billfile_stg(input_path: Path, output_path: Path) -> None:
    """
    Process STG_LN_BILL_MST.parquet (combined MST01-10) file.
    Reads the combined parquet file and writes to output.
    """
    
    print("="*80)
    print("EIQBILEX - Processing STG Bill File")
    print("="*80)
    
    # Check if input file exists
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        sys.exit(1)
    
    print(f"Reading input file: {input_path.name}")
    print("-" * 80)
    
    try:
        # Read the combined parquet file
        df = pl.read_parquet(input_path)
        
        print(f"✓ Loaded {len(df):,} records")
        
        # Validate required columns exist
        required_columns = [
            'BILL_ACCT_NO', 'BILL_NOTE_NO', 'BILL_DUE_DATE', 
            'BILL_PAID_DATE', 'BILL_DAYS_LATE', 'BILL_NOTE_TYPE',
            'BILL_COST_CENTER', 'BILL_TOT_BILLED', 'BILL_PRIN_BILLED',
            'BILL_INT_BILLED', 'BILL_ESC_BILLED', 'BILL_FEE_BILLED',
            'BILL_TOT_BNP', 'BILL_PRIN_BNP', 'BILL_INT_BNP',
            'BILL_ESC_BNP', 'BILL_FEE_BNP'
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"WARNING: Missing columns: {missing_columns}")
        
        print("\nColumn Structure:")
        print(f"  Total Columns: {len(df.columns)}")
        print(f"  Columns: {', '.join(df.columns[:5])}... (showing first 5)")
        
        # Get data statistics
        print("\nData Statistics:")
        print(f"  Total Records: {len(df):,}")
        
        if 'BILL_ACCT_NO' in df.columns:
            print(f"  Unique Accounts: {df['BILL_ACCT_NO'].n_unique():,}")
        
        if 'BILL_DUE_DATE' in df.columns:
            print(f"  Date Range: {df['BILL_DUE_DATE'].min()} to {df['BILL_DUE_DATE'].max()}")
        
        if 'BILL_TOT_BILLED' in df.columns:
            total_billed = df['BILL_TOT_BILLED'].sum()
            print(f"  Total Billed: {total_billed:,.2f}")
        
        if 'BILL_TOT_BNP' in df.columns:
            total_bnp = df['BILL_TOT_BNP'].sum()
            print(f"  Total BNP: {total_bnp:,.2f}")
        
        # Filter out null records (IF ACCTCHK NE .)
        if 'BILL_ACCT_NO' in df.columns:
            initial_count = len(df)
            df = df.filter(pl.col('BILL_ACCT_NO').is_not_null() & (pl.col('BILL_ACCT_NO') != 0))
            filtered_count = initial_count - len(df)
            if filtered_count > 0:
                print(f"\n  Filtered out {filtered_count:,} null/zero account records")
        
        print("-" * 80)
        
        # Write to output parquet
        df.write_parquet(output_path)
        
        output_size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"\n✓ Output written to: {output_path}")
        print(f"  File size: {output_size_mb:.2f} MB")
        print(f"  Final record count: {len(df):,}")
        
        # Show sample
        print("\nSample output (first 5 rows):")
        print(df.head(5))
        
        print("\n" + "="*80)
        print("Processing completed successfully")
        print("="*80)
        
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    # Input: Combined STG file (MST01-10 already merged)
    input_file = Path("STG_LN_BILL_MST.parquet")
    
    # Output: Processed bill file
    output_file = Path("STG_LN_BILL.parquet")
    
    # Process
    process_billfile_stg(input_file, output_file)
