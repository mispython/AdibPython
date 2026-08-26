import polars as pl
import pandas as pd
import pyreadstat
from pathlib import Path
from datetime import datetime, timedelta

def eibrsrgf():
    # Paths
    input_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRSRGF")
    output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRSRGF")
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Get previous day's date (replacing REPTDATE)
    current_date = datetime.now() - timedelta(days=1)
    
    mm = current_date.month
    mm1 = mm - 1 if mm > 1 else 12
    
    reptmon = f"{mm:02d}"
    reptmon1 = f"{mm1:02d}"
    reptyear = str(current_date.year)
    reptday = f"{current_date.day:02d}"
    rdate = current_date.strftime("%d%m%y")
    ndate = f"{current_date.day:02d}{current_date.month:02d}"
    
    print(f"REPTMON: {reptmon}, RDATE: {rdate}")
    print(f"Processing date: {current_date.strftime('%Y-%m-%d')}")
    
    # Read CGCS data from SAS file using pyreadstat
    sas_file = input_path / f"lnnpgs{reptmon}.sas7bdat"
    
    try:
        # Read SAS file using pyreadstat
        df, meta = pyreadstat.read_sas7bdat(
            str(sas_file),
            encoding='latin1'
        )
        
        # Convert to polars DataFrame with lowercase column names
        cgcs_df = pl.from_pandas(df)
        cgcs_df = cgcs_df.rename({col: col.lower() for col in cgcs_df.columns})
        
        print(f"Successfully read {len(cgcs_df)} rows from {sas_file}")
        
        # DEBUG: Check CVAR02 values
        print("\n=== DEBUG: CVAR02 values ===")
        print(f"CVAR02 dtype: {cgcs_df['cvar02'].dtype}")
        
        # Get unique values and their counts
        value_counts = cgcs_df.group_by('cvar02').agg(pl.count().alias('count')).sort('count', descending=True)
        print("\nCVAR02 value counts:")
        for row in value_counts.iter_rows(named=True):
            print(f"  '{row['cvar02']}': {row['count']} rows")
        
        # Check for values with different formats
        print("\n=== Checking for '10' and '63' in different formats ===")
        
        # Check as-is
        filtered_as_is = cgcs_df.filter(pl.col('cvar02').is_in(['10', '63']))
        print(f"As-is match: {len(filtered_as_is)} rows")
        
        # Check with strip
        cgcs_df = cgcs_df.with_columns(
            pl.col('cvar02').str.strip_chars().alias('cvar02_stripped')
        )
        filtered_stripped = cgcs_df.filter(pl.col('cvar02_stripped').is_in(['10', '63']))
        print(f"Stripped match: {len(filtered_stripped)} rows")
        
        # Check if any values contain '10' or '63' as substring
        contains_10 = cgcs_df.filter(pl.col('cvar02').str.contains('10'))
        contains_63 = cgcs_df.filter(pl.col('cvar02').str.contains('63'))
        print(f"\nContains '10': {len(contains_10)} rows")
        print(f"Contains '63': {len(contains_63)} rows")
        
        # Check the first few characters
        cgcs_df = cgcs_df.with_columns(
            pl.col('cvar02').str.slice(0, 2).alias('first_two_chars')
        )
        first_two = cgcs_df.filter(pl.col('first_two_chars').is_in(['10', '63']))
        print(f"\nFirst 2 chars match: {len(first_two)} rows")
        
        # Show some sample data to understand the structure
        print("\n=== Sample data (first 10 rows) ===")
        sample_cols = ['cvar01', 'cvar02', 'cvar03', 'cvar04', 'cvar06', 'cvar07']
        available_cols = [col for col in sample_cols if col in cgcs_df.columns]
        if available_cols:
            print(cgcs_df.select(available_cols).head(10))
        
        # Check if CVAR07 might be the scheme code instead
        if 'cvar07' in cgcs_df.columns:
            print("\n=== CVAR07 values ===")
            print(f"CVAR07 dtype: {cgcs_df['cvar07'].dtype}")
            cvar07_counts = cgcs_df.group_by('cvar07').agg(pl.count().alias('count')).sort('count', descending=True)
            for row in cvar07_counts.iter_rows(named=True):
                print(f"  '{row['cvar07']}': {row['count']} rows")
        
    except FileNotFoundError:
        print(f"File not found: {sas_file}")
        return
    except Exception as e:
        print(f"Error reading SAS file: {e}")
        import traceback
        traceback.print_exc()
        return

if __name__ == "__main__":
    eibrsrgf()
