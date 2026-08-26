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
        print(f"Unique CVAR02 values:")
        unique_values = cgcs_df['cvar02'].unique().to_list()
        print(unique_values)
        print(f"Number of unique values: {len(unique_values)}")
        
        # Check for '10' and '63' in different formats
        print("\n=== Checking for '10' and '63' ===")
        
        # Check as string
        if 'cvar02' in cgcs_df.columns:
            # Try different formats
            cgcs_df = cgcs_df.with_columns([
                pl.col('cvar02').cast(pl.Utf8).alias('cvar02_str'),
                pl.col('cvar02').cast(pl.Utf8).str.strip().alias('cvar02_stripped'),
            ])
            
            print("Values containing '10' or '63' as string:")
            filtered_str = cgcs_df.filter(pl.col('cvar02_str').is_in(['10', '63']))
            print(f"  String match: {len(filtered_str)} rows")
            
            print("Values containing '10' or '63' after stripping:")
            filtered_stripped = cgcs_df.filter(pl.col('cvar02_stripped').is_in(['10', '63']))
            print(f"  Stripped match: {len(filtered_stripped)} rows")
            
            # Check first few values
            print("\nFirst 20 CVAR02 values:")
            for i, val in enumerate(unique_values[:20]):
                print(f"  {i}: '{val}' (type: {type(val)})")
        
    except FileNotFoundError:
        print(f"File not found: {sas_file}")
        return
    except Exception as e:
        print(f"Error reading SAS file: {e}")
        return

if __name__ == "__main__":
    eibrsrgf()
