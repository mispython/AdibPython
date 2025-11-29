import polars as pl
import pyreadstat
import os
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
from sas7bdat import SAS7BDAT

# --- Configuration ---
# Source SAS7BDAT files (adjust paths to your actual SAS datasets)
CHANNEL_SUM_SAS = '/dwh/crm/CHANNEL_SUM.sas7bdat'  # CRMWH.CHANNEL_SUM source
CHANNEL_UPDATE_SAS = '/dwh/crm/CHANNEL_UPDATE.sas7bdat'  # CHN.CHANNEL_UPDATE source

# Calculate last month's year and month
today = datetime.today()
last_month = today.replace(day=1) - timedelta(days=1)
year = last_month.year
month = last_month.month

# Output path structure
OUTPUT_BASE_PATH = "/host/mis/parquet/crm"
OUTPUT_DATA_PATH = f"{OUTPUT_BASE_PATH}/year={year}/month={month:02d}"
os.makedirs(OUTPUT_DATA_PATH, exist_ok=True)

print("=" * 80)
print("CREATING LAST MONTH BASE PARQUET FILES")
print("=" * 80)
print(f"Target period: {year}-{month:02d}")
print(f"Output path: {OUTPUT_DATA_PATH}")
print()

# =============================================================================
# Read and convert CHANNEL_SUM (from CRMWH.CHANNEL_SUM)
# =============================================================================
print("Processing CHANNEL_SUM...")
try:
    # Method 1: Using pyreadstat (recommended for better metadata handling)
    channel_sum_df, meta_channel = pyreadstat.read_sas7bdat(CHANNEL_SUM_SAS)
    
    # Convert to polars for better performance (optional)
    channel_sum_pl = pl.from_pandas(channel_sum_df)
    
    # Save as parquet
    output_file = f"{OUTPUT_DATA_PATH}/CHANNEL_SUM.parquet"
    channel_sum_df.to_parquet(output_file, engine='pyarrow', index=False)
    
    print(f"✓ CHANNEL_SUM saved to: {output_file}")
    print(f"  Records: {len(channel_sum_df)}")
    print(f"  Columns: {list(channel_sum_df.columns)}")
    print(f"  Schema:")
    print(channel_sum_pl.schema)
    print()
    
except FileNotFoundError:
    print(f"⚠ Warning: {CHANNEL_SUM_SAS} not found. Creating empty template...")
    # Create empty template with expected schema
    channel_sum_pl = pl.DataFrame({
        'CHANNEL': pl.Series([], dtype=pl.Utf8),
        'TOLPROMPT': pl.Series([], dtype=pl.Int64),
        'TOLUPDATE': pl.Series([], dtype=pl.Int64),
        'MONTH': pl.Series([], dtype=pl.Utf8)
    })
    output_file = f"{OUTPUT_DATA_PATH}/CHANNEL_SUM.parquet"
    channel_sum_pl.write_parquet(output_file)
    print(f"✓ Empty CHANNEL_SUM template created: {output_file}")
    print()

except Exception as e:
    print(f"✗ Error processing CHANNEL_SUM: {str(e)}")
    print()

# =============================================================================
# Read and convert CHANNEL_UPDATE (from CHN.CHANNEL_UPDATE)
# =============================================================================
print("Processing CHANNEL_UPDATE...")
try:
    # Method 1: Using pyreadstat
    channel_update_df, meta_update = pyreadstat.read_sas7bdat(CHANNEL_UPDATE_SAS)
    
    # Convert to polars (optional)
    channel_update_pl = pl.from_pandas(channel_update_df)
    
    # Save as parquet
    output_file = f"{OUTPUT_DATA_PATH}/CHANNEL_UPDATE.parquet"
    channel_update_df.to_parquet(output_file, engine='pyarrow', index=False)
    
    print(f"✓ CHANNEL_UPDATE saved to: {output_file}")
    print(f"  Records: {len(channel_update_df)}")
    print(f"  Columns: {list(channel_update_df.columns)}")
    print(f"  Schema:")
    print(channel_update_pl.schema)
    print()
    
except FileNotFoundError:
    print(f"⚠ Warning: {CHANNEL_UPDATE_SAS} not found. Creating empty template...")
    # Create empty template with expected schema
    channel_update_pl = pl.DataFrame({
        'LINE': pl.Series([], dtype=pl.Utf8),
        'ATM': pl.Series([], dtype=pl.Int64),
        'EBK': pl.Series([], dtype=pl.Int64),
        'OTC': pl.Series([], dtype=pl.Int64),
        'TOTAL': pl.Series([], dtype=pl.Int64),
        'REPORT_DATE': pl.Series([], dtype=pl.Date)
    })
    output_file = f"{OUTPUT_DATA_PATH}/CHANNEL_UPDATE.parquet"
    channel_update_pl.write_parquet(output_file)
    print(f"✓ Empty CHANNEL_UPDATE template created: {output_file}")
    print()
    
except Exception as e:
    print(f"✗ Error processing CHANNEL_UPDATE: {str(e)}")
    print()

# =============================================================================
# Verification
# =============================================================================
print("=" * 80)
print("VERIFICATION")
print("=" * 80)

# List all created files
created_files = list(Path(OUTPUT_DATA_PATH).glob("*.parquet"))
print(f"Files created in {OUTPUT_DATA_PATH}:")
for file in created_files:
    file_size = file.stat().st_size / (1024 * 1024)  # Size in MB
    print(f"  - {file.name} ({file_size:.2f} MB)")
print()

# Quick data check using DuckDB
try:
    con = duckdb.connect()
    
    # Check CHANNEL_SUM
    channel_sum_path = f"{OUTPUT_DATA_PATH}/CHANNEL_SUM.parquet"
    if os.path.exists(channel_sum_path):
        result = con.execute(f"""
            SELECT 
                COUNT(*) as record_count,
                COUNT(DISTINCT CHANNEL) as unique_channels
            FROM read_parquet('{channel_sum_path}')
        """).fetchdf()
        print("CHANNEL_SUM Summary:")
        print(result)
        print()
        
        # Show sample data
        sample = con.execute(f"""
            SELECT * FROM read_parquet('{channel_sum_path}')
            LIMIT 5
        """).fetchdf()
        print("CHANNEL_SUM Sample (first 5 rows):")
        print(sample)
        print()
    
    # Check CHANNEL_UPDATE
    channel_update_path = f"{OUTPUT_DATA_PATH}/CHANNEL_UPDATE.parquet"
    if os.path.exists(channel_update_path):
        result = con.execute(f"""
            SELECT 
                COUNT(*) as record_count,
                COUNT(DISTINCT LINE) as unique_lines
            FROM read_parquet('{channel_update_path}')
        """).fetchdf()
        print("CHANNEL_UPDATE Summary:")
        print(result)
        print()
        
        # Show sample data
        sample = con.execute(f"""
            SELECT * FROM read_parquet('{channel_update_path}')
            LIMIT 5
        """).fetchdf()
        print("CHANNEL_UPDATE Sample (first 5 rows):")
        print(sample)
        print()
    
    con.close()
    
except Exception as e:
    print(f"⚠ Verification check failed: {str(e)}")
    print()

print("=" * 80)
print("PROCESS COMPLETE")
print("=" * 80)
print(f"Base parquet files created for: {year}-{month:02d}")
print(f"Location: {OUTPUT_DATA_PATH}")
print()
print("Files created:")
print(f"  1. CHANNEL_SUM.parquet")
print(f"  2. CHANNEL_UPDATE.parquet")
