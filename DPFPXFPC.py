# eibaegld.py
import polars as pl
import pyarrow.parquet as pq
import duckdb
from datetime import date, timedelta
import sas7bdat
import pandas as pd
import os

# -----------------------------
# Step 1: Define report date (like SAS SET DEPOSIT.REPTDATE)
# -----------------------------
today = date.today()
today = today - timedelta(days=3)  # testing purposes
REPTMON = f"{today.month:02d}"
REPTDAY = f"{today.day:02d}"
REPTDT = today.strftime("%Y-%m-%d")

# Weekly bucket (NOWK)
day = today.day
if 1 <= day <= 8:
    NOWK = "1"
elif 9 <= day <= 15:
    NOWK = "2"
elif 16 <= day <= 22:
    NOWK = "3"
else:
    NOWK = "4"

print(f"Running EIBAEGLD for {REPTDT}, MON={REPTMON}, DAY={REPTDAY}, NOWK={NOWK}")

# -----------------------------
# Step 2: Define output directory
# -----------------------------
output_dir = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/GOLD/EIBAEGLD"

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)
print(f"Output directory: {output_dir}")

# -----------------------------
# Step 3: Load MIS gold transaction dataset from SAS
# -----------------------------
mis_file = f"/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/gold/goldtran{REPTMON}{NOWK}.sas7bdat"

print(f"Looking for input file: {mis_file}")

try:
    # Using pandas to read SAS with proper type handling
    df_pandas = pd.read_sas(mis_file, format='sas7bdat')
    
    # Convert to Polars DataFrame
    mis_goldtran = pl.from_pandas(df_pandas)
    print(f"Successfully loaded using pandas")
    
except FileNotFoundError:
    raise FileNotFoundError(f"Missing input SAS file: {mis_file}")
except Exception as e:
    print(f"Error reading SAS file with pandas: {e}")
    # Fallback to sas7bdat library if pandas fails
    try:
        with sas7bdat.SAS7BDAT(mis_file) as reader:
            data = list(reader)
            mis_goldtran = pl.DataFrame(data)
        print(f"Successfully loaded using sas7bdat library")
    except Exception as e2:
        raise Exception(f"Failed to load SAS file with both methods: {e2}")

print(f"Loaded {len(mis_goldtran)} rows from SAS dataset: {mis_file}")
print(f"Columns: {mis_goldtran.columns}")

# Display data types to understand the columns
print("\nColumn data types:")
for col, dtype in zip(mis_goldtran.columns, mis_goldtran.dtypes):
    print(f"  {col}: {dtype}")

# -----------------------------
# Step 4: Convert Binary columns to String
# -----------------------------
# Polars can't write Binary columns to CSV, so convert them to String
for col in mis_goldtran.columns:
    if mis_goldtran[col].dtype == pl.Binary:
        print(f"Converting Binary column '{col}' to String")
        mis_goldtran = mis_goldtran.with_columns(
            pl.col(col).cast(pl.Utf8).alias(col)
        )

# -----------------------------
# Step 5: Filter by REPTDATE
# -----------------------------
if "REPTDATE" not in mis_goldtran.columns:
    print("Warning: REPTDATE column not found. Available columns:")
    print(mis_goldtran.columns)
    temp_goldtran = mis_goldtran.clone()
else:
    # Check the data type of REPTDATE
    repdate_dtype = mis_goldtran["REPTDATE"].dtype
    
    print(f"\nREPTDATE data type: {repdate_dtype}")
    print(f"Sample REPTDATE values (first 5): {mis_goldtran['REPTDATE'].head(5).to_list()}")
    
    # Handle different data types
    if repdate_dtype in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
        # SAS date is days since 1960-01-01
        from datetime import datetime
        sas_epoch = datetime(1960, 1, 1)
        
        # Handle float by converting to int first (rounding)
        if repdate_dtype in [pl.Float32, pl.Float64]:
            mis_goldtran = mis_goldtran.with_columns(
                pl.col("REPTDATE").cast(pl.Int64).alias("REPTDATE_INT")
            )
            repdate_col = "REPTDATE_INT"
        else:
            repdate_col = "REPTDATE"
        
        # Convert SAS numeric date to string
        mis_goldtran = mis_goldtran.with_columns(
            pl.col(repdate_col).map_elements(
                lambda x: (sas_epoch + timedelta(days=int(x))).strftime("%Y-%m-%d") if x is not None and not pd.isna(x) else None,
                return_dtype=pl.Utf8
            ).alias("REPTDATE_STR")
        )
        
        # Filter by the string date
        temp_goldtran = mis_goldtran.filter(pl.col("REPTDATE_STR") == REPTDT)
        
    elif repdate_dtype == pl.Date:
        mis_goldtran = mis_goldtran.with_columns(
            pl.col("REPTDATE").cast(pl.Utf8).alias("REPTDATE_STR")
        )
        temp_goldtran = mis_goldtran.filter(pl.col("REPTDATE_STR") == REPTDT)
    else:
        temp_goldtran = mis_goldtran.filter(pl.col("REPTDATE") == REPTDT)

print(f"\nFiltered to {len(temp_goldtran)} rows for date {REPTDT}")
if len(temp_goldtran) > 0:
    print("Sample data (first 5 rows):")
    print(temp_goldtran.head())
else:
    print(f"WARNING: No records found for date {REPTDT}")

# -----------------------------
# Step 6: Write to TEMP dataset in output directory
# -----------------------------
temp_file_parquet = os.path.join(output_dir, f"TEMP_GOLDTRAND_{REPTDAY}.parquet")
temp_file_txt = os.path.join(output_dir, f"TEMP_GOLDTRAND_{REPTDAY}.txt")
temp_file_csv = os.path.join(output_dir, f"TEMP_GOLDTRAND_{REPTDAY}.csv")

# Write Parquet
if len(temp_goldtran) > 0:
    temp_goldtran.write_parquet(temp_file_parquet)
    
    # Write TXT (pipe-delimited text file)
    temp_goldtran.write_csv(temp_file_txt, separator='|')
    
    # Write CSV (comma-delimited)
    temp_goldtran.write_csv(temp_file_csv)
    
    print(f"\nTEMP dataset written to:")
    print(f"  - Parquet: {temp_file_parquet}")
    print(f"  - Text (pipe): {temp_file_txt}")
    print(f"  - CSV: {temp_file_csv}")
else:
    print("\nNo data to write - creating empty files")
    if len(mis_goldtran.columns) > 0:
        empty_df = pl.DataFrame(columns=mis_goldtran.columns)
        empty_df.write_parquet(temp_file_parquet)
        empty_df.write_csv(temp_file_txt, separator='|')
        empty_df.write_csv(temp_file_csv)

# -----------------------------
# Step 7: Export for FTP in output directory
# -----------------------------
export_parquet = os.path.join(output_dir, "SAP_PBB_GOLD_GOLDFTPD.parquet")
export_txt = os.path.join(output_dir, "SAP_PBB_GOLD_GOLDFTPD.txt")

if len(temp_goldtran) > 0:
    temp_goldtran.write_parquet(export_parquet)
    temp_goldtran.write_csv(export_txt, separator='|')
    print(f"\nExport files ready for FTP:")
    print(f"  - Parquet: {export_parquet}")
    print(f"  - Text: {export_txt}")
else:
    print("\nNo data to export - creating empty export files")
    if len(mis_goldtran.columns) > 0:
        empty_df = pl.DataFrame(columns=mis_goldtran.columns)
        empty_df.write_parquet(export_parquet)
        empty_df.write_csv(export_txt, separator='|')

# -----------------------------
# Step 8: Optional DuckDB for appending to MIS
# -----------------------------
try:
    duckdb.sql("INSTALL parquet; LOAD parquet;")
    if len(temp_goldtran) > 0:
        duckdb.sql(f"""
            CREATE OR REPLACE TABLE goldtran AS SELECT * FROM read_parquet('{export_parquet}')
        """)
        print("\nDuckDB table 'goldtran' created successfully")
        result = duckdb.sql("SELECT COUNT(*) FROM goldtran").fetchall()
        print(f"Row count in DuckDB: {result[0][0]}")
    else:
        print("\nNo data to load into DuckDB")
except Exception as e:
    print(f"\nDuckDB operation skipped: {e}")

# -----------------------------
# Step 9: Create metadata file in output directory
# -----------------------------
metadata_file = os.path.join(output_dir, "SAP_PBB_GOLD_GOLDFTPD_metadata.txt")
with open(metadata_file, 'w') as f:
    f.write("=" * 60 + "\n")
    f.write(f"Dataset Export Metadata\n")
    f.write("=" * 60 + "\n")
    f.write(f"Source SAS file: {mis_file}\n")
    f.write(f"Export date: {REPTDT}\n")
    f.write(f"Report month: {REPTMON}\n")
    f.write(f"Report week: {NOWK}\n")
    f.write(f"Report day: {REPTDAY}\n")
    f.write(f"Total rows in source: {len(mis_goldtran)}\n")
    f.write(f"Filtered rows: {len(temp_goldtran)}\n")
    f.write(f"Columns ({len(temp_goldtran.columns)}):\n")
    for col in temp_goldtran.columns:
        f.write(f"  - {col}: {temp_goldtran[col].dtype}\n")
    f.write("-" * 60 + "\n")
    f.write(f"Files created in: {output_dir}\n")
    f.write(f"  - Parquet: {os.path.basename(export_parquet)}\n")
    f.write(f"  - Text: {os.path.basename(export_txt)}\n")
    f.write(f"  - TEMP Parquet: {os.path.basename(temp_file_parquet)}\n")
    f.write(f"  - TEMP Text: {os.path.basename(temp_file_txt)}\n")
    f.write(f"  - TEMP CSV: {os.path.basename(temp_file_csv)}\n")
    f.write("-" * 60 + "\n")
    f.write(f"File formats:\n")
    f.write(f"  - Parquet: Binary columnar storage\n")
    f.write(f"  - Text: Pipe-delimited (|) with headers\n")
    f.write(f"  - CSV: Comma-delimited with headers\n")
    f.write("=" * 60 + "\n")

print(f"\nMetadata file created: {metadata_file}")

# -----------------------------
# Step 10: Summary
# -----------------------------
print("\n" + "=" * 60)
print("PROCESSING COMPLETE")
print("=" * 60)
print(f"Input:  {mis_file} ({len(mis_goldtran)} rows)")
print(f"Output: {len(temp_goldtran)} rows filtered for {REPTDT}")
print(f"Output directory: {output_dir}")
print(f"Files generated: 5 files (Parquet, TXT, CSV, metadata)")
print("=" * 60)

# List all files in output directory
print("\nFiles in output directory:")
for file in sorted(os.listdir(output_dir)):
    file_path = os.path.join(output_dir, file)
    if os.path.isfile(file_path):
        size = os.path.getsize(file_path)
        print(f"  - {file} ({size:,} bytes)")
