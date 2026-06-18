# eibaegld.py
import polars as pl
import pyarrow.parquet as pq
import duckdb
from datetime import date
import sas7bdat  # You'll need to install: pip install sas7bdat

# -----------------------------
# Step 1: Define report date (like SAS SET DEPOSIT.REPTDATE)
# -----------------------------
today = date.today()
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
# Step 2: Load MIS gold transaction dataset from SAS
# -----------------------------
# SAS: MIS.GOLDTRAN&REPTMON&NOWK
# Input format: goldtran{MONTH}{WEEK}.sas7bdat
# Example: goldtran012.sas7bdat (month=01, week=2)
mis_file = f"goldtran{REPTMON}{NOWK}.sas7bdat"

print(f"Looking for input file: {mis_file}")

try:
    # Option 1: Using sas7bdat library
    with sas7bdat.SAS7BDAT(mis_file) as reader:
        # Convert to list of dicts then to Polars DataFrame
        data = list(reader)
        mis_goldtran = pl.DataFrame(data)
    
    # Option 2: Alternative using pandas (if sas7bdat gives issues)
    # import pandas as pd
    # df_pandas = pd.read_sas(mis_file, format='sas7bdat')
    # mis_goldtran = pl.from_pandas(df_pandas)
    
except FileNotFoundError:
    raise FileNotFoundError(f"Missing input SAS file: {mis_file}")
except Exception as e:
    print(f"Error reading SAS file: {e}")
    # Try alternative with pandas
    try:
        import pandas as pd
        df_pandas = pd.read_sas(mis_file, format='sas7bdat')
        mis_goldtran = pl.from_pandas(df_pandas)
        print(f"Successfully loaded using pandas fallback")
    except Exception as e2:
        raise Exception(f"Failed to load SAS file with both methods: {e2}")

print(f"Loaded {len(mis_goldtran)} rows from SAS dataset: {mis_file}")
print(f"Columns: {mis_goldtran.columns}")

# -----------------------------
# Step 3: Filter by REPTDATE
# -----------------------------
# Check if REPTDATE exists in the dataset
if "REPTDATE" not in mis_goldtran.columns:
    print("Warning: REPTDATE column not found. Available columns:")
    print(mis_goldtran.columns)
    # If REPTDATE not found, you might need to use a different column name
    # or skip filtering
    temp_goldtran = mis_goldtran.clone()
else:
    # Check the data type of REPTDATE
    if mis_goldtran["REPTDATE"].dtype in [pl.Int32, pl.Int64]:
        # SAS date is days since 1960-01-01
        # Convert SAS numeric date to string format YYYY-MM-DD
        from datetime import datetime, timedelta
        sas_epoch = datetime(1960, 1, 1)
        mis_goldtran = mis_goldtran.with_columns(
            pl.col("REPTDATE").map_elements(
                lambda x: (sas_epoch + timedelta(days=x)).strftime("%Y-%m-%d") if x is not None else None,
                return_dtype=pl.Utf8
            ).alias("REPTDATE_STR")
        )
        temp_goldtran = mis_goldtran.filter(pl.col("REPTDATE_STR") == REPTDT)
    else:
        # Assume it's already a string or date
        # Convert to string if needed
        if mis_goldtran["REPTDATE"].dtype == pl.Date:
            mis_goldtran = mis_goldtran.with_columns(
                pl.col("REPTDATE").cast(pl.Utf8).alias("REPTDATE_STR")
            )
            temp_goldtran = mis_goldtran.filter(pl.col("REPTDATE_STR") == REPTDT)
        else:
            temp_goldtran = mis_goldtran.filter(pl.col("REPTDATE") == REPTDT)

print(f"Filtered to {len(temp_goldtran)} rows for date {REPTDT}")
if len(temp_goldtran) > 0:
    print("Sample data (first 5 rows):")
    print(temp_goldtran.head())
else:
    print("WARNING: No records found for date {REPTDT}")

# -----------------------------
# Step 4: Write to TEMP dataset
# -----------------------------
# Equivalent to: DATA TEMP.GOLDTRAND&REPTDAY
temp_file_parquet = f"TEMP_GOLDTRAND_{REPTDAY}.parquet"
temp_file_txt = f"TEMP_GOLDTRAND_{REPTDAY}.txt"
temp_file_csv = f"TEMP_GOLDTRAND_{REPTDAY}.csv"

# Write Parquet
if len(temp_goldtran) > 0:
    temp_goldtran.write_parquet(temp_file_parquet)
    
    # Write TXT (pipe-delimited text file - common for SAS export)
    temp_goldtran.write_csv(temp_file_txt, separator='|')
    
    # Write CSV (comma-delimited)
    temp_goldtran.write_csv(temp_file_csv)
    
    print(f"TEMP dataset written to:")
    print(f"  - Parquet: {temp_file_parquet}")
    print(f"  - Text (pipe): {temp_file_txt}")
    print(f"  - CSV: {temp_file_csv}")
else:
    print("No data to write - creating empty files")
    # Create empty files with headers
    if len(mis_goldtran.columns) > 0:
        pl.DataFrame(columns=mis_goldtran.columns).write_parquet(temp_file_parquet)
        pl.DataFrame(columns=mis_goldtran.columns).write_csv(temp_file_txt, separator='|')
        pl.DataFrame(columns=mis_goldtran.columns).write_csv(temp_file_csv)

# -----------------------------
# Step 5: Export for FTP (SAS CPORT replacement)
# -----------------------------
# SAS used PROC CPORT (binary SAS library dump). Here we export Parquet and TXT.
export_parquet = "SAP_PBB_GOLD_GOLDFTPD.parquet"
export_txt = "SAP_PBB_GOLD_GOLDFTPD.txt"

if len(temp_goldtran) > 0:
    temp_goldtran.write_parquet(export_parquet)
    temp_goldtran.write_csv(export_txt, separator='|')
    print(f"Export files ready for FTP:")
    print(f"  - Parquet: {export_parquet}")
    print(f"  - Text: {export_txt}")
else:
    print("No data to export - creating empty export files")
    if len(mis_goldtran.columns) > 0:
        pl.DataFrame(columns=mis_goldtran.columns).write_parquet(export_parquet)
        pl.DataFrame(columns=mis_goldtran.columns).write_csv(export_txt, separator='|')

# -----------------------------
# Step 6: Optional DuckDB for appending to MIS
# -----------------------------
try:
    duckdb.sql("INSTALL parquet; LOAD parquet;")
    if len(temp_goldtran) > 0:
        duckdb.sql(f"""
            CREATE OR REPLACE TABLE goldtran AS SELECT * FROM read_parquet('{export_parquet}')
        """)
        print("DuckDB table 'goldtran' created successfully")
        result = duckdb.sql("SELECT COUNT(*) FROM goldtran").fetchall()
        print(f"Row count in DuckDB: {result[0][0]}")
    else:
        print("No data to load into DuckDB")
except Exception as e:
    print(f"DuckDB operation skipped: {e}")

# -----------------------------
# Step 7: Create a SAS-compatible text file with metadata
# -----------------------------
metadata_file = f"SAP_PBB_GOLD_GOLDFTPD_metadata.txt"
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
    f.write(f"Columns ({len(temp_goldtran.columns)}): {', '.join(temp_goldtran.columns)}\n")
    f.write("-" * 60 + "\n")
    f.write(f"Files created:\n")
    f.write(f"  - Parquet: {export_parquet}\n")
    f.write(f"  - Text: {export_txt}\n")
    f.write(f"  - TEMP Parquet: {temp_file_parquet}\n")
    f.write(f"  - TEMP Text: {temp_file_txt}\n")
    f.write(f"  - TEMP CSV: {temp_file_csv}\n")
    f.write("-" * 60 + "\n")
    f.write(f"File formats:\n")
    f.write(f"  - Parquet: Binary columnar storage\n")
    f.write(f"  - Text: Pipe-delimited (|) with headers\n")
    f.write(f"  - CSV: Comma-delimited with headers\n")
    f.write("=" * 60 + "\n")

print(f"Metadata file created: {metadata_file}")

# -----------------------------
# Step 8: Summary
# -----------------------------
print("\n" + "=" * 60)
print("PROCESSING COMPLETE")
print("=" * 60)
print(f"Input:  {mis_file} ({len(mis_goldtran)} rows)")
print(f"Output: {len(temp_goldtran)} rows filtered for {REPTDT}")
print(f"Files generated: 5 files (Parquet, TXT, CSV, metadata)")
print("=" * 60)
