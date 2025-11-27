from pathlib import Path
import duckdb
import polars as pl
from datetime import datetime, timedelta

# Define paths clearly
DATA_DIR = Path("data")
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)  # Ensure output directory exists

# -----------------------------
# Step 1: Use SAS date format instead of DATEFILE
# -----------------------------
# Calculate dates using SAS format
SAS_ORIGIN = datetime(1960, 1, 1)
REPTDATE = datetime.today() - timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)
RDATE = (REPTDATE - SAS_ORIGIN).days

# Format dates for file naming and filtering
reptyear = REPTDATE.strftime("%y")
reptmon = REPTDATE.strftime("%m")
reptday = REPTDATE.strftime("%d")
reptdte = REPTDATE.strftime("%y%m%d")  # YYMMDD format for integer comparison

print(f"REPTDATE = {REPTDATE.date()}, PREVDATE = {PREVDATE.date()}")
print(f"RDATE (SAS format) = {RDATE}")
print(f"MON={reptmon}, DAY={reptday}, YEAR={reptyear}")
print(f"Filter date (reptdte) = {reptdte}")

# -----------------------------
# Step 2: Load datasets with clear paths - using CSV instead of parquet
# -----------------------------
saving_csv_path = DATA_DIR / "SAVING.csv"
cis_path = DATA_DIR / "CIS_CUSTDLY.parquet"

# Check if files exist before processing
if not saving_csv_path.exists():
    raise FileNotFoundError(f"SAVING.csv file not found: {saving_csv_path}")
if not cis_path.exists():
    raise FileNotFoundError(f"CIS file not found: {cis_path}")

# Read SAVING.csv instead of parquet
saving = pl.read_csv(saving_csv_path)
cis = pl.read_parquet(cis_path)

print(f"SAVING.csv columns: {saving.columns}")
print(f"SAVING.csv shape: {saving.shape}")

# -----------------------------
# Steps 3-5: Your processing logic (adjusted for CSV column names if needed)
# -----------------------------
bright = (
    saving.filter(pl.col("PRODUCT") == 208)
    .with_columns(pl.col("OPENDT").cast(pl.Int64))
    .filter(pl.col("OPENDT") > 0)
    .filter(pl.col("OPENDT") == int(reptdte))
    .select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])
)

cis_processed = (
    cis.with_columns(pl.col("CUSTOPENDATE").cast(pl.Int64))
    .filter(pl.col("CUSTOPENDATE") > 0)
    .with_columns(
        pl.when(pl.col("PRISEC") == 901).then("N").otherwise("Y").alias("JOINT")
    )
    .select(["ACCTNO", "CUSTNAME", "ALIASKEY", "ALIAS", "CUSTOPENDATE", "JOINT"])
)

new = bright.join(cis_processed, on="ACCTNO", how="inner")

convert = new.select([
    pl.col("BRANCH").cast(pl.Utf8).alias("BRANCH"),
    pl.col("ACCTNO").cast(pl.Utf8).alias("ACCTNO"),
    pl.col("ALIAS").alias("NEWIC"),
    pl.col("JOINT"),
    pl.col("CUSTNAME"),
    pl.col("OPENDT").cast(pl.Utf8).alias("OPENDT"),
    pl.col("OPENIND"),
    pl.col("CUSTOPENDATE").cast(pl.Utf8).alias("CUSTOPDT"),
    pl.col("CURBAL").cast(pl.Float64).alias("CURBAL"),
])

print(f"Final output records: {len(convert)}")

# -----------------------------
# Step 6: Save with pathlib - much cleaner
# -----------------------------
output_file = OUTPUT_DIR / f"BRIGHTSTAR_SAVINGS_{reptyear}{reptmon}{reptday}.parquet"
convert.write_parquet(output_file)

print(f"Output written to {output_file}")

# -----------------------------
# Step 7: DuckDB with pathlib path conversion
# -----------------------------
duckdb.sql("INSTALL parquet; LOAD parquet;")

# Convert Path object to string for DuckDB
output_file_str = str(output_file)
duckdb.sql(f"""
    CREATE OR REPLACE TABLE brightstar_savings 
    AS SELECT * FROM read_parquet('{output_file_str}')
""")

print("DuckDB table 'brightstar_savings' ready")
result = duckdb.sql("SELECT COUNT(*) as record_count FROM brightstar_savings").fetchall()
print(f"Records in DuckDB table: {result[0][0]}")
