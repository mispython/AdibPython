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

# Read SAVING.csv with schema overrides to handle scientific notation
saving = pl.read_csv(
    saving_csv_path,
    infer_schema_length=10000,  # Increase schema inference
    schema_overrides={
        'ODXSAMT': pl.Float64,  # Handle scientific notation as float
        'OPENDT': pl.Utf8,      # Read as string first, then convert
        'CURBAL': pl.Float64,   # Handle potential scientific notation
        'ACCTNO': pl.Utf8,      # Account numbers as string to preserve leading zeros
        'BRANCH': pl.Utf8,      # Branch codes as string
    },
    ignore_errors=False,
    null_values=["", "NA", "N/A", "null"]
)

cis = pl.read_parquet(cis_path)

print(f"SAVING.csv columns: {saving.columns}")
print(f"SAVING.csv shape: {saving.shape}")
print(f"CIS columns: {cis.columns}")
print(f"CIS shape: {cis.shape}")

# -----------------------------
# Steps 3-5: Your processing logic (adjusted for actual column names)
# -----------------------------
bright = (
    saving.filter(pl.col("PRODUCT") == 208)
    .with_columns(pl.col("OPENDT").cast(pl.Int64))  # Convert to int after reading as string
    .filter(pl.col("OPENDT") > 0)
    .filter(pl.col("OPENDT") == int(reptdte))
    .select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])
)

print(f"Bright accounts found: {len(bright)}")

# Check which column to use for joint account determination
# Let's examine potential columns for joint account logic
joint_candidate_columns = [col for col in cis.columns if any(keyword in col.upper() for keyword in ['JOINT', 'SEC', 'PRISEC', 'PRCOUNTRY'])]
print(f"Potential joint account columns in CIS: {joint_candidate_columns}")

# Process CIS data with the correct column names
cis_processed = (
    cis.with_columns(pl.col("CUSTOPENDATE").cast(pl.Int64))
    .filter(pl.col("CUSTOPENDATE") > 0)
    .with_columns(
        # Adjust this logic based on the actual joint account column
        # Using PRCOUNTRY as a placeholder - you may need to change this
        pl.when(pl.col("PRCOUNTRY") == 901).then("N").otherwise("Y").alias("JOINT")
    )
    .select(["ACCTNO", "CUSTNAME", "ALIASKEY", "ALIAS", "CUSTOPENDATE", "JOINT"])
)

print(f"CIS processed records: {len(cis_processed)}")

# Join the datasets
new = bright.join(cis_processed, on="ACCTNO", how="inner")

print(f"Joined records: {len(new)}")

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
