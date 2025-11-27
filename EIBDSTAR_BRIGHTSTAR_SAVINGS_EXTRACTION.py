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

# Format dates for file naming and filtering - using YYMMDD like SAS &REPTDTE
reptyear = REPTDATE.strftime("%y")
reptmon = REPTDATE.strftime("%m") 
reptday = REPTDATE.strftime("%d")
reptdte = REPTDATE.strftime("%y%m%d")  # YYMMDD format like SAS &REPTDTE

print(f"REPTDATE = {REPTDATE.date()}, PREVDATE = {PREVDATE.date()}")
print(f"RDATE (SAS format) = {RDATE}")
print(f"MON={reptmon}, DAY={reptday}, YEAR={reptyear}")
print(f"Filter date (reptdte) = {reptdte}")

# -----------------------------
# Step 2: Load datasets
# -----------------------------
saving_csv_path = DATA_DIR / "SAVING.csv"
cis_path = DATA_DIR / "CIS_CUSTDLY.parquet"

# Check if files exist before processing
if not saving_csv_path.exists():
    raise FileNotFoundError(f"SAVING.csv file not found: {saving_csv_path}")
if not cis_path.exists():
    raise FileNotFoundError(f"CIS file not found: {cis_path}")

# Read SAVING.csv - let Polars infer types first to see what we're dealing with
saving = pl.read_csv(saving_csv_path)
print(f"SAVING.csv dtypes: {saving.dtypes}")

# Check PRODUCT column values
product_values = saving.select("PRODUCT").unique().sort("PRODUCT")
print(f"PRODUCT column unique values: {product_values['PRODUCT'].to_list()}")

# Now read with proper schema overrides
saving = pl.read_csv(
    saving_csv_path,
    schema_overrides={
        'PRODUCT': pl.Utf8,  # Read as string since it contains mixed types
        'OPENDT': pl.Utf8,   # Read as string first
        'CURBAL': pl.Float64,
        'ACCTNO': pl.Utf8,
        'BRANCH': pl.Utf8,
    }
)

cis = pl.read_parquet(cis_path)

print(f"SAVING.csv shape: {saving.shape}")
print(f"CIS shape: {cis.shape}")

# Check if PRISEC exists and show sample values
if 'PRISEC' in cis.columns:
    prisec_sample = cis.select('PRISEC').unique().head(10)
    print(f"PRISEC sample values: {prisec_sample['PRISEC'].to_list()}")
else:
    print("❌ PRISEC column not found in CIS data")

# Check recent OPENDT values in SAVING data to find actual dates
recent_open_dates = (
    saving.filter(pl.col("PRODUCT").cast(pl.Utf8) == "208")  # Compare as string
    .filter(pl.col("OPENDT").str.len_chars() > 0)
    .select("OPENDT")
    .unique()
    .sort("OPENDT", descending=True)
    .head(10)
)
print(f"Recent OPENDT values for PRODUCT 208: {recent_open_dates['OPENDT'].to_list()}")

# -----------------------------
# Step 3: Process BRIGHT data (like SAS)
# -----------------------------
bright = (
    saving.filter(pl.col("PRODUCT").cast(pl.Utf8) == "208")  # Compare as string
    .with_columns(pl.col("OPENDT").cast(pl.Int64))
    .filter(pl.col("OPENDT") > 0)
    .filter(pl.col("OPENDT") == int(reptdte))  # Filter for reporting date
    .select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])
)

print(f"Bright accounts found: {len(bright)}")

# If no accounts found for today, show what we would have found with different dates
if len(bright) == 0:
    print("⚠️  No accounts found for the reporting date. Checking other recent dates...")
    # Show accounts from last 30 days
    recent_accounts = (
        saving.filter(pl.col("PRODUCT").cast(pl.Utf8) == "208")
        .with_columns(pl.col("OPENDT").cast(pl.Int64))
        .filter(pl.col("OPENDT") > 0)
        .filter(pl.col("OPENDT") >= 251101)  # November 2025 dates
        .select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])
        .sort("OPENDT", descending=True)
    )
    print(f"Recent PRODUCT 208 accounts (last 30 days): {len(recent_accounts)}")
    if len(recent_accounts) > 0:
        print("Sample of recent accounts:")
        print(recent_accounts.head(5))

# -----------------------------
# Step 4: Process CIS data (with PRISEC field)
# -----------------------------
cis_processed = (
    cis.with_columns(pl.col("CUSTOPENDATE").cast(pl.Int64))
    .filter(pl.col("CUSTOPENDATE") > 0)
    .with_columns(
        # Original SAS logic: IF PRISEC = 901 THEN 'N' ELSE 'Y'
        pl.when(pl.col("PRISEC") == 901)
        .then(pl.lit("N"))
        .otherwise(pl.lit("Y"))
        .alias("JOINT")
    )
    .select(["ACCTNO", "CUSTNAME", "ALIASKEY", "ALIAS", "CUSTOPENDATE", "JOINT"])
)

print(f"CIS processed records: {len(cis_processed)}")

# Check joint account distribution
joint_dist = cis_processed.group_by("JOINT").agg(pl.len())
print(f"Joint account distribution: {joint_dist}")

# -----------------------------
# Step 5: Merge datasets (like SAS MERGE)
# -----------------------------
if len(bright) > 0:
    new = bright.join(cis_processed, on="ACCTNO", how="inner")
    print(f"Joined records: {len(new)}")
else:
    print("⚠️  No accounts to join - creating empty result")
    # Create empty dataframe with correct schema
    new = pl.DataFrame({
        'BRANCH': [], 'ACCTNO': [], 'OPENDT': [], 'CURBAL': [], 'OPENIND': [],
        'CUSTNAME': [], 'ALIASKEY': [], 'ALIAS': [], 'CUSTOPENDATE': [], 'JOINT': []
    })

# -----------------------------
# Step 6: Convert and format
# -----------------------------
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
# Step 7: Save output
# -----------------------------
output_file = OUTPUT_DIR / f"BRIGHTSTAR_SAVINGS_{reptyear}{reptmon}{reptday}.parquet"
convert.write_parquet(output_file)

print(f"Output written to {output_file}")

# -----------------------------
# Step 8: Load to DuckDB (with error handling)
# -----------------------------
try:
    # Try to load parquet extension (might already be installed)
    duckdb.sql("LOAD parquet;")
except Exception as e:
    print(f"Note: Parquet extension already loaded or not needed: {e}")

try:
    output_file_str = str(output_file)
    duckdb.sql(f"""
        CREATE OR REPLACE TABLE brightstar_savings 
        AS SELECT * FROM read_parquet('{output_file_str}')
    """)
    print("DuckDB table 'brightstar_savings' ready")
    result = duckdb.sql("SELECT COUNT(*) as record_count FROM brightstar_savings").fetchall()
    print(f"Records in DuckDB table: {result[0][0]}")
except Exception as e:
    print(f"DuckDB loading skipped: {e}")

if len(convert) == 0:
    print("⚠️  No data processed - no accounts opened on the reporting date")
else:
    print("✅ Program completed successfully!")
