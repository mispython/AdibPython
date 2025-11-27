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

# Read SAVING.csv with proper schema overrides
saving = pl.read_csv(
    saving_csv_path,
    infer_schema_length=0,
    schema_overrides={
        'PRODUCT': pl.Utf8,
        'OPENDT': pl.Utf8,
        'CURBAL': pl.Float64,
        'ACCTNO': pl.Utf8,
        'BRANCH': pl.Utf8,
        'ODXSAMT': pl.Float64,
    },
    null_values=["", "NA", "N/A", "null", "NULL"]
)

print(f"SAVING.csv shape: {saving.shape}")

# Analyze the PRODUCT column to find what we should be filtering for
product_analysis = (
    saving
    .group_by("PRODUCT")
    .agg([
        pl.len().alias("count"),
        pl.col("OPENDT").filter(pl.col("OPENDT").str.len_chars() > 0).count().alias("has_opendt"),
        pl.col("CURBAL").mean().alias("avg_balance")
    ])
    .sort("count", descending=True)
    .head(20)
)

print("Top 20 PRODUCT values by count:")
print(product_analysis)

# Check if there are any PRODUCT values that might be related to "208"
# Look for values ending with 208 or containing 208
potential_208_products = (
    saving
    .filter(pl.col("PRODUCT").str.contains("208"))
    .select("PRODUCT")
    .unique()
    .sort("PRODUCT")
)

print(f"PRODUCT values containing '208': {potential_208_products['PRODUCT'].to_list()}")

# Check recent accounts regardless of PRODUCT to see what dates exist
recent_accounts_all = (
    saving
    .filter(pl.col("OPENDT").str.len_chars() > 0)
    .with_columns(pl.col("OPENDT").cast(pl.Int64))
    .filter(pl.col("OPENDT") > 0)
    .select(["PRODUCT", "BRANCH", "ACCTNO", "OPENDT", "CURBAL"])
    .sort("OPENDT", descending=True)
    .head(10)
)

print("Most recent accounts (all products):")
print(recent_accounts_all)

cis = pl.read_parquet(cis_path)
print(f"CIS shape: {cis.shape}")

# -----------------------------
# Step 3: Process BRIGHT data - NEED TO DETERMINE CORRECT PRODUCT CODE
# -----------------------------
# Since we don't know the correct product code, let's process all accounts
# opened on the reporting date and see what we get
all_recent_accounts = (
    saving
    .with_columns(pl.col("OPENDT").cast(pl.Int64))
    .filter(pl.col("OPENDT") > 0)
    .filter(pl.col("OPENDT") == int(reptdte))
    .select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND", "PRODUCT"])
)

print(f"All accounts opened on {reptdte}: {len(all_recent_accounts)}")
if len(all_recent_accounts) > 0:
    print("Accounts opened on reporting date:")
    print(all_recent_accounts)

# For now, let's process the most common product as a test
most_common_product = product_analysis[0, "PRODUCT"]
print(f"Most common PRODUCT: {most_common_product}")

# Process with the most common product
bright = (
    saving.filter(pl.col("PRODUCT") == most_common_product)
    .with_columns(pl.col("OPENDT").cast(pl.Int64))
    .filter(pl.col("OPENDT") > 0)
    .filter(pl.col("OPENDT") == int(reptdte))
    .select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])
)

print(f"Bright accounts found (using most common product {most_common_product}): {len(bright)}")

# -----------------------------
# Step 4: Process CIS data
# -----------------------------
cis_processed = (
    cis.with_columns(pl.col("CUSTOPENDATE").cast(pl.Int64))
    .filter(pl.col("CUSTOPENDATE") > 0)
    .with_columns(
        pl.when(pl.col("PRISEC") == 901)
        .then(pl.lit("N"))
        .otherwise(pl.lit("Y"))
        .alias("JOINT")
    )
    .select(["ACCTNO", "CUSTNAME", "ALIASKEY", "ALIAS", "CUSTOPENDATE", "JOINT"])
)

print(f"CIS processed records: {len(cis_processed)}")

# -----------------------------
# Step 5: Merge datasets
# -----------------------------
if len(bright) > 0:
    new = bright.join(cis_processed, on="ACCTNO", how="inner")
    print(f"Joined records: {len(new)}")
else:
    print("⚠️  No accounts to join - checking if any accounts exist at all...")
    
    # Check total accounts in SAVING data
    total_accounts = len(saving)
    accounts_with_opendt = len(saving.filter(pl.col("OPENDT").str.len_chars() > 0))
    print(f"Total accounts in SAVING: {total_accounts}")
    print(f"Accounts with OPENDT: {accounts_with_opendt}")
    
    # Create empty result
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
# Step 8: Summary
# -----------------------------
print("\n" + "="*50)
print("SUMMARY")
print("="*50)
print(f"SAVING.csv total records: {len(saving)}")
print(f"PRODUCT values analyzed: {len(product_analysis)}")
print(f"Most common PRODUCT: {most_common_product}")
print(f"Accounts opened on {reptdte}: {len(all_recent_accounts)}")
print(f"Final output records: {len(convert)}")
print("="*50)

if len(convert) == 0:
    print("❌ No data processed. Possible issues:")
    print("   - Wrong PRODUCT code (looking for '208' but it doesn't exist)")
    print("   - No accounts opened on the reporting date")
    print("   - Data format issues")
else:
    print("✅ Program completed successfully!")
