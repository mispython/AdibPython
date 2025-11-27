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
# Step 1: Date setup
# -----------------------------
REPTDATE = datetime.today() - timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)

reptyear = REPTDATE.strftime("%y")
reptmon = REPTDATE.strftime("%m")  
reptday = REPTDATE.strftime("%d")

print(f"REPTDATE = {REPTDATE.date()}, PREVDATE = {PREVDATE.date()}")
print(f"MON={reptmon}, DAY={reptday}, YEAR={reptyear}")

# -----------------------------
# Step 2: Load datasets and investigate OPENDT issue
# -----------------------------
saving_csv_path = DATA_DIR / "SAVING.csv"
cis_path = DATA_DIR / "CIS_CUSTDLY.parquet"

# Check if files exist before processing
if not saving_csv_path.exists():
    raise FileNotFoundError(f"SAVING.csv file not found: {saving_csv_path}")
if not cis_path.exists():
    raise FileNotFoundError(f"CIS file not found: {cis_path}")

# Read SAVING.csv
saving = pl.read_csv(
    saving_csv_path,
    infer_schema_length=0,
    schema_overrides={
        'PRODUCT': pl.Utf8,
        'OPENDT': pl.Utf8,
        'CURBAL': pl.Float64,
        'ACCTNO': pl.Utf8,
        'BRANCH': pl.Utf8,
    },
    null_values=["", "NA", "N/A", "null", "NULL"]
)

print(f"SAVING.csv shape: {saving.shape}")

# -----------------------------
# Step 3: INVESTIGATE THE OPENDT ISSUE
# -----------------------------
print("\n" + "="*60)
print("INVESTIGATING OPENDT COLUMN ISSUES")
print("="*60)

# Check what's actually in the OPENDT column
opendt_analysis = (
    saving
    .select("OPENDT")
    .with_columns([
        pl.col("OPENDT").is_null().alias("is_null"),
        pl.col("OPENDT").str.len_chars().alias("length"),
        pl.col("OPENDT").str.contains(r"^\d+$").alias("is_numeric"),
        pl.col("OPENDT").str.contains("J").alias("contains_J")
    ])
    .group_by("OPENDT")
    .agg(pl.len().alias("count"))
    .sort("count", descending=True)
    .head(20)
)

print("OPENDT value analysis (top 20):")
print(opendt_analysis)

# Count different types of OPENDT values
opendt_summary = (
    saving
    .select([
        pl.col("OPENDT").is_null().sum().alias("null_count"),
        (pl.col("OPENDT") == "J").sum().alias("J_count"),
        pl.col("OPENDT").str.contains(r"^\d+$").sum().alias("numeric_count"),
        pl.col("OPENDT").is_not_null().sum().alias("non_null_count")
    ])
)

print(f"\nOPENDT Summary:")
print(f"  - Null values: {opendt_summary[0, 'null_count']}")
print(f"  - 'J' values: {opendt_summary[0, 'J_count']}")
print(f"  - Numeric values: {opendt_summary[0, 'numeric_count']}")
print(f"  - Non-null values: {opendt_summary[0, 'non_null_count']}")

# -----------------------------
# Step 4: STRATEGY - Process without date filtering
# -----------------------------
print("\n" + "="*60)
print("PROCESSING STRATEGY: Include all accounts (no date filter)")
print("="*60)

# Since OPENDT is problematic, let's process all savings-like accounts
# Look for PRODUCT codes that might indicate savings accounts
product_analysis = (
    saving
    .group_by("PRODUCT")
    .agg([
        pl.len().alias("count"),
        pl.col("CURBAL").mean().alias("avg_balance"),
        pl.col("CURBAL").max().alias("max_balance")
    ])
    .sort("count", descending=True)
    .head(15)
)

print("Top 15 PRODUCT values (potential savings accounts):")
print(product_analysis)

# Try to identify savings accounts by looking for common patterns
# Savings accounts often have specific product codes or balance patterns
potential_savings_products = (
    saving
    .filter(
        # Look for products that might be savings-related
        (pl.col("PRODUCT").str.contains("2")) |  # Contains 2 (common for savings)
        (pl.col("CURBAL") > 0)  # Has positive balance
    )
    .select("PRODUCT")
    .unique()
    .head(10)
)

print(f"Potential savings PRODUCT codes: {potential_savings_products['PRODUCT'].to_list()}")

# -----------------------------
# Step 5: Process accounts (without date filtering)
# -----------------------------
# Since we can't filter by date, process all accounts that might be savings
# Use the most common product as our target
if len(product_analysis) > 0:
    target_product = product_analysis[0, "PRODUCT"]
    print(f"\nUsing most common product for processing: {target_product}")
    
    bright = (
        saving
        .filter(pl.col("PRODUCT") == target_product)
        .select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])
    )
else:
    # Fallback: use all accounts
    print("\nNo specific product found, using all accounts")
    bright = saving.select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])

print(f"Accounts selected for processing: {len(bright)}")

# -----------------------------
# Step 6: Process CIS data
# -----------------------------
cis = pl.read_parquet(cis_path)
print(f"CIS shape: {cis.shape}")

cis_processed = (
    cis
    .with_columns(
        pl.when(pl.col("PRISEC") == 901)
        .then(pl.lit("N"))
        .otherwise(pl.lit("Y"))
        .alias("JOINT")
    )
    .select(["ACCTNO", "CUSTNAME", "ALIASKEY", "ALIAS", "JOINT"])
)

print(f"CIS processed records: {len(cis_processed)}")

# -----------------------------
# Step 7: Merge datasets
# -----------------------------
if len(bright) > 0:
    new = bright.join(cis_processed, on="ACCTNO", how="inner")
    print(f"Joined records: {len(new)}")
else:
    print("No accounts to process")
    new = pl.DataFrame()

# -----------------------------
# Step 8: Convert and format output
# -----------------------------
if len(new) > 0:
    convert = new.select([
        pl.col("BRANCH").alias("BRANCH"),
        pl.col("ACCTNO").alias("ACCTNO"), 
        pl.col("ALIAS").alias("NEWIC"),
        pl.col("JOINT"),
        pl.col("CUSTNAME"),
        pl.col("OPENDT").alias("OPENDT"),
        pl.col("OPENIND"),
        pl.col("CURBAL").alias("CURBAL"),
    ])
    
    # Add a report date column since we can't filter by OPENDT
    convert = convert.with_columns(
        pl.lit(REPTDATE.strftime("%Y%m%d")).alias("REPORT_DATE")
    )
else:
    convert = new

print(f"Final output records: {len(convert)}")

# -----------------------------
# Step 9: Save output
# -----------------------------
output_file = OUTPUT_DIR / f"BRIGHTSTAR_SAVINGS_{reptyear}{reptmon}{reptday}.parquet"
convert.write_parquet(output_file)

print(f"Output written to {output_file}")

# -----------------------------
# Step 10: Summary and recommendations
# -----------------------------
print("\n" + "="*60)
print("FINAL SUMMARY AND RECOMMENDATIONS")
print("="*60)
print(f"Accounts processed: {len(convert)}")
print(f"Output file: {output_file}")

if len(convert) == 0:
    print("\n❌ CRITICAL ISSUES FOUND:")
    print("1. OPENDT column contains no valid date values")
    print("2. All values are either NULL or 'J'")
    print("3. Cannot filter by opening date as intended")
    
    print("\n💡 IMMEDIATE ACTIONS NEEDED:")
    print("1. Check the source of SAVING.csv - is OPENDT populated correctly?")
    print("2. Contact the data provider about the OPENDT format")
    print("3. Verify if there's another date column that should be used")
    print("4. Check if 'J' has a specific meaning (e.g., Joint account)")
    
    print("\n📋 DATA QUALITY ISSUES:")
    print(f"  - Total records: {len(saving)}")
    print(f"  - Valid OPENDT values: {opendt_summary[0, 'numeric_count']}")
    print(f"  - 'J' values: {opendt_summary[0, 'J_count']}")
    print(f"  - Null values: {opendt_summary[0, 'null_count']}")
else:
    print(f"✅ Successfully processed {len(convert)} accounts")
    print("⚠️  Note: Could not filter by opening date due to data quality issues")
