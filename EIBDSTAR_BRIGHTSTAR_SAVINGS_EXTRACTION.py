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
# Step 1: Use MMDDYY format like the original SAS program
# -----------------------------
# The original SAS uses MMDDYY8. format, not SAS numeric dates
REPTDATE = datetime.today() - timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)

# Format dates like the SAS program - MMDDYY format
reptyear = REPTDATE.strftime("%y")  # 2-digit year
reptmon = REPTDATE.strftime("%m")   # 2-digit month  
reptday = REPTDATE.strftime("%d")   # 2-digit day

# These are the formats used in the SAS program:
reptdte_mmddyy = f"{reptmon}{reptday}{reptyear}"  # MMDDYY - for OPENDT comparison (6-digit)
reptdte_yymmdd = f"{reptyear}{reptmon}{reptday}"  # YYMMDD - alternative format

print(f"REPTDATE = {REPTDATE.date()}, PREVDATE = {PREVDATE.date()}")
print(f"MON={reptmon}, DAY={reptday}, YEAR={reptyear}")
print(f"MMDDYY format (reptdte_mmddyy) = {reptdte_mmddyy}")
print(f"YYMMDD format (reptdte_yymmdd) = {reptdte_yymmdd}")

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
        'OPENDT': pl.Utf8,  # Keep as string to handle various formats
        'CURBAL': pl.Float64,
        'ACCTNO': pl.Utf8,
        'BRANCH': pl.Utf8,
        'ODXSAMT': pl.Float64,
    },
    null_values=["", "NA", "N/A", "null", "NULL", "J"]
)

print(f"SAVING.csv shape: {saving.shape}")

# Analyze the OPENDT column to understand the actual format
opendt_sample = (
    saving
    .filter(pl.col("OPENDT").is_not_null())
    .select("OPENDT")
    .unique()
    .filter(pl.col("OPENDT").str.len_chars().is_between(6, 8))  # Reasonable date lengths
    .head(10)
)

print("Sample OPENDT values:")
print(opendt_sample)

# Check what PRODUCT values might correspond to savings accounts
product_analysis = (
    saving
    .group_by("PRODUCT")
    .agg([
        pl.len().alias("count"),
        pl.col("CURBAL").mean().alias("avg_balance")
    ])
    .sort("count", descending=True)
    .head(10)
)

print("Top 10 PRODUCT values:")
print(product_analysis)

cis = pl.read_parquet(cis_path)
print(f"CIS shape: {cis.shape}")

# -----------------------------
# Step 3: Process BRIGHT data - Try different date formats
# -----------------------------
# Try filtering with MMDDYY format (like original SAS)
bright_mmddyy = (
    saving
    .filter(pl.col("OPENDT") == reptdte_mmddyy)  # MMDDYY format
    .select(["PRODUCT", "BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])
)

print(f"Accounts with OPENDT = {reptdte_mmddyy} (MMDDYY): {len(bright_mmddyy)}")

# Try filtering with YYMMDD format 
bright_yymmdd = (
    saving
    .filter(pl.col("OPENDT") == reptdte_yymmdd)  # YYMMDD format
    .select(["PRODUCT", "BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])
)

print(f"Accounts with OPENDT = {reptdte_yymmdd} (YYMMDD): {len(bright_yymmdd)}")

# Use whichever format gives us results
if len(bright_mmddyy) > 0:
    bright = bright_mmddyy
    date_format_used = "MMDDYY"
    print(f"Using MMDDYY format - found {len(bright)} accounts")
elif len(bright_yymmdd) > 0:
    bright = bright_yymmdd  
    date_format_used = "YYMMDD"
    print(f"Using YYMMDD format - found {len(bright)} accounts")
else:
    # No accounts found with either format, show what dates exist
    recent_dates = (
        saving
        .filter(pl.col("OPENDT").str.len_chars().is_between(6, 8))
        .select("OPENDT")
        .unique()
        .sort("OPENDT", descending=True)
        .head(10)
    )
    print(f"No accounts found for today. Recent OPENDT values: {recent_dates['OPENDT'].to_list()}")
    
    # Create empty result
    bright = pl.DataFrame()
    date_format_used = "None"

# If we found accounts, filter for PRODUCT = 208 (or similar)
if len(bright) > 0:
    # Check if PRODUCT 208 exists in the found accounts
    products_in_bright = bright.select("PRODUCT").unique()
    print(f"PRODUCT values in found accounts: {products_in_bright['PRODUCT'].to_list()}")
    
    # Try to find a product code that might be savings account
    # Look for products containing savings-related patterns
    savings_products = (
        bright
        .filter(
            pl.col("PRODUCT").str.contains("208") |  # Contains 208
            pl.col("PRODUCT").str.contains("2")      # Contains 2 (savings indicator)
        )
        .select("PRODUCT")
        .unique()
    )
    
    if len(savings_products) > 0:
        target_product = savings_products[0, "PRODUCT"]
        print(f"Using product code for savings: {target_product}")
        
        bright = bright.filter(pl.col("PRODUCT") == target_product)
    else:
        # Use all found accounts if no specific product filter
        print("No specific savings product found, using all accounts")
    
    bright = bright.select(["BRANCH", "ACCTNO", "OPENDT", "CURBAL", "OPENIND"])

print(f"Final bright accounts: {len(bright)}")

# -----------------------------
# Step 4: Process CIS data
# -----------------------------
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
# Step 5: Merge datasets
# -----------------------------
if len(bright) > 0:
    new = bright.join(cis_processed, on="ACCTNO", how="inner")
    print(f"Joined records: {len(new)}")
else:
    print("No accounts to process")
    new = pl.DataFrame()

# -----------------------------
# Step 6: Convert and format output
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
else:
    convert = new

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
print("PROGRAM SUMMARY")
print("="*50)
print(f"Date format used: {date_format_used}")
print(f"Reporting date: {reptdte_mmddyy} (MMDDYY)")
print(f"Accounts found: {len(convert)}")
print(f"Output file: {output_file}")
print("="*50)

if len(convert) == 0:
    print("\n💡 SUGGESTIONS:")
    print("1. Check the actual OPENDT format in your SAVING.csv")
    print("2. Verify the correct PRODUCT code for savings accounts") 
    print("3. Check if accounts exist for the reporting date")
    print("4. The date format might be different (YYYYMMDD, DDMMYY, etc.)")
else:
    print("✅ Success! Accounts processed and saved.")
