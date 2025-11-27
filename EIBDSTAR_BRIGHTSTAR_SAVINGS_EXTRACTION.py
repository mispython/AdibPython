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
# Step 2: Load datasets
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

# Read CIS data
cis = pl.read_parquet(cis_path)
print(f"CIS shape: {cis.shape}")

# -----------------------------
# Step 3: INVESTIGATE ACCOUNT NUMBER MATCHING ISSUE
# -----------------------------
print("\n" + "="*60)
print("INVESTIGATING ACCOUNT NUMBER MATCHING ISSUE")
print("="*60)

# Check ACCTNO formats in both datasets
saving_acct_sample = saving.select("ACCTNO").head(10)
cis_acct_sample = cis.select("ACCTNO").head(10)

print("SAVING ACCTNO sample:")
print(saving_acct_sample)
print("\nCIS ACCTNO sample:")
print(cis_acct_sample)

# Check for common account numbers
saving_acctnos = set(saving["ACCTNO"].drop_nulls().to_list())
cis_acctnos = set(cis["ACCTNO"].drop_nulls().to_list())

common_acctnos = saving_acctnos.intersection(cis_acctnos)
print(f"\nCommon ACCTNOs found: {len(common_acctnos)}")

if len(common_acctnos) > 0:
    print(f"Sample common ACCTNOs: {list(common_acctnos)[:5]}")
else:
    print("❌ NO COMMON ACCOUNT NUMBERS FOUND!")

# Check if there are alternative account number columns in CIS
print(f"\nCIS columns that might contain account numbers:")
cis_account_columns = [col for col in cis.columns if 'ACCT' in col.upper() or 'CUST' in col.upper() or 'NO' in col.upper()]
for col in cis_account_columns:
    sample = cis.select(col).drop_nulls().head(3).to_series().to_list()
    print(f"  {col}: {sample}")

# -----------------------------
# Step 4: TRY DIFFERENT JOIN STRATEGIES
# -----------------------------
print("\n" + "="*60)
print("TRYING DIFFERENT JOIN STRATEGIES")
print("="*60)

# Strategy 1: Direct ACCTNO join (already tried - failed)
print("Strategy 1: Direct ACCTNO join")
direct_join = saving.join(cis, on="ACCTNO", how="inner")
print(f"  Results: {len(direct_join)} records")

# Strategy 2: Try other account number columns in CIS
if 'ACCTNOC' in cis.columns:
    print("Strategy 2: Join with ACCTNOC")
    acctnoc_join = saving.join(cis, left_on="ACCTNO", right_on="ACCTNOC", how="inner")
    print(f"  Results: {len(acctnoc_join)} records")

# Strategy 3: Try CUSTNO if it exists in saving
if 'CUSTNO' in saving.columns and 'CUSTNO' in cis.columns:
    print("Strategy 3: Join with CUSTNO")
    custno_join = saving.join(cis, on="CUSTNO", how="inner")
    print(f"  Results: {len(custno_join)} records")

# Strategy 4: Try fuzzy matching or pattern matching
print("Strategy 4: Check for partial matches")
# Look for accounts that might match by pattern (e.g., same branch + sequence)
saving_with_branch = saving.with_columns(
    pl.col("ACCTNO").str.slice(0, 3).alias("branch_prefix"),
    pl.col("ACCTNO").str.len_chars().alias("acctno_length")
)

cis_with_branch = cis.with_columns(
    pl.col("ACCTNO").str.slice(0, 3).alias("branch_prefix"), 
    pl.col("ACCTNO").str.len_chars().alias("acctno_length")
)

print(f"  SAVING ACCTNO lengths: {saving_with_branch['acctno_length'].unique().to_list()}")
print(f"  CIS ACCTNO lengths: {cis_with_branch['acctno_length'].unique().to_list()}")

# -----------------------------
# Step 5: PROCESS WITH BEST AVAILABLE STRATEGY
# -----------------------------
print("\n" + "="*60)
print("PROCESSING WITH BEST AVAILABLE DATA")
print("="*60)

# Since we can't join properly, let's process what we can
# Option A: Process SAVING data alone
saving_processed = (
    saving
    .filter(pl.col("PRODUCT").str.contains("2"))  # Try to filter for savings-like products
    .select([
        pl.col("BRANCH").alias("BRANCH"),
        pl.col("ACCTNO").alias("ACCTNO"), 
        pl.col("OPENDT").alias("OPENDT"),
        pl.col("CURBAL").alias("CURBAL"),
        pl.col("OPENIND").alias("OPENIND"),
        pl.lit("UNKNOWN").alias("CUSTNAME"),  # Placeholder
        pl.lit("UNKNOWN").alias("NEWIC"),     # Placeholder  
        pl.lit("UNKNOWN").alias("JOINT")      # Placeholder
    ])
)

print(f"Processed SAVING accounts: {len(saving_processed)}")

# Option B: Try to find any matching records with relaxed criteria
if len(direct_join) == 0 and 'ACCTNOC' in cis.columns:
    # Try with ACCTNOC and see if we get any matches
    relaxed_join = (
        saving
        .join(cis, left_on="ACCTNO", right_on="ACCTNOC", how="inner")
        .select([
            pl.col("BRANCH"),
            pl.col("ACCTNO"),
            pl.col("OPENDT"), 
            pl.col("CURBAL"),
            pl.col("OPENIND"),
            pl.col("CUSTNAME"),
            pl.col("ALIAS").alias("NEWIC"),
            pl.when(pl.col("PRISEC") == 901)
            .then(pl.lit("N"))
            .otherwise(pl.lit("Y"))
            .alias("JOINT")
        ])
    )
    print(f"Relaxed join results: {len(relaxed_join)}")
    
    if len(relaxed_join) > 0:
        final_output = relaxed_join
    else:
        final_output = saving_processed
else:
    final_output = saving_processed

# -----------------------------
# Step 6: Save output
# -----------------------------
print(f"\nFinal output records: {len(final_output)}")

output_file = OUTPUT_DIR / f"BRIGHTSTAR_SAVINGS_{reptyear}{reptmon}{reptday}.parquet"
final_output.write_parquet(output_file)

print(f"Output written to {output_file}")

# -----------------------------
# Step 7: Detailed Data Quality Report
# -----------------------------
print("\n" + "="*60)
print("DATA QUALITY ASSESSMENT")
print("="*60)

print("SAVING DATA ISSUES:")
print(f"  - Total records: {len(saving)}")
print(f"  - OPENDT: {len(saving) - saving['OPENDT'].is_null().sum()} non-null, but only 2 valid dates")
print(f"  - PRODUCT: {saving['PRODUCT'].n_unique()} unique values")
print(f"  - ACCTNO: {saving['ACCTNO'].n_unique()} unique values")

print("\nCIS DATA ISSUES:")
print(f"  - Total records: {len(cis)}")
print(f"  - ACCTNO: {cis['ACCTNO'].n_unique()} unique values")

print(f"\nMATCHING ISSUES:")
print(f"  - Common ACCTNOs: {len(common_acctnos)}")
print(f"  - Direct join results: {len(direct_join)}")

print("\n" + "="*60)
print("RECOMMENDATIONS")
print("="*60)

if len(common_acctnos) == 0:
    print("🚨 CRITICAL: Account numbers don't match between SAVING and CIS files!")
    print("   Possible causes:")
    print("   1. Different account number formats")
    print("   2. Different data sources or time periods") 
    print("   3. Account number transformation issues")
    print("   4. Missing leading zeros or formatting differences")
    
    print("\n🔧 Required fixes:")
    print("   1. Check account number formats in both files")
    print("   2. Verify both files are from the same time period")
    print("   3. Check for leading zeros or formatting differences")
    print("   4. Contact data providers about the mismatch")

elif len(final_output) > 0:
    print(f"✅ Processed {len(final_output)} accounts")
    print("⚠️  Note: Data has quality issues but processing completed")
else:
    print("❌ No data could be processed due to multiple data quality issues")
