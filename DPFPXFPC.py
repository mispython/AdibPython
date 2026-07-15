import polars as pl
from datetime import datetime, date, timedelta
from pathlib import Path
import shutil
import pyreadstat
import pandas as pd
import numpy as np

# ==================== SETUP ====================
BASE_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
DEPOBACK_PATH = BASE_PATH / "input" / "prod" / "MNI"
BNM_PATH = BASE_PATH / "output" / "EIBQFDSP"
OUTPUT_PATH = BASE_PATH / "output" / "EIBQFDSP"

# Create output directory if it doesn't exist
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ==================== REPTDATE CALCULATIONS ====================
print("Calculating report dates...")
today = date.today()
reptdate = date(today.year, today.month, 1) - timedelta(days=1)

day_val = reptdate.day
mm = reptdate.month

# Determine week and start dates
if day_val == 8:
    sdd, wk, wk1 = 1, '1', '4'
elif day_val == 15:
    sdd, wk, wk1 = 9, '2', '1'
elif day_val == 22:
    sdd, wk, wk1 = 16, '3', '2'
else:
    sdd, wk, wk1, wk2, wk3 = 23, '4', '3', '2', '1'

# Calculate previous months
if wk == '1':
    mm1 = mm - 1
    if mm1 == 0:
        mm1 = 12
else:
    mm1 = mm

mm2 = mm - 1
if mm2 == 0:
    mm2 = 12

sdate = date(reptdate.year, mm, sdd)

NOWK = wk
NOWK1 = wk1
NOWK2 = wk2 if 'wk2' in locals() else None
NOWK3 = wk3 if 'wk3' in locals() else None
REPTMON = f"{mm:02d}"
REPTMON1 = f"{mm1:02d}"
REPTMON2 = f"{mm2:02d}"
REPTYEAR = str(reptdate.year)
REPTDAY = f"{day_val:02d}"
RDATE = reptdate.strftime("%d%m%Y")
SDATE = sdate.strftime("%d%m%Y")
SDESC = "PUBLIC BANK BERHAD"

print(f"Report Date: {RDATE}, Week: {NOWK}")
print(f"DEPOBACK_PATH: {DEPOBACK_PATH}")
print(f"BNM_PATH: {BNM_PATH}")
print(f"OUTPUT_PATH: {OUTPUT_PATH}")

# ==================== COPY FILES ====================
print("Copying files from DEPOBACK to BNM...")

# Only copy what's needed
files_to_copy = ["fdmthly.sas7bdat"]
for file in files_to_copy:
    src = DEPOBACK_PATH / file
    dst = BNM_PATH / file
    if src.exists():
        shutil.copy2(src, dst)
        print(f"Copied: {file}")
    else:
        print(f"Error: {file} not found in {DEPOBACK_PATH}")
        exit(1)

# ==================== FORMAT FUNCTIONS ====================
def kremmth_format(value):
    """Format remaining months to KREMMTH codes"""
    if value is None or pd.isna(value):
        return None
    elif value < 0:
        return '51'
    elif 0 <= value < 1:
        return '52'
    elif 1 <= value < 2:
        return '53'
    elif 2 <= value < 3:
        return '54'
    elif 3 <= value < 4:
        return '81'
    elif 4 <= value < 5:
        return '82'
    elif 5 <= value < 6:
        return '83'
    elif 6 <= value < 7:
        return '84'
    elif 7 <= value < 8:
        return '85'
    elif 8 <= value < 9:
        return '86'
    elif 9 <= value < 10:
        return '87'
    elif 10 <= value < 11:
        return '88'
    elif 11 <= value < 12:
        return '89'
    else:
        return '60'

# ==================== DATE HELPER FUNCTIONS ====================
def is_leap_year(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

def days_in_month(year, month):
    if month == 2:
        return 29 if is_leap_year(year) else 28
    elif month in [4, 6, 9, 11]:
        return 30
    return 31

def yyyymmdd_to_date(yyyymmdd):
    """Convert YYYYMMDD numeric value to Python date"""
    if yyyymmdd is None or pd.isna(yyyymmdd):
        return None
    try:
        # Convert to string and parse
        date_str = str(int(yyyymmdd))
        if len(date_str) == 8:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            return date(year, month, day)
        return None
    except:
        return None

def sas_date_to_python(sas_date):
    """Convert SAS numeric date (days since 1960-01-01) to Python date"""
    if sas_date is None or pd.isna(sas_date):
        return None
    try:
        # SAS dates are days since 1960-01-01
        base_date = date(1960, 1, 1)
        return base_date + timedelta(days=int(sas_date))
    except:
        return None

# ==================== READ SAS DATASET WITH PYREADSTAT ====================
def read_sas_dataset(filepath):
    """Read SAS dataset using pyreadstat and convert to Polars DataFrame"""
    try:
        # Read SAS file with pyreadstat
        df, meta = pyreadstat.read_sas7bdat(filepath)
        print(f"Read {len(df)} records from {filepath.name}")
        print(f"Columns: {', '.join(df.columns.tolist())}")
        
        # Convert to Polars DataFrame
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        raise

# ==================== REMMTH CALCULATION ====================
def calculate_remmth_from_yyyymmdd(matdate_val, reptdate_val):
    """Calculate remaining months from YYYYMMDD date value"""
    if matdate_val is None or pd.isna(matdate_val):
        return None
    
    try:
        # Convert YYYYMMDD to date
        fddt = yyyymmdd_to_date(matdate_val)
        if fddt is None:
            return None
        
        # Calculate remaining months
        rpyr = reptdate_val.year
        rpmth = reptdate_val.month
        rpday = reptdate_val.day
        
        fdyr = fddt.year
        fdmth = fddt.month
        fdday = fddt.day
        
        # Adjust FDDAY if it equals days in FDMTH
        fd_days_in_month = days_in_month(fdyr, fdmth)
        rp_days_in_month = days_in_month(rpyr, rpmth)
        
        if fdday == fd_days_in_month:
            fdday = rp_days_in_month
        
        # Calculate differences
        remy = fdyr - rpyr
        remm = fdmth - rpmth
        remd = fdday - rpday
        
        # Convert to months - ensure float return
        return float(remy * 12 + remm + remd / rp_days_in_month)
    except:
        return None

def calculate_remmth(row, reptdate_val):
    """Calculate remaining months - wrapper function"""
    openind = row.get("OPENIND", "")
    
    # Handle closed accounts
    if openind == "D":
        return -1.0
    
    # Skip non-open accounts
    if openind != "O":
        return None
    
    # Parse maturity date
    matdate_val = row.get("MATDATE")
    return calculate_remmth_from_yyyymmdd(matdate_val, reptdate_val)

# ==================== PROCESS FDMTHLY DATA ====================
print("Processing FDMTHLY data...")
fdmthly_file = BNM_PATH / "fdmthly.sas7bdat"

if not fdmthly_file.exists():
    print(f"Error: {fdmthly_file} not found!")
    exit(1)

try:
    fdmthly = read_sas_dataset(fdmthly_file)
    print(f"Loaded {len(fdmthly)} records from fdmthly.sas7bdat")
except Exception as e:
    print(f"Error reading dataset: {e}")
    exit(1)

# ==================== DEBUG: Check MATDATE values ====================
print("\n" + "="*60)
print("DEBUG: Checking MATDATE values")
print("="*60)

if "MATDATE" in fdmthly.columns:
    print(f"MATDATE dtype: {fdmthly['MATDATE'].dtype}")
    
    # Sample MATDATE values
    sample_matdate = fdmthly.select(pl.col("MATDATE")).head(10)
    print("\nSample MATDATE values (first 10):")
    print(sample_matdate)
    
    # Check how many MATDATE are null
    null_count = fdmthly.filter(pl.col("MATDATE").is_null()).height
    print(f"\nRecords with null MATDATE: {null_count:,}")
    print(f"Records with non-null MATDATE: {fdmthly.height - null_count:,}")
    
    # Check MATDATE range
    try:
        if fdmthly['MATDATE'].dtype in [pl.Int32, pl.Int64, pl.Float32, pl.Float64]:
            min_val = fdmthly['MATDATE'].min()
            max_val = fdmthly['MATDATE'].max()
            print(f"MATDATE range: {min_val:.0f} to {max_val:.0f}")
            
            # Convert to dates for understanding
            min_date = yyyymmdd_to_date(min_val)
            max_date = yyyymmdd_to_date(max_val)
            if min_date and max_date:
                print(f"MATDATE date range: {min_date} to {max_date}")
    except:
        pass

# ==================== DEBUG: Check CUSTCODE values ====================
print("\n" + "="*60)
print("DEBUG: Checking CUSTCODE values")
print("="*60)

if "CUSTCODE" in fdmthly.columns:
    # Ensure CUSTCODE is string
    fdmthly = fdmthly.with_columns([
        pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars().alias("CUSTCODE")
    ])
    
    custcode_values = fdmthly.select(pl.col("CUSTCODE")).unique().head(20)
    print(f"Sample CUSTCODE values (first 20 unique):")
    print(custcode_values)
    
    print(f"CUSTCODE dtype: {fdmthly['CUSTCODE'].dtype}")
    
    # Count records with CUSTCODE in our target ranges
    target_codes_1 = ["81", "82", "83", "84"]
    target_codes_2 = ["85", "86", "87", "88", "89", "90", "91", "92", "95", "96", "98", "99"]
    
    count_group1 = fdmthly.filter(pl.col("CUSTCODE").is_in(target_codes_1)).height
    count_group2 = fdmthly.filter(pl.col("CUSTCODE").is_in(target_codes_2)).height
    
    print(f"\nRecords with CUSTCODE in group 1 (81-84): {count_group1:,}")
    print(f"Records with CUSTCODE in group 2 (85-99): {count_group2:,}")

# Filter open accounts (using string values)
open_inds = ["O", "D"]
if "OPENIND" in fdmthly.columns:
    # Handle different data types for OPENIND
    fdmthly = fdmthly.with_columns([
        pl.col("OPENIND").cast(pl.Utf8).str.strip_chars()
    ])
    original_count = fdmthly.height
    fdmthly = fdmthly.filter(pl.col("OPENIND").is_in(open_inds))
    print(f"\nRecords after filtering OPENIND in ['O','D']: {fdmthly.height:,} (filtered out {original_count - fdmthly.height:,})")
else:
    print("Warning: OPENIND column not found!")

# Calculate REMMTH for each row
reptdate_val = datetime.strptime(RDATE, "%d%m%Y").date()

# Apply REMMTH calculation row by row
print("\nCalculating REMMTH for each record...")

# MATDATE is YYYYMMDD format, convert directly
print("MATDATE is in YYYYMMDD format - converting directly...")

# Convert MATDATE to date using our function
fdmthly = fdmthly.with_columns([
    pl.col("MATDATE").map_elements(
        lambda x: yyyymmdd_to_date(x),
        return_dtype=pl.Date
    ).alias("MATDATE_DATE")
])

# Now calculate REMMTH using the date
fdmthly = fdmthly.with_columns([
    pl.struct(["OPENIND", "MATDATE_DATE"]).map_elements(
        lambda x: calculate_remmth_from_yyyymmdd(
            x.get("MATDATE_DATE").year * 10000 + 
            x.get("MATDATE_DATE").month * 100 + 
            x.get("MATDATE_DATE").day
            if x.get("MATDATE_DATE") is not None else None,
            reptdate_val
        ) if x.get("OPENIND") == "O" else (-1.0 if x.get("OPENIND") == "D" else None),
        return_dtype=pl.Float64
    ).alias("REMMTH")
])

# Check how many records have valid REMMTH (not null)
valid_remmth = fdmthly.filter(pl.col("REMMTH").is_not_null()).height
print(f"Records with valid REMMTH (not null): {valid_remmth:,} ({(valid_remmth/fdmthly.height*100):.2f}%)")

# Filter out records with negative REMMTH (closed accounts)
fdmthly = fdmthly.filter(pl.col("REMMTH") >= 0)
print(f"Records after filtering negative REMMTH: {fdmthly.height:,}")

# Select required columns
columns_to_select = []
for col in ["BIC", "CUSTCODE", "REMMTH", "CURBAL"]:
    if col in fdmthly.columns:
        columns_to_select.append(col)
    else:
        print(f"Warning: Column {col} not found")

if not columns_to_select:
    print("Error: Required columns not found!")
    print(f"Available columns: {fdmthly.columns}")
    exit(1)

alm = fdmthly.select(columns_to_select)

# Rename columns to standard names if needed
rename_dict = {}
for col in alm.columns:
    if col.upper() in ["BIC", "CUSTCODE", "CURBAL"]:
        rename_dict[col] = col.upper()
    elif col.upper() == "REMMTH":
        rename_dict[col] = "REMMTH"

if rename_dict:
    alm = alm.rename(rename_dict)

# ==================== SUMMARIZE DATA ====================
print("\nSummarizing data...")
alm_summary = alm.group_by(["BIC", "CUSTCODE", "REMMTH"]).agg([
    pl.col("CURBAL").sum().alias("AMOUNT")
])

print(f"Records after summary: {alm_summary.height:,}")

# Also show how many records have CUSTCODE in target ranges
if alm_summary.height > 0:
    print("\nRecords in summary with CUSTCODE in target ranges:")
    target_codes_1 = ["81", "82", "83", "84"]
    target_codes_2 = ["85", "86", "87", "88", "89", "90", "91", "92", "95", "96", "98", "99"]
    
    count_summary_group1 = alm_summary.filter(pl.col("CUSTCODE").is_in(target_codes_1)).height
    count_summary_group2 = alm_summary.filter(pl.col("CUSTCODE").is_in(target_codes_2)).height
    print(f"  Group 1 (81-84): {count_summary_group1:,}")
    print(f"  Group 2 (85-99): {count_summary_group2:,}")

# ==================== CREATE ALMDEPT DATASET ====================
print("\nCreating BNM codes...")

if alm_summary.height > 0:
    # Convert CUSTCODE to string for proper filtering
    alm_summary = alm_summary.with_columns([
        pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars().alias("CUSTCODE_STR")
    ])

    almdept = alm_summary.with_columns([
        pl.col("REMMTH").map_elements(kremmth_format, return_dtype=pl.Utf8).alias("RM")
    ]).with_columns([
        pl.when(pl.col("CUSTCODE_STR").is_in(["81", "82", "83", "84"]))
        .then(pl.concat_str([pl.col("BIC"), pl.col("CUSTCODE_STR"), pl.col("RM"), pl.lit("0000Y")], separator=""))
        .when(pl.col("CUSTCODE_STR").is_in(["85", "86", "87", "88", "89", "90", "91", "92", "95", "96", "98", "99"]))
        .then(pl.concat_str([pl.col("BIC"), pl.lit("85"), pl.col("RM"), pl.lit("0000Y")], separator=""))
        .otherwise(None)
        .alias("BNMCODE")
    ]).filter(pl.col("BNMCODE").is_not_null()).select(["BNMCODE", "AMOUNT", "CUSTCODE", "REMMTH"])

    print(f"Records after BNMCODE creation: {almdept.height:,}")
else:
    almdept = pl.DataFrame(schema={"BNMCODE": pl.Utf8, "AMOUNT": pl.Float64, "CUSTCODE": pl.Utf8, "REMMTH": pl.Float64})
    print("No records in ALM summary")

# ==================== GENERATE REPORTS ====================
def generate_report(data, bic_prefix, title, report_suffix=""):
    """Generate formatted report for specific BIC prefix"""
    if data.height == 0:
        print(f"No data for {bic_prefix}")
        return
    
    report_data = data.filter(pl.col("BNMCODE").str.starts_with(bic_prefix))
    
    if len(report_data) == 0:
        print(f"No data for {bic_prefix}")
        return
    
    # Summarize by BNMCODE
    summary = report_data.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum().alias("AMOUNT")
    ]).sort("BNMCODE")
    
    # Generate report as text file
    report_file = OUTPUT_PATH / f"REPORT_{bic_prefix}_{report_suffix}_{RDATE}.txt"
    total_amount = summary["AMOUNT"].sum()
    
    with open(report_file, 'w') as f:
        f.write(" " * 40 + "SPECIAL PURPOSE ITEMS (QUARTERLY): EXTERNAL LIABILITIES\n")
        f.write(" " * 50 + f"AS AT {RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}\n")
        f.write(" " * 45 + f"{title}\n\n")
        f.write("=" * 80 + "\n")
        f.write(f"{'BNMCODE':<20} {'AMOUNT':>20}\n")
        f.write("-" * 80 + "\n")
        
        for row in summary.iter_rows(named=True):
            f.write(f"{row['BNMCODE']:<20} {row['AMOUNT']:>20,.2f}\n")
        
        f.write("-" * 80 + "\n")
        f.write(f"{'TOTAL':<20} {total_amount:>20,.2f}\n")
        f.write("=" * 80 + "\n")
    
    print(f"Report saved: {report_file}")
    return summary

# Generate reports for different BIC prefixes
print("\nGenerating reports...")

# Report for 42130
report_42130 = generate_report(almdept, "42130", "CODE 81 & 85 FOR 42130-80-XX-0000Y", "42130")

# Report for 42132
report_42132 = generate_report(almdept, "42132", "CODE 81 & 85 FOR 42132-80-XX-0000Y", "42132")

# Report for 42630
print("Generating FCY FD report...")
report_42630 = almdept.filter(pl.col("BNMCODE").str.starts_with("42630"))
if len(report_42630) > 0:
    summary_42630 = report_42630.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum().alias("AMOUNT")
    ]).sort("BNMCODE")
    
    total_42630 = summary_42630["AMOUNT"].sum()
    
    report_file = OUTPUT_PATH / f"REPORT_42630_{RDATE}.txt"
    with open(report_file, 'w') as f:
        f.write(" " * 40 + "REPORT ON EXTERNAL LIABILITIES FOR FCY FD FROM FNBE (85)\n")
        f.write(" " * 50 + f"AS AT {RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}\n\n")
        f.write("=" * 80 + "\n")
        f.write(f"{'BNMCODE':<20} {'AMOUNT':>20}\n")
        f.write("-" * 80 + "\n")
        
        for row in summary_42630.iter_rows(named=True):
            f.write(f"{row['BNMCODE']:<20} {row['AMOUNT']:>20,.2f}\n")
        
        f.write("-" * 80 + "\n")
        f.write(f"{'TOTAL':<20} {total_42630:>20,.2f}\n")
        f.write("=" * 80 + "\n")
    
    print(f"FCY FD report saved: {report_file}")

# ==================== SAVE PROCESSED DATA ====================
print("\nSaving processed data...")
if alm.height > 0:
    alm.write_parquet(OUTPUT_PATH / f"ALM_{REPTMON}_{NOWK}_{REPTYEAR}.parquet")
if almdept.height > 0:
    almdept.write_parquet(OUTPUT_PATH / f"ALMDEPT_{REPTMON}_{NOWK}_{REPTYEAR}.parquet")

# ==================== SUMMARY STATISTICS ====================
print("\nSUMMARY STATISTICS:")
print("=" * 60)
print(f"Total ALM records: {len(alm):,}")
print(f"Total ALMDEPT records: {len(almdept):,}")
print(f"Report Date: {RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}")
print(f"Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")

if len(almdept) > 0:
    print("\nAmount Distribution by BNMCODE prefix:")
    for prefix in ["42130", "42132", "42630"]:
        amount = almdept.filter(pl.col("BNMCODE").str.starts_with(prefix))["AMOUNT"].sum()
        if amount > 0:
            print(f"  {prefix}: {amount:>20,.2f}")
else:
    print("\nNo records in ALMDEPT.")
    print("\nCheck the debug output above to see:")
    print("1. MATDATE values and their format")
    print("2. How many records had valid REMMTH")
    print("3. How many records have target CUSTCODEs")

print("\nProcessing complete!")

# ==================== OPTIONAL: EXPORT TO CSV FOR REVIEW ====================
print("\nExporting to CSV for review...")
if alm.height > 0:
    alm.write_csv(OUTPUT_PATH / f"ALM_{REPTMON}_{NOWK}_{REPTYEAR}.csv")
if almdept.height > 0:
    almdept.write_csv(OUTPUT_PATH / f"ALMDEPT_{REPTMON}_{NOWK}_{REPTYEAR}.csv")
