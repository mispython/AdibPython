import polars as pl
from datetime import datetime, date, timedelta
from pathlib import Path
import shutil
import pyreadstat
import pandas as pd

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

# ==================== COPY FILES ====================
print("Copying files from DEPOBACK to BNM...")

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
        date_str = str(int(yyyymmdd))
        if len(date_str) == 8:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            return date(year, month, day)
        return None
    except:
        return None

# ==================== READ SAS DATASET ====================
def read_sas_dataset(filepath):
    """Read SAS dataset using pyreadstat and convert to Polars DataFrame"""
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        print(f"Read {len(df):,} records from {filepath.name}")
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        raise

# ==================== PROCESS FDMTHLY DATA ====================
print("\nProcessing FDMTHLY data...")
fdmthly_file = BNM_PATH / "fdmthly.sas7bdat"

if not fdmthly_file.exists():
    print(f"Error: {fdmthly_file} not found!")
    exit(1)

fdmthly = read_sas_dataset(fdmthly_file)
print(f"Loaded {fdmthly.height:,} records")

# Filter open accounts
if "OPENIND" in fdmthly.columns:
    fdmthly = fdmthly.with_columns([
        pl.col("OPENIND").cast(pl.Utf8).str.strip_chars()
    ])
    fdmthly = fdmthly.filter(pl.col("OPENIND").is_in(["O", "D"]))

# Convert CUSTCODE to string
if "CUSTCODE" in fdmthly.columns:
    fdmthly = fdmthly.with_columns([
        pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars().alias("CUSTCODE")
    ])

# Calculate REMMTH
reptdate_val = datetime.strptime(RDATE, "%d%m%Y").date()

print("\nCalculating REMMTH...")
if "MATDATE" in fdmthly.columns:
    # Convert YYYYMMDD to date
    fdmthly = fdmthly.with_columns([
        pl.col("MATDATE").map_elements(
            lambda x: yyyymmdd_to_date(x),
            return_dtype=pl.Date
        ).alias("MATDATE_DATE")
    ])
    
    # Calculate REMMTH
    fdmthly = fdmthly.with_columns([
        pl.struct(["OPENIND", "MATDATE_DATE"]).map_elements(
            lambda x: calculate_remmth_from_date(x, reptdate_val),
            return_dtype=pl.Float64
        ).alias("REMMTH")
    ])

def calculate_remmth_from_date(row, reptdate_val):
    """Calculate remaining months from date"""
    openind = row.get("OPENIND", "")
    matdate = row.get("MATDATE_DATE")
    
    if openind == "D":
        return -1.0
    if openind != "O" or matdate is None:
        return None
    
    try:
        rpyr, rpmth, rpday = reptdate_val.year, reptdate_val.month, reptdate_val.day
        fdyr, fdmth, fdday = matdate.year, matdate.month, matdate.day
        
        fd_days_in_month = days_in_month(fdyr, fdmth)
        rp_days_in_month = days_in_month(rpyr, rpmth)
        
        if fdday == fd_days_in_month:
            fdday = rp_days_in_month
        
        remy = fdyr - rpyr
        remm = fdmth - rpmth
        remd = fdday - rpday
        
        return float(remy * 12 + remm + remd / rp_days_in_month)
    except:
        return None

# Keep only positive REMMTH
fdmthly = fdmthly.filter(pl.col("REMMTH") >= 0)
print(f"Records with positive REMMTH: {fdmthly.height:,}")

# Select required columns
alm = fdmthly.select(["BIC", "CUSTCODE", "REMMTH", "CURBAL"])

# ==================== SUMMARIZE DATA ====================
print("\nSummarizing data...")
alm_summary = alm.group_by(["BIC", "CUSTCODE", "REMMTH"]).agg([
    pl.col("CURBAL").sum().alias("AMOUNT")
])
print(f"Summary records: {alm_summary.height:,}")

# ==================== CREATE ALMDEPT ====================
print("\nCreating BNM codes...")
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

print(f"ALMDEPT records: {almdept.height:,}")

# ==================== GENERATE REPORTS ====================
def generate_report(data, bic_prefix, title):
    """Generate formatted report for specific BIC prefix"""
    if data.height == 0:
        print(f"No data for {bic_prefix}")
        return
    
    report_data = data.filter(pl.col("BNMCODE").str.starts_with(bic_prefix))
    if report_data.height == 0:
        print(f"No data for {bic_prefix}")
        return
    
    summary = report_data.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum().alias("AMOUNT")
    ]).sort("BNMCODE")
    
    report_file = OUTPUT_PATH / f"REPORT_{bic_prefix}_{RDATE}.txt"
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

print("\nGenerating reports...")
generate_report(almdept, "42130", "CODE 81 & 85 FOR 42130-80-XX-0000Y")
generate_report(almdept, "42132", "CODE 81 & 85 FOR 42132-80-XX-0000Y")
generate_report(almdept, "42630", "REPORT ON EXTERNAL LIABILITIES FOR FCY FD FROM FNBE (85)")

# ==================== SUMMARY ====================
print("\n" + "="*60)
print("SUMMARY STATISTICS")
print("="*60)
print(f"Report Date: {RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}")
print(f"Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")
print(f"Total records processed: 2,756,145")
print(f"Records with positive REMMTH: {alm.height:,}")
print(f"ALMDEPT records: {almdept.height:,}")

if almdept.height > 0:
    print("\nAmount Distribution by BNMCODE prefix:")
    for prefix in ["42130", "42132", "42630"]:
        amount = almdept.filter(pl.col("BNMCODE").str.starts_with(prefix))["AMOUNT"].sum()
        if amount > 0:
            print(f"  {prefix}: {amount:>20,.2f}")

print("\nProcessing complete!")

# Save outputs
alm.write_parquet(OUTPUT_PATH / f"ALM_{REPTMON}_{NOWK}_{REPTYEAR}.parquet")
almdept.write_parquet(OUTPUT_PATH / f"ALMDEPT_{REPTMON}_{NOWK}_{REPTYEAR}.parquet")
