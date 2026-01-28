import polars as pl
from datetime import datetime, date, timedelta
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
DEPOBACK_PATH = BASE_PATH / "depoback"
BNM_PATH = BASE_PATH / "bnm"
OUTPUT_PATH = BASE_PATH / "output"

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
import shutil

files_to_copy = ["FDWKLY.parquet", "FDMTHLY.parquet"]
for file in files_to_copy:
    src = DEPOBACK_PATH / file
    dst = BNM_PATH / file
    if src.exists():
        shutil.copy2(src, dst)
        print(f"Copied: {file}")

# ==================== FORMAT FUNCTIONS ====================
def kremmth_format(value):
    """Format remaining months to KREMMTH codes"""
    if value < 0:
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

# ==================== REMMTH CALCULATION ====================
def calculate_remmth(row, reptdate_val):
    """Calculate remaining months"""
    if row["OPENIND"] == "D":
        return -1
    
    if row["OPENIND"] != "O":
        return None
    
    # Parse maturity date
    if not row["MATDATE"]:
        return None
    
    try:
        fddt = datetime.strptime(str(row["MATDATE"]).zfill(8), "%Y%m%d").date()
    except:
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
    
    # Convert to months
    return remy * 12 + remm + remd / rp_days_in_month

# ==================== PROCESS FDMTHLY DATA ====================
print("Processing FDMTHLY data...")
fdmthly = pl.read_parquet(BNM_PATH / "FDMTHLY.parquet")

# Filter open accounts
fdmthly = fdmthly.filter(pl.col("OPENIND").is_in(["O", "D"]))

# Calculate REMMTH for each row
reptdate_val = datetime.strptime(RDATE, "%d%m%Y").date()
fdmthly = fdmthly.with_columns([
    pl.struct(["OPENIND", "MATDATE"]).apply(
        lambda x: calculate_remmth(x, reptdate_val)
    ).alias("REMMTH")
])

# Select required columns
alm = fdmthly.select(["BIC", "CUSTCODE", "REMMTH", "CURBAL"])

# ==================== SUMMARIZE DATA ====================
print("Summarizing data...")
alm_summary = alm.group_by(["BIC", "CUSTCODE", "REMMTH"]).agg([
    pl.col("CURBAL").sum().alias("AMOUNT")
])

# ==================== CREATE ALMDEPT DATASET ====================
print("Creating BNM codes...")
almdept = alm_summary.with_columns([
    pl.col("REMMTH").map_elements(kremmth_format, return_dtype=pl.Utf8).alias("RM")
]).with_columns([
    pl.when(pl.col("CUSTCODE").is_in(["81", "82", "83", "84"]))
    .then(pl.concat_str([pl.col("BIC"), pl.col("CUSTCODE"), pl.col("RM"), pl.lit("0000Y")], separator=""))
    .when(pl.col("CUSTCODE").is_in(["85", "86", "87", "88", "89", "90", "91", "92", "95", "96", "98", "99"]))
    .then(pl.concat_str([pl.col("BIC"), pl.lit("85"), pl.col("RM"), pl.lit("0000Y")], separator=""))
    .otherwise(None)
    .alias("BNMCODE")
]).filter(pl.col("BNMCODE").is_not_null()).select(["BNMCODE", "AMOUNT", "CUSTCODE", "REMMTH"])

# ==================== GENERATE REPORTS ====================
def generate_report(data, bic_prefix, title):
    """Generate formatted report for specific BIC prefix"""
    report_data = data.filter(pl.col("BNMCODE").str.starts_with(bic_prefix))
    
    if len(report_data) == 0:
        print(f"No data for {bic_prefix}")
        return
    
    # Summarize by BNMCODE
    summary = report_data.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum().alias("AMOUNT")
    ]).sort("BNMCODE")
    
    # Generate report
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
    return summary

# Generate reports for different BIC prefixes
print("\nGenerating reports...")

# Report for 42130
report_42130 = generate_report(almdept, "42130", "CODE 81 & 85 FOR 42130-80-XX-0000Y")

# Report for 42132
report_42132 = generate_report(almdept, "42132", "CODE 81 & 85 FOR 42132-80-XX-0000Y")

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
alm.write_parquet(OUTPUT_PATH / f"ALM_{REPTMON}_{NOWK}_{REPTYEAR}.parquet")
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

print("\nProcessing complete!")
