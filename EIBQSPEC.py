import polars as pl
from datetime import datetime, date
from pathlib import Path

# ==================== SETUP ====================
BASE_PATH = Path("/path/to/data")
DEP_PATH = BASE_PATH / "dep"
BNM_PATH = BASE_PATH / "bnm"
OUTPUT_PATH = BASE_PATH / "output"

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

# ==================== REPTDATE PROCESSING ====================
print("Processing report date...")
reptdate_df = pl.read_parquet(DEP_PATH / "REPTDATE.parquet")
reptdate_val = reptdate_df[0, "REPTDATE"]

day_val = reptdate_val.day
mm = reptdate_val.month

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

sdate = date(reptdate_val.year, mm, sdd)

NOWK = wk
NOWK1 = wk1
NOWK2 = wk2 if 'wk2' in locals() else None
NOWK3 = wk3 if 'wk3' in locals() else None
REPTMON = f"{mm:02d}"
REPTMON1 = f"{mm1:02d}"
REPTMON2 = f"{mm2:02d}"
REPTYEAR = str(reptdate_val.year)
REPTDAY = f"{day_val:02d}"
RDATE = reptdate_val.strftime("%d%m%Y")
SDATE = sdate.strftime("%d%m%Y")

print(f"Report Date: {RDATE}, Week: {NOWK}")

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
    
    # Get report date components
    rpyr = reptdate_val.year
    rpmth = reptdate_val.month
    rpday = reptdate_val.day
    
    # Get maturity date components
    fdyr = fddt.year
    fdmth = fddt.month
    fdday = fddt.day
    
    # Days in months
    fd_days_in_month = days_in_month(fdyr, fdmth)
    rp_days_in_month = days_in_month(rpyr, rpmth)
    
    # Adjust FDDAY if it equals days in FDMTH
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
    .when(pl.col("CUSTCODE").is_in(["85", "86", "90", "91", "92", "95", "96", "98", "99"]))
    .then(pl.concat_str([pl.col("BIC"), pl.lit("85"), pl.col("RM"), pl.lit("0000Y")], separator=""))
    .otherwise(None)
    .alias("BNMCODE")
]).filter(pl.col("BNMCODE").is_not_null()).select(["BNMCODE", "AMOUNT", "CUSTCODE", "REMMTH"])

# ==================== GENERATE REPORTS ====================
def generate_report(data, bic_prefix, title, report_type="special"):
    """Generate formatted report for specific BIC prefix"""
    report_data = data.filter(pl.col("BNMCODE").str.starts_with(bic_prefix))
    
    if len(report_data) == 0:
        print(f"No data for {bic_prefix}")
        return None
    
    # Summarize by BNMCODE
    summary = report_data.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum().alias("AMOUNT")
    ]).sort("BNMCODE")
    
    total_amount = summary["AMOUNT"].sum()
    
    # Generate report file
    if report_type == "special":
        report_file = OUTPUT_PATH / f"SPECIAL_REPORT_{bic_prefix}_{RDATE}.txt"
    else:
        report_file = OUTPUT_PATH / f"FCY_FD_REPORT_{bic_prefix}_{RDATE}.txt"
    
    with open(report_file, 'w') as f:
        if report_type == "special":
            f.write(" " * 40 + "SPECIAL PURPOSE ITEMS (QUARTERLY): EXTERNAL LIABILITIES\n")
            f.write(" " * 50 + f"AS AT {RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}\n")
            f.write(" " * 45 + f"{title}\n\n")
        else:
            f.write(" " * 40 + "REPORT ON EXTERNAL LIABILITIES FOR FCY FD FROM FNBE (85)\n")
            f.write(" " * 50 + f"AS AT {RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}\n\n")
        
        f.write("=" * 80 + "\n")
        f.write(f"{'BNMCODE':<20} {'AMOUNT':>20}\n")
        f.write("-" * 80 + "\n")
        
        for row in summary.iter_rows(named=True):
            f.write(f"{row['BNMCODE']:<20} {row['AMOUNT']:>20,.2f}\n")
        
        f.write("-" * 80 + "\n")
        f.write(f"{'TOTAL':<20} {total_amount:>20,.2f}\n")
        f.write("=" * 80 + "\n")
    
    print(f"Report saved: {report_file}")
    return summary, total_amount

# Generate reports for different BIC prefixes
print("\nGenerating reports...")

# Report for 42130
print("Generating report for 42130...")
report_42130, total_42130 = generate_report(
    almdept, "42130", 
    "CODE 81 & 85 FOR 42130-80-XX-0000Y", 
    "special"
)

# Report for 42132
print("Generating report for 42132...")
report_42132, total_42132 = generate_report(
    almdept, "42132", 
    "CODE 81 & 85 FOR 42132-80-XX-0000Y", 
    "special"
)

# Report for 42630 (FCY FD)
print("Generating FCY FD report for 42630...")
report_42630, total_42630 = generate_report(
    almdept, "42630", 
    "", 
    "fcy"
)

# ==================== SAVE PROCESSED DATA ====================
print("\nSaving processed data...")
alm.write_parquet(OUTPUT_PATH / f"ALM_{REPTMON}_{NOWK}_{REPTYEAR}.parquet")
alm_summary.write_parquet(OUTPUT_PATH / f"ALM_SUMMARY_{REPTMON}_{NOWK}_{REPTYEAR}.parquet")
almdept.write_parquet(OUTPUT_PATH / f"ALMDEPT_{REPTMON}_{NOWK}_{REPTYEAR}.parquet")

# ==================== SUMMARY STATISTICS ====================
print("\n" + "=" * 60)
print("PROCESSING SUMMARY")
print("=" * 60)
print(f"Report Date: {reptdate_val.strftime('%d/%m/%Y')}")
print(f"Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")
print(f"Total FDMTHLY records processed: {len(fdmthly):,}")
print(f"Total ALM records: {len(alm):,}")
print(f"Total ALMDEPT records: {len(almdept):,}")

if len(almdept) > 0:
    print("\nAMOUNT SUMMARY BY BIC PREFIX:")
    print("-" * 40)
    
    prefixes = ["42130", "42132", "42630"]
    totals = {}
    
    for prefix in prefixes:
        total = almdept.filter(pl.col("BNMCODE").str.starts_with(prefix))["AMOUNT"].sum()
        totals[prefix] = total
        print(f"  {prefix}: {total:>20,.2f}")
    
    grand_total = almdept["AMOUNT"].sum()
    print("-" * 40)
    print(f"  GRAND TOTAL: {grand_total:>20,.2f}")
    
    # Distribution by CUSTCODE category
    print("\nDISTRIBUTION BY CUSTOMER CODE CATEGORY:")
    print("-" * 40)
    
    category_81_84 = almdept.filter(pl.col("CUSTCODE").is_in(["81", "82", "83", "84"]))["AMOUNT"].sum()
    category_85_plus = almdept.filter(pl.col("CUSTCODE").is_in(["85", "86", "90", "91", "92", "95", "96", "98", "99"]))["AMOUNT"].sum()
    
    print(f"  Codes 81-84: {category_81_84:>20,.2f}")
    print(f"  Codes 85+  : {category_85_plus:>20,.2f}")
    
    # REMMTH distribution
    print("\nDISTRIBUTION BY MATURITY BUCKET:")
    print("-" * 40)
    
    # Apply KREMMTH format to group
    almdept_with_rm = almdept.with_columns([
        pl.col("REMMTH").map_elements(kremmth_format, return_dtype=pl.Utf8).alias("RM_CODE")
    ])
    
    rm_distribution = almdept_with_rm.group_by("RM_CODE").agg([
        pl.col("AMOUNT").sum().alias("TOTAL")
    ]).sort("RM_CODE")
    
    for row in rm_distribution.iter_rows(named=True):
        rm_desc = {
            '51': 'NEGATIVE',
            '52': '0-1 MONTH',
            '53': '1-2 MONTHS',
            '54': '2-3 MONTHS',
            '81': '3-4 MONTHS',
            '82': '4-5 MONTHS',
            '83': '5-6 MONTHS',
            '84': '6-7 MONTHS',
            '85': '7-8 MONTHS',
            '86': '8-9 MONTHS',
            '87': '9-10 MONTHS',
            '88': '10-11 MONTHS',
            '89': '11-12 MONTHS',
            '60': '>12 MONTHS'
        }.get(row['RM_CODE'], row['RM_CODE'])
        
        print(f"  {rm_desc:<15} {row['TOTAL']:>20,.2f}")

print("\n" + "=" * 60)
print("PROCESSING COMPLETE")
print("=" * 60)
