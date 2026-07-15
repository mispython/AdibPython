import polars as pl
from datetime import datetime, date, timedelta
from pathlib import Path
import shutil
import pyreadstat
import pandas as pd
import math

# ==================== SETUP ====================
BASE_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS")
DEPOBACK_PATH = BASE_PATH / "input" / "prod" / "MNI"
BNM_PATH = BASE_PATH / "output" / "EIBQFDSP"
OUTPUT_PATH = BASE_PATH / "output" / "EIBQFDSP"

OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ==================== REPTDATE CALCULATIONS ====================
print("Calculating report dates...")
today = date.today()
reptdate = date(today.year, today.month, 1) - timedelta(days=1)

day_val = reptdate.day
mm = reptdate.month

if day_val == 8:
    sdd, wk, wk1 = 1, '1', '4'
elif day_val == 15:
    sdd, wk, wk1 = 9, '2', '1'
elif day_val == 22:
    sdd, wk, wk1 = 16, '3', '2'
else:
    sdd, wk, wk1, wk2, wk3 = 23, '4', '3', '2', '1'

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
REPTMON = f"{mm:02d}"
REPTYEAR = str(reptdate.year)
RDATE = reptdate.strftime("%d%m%Y")
SDATE = sdate.strftime("%d%m%Y")

print(f"Report Date: {RDATE}, Week: {NOWK}")
print(f"Reptdate: {reptdate}")

# ==================== COPY FILES ====================
print("Copying files from DEPOBACK to BNM...")
files_to_copy = ["fdmthly.sas7bdat"]
for file in files_to_copy:
    src = DEPOBACK_PATH / file
    dst = BNM_PATH / file
    if src.exists():
        shutil.copy2(src, dst)
        print(f"Copied: {file}")

# ==================== FUNCTIONS ====================
def is_leap_year(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

def days_in_month(year, month):
    if month == 2:
        return 29 if is_leap_year(year) else 28
    elif month in [4, 6, 9, 11]:
        return 30
    return 31

def sas_date_to_python_date(yyyymmdd):
    """Convert SAS YYYYMMDD numeric to Python date - matching SAS INPUT(PUT(MATDATE,Z8.),YYMMDD8.)"""
    if yyyymmdd is None or pd.isna(yyyymmdd):
        return None
    try:
        # SAS uses Z8. format which pads with zeros to 8 digits
        # Then YYMMDD8. reads as YYYYMMDD
        date_str = f"{int(yyyymmdd):08d}"
        if len(date_str) == 8:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            # Validate date
            if 1 <= month <= 12 and 1 <= day <= days_in_month(year, month):
                return date(year, month, day)
        return None
    except:
        return None

def calculate_remmth_sas(row, reptdate_val):
    """
    Calculate remaining months - matching SAS %REMMTH macro exactly:

        FDYR  = YEAR(FDDATE);
        FDMTH = MONTH(FDDATE);
        FDDAY = DAY(FDDATE);
        IF FDDAY = FDDAYS(FDMTH) THEN FDDAY = RPDAYS(RPMTH);
        REMY = FDYR - RPYR;
        REMM = FDMTH - RPMTH;
        REMD = FDDAY - RPDAY;
        REMMTH = REMY*12 + REMM + REMD/RPDAYS(RPMTH);

    OPENIND='D' -> REMMTH = -1 (handled by caller/SAS DATA step directly)
    OPENIND not in ('O','D') -> no REMMTH assigned (missing), matching SAS
    where only OPENIND='O' branches into %REMMTH and OPENIND='D' sets -1;
    any other value leaves REMMTH as SAS-missing.
    """
    openind = row.get("OPENIND", "")
    matdate = row.get("MATDATE")

    if openind == "D":
        return -1.0
    if openind != "O":
        return None

    if matdate is None or pd.isna(matdate):
        return None

    try:
        # Convert SAS date using Z8. format (same as SAS)
        fddt = sas_date_to_python_date(matdate)
        if fddt is None:
            return None

        rpyr, rpmth, rpday = reptdate_val.year, reptdate_val.month, reptdate_val.day
        fdyr, fdmth, fdday = fddt.year, fddt.month, fddt.day

        # Get days in month for maturity date
        fd_days_in_month = days_in_month(fdyr, fdmth)
        # Get days in month for report date
        rp_days_in_month = days_in_month(rpyr, rpmth)

        # SAS logic: IF FDDAY = FDDAYS(FDMTH) THEN FDDAY=RPDAYS(RPMTH)
        if fdday == fd_days_in_month:
            fdday = rp_days_in_month

        # Calculate differences
        remy = fdyr - rpyr
        remm = fdmth - rpmth
        remd = fdday - rpday

        # Calculate REMMTH
        remmth = remy * 12 + remm + remd / rp_days_in_month

        return remmth
    except:
        return None

def kremmth_format(value):
    """
    Format remaining months to KREMMTH codes - matching the SAS PROC FORMAT exactly:

        VALUE KREMMTH
          LOW - 0   = '51'
            0 - 1   = '52'
            1 - 2   = '53'
            2 - 3   = '54'
            3 - 4   = '81'
            4 - 5   = '82'
            5 - 6   = '83'
            6 - 7   = '84'
            7 - 8   = '85'
            8 - 9   = '86'
            9 - 10  = '87'
           10 - 11  = '88'
           11 - 12  = '89'
           12 - HIGH = '60';

    IMPORTANT: In a SAS PROC FORMAT VALUE statement, when adjacent ranges
    share a boundary point (e.g. LOW-0 and 0-1 both "contain" 0), SAS
    resolves the value to whichever range is listed FIRST. That makes
    every integer boundary belong to the LOWER range, i.e. the effective
    intervals are (LOW,0], (0,1], (1,2], ... (11,12], (12,HIGH) -
    left-exclusive / right-inclusive, NOT the left-inclusive /
    right-exclusive convention used in the earlier version of this
    script. This matters a lot in practice: REMD = FDDAY - RPDAY is
    frequently 0 (maturity date falls on the same day-of-month as the
    report date for round-tenor deposits), which produces an exact
    integer REMMTH and therefore a boundary hit on nearly every run.
    """
    if value is None or pd.isna(value):
        return None

    if value <= 0:
        return '51'
    elif value <= 1:
        return '52'
    elif value <= 2:
        return '53'
    elif value <= 3:
        return '54'
    elif value <= 4:
        return '81'
    elif value <= 5:
        return '82'
    elif value <= 6:
        return '83'
    elif value <= 7:
        return '84'
    elif value <= 8:
        return '85'
    elif value <= 9:
        return '86'
    elif value <= 10:
        return '87'
    elif value <= 11:
        return '88'
    elif value <= 12:
        return '89'
    else:
        return '60'

def read_sas_dataset(filepath):
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        print(f"Read {len(df):,} records from {filepath.name}")
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        raise

# ==================== PROCESS DATA ====================
print("\nProcessing FDMTHLY data...")
fdmthly_file = BNM_PATH / "fdmthly.sas7bdat"

if not fdmthly_file.exists():
    print(f"Error: {fdmthly_file} not found!")
    exit(1)

fdmthly = read_sas_dataset(fdmthly_file)
print(f"Loaded {fdmthly.height:,} records")

# Filter open/closed accounts - matches SAS: only OPENIND='O' or 'D' rows
# get a REMMTH assigned at all (everything else stays missing and is
# dropped downstream by CUSTCODE filtering in ALMDEPT).
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

# Calculate REMMTH using SAS method
reptdate_val = datetime.strptime(RDATE, "%d%m%Y").date()

print("\nCalculating REMMTH using SAS method...")
fdmthly = fdmthly.with_columns([
    pl.struct(["OPENIND", "MATDATE"]).map_elements(
        lambda x: calculate_remmth_sas(x, reptdate_val),
        return_dtype=pl.Float64
    ).alias("REMMTH")
])

# NOTE: The SAS DATA step that builds ALM does NOT filter out negative
# REMMTH values. OPENIND='D' rows get REMMTH=-1 and flow straight into
# PROC SUMMARY / ALMDEPT like everything else (they land in KREMMTH='51'
# if their CUSTCODE happens to be in the reported ranges). The previous
# version of this script filtered `REMMTH >= 0` here, which silently
# dropped all closed-account records before they ever reached ALMDEPT.
# That filter has been removed to match SAS behavior exactly.
print(f"Records with assigned REMMTH: {fdmthly.filter(pl.col('REMMTH').is_not_null()).height:,}")
fdmthly = fdmthly.filter(pl.col("REMMTH").is_not_null())

# Select required columns
alm = fdmthly.select(["BIC", "CUSTCODE", "REMMTH", "CURBAL"])

# ==================== FILTER FOR RELEVANT BIC PREFIXES ====================
alm = alm.filter(
    pl.col("BIC").str.starts_with("42130") |
    pl.col("BIC").str.starts_with("42132") |
    pl.col("BIC").str.starts_with("42630")
)
print(f"Records after BIC filter: {alm.height:,}")

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
    # CUSTCODE 81-84: Keep original CUSTCODE
    pl.when(pl.col("CUSTCODE_STR").is_in(["81", "82", "83", "84"]))
    .then(pl.concat_str([pl.col("BIC"), pl.col("CUSTCODE_STR"), pl.col("RM"), pl.lit("0000Y")], separator=""))
    # CUSTCODE 85-99: Map to 85
    .when(pl.col("CUSTCODE_STR").is_in(["85", "86", "87", "88", "89", "90", "91", "92", "95", "96", "98", "99"]))
    .then(pl.concat_str([pl.col("BIC"), pl.lit("85"), pl.col("RM"), pl.lit("0000Y")], separator=""))
    .otherwise(None)
    .alias("BNMCODE")
]).filter(pl.col("BNMCODE").is_not_null()).select(["BNMCODE", "AMOUNT", "CUSTCODE", "REMMTH"])

print(f"ALMDEPT records: {almdept.height:,}")

# ==================== DEBUG: Show 42130 distribution ====================
print("\n" + "="*60)
print("42130 Distribution:")
print("="*60)

bic_42130 = almdept.filter(pl.col("BNMCODE").str.starts_with("42130"))
if bic_42130.height > 0:
    summary_42130 = bic_42130.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum().alias("AMOUNT")
    ]).sort("BNMCODE")

    print("BNMCODE and AMOUNT for 42130:")
    for row in summary_42130.iter_rows(named=True):
        print(f"{row['BNMCODE']}: {row['AMOUNT']:,.2f}")

    total = summary_42130["AMOUNT"].sum()
    print(f"\nTotal: {total:,.2f}")

# ==================== GENERATE PRODUCTION-STYLE REPORT ====================
def generate_combined_report(data, report_date):
    """Generate a single combined report matching production exactly"""
    if data.height == 0:
        print("No data available!")
        return

    report_file = OUTPUT_PATH / f"REPORT_EXTERNAL_LIABILITIES_{report_date}.txt"

    # Define report sections
    sections = [
        {
            "prefix": "42130",
            "title": "CODE 81 & 85 FOR 42130-80-XX-0000Y"
        },
        {
            "prefix": "42132",
            "title": "CODE 81 & 85 FOR 42132-80-XX-0000Y"
        },
        {
            "prefix": "42630",
            "title": "REPORT ON EXTERNAL LIABILITIES FOR FCY FD FROM FNBE (85)"
        }
    ]

    with open(report_file, 'w') as f:
        for section_idx, section in enumerate(sections):
            prefix = section["prefix"]
            title = section["title"]

            # Filter data for this section
            section_data = data.filter(pl.col("BNMCODE").str.starts_with(prefix))

            if section_data.height == 0:
                print(f"No data for {prefix}")
                continue

            # Summarize by BNMCODE
            summary = section_data.group_by("BNMCODE").agg([
                pl.col("AMOUNT").sum().alias("AMOUNT")
            ]).sort("BNMCODE")

            total_amount = summary["AMOUNT"].sum()

            # Header - exactly like production
            f.write(" " * 40 + "SPECIAL PURPOSE ITEMS (QUARTERLY): EXTERNAL LIABILITIES\n")
            f.write(" " * 50 + f"AS AT {report_date[:2]}/{report_date[2:4]}/{report_date[4:]}\n")
            f.write(" " * 45 + f"{title}\n\n")

            # Column headers - exactly like production
            f.write("Obs       BNMCODE            AMOUNT\n")

            # Data rows with observation numbers
            obs_num = 1
            for row in summary.iter_rows(named=True):
                f.write(f"{obs_num:>3}    {row['BNMCODE']:<18} {row['AMOUNT']:>20,.2f}\n")
                obs_num += 1

            # Total line - exactly like production with spacing
            f.write(" " * 25 + "=============\n")
            f.write(f"{' ':<8} {'TOTAL':<18} {total_amount:>20,.2f}\n")

            # Add blank line between sections
            if section_idx < len(sections) - 1 and section_data.height > 0:
                next_section_data = data.filter(pl.col("BNMCODE").str.starts_with(sections[section_idx + 1]["prefix"]))
                if next_section_data.height > 0:
                    f.write("\n\n")

    print(f"Combined report saved: {report_file}")
    return report_file

# Generate combined report
print("\nGenerating combined report...")
generate_combined_report(almdept, RDATE)

# ==================== SUMMARY ====================
print("\n" + "="*60)
print("SUMMARY STATISTICS")
print("="*60)
print(f"Report Date: {RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}")
print(f"Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")
print(f"ALMDEPT records: {almdept.height:,}")

if almdept.height > 0:
    print("\nAmount Distribution by BNMCODE prefix:")
    for prefix in ["42130", "42132", "42630"]:
        amount = almdept.filter(pl.col("BNMCODE").str.starts_with(prefix))["AMOUNT"].sum()
        if amount > 0:
            print(f"  {prefix}: {amount:>20,.2f}")

print("\nProcessing complete!")

# Save outputs
if alm.height > 0:
    alm.write_parquet(OUTPUT_PATH / f"ALM_{REPTMON}_{NOWK}_{REPTYEAR}.parquet")
if almdept.height > 0:
    almdept.write_parquet(OUTPUT_PATH / f"ALMDEPT_{REPTMON}_{NOWK}_{REPTYEAR}.parquet")
