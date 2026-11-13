import polars as pl
import pyreadstat
from datetime import datetime, date, timedelta
from pathlib import Path

# ==================== SETUP ====================
BNM_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI")
OUTPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQSPEC")

# Ensure output directory exists
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# ==================== KREMMTH FORMAT (from SAS PROC FORMAT) ====================
def kremmth_format(value):
    """KREMMTH format - matches SAS VALUE statement exactly"""
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

# ==================== READ SAS FUNCTION ====================
def read_sas_file(filepath):
    """Read SAS7BDAT file using pyreadstat and return as Polars DataFrame"""
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        return pl.from_pandas(df), meta
    except Exception as e:
        print(f"Error reading SAS file {filepath}: {e}")
        return None, None

# ==================== REPTDATE PROCESSING (matches SAS REPTDATE DATA step) ====================
print("Processing report date...")

# Hardcode report date as yesterday (equivalent to SET DEP.REPTDATE)
reptdate_val = date.today() - timedelta(days=1)

day_val = reptdate_val.day
mm = reptdate_val.month

# SELECT(DAY(REPTDATE)) logic from SAS
if day_val == 8:
    sdd, wk, wk1 = 1, '1', '4'
elif day_val == 15:
    sdd, wk, wk1 = 9, '2', '1'
elif day_val == 22:
    sdd, wk, wk1 = 16, '3', '2'
else:
    sdd, wk, wk1, wk2, wk3 = 23, '4', '3', '2', '1'

# MM1 calculation
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
NOWK2 = wk2 if 'wk2' in locals() else ''
NOWK3 = wk3 if 'wk3' in locals() else ''
REPTMON = f"{mm:02d}"
REPTMON1 = f"{mm1:02d}"
REPTMON2 = f"{mm2:02d}"
REPTYEAR = str(reptdate_val.year)
REPTDAY = f"{day_val:02d}"
RDATE = reptdate_val.strftime("%d%m%Y")
SDATE = sdate.strftime("%d%m%Y")

print(f"Report Date: {RDATE}, Week: {NOWK}")

# ==================== REMMTH CALCULATION (matches SAS %MACRO REMMTH) ====================
def calculate_remmth(row, reptdate_val):
    """Calculate remaining months - matches SAS %REMMTH macro"""
    if row["OPENIND"] == "D":
        return -1.0
    
    if row["OPENIND"] != "O":
        return None
    
    if not row["MATDATE"]:
        return None
    
    try:
        matdate_val = row["MATDATE"]
        if isinstance(matdate_val, (int, float)):
            try:
                fddt = date(1960, 1, 1) + timedelta(days=int(matdate_val))
            except:
                fddt = datetime.strptime(str(int(matdate_val)).zfill(8), "%Y%m%d").date()
        elif isinstance(matdate_val, (date, datetime)):
            fddt = matdate_val
        else:
            fddt = datetime.strptime(str(matdate_val).zfill(8), "%Y%m%d").date()
    except Exception as e:
        print(f"Error parsing MATDATE {row['MATDATE']}: {e}")
        return None
    
    rpyr = reptdate_val.year
    rpmth = reptdate_val.month
    rpday = reptdate_val.day
    
    fdyr = fddt.year
    fdmth = fddt.month
    fdday = fddt.day
    
    fd_days_in_month = days_in_month(fdyr, fdmth)
    rp_days_in_month = days_in_month(rpyr, rpmth)
    
    # IF FDDAY = FDDAYS(FDMTH) THEN FDDAY=RPDAYS(RPMTH);
    if fdday == fd_days_in_month:
        fdday = rp_days_in_month
    
    remy = fdyr - rpyr
    remm = fdmth - rpmth
    remd = fdday - rpday
    
    return float(remy * 12 + remm + remd / rp_days_in_month)

# ==================== PROCESS FDMTHLY DATA (matches SAS DATA ALM) ====================
print("Processing FDMTHLY data...")

fdmthly_file = BNM_PATH / "fdmthly.sas7bdat"
if not fdmthly_file.exists():
    print(f"FDMTHLY file not found at {fdmthly_file}")
    exit(1)

fdmthly, meta_fdmthly = read_sas_file(fdmthly_file)
if fdmthly is None:
    print("Failed to read FDMTHLY data")
    exit(1)

print(f"Read {len(fdmthly)} records from FDMTHLY")
print(f"SAS metadata: {meta_fdmthly.number_columns} columns, {meta_fdmthly.number_rows} rows")

# IF OPENIND='O' THEN DO; ... ELSE IF OPENIND='D' THEN REMMTH=-1;
fdmthly = fdmthly.filter(pl.col("OPENIND").is_in(["O", "D"]))

print("Calculating REMMTH...")
fdmthly = fdmthly.with_columns([
    pl.struct(["OPENIND", "MATDATE"]).map_elements(
        lambda x: calculate_remmth(x, reptdate_val),
        return_dtype=pl.Float64
    ).alias("REMMTH")
])

# KEEP=BIC CUSTCODE REMMTH CURBAL
alm = fdmthly.select(["BIC", "CUSTCODE", "REMMTH", "CURBAL"])

# ==================== PROC SUMMARY (matches SAS PROC SUMMARY DATA=ALM) ====================
print("Summarizing data...")
alm_summary = alm.group_by(["BIC", "CUSTCODE", "REMMTH"]).agg([
    pl.col("CURBAL").sum().alias("AMOUNT")
])

# ==================== DATA ALMDEPT (matches SAS DATA ALMDEPT) ====================
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

# ==================== GENERATE REPORTS (matches SAS PROC PRINT + TITLE) ====================
def generate_sas_report(data, bic_prefix, title_lines, output_file):
    """Generate report matching SAS PROC PRINT output with SUM statement"""
    report_data = data.filter(pl.col("BNMCODE").str.starts_with(bic_prefix))
    
    if len(report_data) == 0:
        print(f"No data for {bic_prefix}")
        # SAS still creates the report file, just with header and no rows
        with open(output_file, 'w') as f:
            for line in title_lines:
                f.write(line + "\n")
            f.write("\n")
            f.write(" " * 10 + "BNMCODE" + " " * 10 + "AMOUNT" + "\n")
            f.write(" " * 80 + "\n")
        print(f"Report saved: {output_file}")
        return None, 0.0
    
    # PROC SUMMARY DATA=ALMDEPT NWAY; WHERE SUBSTR(BNMCODE,1,5) = prefix; CLASS BNMCODE; VAR AMOUNT;
    summary = report_data.group_by("BNMCODE").agg([
        pl.col("AMOUNT").sum().alias("AMOUNT")
    ]).sort("BNMCODE")
    
    total_amount = summary["AMOUNT"].sum()
    
    # Write report matching SAS PROC PRINT layout
    with open(output_file, 'w') as f:
        # TITLE statements
        for line in title_lines:
            f.write(line + "\n")
        f.write("\n")
        
        # Header line
        f.write(" " * 10 + "BNMCODE" + " " * 10 + "AMOUNT" + "\n")
        f.write(" " * 80 + "\n")
        
        # Data rows
        for row in summary.iter_rows(named=True):
            f.write(f"{row['BNMCODE']:<20} {row['AMOUNT']:>20,.2f}\n")
        
        # SUM AMOUNT; line
        f.write(" " * 80 + "\n")
        f.write(f"{'SUM':<20} {total_amount:>20,.2f}\n")
        f.write(" " * 80 + "\n")
    
    print(f"Report saved: {output_file}")
    return summary, total_amount

# ==================== REPORT 1: 42130 ====================
print("\nGenerating report for 42130...")
report_42130, total_42130 = generate_sas_report(
    almdept, "42130",
    [
        "SPECIAL PURPOSE ITEMS (QUARTERLY): EXTERNAL LIABILITIES",
        f"AS AT {RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}",
        "CODE 81 & 85 FOR 42130-80-XX-0000Y"
    ],
    OUTPUT_PATH / f"ALMDEPT_42130_{RDATE}.txt"
)

# ==================== REPORT 2: 42132 ====================
print("Generating report for 42132...")
report_42132, total_42132 = generate_sas_report(
    almdept, "42132",
    [
        "SPECIAL PURPOSE ITEMS (QUARTERLY): EXTERNAL LIABILITIES",
        f"AS AT {RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}",
        "CODE 81 & 85 FOR 42132-80-XX-0000Y"
    ],
    OUTPUT_PATH / f"ALMDEPT_42132_{RDATE}.txt"
)

# ==================== REPORT 3: 42630 ====================
print("Generating report for 42630...")
report_42630, total_42630 = generate_sas_report(
    almdept, "42630",
    [
        "REPORT ON EXTERNAL LIABILITIES FOR FCY FD FROM FNBE (85)",
        f"AS AT {RDATE[:2]}/{RDATE[2:4]}/{RDATE[4:]}"
    ],
    OUTPUT_PATH / f"ALMDEPT_42630_{RDATE}.txt"
)

# ==================== SUMMARY STATISTICS ====================
print("\n" + "=" * 60)
print("PROCESSING SUMMARY")
print("=" * 60)
print(f"Report Date: {reptdate_val.strftime('%d/%m/%Y')}")
print(f"Week: {NOWK}, Month: {REPTMON}, Year: {REPTYEAR}")
print(f"Total FDMTHLY records processed: {len(fdmthly):,}")
print(f"Total ALM records: {len(alm):,}")
print(f"Total ALMDEPT records: {len(almdept):,}")

print("\nREPORT GENERATION SUMMARY:")
print("-" * 40)
print(f"42130 Special Report: {'Total = ' + format(total_42130, ',.2f') if total_42130 > 0 else 'No data'}")
print(f"42132 Special Report: {'Total = ' + format(total_42132, ',.2f') if total_42132 > 0 else 'No data'}")
print(f"42630 FCY FD Report:  {'Total = ' + format(total_42630, ',.2f') if total_42630 > 0 else 'No data'}")

print("\n" + "=" * 60)
print("PROCESSING COMPLETE")
print("=" * 60)
