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
print(f"Month: {mm}, Day: {day_val}, Year: {reptdate.year}")

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

def yyyymmdd_to_date(yyyymmdd):
    if yyyymmdd is None or pd.isna(yyyymmdd):
        return None
    try:
        date_str = str(int(yyyymmdd))
        if len(date_str) == 8:
            return date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8]))
        return None
    except:
        return None

def calculate_remmth_original(row, reptdate_val):
    """Original SAS logic from the code"""
    openind = row.get("OPENIND", "")
    matdate = row.get("MATDATE_DATE")
    
    if openind == "D":
        return -1.0
    if openind != "O" or matdate is None:
        return None
    
    try:
        rpyr, rpmth, rpday = reptdate_val.year, reptdate_val.month, reptdate_val.day
        fdyr, fdmth, fdday = matdate.year, matdate.month, matdate.day
        
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
    fdmthly = fdmthly.with_columns([
        pl.col("MATDATE").map_elements(
            lambda x: yyyymmdd_to_date(x),
            return_dtype=pl.Date
        ).alias("MATDATE_DATE")
    ])
    
    fdmthly = fdmthly.with_columns([
        pl.struct(["OPENIND", "MATDATE_DATE"]).map_elements(
            lambda x: calculate_remmth_original(x, reptdate_val),
            return_dtype=pl.Float64
        ).alias("REMMTH")
    ])

# Keep only positive REMMTH
fdmthly = fdmthly.filter(pl.col("REMMTH") >= 0)
print(f"Records with positive REMMTH: {fdmthly.height:,}")

# ==================== DEBUG: Analyze REMMTH Distribution ====================
print("\n" + "="*60)
print("DEBUG: REMMTH Analysis")
print("="*60)

# Check REMMTH values near boundaries
print("\nREMMTH values near integer boundaries:")
boundary_check = fdmthly.filter(
    (pl.col("REMMTH") >= 0.9) & (pl.col("REMMTH") <= 1.1) |
    (pl.col("REMMTH") >= 1.9) & (pl.col("REMMTH") <= 2.1) |
    (pl.col("REMMTH") >= 2.9) & (pl.col("REMMTH") <= 3.1) |
    (pl.col("REMMTH") >= 3.9) & (pl.col("REMMTH") <= 4.1)
).select(["REMMTH", "BIC", "CUSTCODE", "CURBAL"]).head(20)
print(boundary_check)

# Check MATDATE values for those records
print("\nSample MATDATE values:")
matdate_sample = fdmthly.select(["MATDATE", "MATDATE_DATE", "REMMTH"]).head(20)
print(matdate_sample)

# ==================== FILTER FOR RELEVANT BIC PREFIXES ====================
alm = fdmthly.select(["BIC", "CUSTCODE", "REMMTH", "CURBAL"])

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

# ==================== DEBUG: Show REMMTH distribution for 42130 ====================
print("\n" + "="*60)
print("DEBUG: REMMTH distribution for BIC 42130")
print("="*60)

bic_42130 = alm_summary.filter(pl.col("BIC") == "42130")
if bic_42130.height > 0:
    # Show how many records per REMMTH value
    remmth_dist = bic_42130.group_by("REMMTH").agg([
        pl.len().alias("COUNT"),
        pl.col("AMOUNT").sum().alias("TOTAL_AMOUNT")
    ]).sort("REMMTH")
    print("REMMTH distribution:")
    print(remmth_dist)
    
    # Show which CUSTCODEs are present
    custcode_dist = bic_42130.group_by("CUSTCODE").agg([
        pl.len().alias("COUNT")
    ]).sort("CUSTCODE")
    print("\nCUSTCODE distribution for 42130:")
    print(custcode_dist)

# ==================== CREATE ALMDEPT with different methods ====================
print("\n" + "="*60)
print("TESTING DIFFERENT KREMMTH METHODS")
print("="*60)

def kremmth_method1(value):
    """Method 1: Original with truncation"""
    if value is None or pd.isna(value):
        return None
    # Truncate to 12 decimal places
    truncated = math.floor(value * 1e12) / 1e12
    if truncated < 0:
        return '51'
    elif 0 <= truncated < 1:
        return '52'
    elif 1 <= truncated < 2:
        return '53'
    elif 2 <= truncated < 3:
        return '54'
    elif 3 <= truncated < 4:
        return '81'
    elif 4 <= truncated < 5:
        return '82'
    elif 5 <= truncated < 6:
        return '83'
    elif 6 <= truncated < 7:
        return '84'
    elif 7 <= truncated < 8:
        return '85'
    elif 8 <= truncated < 9:
        return '86'
    elif 9 <= truncated < 10:
        return '87'
    elif 10 <= truncated < 11:
        return '88'
    elif 11 <= truncated < 12:
        return '89'
    else:
        return '60'

def kremmth_method2(value):
    """Method 2: Floor approach (matching SAS integer truncation)"""
    if value is None or pd.isna(value):
        return None
    # Use floor for exact integer comparison
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

def kremmth_method3(value):
    """Method 3: Round to 10 decimal places then truncate"""
    if value is None or pd.isna(value):
        return None
    rounded = round(value, 10)
    if rounded < 0:
        return '51'
    elif 0 <= rounded < 1:
        return '52'
    elif 1 <= rounded < 2:
        return '53'
    elif 2 <= rounded < 3:
        return '54'
    elif 3 <= rounded < 4:
        return '81'
    elif 4 <= rounded < 5:
        return '82'
    elif 5 <= rounded < 6:
        return '83'
    elif 6 <= rounded < 7:
        return '84'
    elif 7 <= rounded < 8:
        return '85'
    elif 8 <= rounded < 9:
        return '86'
    elif 9 <= rounded < 10:
        return '87'
    elif 10 <= rounded < 11:
        return '88'
    elif 11 <= rounded < 12:
        return '89'
    else:
        return '60'

# Test all methods on a sample
sample_remmth = [0.9999999999, 1.0000000001, 1.9999999999, 2.0000000001, 2.9999999999, 3.0000000001]
print("\nTesting boundary values:")
print(f"{'Value':<20} {'Method1':<10} {'Method2':<10} {'Method3':<10}")
print("-" * 50)
for val in sample_remmth:
    print(f"{val:<20} {kremmth_method1(val):<10} {kremmth_method2(val):<10} {kremmth_method3(val):<10}")

# ==================== CREATE ALMDEPT with Method 1 ====================
print("\nCreating BNM codes with Method 1 (truncation)...")
alm_summary = alm_summary.with_columns([
    pl.col("CUSTCODE").cast(pl.Utf8).str.strip_chars().alias("CUSTCODE_STR")
])

almdept1 = alm_summary.with_columns([
    pl.col("REMMTH").map_elements(kremmth_method1, return_dtype=pl.Utf8).alias("RM")
]).with_columns([
    pl.when(pl.col("CUSTCODE_STR").is_in(["81", "82", "83", "84"]))
    .then(pl.concat_str([pl.col("BIC"), pl.col("CUSTCODE_STR"), pl.col("RM"), pl.lit("0000Y")], separator=""))
    .when(pl.col("CUSTCODE_STR").is_in(["85", "86", "87", "88", "89", "90", "91", "92", "95", "96", "98", "99"]))
    .then(pl.concat_str([pl.col("BIC"), pl.lit("85"), pl.col("RM"), pl.lit("0000Y")], separator=""))
    .otherwise(None)
    .alias("BNMCODE")
]).filter(pl.col("BNMCODE").is_not_null()).select(["BNMCODE", "AMOUNT", "CUSTCODE", "REMMTH"])

print(f"ALMDEPT Method 1 records: {almdept1.height:,}")

# Create ALMDEPT with Method 2
print("\nCreating BNM codes with Method 2 (floor)...")
almdept2 = alm_summary.with_columns([
    pl.col("REMMTH").map_elements(kremmth_method2, return_dtype=pl.Utf8).alias("RM")
]).with_columns([
    pl.when(pl.col("CUSTCODE_STR").is_in(["81", "82", "83", "84"]))
    .then(pl.concat_str([pl.col("BIC"), pl.col("CUSTCODE_STR"), pl.col("RM"), pl.lit("0000Y")], separator=""))
    .when(pl.col("CUSTCODE_STR").is_in(["85", "86", "87", "88", "89", "90", "91", "92", "95", "96", "98", "99"]))
    .then(pl.concat_str([pl.col("BIC"), pl.lit("85"), pl.col("RM"), pl.lit("0000Y")], separator=""))
    .otherwise(None)
    .alias("BNMCODE")
]).filter(pl.col("BNMCODE").is_not_null()).select(["BNMCODE", "AMOUNT", "CUSTCODE", "REMMTH"])

print(f"ALMDEPT Method 2 records: {almdept2.height:,}")

# Create ALMDEPT with Method 3
print("\nCreating BNM codes with Method 3 (round)...")
almdept3 = alm_summary.with_columns([
    pl.col("REMMTH").map_elements(kremmth_method3, return_dtype=pl.Utf8).alias("RM")
]).with_columns([
    pl.when(pl.col("CUSTCODE_STR").is_in(["81", "82", "83", "84"]))
    .then(pl.concat_str([pl.col("BIC"), pl.col("CUSTCODE_STR"), pl.col("RM"), pl.lit("0000Y")], separator=""))
    .when(pl.col("CUSTCODE_STR").is_in(["85", "86", "87", "88", "89", "90", "91", "92", "95", "96", "98", "99"]))
    .then(pl.concat_str([pl.col("BIC"), pl.lit("85"), pl.col("RM"), pl.lit("0000Y")], separator=""))
    .otherwise(None)
    .alias("BNMCODE")
]).filter(pl.col("BNMCODE").is_not_null()).select(["BNMCODE", "AMOUNT", "CUSTCODE", "REMMTH"])

print(f"ALMDEPT Method 3 records: {almdept3.height:,}")

# ==================== COMPARE METHODS FOR 42130 ====================
print("\n" + "="*60)
print("COMPARING METHODS FOR 42130")
print("="*60)

for method_name, method_data in [("Method 1", almdept1), ("Method 2", almdept2), ("Method 3", almdept3)]:
    if method_data.height > 0:
        bic_data = method_data.filter(pl.col("BNMCODE").str.starts_with("42130"))
        if bic_data.height > 0:
            total = bic_data["AMOUNT"].sum()
            print(f"\n{method_name} total for 42130: {total:,.2f}")
            print("Top 5 BNMCODEs:")
            top5 = bic_data.group_by("BNMCODE").agg([
                pl.col("AMOUNT").sum().alias("AMOUNT")
            ]).sort("BNMCODE").head(5)
            print(top5)

print("\n" + "="*60)
print("RECOMMENDATION: Check which method matches production")
print("="*60)
print("Compare the output above with production to see which method")
print("produces the same distribution of amounts across BNMCODEs.")
