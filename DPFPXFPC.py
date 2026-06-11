# ============================================================
# JOB NAME : EIBMBTAT (Python version)
# DESC     : Average Original Tenure for BA Primary Rediscounted
# INPUT    : SAS7BDAT (SAS dataset)
# OUTPUT   : Parquet + CSV + Report
# ============================================================

import pandas as pd
import sas7bdat
from datetime import datetime, timedelta
import os
import sys

# ============================================================
# 0. DEFINE PATHS
# ============================================================

# Input paths
BTRWH_BASE_PATH = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/btrad" 

# Output paths
OUTPUT_DIR = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/BTRADE" 


# ============================================================
# 1. CALCULATE REPORT DATE (YESTERDAY'S DATE)
# ============================================================

# Get yesterday's date
#yesterday = datetime.now() - timedelta(days=1)
yesterday = datetime(2026, 6, 8)
REPTDATE = yesterday

# Calculate NOWK based on DAY of REPTDATE (SAS logic)
day = REPTDATE.day
if day == 8:
    NOWK = '1'
elif day == 15:
    NOWK = '2'
elif day == 22:
    NOWK = '3'
else:
    NOWK = '4'

REPTYEAR = REPTDATE.strftime("%y")  # YEAR2. format in SAS (2-digit year)
REPTMON  = REPTDATE.strftime("%m")  # Z2. format (leading zero)
RDATE    = REPTDATE.strftime("%d/%m/%Y")  # DDMMYY10. format

print(f"Report Date (Yesterday): {RDATE}")
print(f"Original REPTDATE value: {REPTDATE.strftime('%Y-%m-%d')}")
print(f"Day of month: {day} → Week number: {NOWK}")
print(f"Period: {REPTMON}/{NOWK}/{REPTYEAR}")
print("="*60)

# ============================================================
# 2. LOAD BTRAD DATASET (DYNAMIC NAME)
# SAS: BTRWH.BTRAD&REPTMON&NOWK&REPTYEAR
# ============================================================

btrad_sas = f"{BTRWH_BASE_PATH}{REPTMON}{NOWK}{REPTYEAR}.sas7bdat"
print(f"Looking for file: {btrad_sas}")

if not os.path.exists(btrad_sas):
    raise FileNotFoundError(f"BTRAD input file not found: {btrad_sas}")

def load_sas(path):
    """Load SAS .sas7bdat file into pandas DataFrame"""
    print(f"Loading: {path}")
    with sas7bdat.SAS7BDAT(path) as f:
        df = f.to_data_frame()
    return df

btrad = load_sas(btrad_sas)
print(f"Original records in BTRAD: {len(btrad):,}")

# ============================================================
# 3. FILTER DATA (SAS WHERE CLAUSE)
# Note: Preserving duplicate '34470' as in original SAS
# ============================================================

FACILITY_LIST = [
    '34411', '34412', '34421', '34422', '34440', '34470',
    '34470', '34480', '34490'  # Duplicate preserved from original
]

LIABCODE_LIST = ['BAE', 'BAI', 'BAP', 'BAS']

# Apply filters
btrad = btrad[
    (btrad["FACILITY"].isin(FACILITY_LIST)) &
    (btrad["LIABCODE"].isin(LIABCODE_LIST)) &
    (btrad["UTRDF"] == "D") &
    (btrad["BALANCE"] > 0)
].copy()

print(f"Records after filtering: {len(btrad):,}")

if len(btrad) == 0:
    print("WARNING: No records after filtering. Exiting.")
    sys.exit(0)

# ============================================================
# 4. CALCULATE TENURE & TOTAMT
# SAS: TENURE = (MATDATE-ISSDTE)+1
# ============================================================

# Convert to datetime if they aren't already
btrad["ISSDTE"]  = pd.to_datetime(btrad["ISSDTE"])
btrad["MATDATE"] = pd.to_datetime(btrad["MATDATE"])

# Calculate tenure in days (difference + 1)
btrad["TENURE"] = (btrad["MATDATE"] - btrad["ISSDTE"]).dt.days + 1
btrad["TOTAMT"] = btrad["FCVALUE"] * btrad["TENURE"]

# Keep only necessary columns (matching SAS KEEP statement)
btrad = btrad[
    ["BRANCH", "ACCTNOX", "TRANSREF", "FCVALUE", 
     "MATDATE", "ISSDTE", "TENURE", "TOTAMT"]
]

# ============================================================
# 5. SORT BY BRANCH (equivalent to PROC SORT)
# ============================================================

btrad = btrad.sort_values("BRANCH")

# ============================================================
# 6. PROC SUMMARY (BY BRANCH, SUM of FCVALUE and TOTAMT)
# SAS: PROC SUMMARY DATA=BTRAD NWAY; BY BRANCH; VAR FCVALUE TOTAMT;
#      OUTPUT OUT=AVGTENURE(DROP=_FREQ_ _TYPE_) SUM=;
# ============================================================

avgt = (
    btrad
    .groupby("BRANCH", as_index=False)
    .agg({
        "FCVALUE": "sum",
        "TOTAMT": "sum"
    })
)

print(f"Number of branches after aggregation: {len(avgt):,}")

# ============================================================
# 7. CALCULATE TENURE (SAS DATA step after PROC SUMMARY)
# ============================================================

avgt["TENURE"] = avgt["TOTAMT"] / avgt["FCVALUE"]

# Format as 8. (SAS format 8. with 2 decimals default)
avgt["TENURE"] = avgt["TENURE"].round(2)

# ============================================================
# 8. GENERATE PROC REPORT OUTPUT (Console/Text report)
# ============================================================

print("\n" + "="*86)
print(" P U B L I C   B A N K   B E R H A D".center(86))
print(" MANAGEMENT ACCOUNTING, FINANCE DIVISION".center(86))
print("="*86)
print(f" REPORT ID    : EIBMBTAT".center(86))
print(f" REPORT TITLE : AVERAGE ORIGINAL TENURE FOR BA PRIMARY REDISCOUNTED".center(86))
print(f" REPORT DATE  : {RDATE}".center(86))
print("="*86)

# Create the report dataframe with formatted columns
report_df = avgt.copy()
report_df["TOTAMT_FMT"] = report_df["TOTAMT"].apply(lambda x: f"{x:,.2f}")
report_df["FCVALUE_FMT"] = report_df["FCVALUE"].apply(lambda x: f"{x:,.2f}")
report_df["TENURE_FMT"] = report_df["TENURE"].apply(lambda x: f"{x:.2f}")

# Print header
print(f"{'BRANCH':<10} {'TOTAL AMOUNT (RM)':<30} {'BALANCE (RM)':<20} {'AVG TENURE (DAY)':<20}")
print("-"*86)

# Print each row
for _, row in report_df.iterrows():
    print(f"{row['BRANCH']:<10} {row['TOTAMT_FMT']:<30} {row['FCVALUE_FMT']:<20} {row['TENURE_FMT']:<20}")

# Calculate totals and overall average
total_totamt = avgt["TOTAMT"].sum()
total_fcvalue = avgt["FCVALUE"].sum()
overall_avg_tenure = total_totamt / total_fcvalue

# Print footer (RBREAK AFTER)
print("-"*86)
print(f"{'TOTAL :':<15} {total_totamt:>22,.2f} {total_fcvalue:>18,.2f} {'AVG :':<10} {overall_avg_tenure:>9.2f}")
print("="*86)

# ============================================================
# 9. SAVE OUTPUT DATASETS (Parquet and CSV)
# ============================================================

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Output filenames
base_filename = f"EIBMBTAT_AVGTENURE_{REPTMON}{NOWK}{REPTYEAR}"
OUTPUT_PARQUET = os.path.join(OUTPUT_DIR, f"{base_filename}.parquet")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, f"{base_filename}.csv")

# Save dataset (same as SAS dataset AVGTENURE)
avgt_output = avgt[["BRANCH", "TOTAMT", "FCVALUE", "TENURE"]].copy()
avgt_output["REPORT_DATE"] = RDATE
avgt_output["REPTDATE_RAW"] = REPTDATE.strftime("%Y-%m-%d")
avgt_output["REPTMON"] = REPTMON
avgt_output["NOWK"] = NOWK
avgt_output["REPTYEAR"] = REPTYEAR

# Save as Parquet
try:
    avgt_output.to_parquet(
        OUTPUT_PARQUET,
        engine="pyarrow",
        compression="snappy",
        index=False
    )
    print(f"\nDataset saved to Parquet: {OUTPUT_PARQUET}")
except Exception as e:
    print(f"Error saving Parquet: {e}")
    print("Falling back to CSV only...")

# Save as CSV
try:
    avgt_output.to_csv(
        OUTPUT_CSV,
        index=False,
        encoding='utf-8'
    )
    print(f"Dataset saved to CSV: {OUTPUT_CSV}")
except Exception as e:
    print(f"Error saving CSV: {e}")

# ============================================================
# 10. OPTIONAL: Save the report as text file
# ============================================================

try:
    report_txt = os.path.join(OUTPUT_DIR, f"{base_filename}_report.txt")
    with open(report_txt, 'w') as f:
        # Redirect print output to file (simpler approach)
        original_stdout = sys.stdout
        sys.stdout = f
        
        print("="*86)
        print(" P U B L I C   B A N K   B E R H A D".center(86))
        print(" MANAGEMENT ACCOUNTING, FINANCE DIVISION".center(86))
        print("="*86)
        print(f" REPORT ID    : EIBMBTAT".center(86))
        print(f" REPORT TITLE : AVERAGE ORIGINAL TENURE FOR BA PRIMARY REDISCOUNTED".center(86))
        print(f" REPORT DATE  : {RDATE}".center(86))
        print("="*86)
        print(f"{'BRANCH':<10} {'TOTAL AMOUNT (RM)':<30} {'BALANCE (RM)':<20} {'AVG TENURE (DAY)':<20}")
        print("-"*86)
        
        for _, row in report_df.iterrows():
            print(f"{row['BRANCH']:<10} {row['TOTAMT_FMT']:<30} {row['FCVALUE_FMT']:<20} {row['TENURE_FMT']:<20}")
        
        print("-"*86)
        print(f"{'TOTAL :':<15} {total_totamt:>22,.2f} {total_fcvalue:>18,.2f} {'AVG :':<10} {overall_avg_tenure:>9.2f}")
        print("="*86)
        
        sys.stdout = original_stdout
    
    print(f"Report saved to text file: {report_txt}")
except Exception as e:
    print(f"Error saving report text file: {e}")

# ============================================================
# 11. DISPLAY EXECUTION SUMMARY
# ============================================================
print("\n" + "="*60)
print("EIBMBTAT job completed successfully")
print("="*60)
print(f"Execution Date & Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Report Date (Yesterday): {RDATE}")
print(f"Input BTRAD file: {btrad_sas}")
print(f"Records processed: {len(btrad):,}")
print(f"Branches summarized: {len(avgt):,}")
print(f"Total Balance (FCVALUE): RM {total_fcvalue:,.2f}")
print(f"Total Amount (TOTAMT): RM {total_totamt:,.2f}")
print(f"Overall Average Tenure: {overall_avg_tenure:.2f} days")
print(f"Output directory: {OUTPUT_DIR}")
print("="*60)

# ============================================================
# END OF JOB
# ============================================================
