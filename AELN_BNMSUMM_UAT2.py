import polars as pl
import pandas as pd
from datetime import datetime
from pathlib import Path

# Base paths
BASE_INPUT_PATH = Path("/pythonITD/mis_dev/source_data")
BASE_OUTPUT_PATH = Path("/host/mis/parquet")
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# File paths
REPTDATE_FILE = BASE_INPUT_PATH / "reptdate.sas7bdat"
EGOLD_FILE = BASE_INPUT_PATH / "EGOLD.txt"
OTHER_FILE = BASE_INPUT_PATH / "OTHR.txt"

# Read REPTDATE from SAS file using pandas then convert to polars
REPTDATE_pandas = pd.read_sas(REPTDATE_FILE)
REPTDATE_df = pl.from_pandas(REPTDATE_pandas)
REPTDATE_value = REPTDATE_df["REPTDATE"][0]

# Convert SAS date (days since 1960-01-01) to Python datetime
# SAS date is stored as number of days since January 1, 1960
from datetime import timedelta
SAS_EPOCH = datetime(1960, 1, 1)
REPTDATE = SAS_EPOCH + timedelta(days=int(REPTDATE_value))

day = REPTDATE.day
month = REPTDATE.month
year = REPTDATE.year

# Determine week number (CALL SYMPUT logic)
if 1 <= day <= 8:
    NOWK = "1"
elif 9 <= day <= 15:
    NOWK = "2"
elif 16 <= day <= 22:
    NOWK = "3"
else:
    NOWK = "4"

REPTYEAR = str(year)[-2:]  # YEAR2.
REPTMON = f"{month:02d}"    # Z2.
REPTDAY = f"{day:02d}"      # Z2.
REPTDT = REPTDATE.strftime("%Y%m%d")  # 8.

# Read EGOLD fixed-width file
# Based on actual file format analysis
egold_data = []
try:
    with open(EGOLD_FILE, 'r') as f:
        egold_lines = f.readlines()
    
    for line in egold_lines:
        if len(line) < 80:  # Skip short lines
            continue
        try:
            date_str = line[0:8].strip()
            egold_data.append({
                'TRXNYY': int(date_str[0:4]),      # Year from date
                'TRXNMM': int(date_str[4:6]),      # Month from date
                'TRXNDD': int(date_str[6:8]),      # Day from date
                'ACCTNO': line[8:20].strip(),      # Account number
                'MPURCGM': line[20:35].strip(),    # Purchase grams
                'MSALEGM': line[35:50].strip(),    # Sale grams
                'BRANCH': line[50:55].strip(),     # Branch
                'MPURCPR': float(line[55:70].strip() or '0'),    # Purchase price
                'MPURCAMT': float(line[70:85].strip() or '0'),   # Purchase amount
                'MSALEPR': float(line[85:100].strip() or '0'),   # Sale price
                'MSALEAMT': float(line[100:115].strip() or '0')  # Sale amount
            })
        except (ValueError, IndexError) as e:
            continue  # Skip malformed lines
except FileNotFoundError:
    print(f"Warning: {EGOLD_FILE} not found. Creating empty EGOLD dataset.")

if egold_data:
    EGOLD = pl.DataFrame(egold_data)
    # Create TRXNDATE and REPTDATE
    EGOLD = EGOLD.with_columns([
        pl.date(pl.col("TRXNYY"), pl.col("TRXNMM"), pl.col("TRXNDD")).alias("TRXNDATE"),
        pl.lit(int(REPTDT)).alias("REPTDATE"),
        pl.lit("EBANKING").alias("CHANNELIND")
    ])
else:
    # Create empty DataFrame with correct schema
    EGOLD = pl.DataFrame({
        'TRXNYY': [],
        'TRXNMM': [],
        'TRXNDD': [],
        'ACCTNO': [],
        'MPURCGM': [],
        'MSALEGM': [],
        'BRANCH': [],
        'MPURCPR': [],
        'MPURCAMT': [],
        'MSALEPR': [],
        'MSALEAMT': [],
        'TRXNDATE': [],
        'REPTDATE': [],
        'CHANNELIND': []
    })

# Read OTHER fixed-width file
# Based on actual file format - includes TRANCODE and CHANNEL at the end
other_data = []
try:
    with open(OTHER_FILE, 'r') as f:
        other_lines = f.readlines()
    
    for line in other_lines:
        if len(line) < 115:  # Skip short lines
            continue
        try:
            date_str = line[0:8].strip()
            other_data.append({
                'TRXNYY': int(date_str[0:4]),      # Year from date
                'TRXNMM': int(date_str[4:6]),      # Month from date
                'TRXNDD': int(date_str[6:8]),      # Day from date
                'ACCTNO': line[8:20].strip(),      # Account number
                'MPURCGM': line[20:35].strip(),    # Purchase grams
                'MSALEGM': line[35:50].strip(),    # Sale grams
                'BRANCH': line[50:55].strip(),     # Branch
                'MPURCPR': float(line[55:70].strip() or '0'),    # Purchase price
                'MPURCAMT': float(line[70:85].strip() or '0'),   # Purchase amount
                'MSALEPR': float(line[85:100].strip() or '0'),   # Sale price
                'MSALEAMT': float(line[100:115].strip() or '0'), # Sale amount
                'TRANCODE': line[115:120].strip() if len(line) > 115 else '',  # Transaction code
                'CHANNEL': line[120:125].strip() if len(line) > 120 else ''    # Channel
            })
        except (ValueError, IndexError) as e:
            continue  # Skip malformed lines
except FileNotFoundError:
    print(f"Warning: {OTHER_FILE} not found. Creating empty OTHER dataset.")

if other_data:
    OTHER = pl.DataFrame(other_data)
    # Create TRXNDATE and REPTDATE
    OTHER = OTHER.with_columns([
        pl.date(pl.col("TRXNYY"), pl.col("TRXNMM"), pl.col("TRXNDD")).alias("TRXNDATE"),
        pl.lit(int(REPTDT)).alias("REPTDATE"),
        pl.lit("OTHER").alias("CHANNELIND")
    ])
else:
    # Create empty DataFrame with correct schema
    OTHER = pl.DataFrame({
        'TRXNYY': [],
        'TRXNMM': [],
        'TRXNDD': [],
        'ACCTNO': [],
        'MPURCGM': [],
        'MSALEGM': [],
        'BRANCH': [],
        'MPURCPR': [],
        'MPURCAMT': [],
        'MSALEPR': [],
        'MSALEAMT': [],
        'TRANCODE': [],
        'CHANNEL': [],
        'TRXNDATE': [],
        'REPTDATE': [],
        'CHANNELIND': []
    })

# Combine EGOLD and OTHER
GOLDTRAN = pl.concat([EGOLD, OTHER], how="diagonal")

# APPEND MACRO logic
target_name = f"MIS_GOLDTRAN{REPTMON}{NOWK}"

# Check if this is a week start day (01, 09, 16, or 23)
if REPTDAY in ["01", "09", "16", "23"]:
    # Start new dataset - equivalent to creating new MIS.GOLDTRAN dataset
    MIS_GOLDTRAN = GOLDTRAN
else:
    # Append logic - load existing, remove current REPTDATE, then append
    # Look for existing file in output partitions
    existing_files = list(BASE_OUTPUT_PATH.glob(f"year={year}/month={REPTMON}/day=*/{target_name}.parquet"))
    
    if existing_files:
        # Load the most recent existing file
        MIS_GOLDTRAN = pl.read_parquet(existing_files[-1])
        # Remove records with current REPTDATE (IF REPTDATE EQ &REPTDT THEN DELETE)
        MIS_GOLDTRAN = MIS_GOLDTRAN.filter(pl.col("REPTDATE") != int(REPTDT))
        # Append new data (PROC APPEND)
        MIS_GOLDTRAN = pl.concat([MIS_GOLDTRAN, GOLDTRAN], how="diagonal")
    else:
        # No existing file found, start fresh
        MIS_GOLDTRAN = GOLDTRAN

# Create Hive partition structure: /year=YYYY/month=MM/day=DD/
output_path = BASE_OUTPUT_PATH / f"year={year}" / f"month={REPTMON}" / f"day={REPTDAY}"
output_path.mkdir(parents=True, exist_ok=True)

# Save to output path (equivalent to MIS.GOLDTRAN&REPTMON&NOWK)
output_file = output_path / f"{target_name}.parquet"
MIS_GOLDTRAN.write_parquet(output_file)

# Save TEMP copy (equivalent to TEMP.GOLDTRAN&REPTMON&NOWK&REPTYEAR)
temp_name = f"TEMP_GOLDTRAN{REPTMON}{NOWK}{REPTYEAR}"
temp_file = output_path / f"{temp_name}.parquet"
MIS_GOLDTRAN.write_parquet(temp_file)

print(f"Processing complete!")
print(f"Report Date: {REPTDATE.strftime('%Y-%m-%d')}")
print(f"Week: {NOWK}")
print(f"Output saved to: {output_path}")
print(f"Records processed: {len(MIS_GOLDTRAN)}")
