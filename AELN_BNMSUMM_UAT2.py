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
REPTDATE = datetime.strptime(str(REPTDATE_value), "%Y-%m-%d")

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
# INPUT positions: @002 TRXNYY 4. @006 TRXNMM 2. @008 TRXNDD 2. etc.
with open(EGOLD_FILE, 'r') as f:
    egold_lines = f.readlines()

egold_data = []
for line in egold_lines:
    if len(line) < 123:  # Skip short lines
        continue
    try:
        egold_data.append({
            'TRXNYY': int(line[1:5]),           # @002, 4 chars
            'TRXNMM': int(line[5:7]),           # @006, 2 chars
            'TRXNDD': int(line[7:9]),           # @008, 2 chars
            'ACCTNO': line[12:22].strip(),      # @013, 10 chars
            'MPURCGM': line[25:35].strip(),     # @026, 10 chars
            'MSALEGM': line[41:51].strip(),     # @042, 10 chars
            'BRANCH': line[57:60].strip(),      # @058, 3 chars
            'MPURCPR': float(line[63:74]),      # @064, 11.6
            'MPURCAMT': float(line[77:91]),     # @078, 14.2
            'MSALEPR': float(line[94:105]),     # @095, 11.6
            'MSALEAMT': float(line[108:122])    # @109, 14.2
        })
    except (ValueError, IndexError):
        continue  # Skip malformed lines

EGOLD = pl.DataFrame(egold_data)

# Create TRXNDATE and REPTDATE
EGOLD = EGOLD.with_columns([
    pl.date(pl.col("TRXNYY"), pl.col("TRXNMM"), pl.col("TRXNDD")).alias("TRXNDATE"),
    pl.lit(int(REPTDT)).alias("REPTDATE"),
    pl.lit("EBANKING").alias("CHANNELIND")
])

# Read OTHER fixed-width file
# INPUT positions: same as EGOLD plus @125 TRANCODE 3. @128 CHANNEL 3.
with open(OTHER_FILE, 'r') as f:
    other_lines = f.readlines()

other_data = []
for line in other_lines:
    if len(line) < 131:  # Skip short lines
        continue
    try:
        other_data.append({
            'TRXNYY': int(line[1:5]),           # @002, 4 chars
            'TRXNMM': int(line[5:7]),           # @006, 2 chars
            'TRXNDD': int(line[7:9]),           # @008, 2 chars
            'ACCTNO': line[12:22].strip(),      # @013, 10 chars
            'MPURCGM': line[25:35].strip(),     # @026, 10 chars
            'MSALEGM': line[41:51].strip(),     # @042, 10 chars
            'BRANCH': line[57:60].strip(),      # @058, 3 chars
            'MPURCPR': float(line[63:74]),      # @064, 11.6
            'MPURCAMT': float(line[77:91]),     # @078, 14.2
            'MSALEPR': float(line[94:105]),     # @095, 11.6
            'MSALEAMT': float(line[108:122]),   # @109, 14.2
            'TRANCODE': line[124:127].strip(),  # @125, 3 chars
            'CHANNEL': line[127:130].strip()    # @128, 3 chars
        })
    except (ValueError, IndexError):
        continue  # Skip malformed lines

OTHER = pl.DataFrame(other_data)

# Create TRXNDATE and REPTDATE
OTHER = OTHER.with_columns([
    pl.date(pl.col("TRXNYY"), pl.col("TRXNMM"), pl.col("TRXNDD")).alias("TRXNDATE"),
    pl.lit(int(REPTDT)).alias("REPTDATE"),
    pl.lit("OTHER").alias("CHANNELIND")
])

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
