import polars as pl
import os
import saspy
from datetime import datetime, timedelta
from pathlib import Path
import pyarrow.parquet as pq

# =============================================================================
# INITIALIZATION
# =============================================================================
sas = saspy.SASsession() if saspy else None
REPTDATE = datetime.today() - timedelta(days=1)

# CRITICAL FIX: Determine correct month for output
# If today is Dec 1st and we're processing yesterday (Nov 30th),
# we should write to November directory for consistency
OUTPUT_YEAR = REPTDATE.year
OUTPUT_MONTH = REPTDATE.month

print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d}")
print(f"OUTPUT TO: year={OUTPUT_YEAR}, month={OUTPUT_MONTH:02d}")

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_INPUT = Path("/host/cis/parquet/")
BASE_PATH = "/host/mis/parquet/crm"

# Input paths based on report date
CIS_YEAR, CIS_MONTH, CIS_DAY = REPTDATE.year, REPTDATE.month, REPTDATE.day
CIS_DATA_PATH = BASE_INPUT / f"year={CIS_YEAR}/month={CIS_MONTH:02d}/day={CIS_DAY:02d}"
TODAY_CHANNEL_SUM = CIS_DATA_PATH / "CIPHONET_ALL_SUMMARY.parquet"
TODAY_CHANNEL_UPDATE = CIS_DATA_PATH / "CIPHONET_FULL_SUMMARY.parquet"
OTC_SUMMARY_SRC = CIS_DATA_PATH / "CIPHONET_OTC_SUMMARY.parquet"

# Output paths based on OUTPUT month (not necessarily current calendar month)
OUTPUT_MONTH_PATH = f"{BASE_PATH}/year={OUTPUT_YEAR}/month={OUTPUT_MONTH:02d}"
OTC_DETAIL_PATH = f"{OUTPUT_MONTH_PATH}/otc_detail"

print(f"\nINPUT FROM: {CIS_DATA_PATH}")
print(f"OUTPUT TO: {OTC_DETAIL_PATH}")

for path in [OUTPUT_MONTH_PATH, OTC_DETAIL_PATH]:
    os.makedirs(path, exist_ok=True)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def parquet_exists(path):
    try:
        return pq.read_schema(str(path)) is not None
    except:
        return False

def read_brcode_lookup():
    """Read active branches from LKP_BRANCH"""
    try:
        with open("/sasdata/rawdata/lookup/LKP_BRANCH", 'r') as f:
            branches = []
            for line in f:
                if line.strip() and len(line) >= 4:
                    try:
                        branch_no = int(line[1:4].strip())
                        if line[49:50].strip() in ['O', 'A']:  # Active status
                            branches.append(branch_no)
                    except:
                        continue
            return pl.DataFrame({'BRANCHNO': branches})
    except:
        return pl.DataFrame({'BRANCHNO': []})

def write_to_sas(df, sas_path, dataset_name):
    """Write DataFrame to SAS dataset"""
    if not sas:
        return False
    try:
        lib_ref = "otc_temp"
        sas.submit(f"libname {lib_ref} '{sas_path}';")
        sas.df2sd(df.to_pandas(), table=dataset_name, libref=lib_ref)
        sas.submit(f"libname {lib_ref} clear;")
        return True
    except:
        return False

# =============================================================================
# PROCESS TODAY'S DATA
# =============================================================================
print("\n>>> READING TODAY'S DATA")

# Read and normalize channel data
today_channel = (pl.read_parquet(TODAY_CHANNEL_SUM)
                 .rename({"PROMPT": "TOLPROMPT", "UPDATED": "TOLUPDATE"})
                 .select(["CHANNEL", "TOLPROMPT", "TOLUPDATE"])
                 .with_columns(pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH"))
                 ) if parquet_exists(TODAY_CHANNEL_SUM) else pl.DataFrame()

today_update = (pl.read_parquet(TODAY_CHANNEL_UPDATE)
                .with_row_index()
                .with_columns(
                    pl.when(pl.col("index") == 1).then("TOTAL PROMPT BASE")
                     .when(pl.col("index") == 2).then("TOTAL UPDATED")
                     .alias("LINE")
                )
                .select(["LINE", "ATM", "EBK", "OTC", "TOTAL"])
                .with_columns(pl.lit(REPTDATE).alias("REPORT_DATE"))
                ) if parquet_exists(TODAY_CHANNEL_UPDATE) else pl.DataFrame()

print(f"  Channel: {len(today_channel)} | Update: {len(today_update)}")

# =============================================================================
# UPDATE MONTHLY DATA
# =============================================================================
print("\n>>> UPDATING MONTHLY DATA")

# Monthly files in the OUTPUT month directory
MONTHLY_CHANNEL_SUM = f"{OUTPUT_MONTH_PATH}/CHANNEL_SUM.parquet"
MONTHLY_CHANNEL_UPDATE = f"{OUTPUT_MONTH_PATH}/CHANNEL_UPDATE.parquet"

# Read existing monthly data
existing_channel = (pl.read_parquet(MONTHLY_CHANNEL_SUM) 
                   if os.path.exists(MONTHLY_CHANNEL_SUM) 
                   else pl.DataFrame())
existing_update = (pl.read_parquet(MONTHLY_CHANNEL_UPDATE) 
                  if os.path.exists(MONTHLY_CHANNEL_UPDATE) 
                  else pl.DataFrame())

# Append today's data
final_channel = pl.concat([existing_channel, today_channel])
final_update = pl.concat([existing_update, today_update])

# Write monthly data
final_channel.write_parquet(MONTHLY_CHANNEL_SUM)
final_update.write_parquet(MONTHLY_CHANNEL_UPDATE)

print(f"  Updated: Channel={len(final_channel)}, Update={len(final_update)}")

# =============================================================================
# PROCESS OTC_DETAIL
# =============================================================================
print("\n>>> PROCESSING OTC_DETAIL")

# Read branch data
bcode_df = read_brcode_lookup().sort("BRANCHNO")
print(f"  Active branches: {len(bcode_df)}")

# Read OTC summary
if parquet_exists(OTC_SUMMARY_SRC):
    branch_df = pl.read_parquet(OTC_SUMMARY_SRC).select([
        pl.col("BRANCHNO").cast(pl.Int64),
        pl.col("PROMPT").alias("TOLPROMPT").cast(pl.Int64).fill_null(0),
        pl.col("UPDATED").alias("TOLUPDATE").cast(pl.Int64).fill_null(0)
    ])
else:
    branch_df = pl.DataFrame({'BRANCHNO': [], 'TOLPROMPT': [], 'TOLUPDATE': []})

# Merge data
otc_detail = bcode_df.join(branch_df, on="BRANCHNO", how="left").with_columns([
    pl.col("TOLPROMPT").fill_null(0),
    pl.col("TOLUPDATE").fill_null(0)
])

print(f"  OTC records: {len(otc_detail)}")
print(f"  With data: {otc_detail.filter(pl.col('TOLPROMPT') > 0).height}")

# =============================================================================
# WRITE TO SAS DATASET
# =============================================================================
print(f"\n>>> WRITING OTC_DETAIL TO SAS DATASET")
print(f"  Path: {OTC_DETAIL_PATH}")

sas_success = write_to_sas(otc_detail, OTC_DETAIL_PATH, "OTC_DETAIL")

if sas_success:
    # Verify the file was created
    sas_file = f"{OTC_DETAIL_PATH}/OTC_DETAIL.sas7bdat"
    if os.path.exists(sas_file):
        file_size = os.path.getsize(sas_file)
        print(f"  ✓ SAS dataset created: {sas_file}")
        print(f"  ✓ File size: {file_size:,} bytes")
    else:
        print(f"  ✗ SAS file not found at expected location")
        # Try alternative naming
        alt_file = f"{OTC_DETAIL_PATH}/otc_detail.sas7bdat"
        if os.path.exists(alt_file):
            print(f"  ✓ Found with lowercase name: {alt_file}")
else:
    # Fallback to Parquet
    print(f"  ✗ SAS write failed, saving as Parquet...")
    fallback_path = f"{OUTPUT_MONTH_PATH}/OTC_DETAIL.parquet"
    otc_detail.write_parquet(fallback_path)
    print(f"  ✓ Saved as Parquet: {fallback_path}")

# =============================================================================
# VERIFICATION
# =============================================================================
print("\n>>> VERIFICATION")

# List all files in the output directory
print(f"\nFiles in {OTC_DETAIL_PATH}:")
if os.path.exists(OTC_DETAIL_PATH):
    files = os.listdir(OTC_DETAIL_PATH)
    if files:
        for file in sorted(files):
            filepath = os.path.join(OTC_DETAIL_PATH, file)
            size = os.path.getsize(filepath)
            print(f"  {file} ({size:,} bytes)")
    else:
        print(f"  Directory is empty!")
else:
    print(f"  Directory does not exist!")

# Check if SAS file exists with different naming
print(f"\nChecking for SAS files...")
potential_files = [
    f"{OTC_DETAIL_PATH}/OTC_DETAIL.sas7bdat",
    f"{OTC_DETAIL_PATH}/otc_detail.sas7bdat", 
    f"{OTC_DETAIL_PATH}/OTC_DETAIL.sas7bndx",
    f"{OTC_DETAIL_PATH}/otc_detail.sas7bndx"
]

for file in potential_files:
    if os.path.exists(file):
        print(f"  ✓ Found: {os.path.basename(file)}")

print("\n" + "=" * 60)
print(f"PROCESS COMPLETE")
print(f"Report Date: {REPTDATE:%Y-%m-%d}")
print(f"Output Month: {OUTPUT_YEAR}-{OUTPUT_MONTH:02d}")
print(f"OTC Detail Path: {OTC_DETAIL_PATH}")
print("=" * 60)
