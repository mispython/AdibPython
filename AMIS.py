import polars as pl
import pyreadstat
import os
import duckdb
import struct
from datetime import datetime, timedelta
from pathlib import Path
import pyarrow.parquet as pq

# =============================================================================
# DATE CALCULATIONS
# =============================================================================
# Batch date is TODAY - 1
REPTDATE = datetime.today() - timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)
SAS_ORIGIN = datetime(1960, 1, 1)
RDATE = (REPTDATE - SAS_ORIGIN).days

# Formatted date variables
REPTYEAR = f"{REPTDATE.year % 100:02d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"

# Calculate last month (BASE) and current month (NEW BASE)
current_month_date = REPTDATE.replace(day=1)
last_month_date = current_month_date - timedelta(days=1)

LAST_MONTH_YEAR = last_month_date.year
LAST_MONTH_NUM = last_month_date.month

CURRENT_MONTH_YEAR = current_month_date.year
CURRENT_MONTH_NUM = current_month_date.month

# CIS path for today's batch data
CIS_YEAR = REPTDATE.year
CIS_MONTH = REPTDATE.month
CIS_DAY = REPTDATE.day

print("=" * 80)
print("BATCH DATE CALCULATIONS")
print("=" * 80)
print(f"Batch Date (REPTDATE): {REPTDATE.strftime('%Y-%m-%d')}")
print(f"Previous Date (PREVDATE): {PREVDATE.strftime('%Y-%m-%d')}")
print(f"SAS Date (RDATE): {RDATE}")
print()
print(f"LAST MONTH (FINAL):       {LAST_MONTH_YEAR}-{LAST_MONTH_NUM:02d}")
print(f"CURRENT MONTH (ACTIVE):   {CURRENT_MONTH_YEAR}-{CURRENT_MONTH_NUM:02d}")
print(f"CIS DATA PATH:            year={CIS_YEAR}/month={CIS_MONTH}/day={CIS_DAY}")
print("=" * 80)
print()

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_INPUT_PATH = Path("/host/cis/parquet/")
BASE_PATH = "/host/mis/parquet/crm"

# Today's CIS input data paths (dynamic based on REPTDATE)
CIS_DATA_PATH = BASE_INPUT_PATH / f"year={CIS_YEAR}/month={CIS_MONTH}/day={CIS_DAY}"
TODAY_CHANNEL_SUM = CIS_DATA_PATH / "CIPHONET_ALL_SUMMARY.parquet"
TODAY_CHANNEL_UPDATE = CIS_DATA_PATH / "CIPHONET_FULL_SUMMARY.parquet"
OTC_SUMMARY_SRC = CIS_DATA_PATH / "CIPHONET_OTC_SUMMARY.parquet"  # For BRANCH data

# Lookup file
BCODE_LOOKUP = "/sasdata/rawdata/lookup/LKP_BRANCH"

# Last month paths (FINAL - no more updates after month end)
LAST_MONTH_PATH = f"{BASE_PATH}/year={LAST_MONTH_YEAR}/month={LAST_MONTH_NUM:02d}"
os.makedirs(LAST_MONTH_PATH, exist_ok=True)
LAST_CHANNEL_SUM = f"{LAST_MONTH_PATH}/CHANNEL_SUM.parquet"
LAST_CHANNEL_UPDATE = f"{LAST_MONTH_PATH}/CHANNEL_UPDATE.parquet"

# Current month paths (ACTIVE - accumulating daily)
CURRENT_MONTH_PATH = f"{BASE_PATH}/year={CURRENT_MONTH_YEAR}/month={CURRENT_MONTH_NUM:02d}"
os.makedirs(CURRENT_MONTH_PATH, exist_ok=True)
CURRENT_CHANNEL_SUM = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
CURRENT_CHANNEL_UPDATE = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"

# OTC_DETAIL output (CRMWH library)
CRMWH_DIR = Path(BASE_PATH) / "CRMWH"
CRMWH_DIR.mkdir(parents=True, exist_ok=True)

print(f"CIS Input Path:        {CIS_DATA_PATH}")
print(f"Last Month (FINAL):    {LAST_MONTH_PATH}")
print(f"Current Month (ACTIVE):{CURRENT_MONTH_PATH}")
print(f"CRMWH Output:          {CRMWH_DIR}")
print()

# =============================================================================
# BRCODE LOOKUP READER (EBCDIC Format)
# =============================================================================

# BRCODE Layout
LRECL = 80
LAYOUT = {
    'BRCODE': {'type': 'packed', 'slice': slice(1, 9, None)},
    'BRABBR': {'type': 'ebcdic', 'slice': slice(5, 8, None)},
    'BRSTAT': {'type': 'ebcdic', 'slice': slice(11, 40, None)},
    'BRSTATEIND': {'type': 'ebcdic', 'slice': slice(44, 45, None)},
    'BRSTATUS': {'type': 'ebcdic', 'slice': slice(49, 50, None)}
}

def unpack_packed_decimal(packed_bytes):
    """
    Unpack IBM packed decimal (COMP-3) to integer
    Each byte contains 2 digits except last byte (1 digit + sign)
    """
    if not packed_bytes or len(packed_bytes) == 0:
        return None
    
    result = 0
    for i, byte in enumerate(packed_bytes[:-1]):
        high = (byte >> 4) & 0x0F
        low = byte & 0x0F
        result = result * 100 + high * 10 + low
    
    # Last byte: high nibble is digit, low nibble is sign
    last_byte = packed_bytes[-1]
    last_digit = (last_byte >> 4) & 0x0F
    result = result * 10 + last_digit
    
    # Check sign (0xD = negative, 0xC/0xF = positive)
    sign = last_byte & 0x0F
    if sign == 0x0D:
        result = -result
    
    return result

def decode_ebcdic(ebcdic_bytes):
    """Decode EBCDIC bytes to ASCII string"""
    try:
        return ebcdic_bytes.decode('cp037').strip()
    except:
        return ''

def read_brcode_lookup(filepath):
    """
    Read LKP_BRANCH EBCDIC file and return DataFrame
    
    SAS equivalent:
    DATA BCODE;
       INFILE BCODE;
       INPUT @002 BRANCHNO 3.;
    RUN;
    """
    records = []
    
    try:
        with open(filepath, 'rb') as f:
            while True:
                record = f.read(LRECL)
                if not record or len(record) < LRECL:
                    break
                
                row = {}
                
                for field_name, field_info in LAYOUT.items():
                    field_slice = field_info['slice']
                    field_type = field_info['type']
                    field_bytes = record[field_slice]
                    
                    if field_type == 'packed':
                        # Packed decimal - convert to integer
                        row[field_name] = unpack_packed_decimal(field_bytes)
                    elif field_type == 'ebcdic':
                        # EBCDIC string - convert to ASCII
                        row[field_name] = decode_ebcdic(field_bytes)
                    else:
                        row[field_name] = None
                
                records.append(row)
        
        # Convert to Polars DataFrame
        df = pl.DataFrame(records)
        
        # Match SAS data types and rename BRCODE to BRANCHNO
        df = df.select([
            pl.col('BRCODE').cast(pl.Int64).alias('BRANCHNO'),
            pl.col('BRABBR').cast(pl.Utf8),
            pl.col('BRSTAT').cast(pl.Utf8),
            pl.col('BRSTATEIND').cast(pl.Utf8),
            pl.col('BRSTATUS').cast(pl.Utf8)
        ])
        
        # Filter for active branches only (optional - matching SAS logic)
        df = df.filter(pl.col('BRSTATUS') == 'A')
        
        return df
        
    except FileNotFoundError:
        print(f"⚠ Lookup file not found: {filepath}")
        return pl.DataFrame({'BRANCHNO': pl.Series([], dtype=pl.Int64)})
    except Exception as e:
        print(f"✗ Error reading lookup file: {str(e)}")
        return pl.DataFrame({'BRANCHNO': pl.Series([], dtype=pl.Int64)})

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def parquet_exists(path):
    """Check if parquet file exists and is readable"""
    try:
        if isinstance(path, Path):
            path = str(path)
        pq.read_schema(path)
        return True
    except Exception:
        return False

def normalize_channel_sum_schema(df):
    """Normalize CHANNEL_SUM schema - handle PROMPT/TOLPROMPT and UPDATED/TOLUPDATE"""
    if "PROMPT" in df.columns and "TOLPROMPT" not in df.columns:
        df = df.rename({"PROMPT": "TOLPROMPT"})
    if "UPDATED" in df.columns and "TOLUPDATE" not in df.columns:
        df = df.rename({"UPDATED": "TOLUPDATE"})
    
    for col in df.columns:
        if df[col].dtype == pl.Object:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))
    
    if "MONTH" not in df.columns:
        month_str = REPTDATE.strftime("%b%y").upper()
        df = df.with_columns(pl.lit(month_str).alias("MONTH"))
    
    return df.select([
        pl.col("CHANNEL").cast(pl.Utf8),
        pl.col("TOLPROMPT").cast(pl.Int64),
        pl.col("TOLUPDATE").cast(pl.Int64),
        pl.col("MONTH").cast(pl.Utf8)
    ])

def process_full_summary_to_channel_update(df):
    """Convert CIPHONET_FULL_SUMMARY to CHANNEL_UPDATE format"""
    df = df.with_row_index(name="_N_", offset=1)
    
    df = df.with_columns(
        pl.when(pl.col("_N_") == 1).then(pl.lit("TOTAL PROMPT BASE"))
         .when(pl.col("_N_") == 2).then(pl.lit("TOTAL UPDATED"))
         .otherwise(pl.lit(None))
         .alias("LINE")
    )
    
    for col in df.columns:
        if col != "REPORT_DATE" and df[col].dtype == pl.Object:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))
    
    if "REPORT_DATE" not in df.columns:
        df = df.with_columns(pl.lit(REPTDATE).alias("REPORT_DATE"))
    elif df["REPORT_DATE"].dtype == pl.Utf8 or df["REPORT_DATE"].dtype == pl.Object:
        df = df.with_columns(
            pl.col("REPORT_DATE").str.to_date(format="%d/%m/%Y", strict=False)
            .fill_null(
                pl.col("REPORT_DATE").str.to_date(format="%Y-%m-%d", strict=False)
            )
            .alias("REPORT_DATE")
        )
    
    return df.select([
        pl.col("LINE").cast(pl.Utf8),
        pl.col("ATM").cast(pl.Int64),
        pl.col("EBK").cast(pl.Int64),
        pl.col("OTC").cast(pl.Int64),
        pl.col("TOTAL").cast(pl.Int64),
        pl.col("REPORT_DATE")
    ])

def normalize_channel_update_schema(df):
    """Normalize CHANNEL_UPDATE schema - handle DESC/LINE and DATE/REPORT_DATE"""
    if "DESC" in df.columns and "LINE" not in df.columns:
        df = df.rename({"DESC": "LINE"})
    if "DATE" in df.columns and "REPORT_DATE" not in df.columns:
        df = df.rename({"DATE": "REPORT_DATE"})
    
    for col in df.columns:
        if col != "REPORT_DATE" and df[col].dtype == pl.Object:
            if col in ["LINE"]:
                df = df.with_columns(pl.col(col).cast(pl.Utf8))
    
    if "REPORT_DATE" in df.columns:
        if df["REPORT_DATE"].dtype == pl.Utf8 or df["REPORT_DATE"].dtype == pl.Object:
            print(f"  Converting REPORT_DATE from string to date...")
            df = df.with_columns(
                pl.col("REPORT_DATE").str.to_date(format="%d/%m/%Y", strict=False)
                .fill_null(
                    pl.col("REPORT_DATE").str.to_date(format="%Y-%m-%d", strict=False)
                )
                .alias("REPORT_DATE")
            )
    
    return df.select([
        pl.col("LINE").cast(pl.Utf8),
        pl.col("ATM").cast(pl.Int64),
        pl.col("EBK").cast(pl.Int64),
        pl.col("OTC").cast(pl.Int64),
        pl.col("TOTAL").cast(pl.Int64),
        pl.col("REPORT_DATE")
    ])

def read_or_create_empty(path, schema_dict, normalize_func=None):
    """Read parquet if exists, otherwise create empty DataFrame with schema"""
    if parquet_exists(path):
        df = pl.read_parquet(path)
        print(f"  Original columns: {df.columns}")
        print(f"  Original schema: {df.schema}")
        
        if normalize_func:
            df = normalize_func(df)
            print(f"  Normalized schema: {df.schema}")
        
        return df
    else:
        print(f"⚠ File not found: {path}, creating empty DataFrame")
        return pl.DataFrame(schema=schema_dict)

# =============================================================================
# READ TODAY'S CIS DATA
# =============================================================================
print("=" * 80)
print("READING TODAY'S CIS DATA")
print("=" * 80)

channel_sum_schema = {
    'CHANNEL': pl.Utf8,
    'TOLPROMPT': pl.Int64,
    'TOLUPDATE': pl.Int64,
    'MONTH': pl.Utf8
}

channel_update_schema = {
    'LINE': pl.Utf8,
    'ATM': pl.Int64,
    'EBK': pl.Int64,
    'OTC': pl.Int64,
    'TOTAL': pl.Int64,
    'REPORT_DATE': pl.Date
}

# Read today's CHANNEL_SUM data
print(f"Reading: {TODAY_CHANNEL_SUM}")
if parquet_exists(TODAY_CHANNEL_SUM):
    today_channel = pl.read_parquet(TODAY_CHANNEL_SUM)
    today_channel = normalize_channel_sum_schema(today_channel)
    print(f"  ✓ Records: {len(today_channel)}")
else:
    print("  ⚠ Today's data not found!")
    today_channel = pl.DataFrame(schema=channel_sum_schema)
print()

# Read today's CHANNEL_UPDATE data
print(f"Reading: {TODAY_CHANNEL_UPDATE}")
if parquet_exists(TODAY_CHANNEL_UPDATE):
    today_update = pl.read_parquet(TODAY_CHANNEL_UPDATE)
    today_update = process_full_summary_to_channel_update(today_update)
    print(f"  ✓ Records: {len(today_update)}")
else:
    print("  ⚠ Today's data not found!")
    today_update = pl.DataFrame(schema=channel_update_schema)
print()

# =============================================================================
# PROCESS LAST MONTH (FINAL - append today if still in last month)
# =============================================================================
print("=" * 80)
print(f"PROCESSING LAST MONTH (FINAL): {LAST_MONTH_YEAR}-{LAST_MONTH_NUM:02d}")
print("=" * 80)

# Check if today's batch belongs to last month
is_last_month_batch = (CIS_YEAR == LAST_MONTH_YEAR and CIS_MONTH == LAST_MONTH_NUM)

if is_last_month_batch:
    print(f"⚠ Today's batch ({REPTDATE.strftime('%Y-%m-%d')}) belongs to LAST MONTH")
    print(f"  Will append to last month's files...")
    print()
    
    # Read existing last month data
    print(f"Reading: {LAST_CHANNEL_SUM}")
    last_month_channel = read_or_create_empty(
        LAST_CHANNEL_SUM, 
        channel_sum_schema,
        normalize_channel_sum_schema
    )
    print(f"  Existing records: {len(last_month_channel)}")
    
    # Append today's data
    final_last_channel = pl.concat([last_month_channel, today_channel], how="vertical")
    final_last_channel.write_parquet(LAST_CHANNEL_SUM)
    print(f"  ✓ Updated LAST MONTH: {LAST_CHANNEL_SUM}")
    print(f"  Total records: {len(final_last_channel)}")
    print()
    
    # Same for CHANNEL_UPDATE
    print(f"Reading: {LAST_CHANNEL_UPDATE}")
    last_month_update = read_or_create_empty(
        LAST_CHANNEL_UPDATE, 
        channel_update_schema,
        normalize_channel_update_schema
    )
    print(f"  Existing records: {len(last_month_update)}")
    
    final_last_update = pl.concat([last_month_update, today_update], how="vertical")
    final_last_update.write_parquet(LAST_CHANNEL_UPDATE)
    print(f"  ✓ Updated LAST MONTH: {LAST_CHANNEL_UPDATE}")
    print(f"  Total records: {len(final_last_update)}")
    print()
else:
    print(f"✓ Today's batch ({REPTDATE.strftime('%Y-%m-%d')}) is in CURRENT MONTH")
    print(f"  Last month files remain unchanged (final/closed)")
    print()

# =============================================================================
# PROCESS CURRENT MONTH (ACTIVE - always append)
# =============================================================================
print("=" * 80)
print(f"PROCESSING CURRENT MONTH (ACTIVE): {CURRENT_MONTH_YEAR}-{CURRENT_MONTH_NUM:02d}")
print("=" * 80)

# If today is first day of current month, use last month as base
# Otherwise, use existing current month data
if CIS_DAY == 1 and not is_last_month_batch:
    print(f"⚠ First day of month - copying LAST MONTH as base for CURRENT MONTH")
    print()
    
    # Copy last month as base for current month
    print(f"Reading last month base: {LAST_CHANNEL_SUM}")
    current_base_channel = read_or_create_empty(
        LAST_CHANNEL_SUM, 
        channel_sum_schema,
        normalize_channel_sum_schema
    )
    print(f"  Base records: {len(current_base_channel)}")
    
    print(f"Reading last month base: {LAST_CHANNEL_UPDATE}")
    current_base_update = read_or_create_empty(
        LAST_CHANNEL_UPDATE, 
        channel_update_schema,
        normalize_channel_update_schema
    )
    print(f"  Base records: {len(current_base_update)}")
    print()
else:
    # Read existing current month data
    print(f"Reading existing current month: {CURRENT_CHANNEL_SUM}")
    current_base_channel = read_or_create_empty(
        CURRENT_CHANNEL_SUM, 
        channel_sum_schema,
        normalize_channel_sum_schema
    )
    print(f"  Existing records: {len(current_base_channel)}")
    
    print(f"Reading existing current month: {CURRENT_CHANNEL_UPDATE}")
    current_base_update = read_or_create_empty(
        CURRENT_CHANNEL_UPDATE, 
        channel_update_schema,
        normalize_channel_update_schema
    )
    print(f"  Existing records: {len(current_base_update)}")
    print()

# Only append today's data if it belongs to current month
if not is_last_month_batch:
    print("Appending today's data to current month...")
    
    # Combine and save CHANNEL_SUM
    final_current_channel = pl.concat([current_base_channel, today_channel], how="vertical")
    final_current_channel.write_parquet(CURRENT_CHANNEL_SUM)
    print(f"  ✓ Updated CURRENT MONTH: {CURRENT_CHANNEL_SUM}")
    print(f"  Total records: {len(final_current_channel)}")
    
    # Combine and save CHANNEL_UPDATE
    final_current_update = pl.concat([current_base_update, today_update], how="vertical")
    final_current_update.write_parquet(CURRENT_CHANNEL_UPDATE)
    print(f"  ✓ Updated CURRENT MONTH: {CURRENT_CHANNEL_UPDATE}")
    print(f"  Total records: {len(final_current_update)}")
    print()
else:
    print("⚠ Today's data belongs to last month - current month unchanged")
    final_current_channel = current_base_channel
    final_current_update = current_base_update
    print()

# =============================================================================
# PROCESS OTC_DETAIL (BRANCH + BCODE)
# =============================================================================
print("=" * 80)
print("PROCESSING OTC_DETAIL (BRANCH + BCODE)")
print("=" * 80)

# Read BCODE from lookup file
print(f"Reading BCODE from: {BCODE_LOOKUP}")
bcode_df = read_brcode_lookup(BCODE_LOOKUP)
print(f"  Total branches in lookup: {len(bcode_df)}")
if len(bcode_df) > 0:
    print(f"  BRANCHNO range: {bcode_df['BRANCHNO'].min()} - {bcode_df['BRANCHNO'].max()}")
    print("  Sample:")
    print(bcode_df.head(10))
print()

# Read BRANCH data from OTC_SUMMARY
print(f"Reading BRANCH from: {OTC_SUMMARY_SRC}")
branch_schema = {
    'BRANCHNO': pl.Int64,
    'TOLPROMPT': pl.Int64,
    'TOLUPDATE': pl.Int64
}

if parquet_exists(OTC_SUMMARY_SRC):
    branch_source = pl.read_parquet(OTC_SUMMARY_SRC)
    print(f"  OTC_SUMMARY records: {len(branch_source)}")
    print(f"  Columns: {branch_source.columns}")
    
    # Check if BRANCHNO column exists, if not try to extract from other columns
    if "BRANCHNO" not in branch_source.columns:
        print("  ⚠ BRANCHNO not found in OTC_SUMMARY, creating placeholder")
        branch_df = pl.DataFrame(schema=branch_schema)
    else:
        # Transform to match SAS BRANCH structure
        branch_df = (
            branch_source
            .with_columns([
                pl.col("BRANCHNO").cast(pl.Int64)
            ])
            .rename({
                "PROMPT": "TOLPROMPT",
                "UPDATED": "TOLUPDATE"
            })
            .select(["BRANCHNO", "TOLPROMPT", "TOLUPDATE"])
        )
        print(f"  BRANCH records: {len(branch_df)}")
        print("  Sample:")
        print(branch_df.head(10))
else:
    print("  ⚠ OTC_SUMMARY file not found, creating empty BRANCH DataFrame")
    branch_df = pl.DataFrame(schema=branch_schema)
print()

# Sort both DataFrames
bcode_sorted = bcode_df.select(['BRANCHNO']).sort("BRANCHNO")
branch_sorted = branch_df.sort("BRANCHNO")

print(f"Sorted BCODE records: {len(bcode_sorted)}")
print(f"Sorted BRANCH records: {len(branch_sorted)}")
print()

# Merge (SAS: MERGE BCODE(IN=A) BRANCH(IN=B); BY BRANCHNO; IF A;)
print("Merging BCODE and BRANCH...")
otc_detail = (
    bcode_sorted.join(
        branch_sorted,
        on="BRANCHNO",
        how="left"  # Keep all BCODE rows (IF A)
    )
    .with_columns([
        pl.col("TOLPROMPT").fill_null(0),
        pl.col("TOLUPDATE").fill_null(0),
    ])
)

print(f"  Merged records: {len(otc_detail)}")
print(f"  Branches with data: {otc_detail.filter(pl.col('TOLPROMPT') > 0).height}")
print(f"  Branches without data: {otc_detail.filter(pl.col('TOLPROMPT') == 0).height}")
print()

# Output: CRMWH.OTC_DETAIL_MMYY
otc_name = f"OTC_DETAIL_{REPTMON}{REPTYEAR}"
otc_path = CRMWH_DIR / f"{otc_name}.parquet"
otc_detail.write_parquet(otc_path)
print(f"✓ OTC detail written to: {otc_path}")
print()

print("OTC_DETAIL Summary:")
print(otc_detail.describe())
print()

# =============================================================================
# VERIFICATION WITH DUCKDB
# =============================================================================
print("=" * 80)
print("VERIFICATION")
print("=" * 80)

con = duckdb.connect()

# Verify LAST MONTH
if is_last_month_batch:
    print(f"LAST MONTH ({LAST_MONTH_YEAR}-{LAST_MONTH_NUM:02d}) - CHANNEL_SUM:")
    try:
        con.register('last_channel_sum', final_last_channel.to_arrow())
        result = con.execute("""
            SELECT 
                CHANNEL,
                COUNT(*) as record_count,
                SUM(TOLPROMPT) as total_prompt,
                SUM(TOLUPDATE) as total_update
            FROM last_channel_sum
            GROUP BY CHANNEL
            ORDER BY CHANNEL
        """).fetchdf()
        print(result)
    except Exception as e:
        print(f"⚠ Verification failed: {str(e)}")
    print()

# Verify CURRENT MONTH
if not is_last_month_batch:
    print(f"CURRENT MONTH ({CURRENT_MONTH_YEAR}-{CURRENT_MONTH_NUM:02d}) - CHANNEL_SUM:")
    try:
        con.register('current_channel_sum', final_current_channel.to_arrow())
        result = con.execute("""
            SELECT 
                CHANNEL,
                COUNT(*) as record_count,
                SUM(TOLPROMPT) as total_prompt,
                SUM(TOLUPDATE) as total_update
            FROM current_channel_sum
            GROUP BY CHANNEL
            ORDER BY CHANNEL
        """).fetchdf()
        print(result)
    except Exception as e:
        print(f"⚠ Verification failed: {str(e)}")
    print()

# Verify OTC_DETAIL
print(f"OTC_DETAIL ({REPTMON}{REPTYEAR}):")
try:
    con.register('otc_detail', otc_detail.to_arrow())
    result = con.execute("""
        SELECT 
            COUNT(*) as total_branches,
            SUM(TOLPROMPT) as total_prompt,
            SUM(TOLUPDATE) as total_update,
            COUNT(CASE WHEN TOLPROMPT > 0 THEN 1 END) as branches_with_data
        FROM otc_detail
    """).fetchdf()
    print(result)
except Exception as e:
    print(f"⚠ Verification failed: {str(e)}")
print()

con.close()

# =============================================================================
# FINAL SUMMARY
# =============================================================================
print("=" * 80)
print("PROCESS COMPLETE")
print("=" * 80)
print(f"Batch Date: {REPTDATE.strftime('%Y-%m-%d')}")
print()
print("FILES UPDATED:")

if is_last_month_batch:
    print(f"LAST MONTH (FINAL): {LAST_MONTH_YEAR}-{LAST_MONTH_NUM:02d}")
    print(f"  1. {LAST_CHANNEL_SUM}")
    print(f"     Records: {len(final_last_channel)}")
    print(f"  2. {LAST_CHANNEL_UPDATE}")
    print(f"     Records: {len(final_last_update)}")
else:
    print(f"CURRENT MONTH (ACTIVE): {CURRENT_MONTH_YEAR}-{CURRENT_MONTH_NUM:02d}")
    print(f"  1. {CURRENT_CHANNEL_SUM}")
    print(f"     Records: {len(final_current_channel)}")
    print(f"  2. {CURRENT_CHANNEL_UPDATE}")
    print(f"     Records: {len(final_current_update)}")

print()
print(f"OTC DETAIL:")
print(f"  3. {otc_path}")
print(f"     Branches: {len(otc_detail)}")
print()
print(f"DATA SOURCES:")
print(f"  CIS: {CIS_DATA_PATH}")
print(f"  Lookup: {BCODE_LOOKUP}")
print("=" * 80)
