import polars as pl
import os
import sys
import duckdb
import struct
from datetime import datetime, timedelta
from pathlib import Path
import pyarrow.parquet as pq

# =============================================================================
# DYNAMIC IMPORT OF BRCODE_LAYOUT
# =============================================================================
var_path = "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/PARQUET_LAYOUT"
sys.path.insert(0, var_path)

try:
    from BRCODE_layout import LRECL, LAYOUT
    print(f"✓ Loaded BRCODE_layout from: {var_path}")
except ImportError as e:
    print(f"✗ Failed to import BRCODE_layout from {var_path}: {e}")
    sys.exit(1)

# =============================================================================
# DATE CALCULATIONS
# =============================================================================
REPTDATE = datetime.today() - timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)
SAS_ORIGIN = datetime(1960, 1, 1)
RDATE = (REPTDATE - SAS_ORIGIN).days

REPTYEAR = f"{REPTDATE.year % 100:02d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"

current_month_date = REPTDATE.replace(day=1)
last_month_date = current_month_date - timedelta(days=1)

LAST_MONTH_YEAR = last_month_date.year
LAST_MONTH_NUM = last_month_date.month
CURRENT_MONTH_YEAR = current_month_date.year
CURRENT_MONTH_NUM = current_month_date.month

CIS_YEAR = REPTDATE.year
CIS_MONTH = REPTDATE.month
CIS_DAY = REPTDATE.day

print("=" * 80)
print(f"BATCH DATE: {REPTDATE.strftime('%Y-%m-%d')} | "
      f"LAST MONTH: {LAST_MONTH_YEAR}-{LAST_MONTH_NUM:02d} | "
      f"CURRENT MONTH: {CURRENT_MONTH_YEAR}-{CURRENT_MONTH_NUM:02d}")
print("=" * 80)

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_INPUT_PATH = Path("/host/cis/parquet/")
BASE_PATH = "/host/mis/parquet/crm"
BCODE_LOOKUP = "/sasdata/rawdata/lookup/LKP_BRANCH"

CIS_DATA_PATH = BASE_INPUT_PATH / f"year={CIS_YEAR}/month={CIS_MONTH}/day={CIS_DAY}"
TODAY_CHANNEL_SUM = CIS_DATA_PATH / "CIPHONET_ALL_SUMMARY.parquet"
TODAY_CHANNEL_UPDATE = CIS_DATA_PATH / "CIPHONET_FULL_SUMMARY.parquet"
OTC_SUMMARY_SRC = CIS_DATA_PATH / "CIPHONET_OTC_SUMMARY.parquet"

LAST_MONTH_PATH = f"{BASE_PATH}/year={LAST_MONTH_YEAR}/month={LAST_MONTH_NUM:02d}"
CURRENT_MONTH_PATH = f"{BASE_PATH}/year={CURRENT_MONTH_YEAR}/month={CURRENT_MONTH_NUM:02d}"
CRMWH_DIR = Path(BASE_PATH) / "CRMWH"

for path in [LAST_MONTH_PATH, CURRENT_MONTH_PATH, str(CRMWH_DIR)]:
    os.makedirs(path, exist_ok=True)

LAST_CHANNEL_SUM = f"{LAST_MONTH_PATH}/CHANNEL_SUM.parquet"
LAST_CHANNEL_UPDATE = f"{LAST_MONTH_PATH}/CHANNEL_UPDATE.parquet"
CURRENT_CHANNEL_SUM = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
CURRENT_CHANNEL_UPDATE = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"

# =============================================================================
# EBCDIC READER FUNCTIONS
# =============================================================================
def unpack_packed_decimal(packed_bytes):
    """Unpack IBM packed decimal (COMP-3)"""
    if not packed_bytes:
        return None
    result = 0
    for byte in packed_bytes[:-1]:
        result = result * 100 + ((byte >> 4) & 0x0F) * 10 + (byte & 0x0F)
    last_byte = packed_bytes[-1]
    result = result * 10 + ((last_byte >> 4) & 0x0F)
    return -result if (last_byte & 0x0F) == 0x0D else result

def decode_ebcdic(ebcdic_bytes):
    """Decode EBCDIC to ASCII"""
    try:
        return ebcdic_bytes.decode('cp037').strip()
    except:
        return ''

def read_brcode_lookup(filepath):
    """Read LKP_BRANCH using BRCODE_layout.py structure"""
    records = []
    try:
        with open(filepath, 'rb') as f:
            while True:
                record = f.read(LRECL)
                if not record or len(record) < LRECL:
                    break
                row = {}
                for field_name, field_info in LAYOUT.items():
                    field_bytes = record[field_info['slice']]
                    if field_info['type'] == 'packed':
                        row[field_name] = unpack_packed_decimal(field_bytes)
                    elif field_info['type'] == 'ebcdic':
                        row[field_name] = decode_ebcdic(field_bytes)
                records.append(row)
        
        df = pl.DataFrame(records).select([
            pl.col('BRCODE').cast(pl.Int64).alias('BRANCHNO'),
            pl.col('BRSTATUS').cast(pl.Utf8)
        ]).filter(pl.col('BRSTATUS') == 'A').select(['BRANCHNO'])
        
        return df
    except Exception as e:
        print(f"⚠ Error reading {filepath}: {e}")
        return pl.DataFrame({'BRANCHNO': pl.Series([], dtype=pl.Int64)})

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def parquet_exists(path):
    try:
        pq.read_schema(str(path))
        return True
    except:
        return False

def normalize_channel_sum(df):
    if "PROMPT" in df.columns:
        df = df.rename({"PROMPT": "TOLPROMPT", "UPDATED": "TOLUPDATE"})
    for col in df.columns:
        if df[col].dtype == pl.Object:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))
    if "MONTH" not in df.columns:
        df = df.with_columns(pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH"))
    return df.select(["CHANNEL", "TOLPROMPT", "TOLUPDATE", "MONTH"]).cast({
        "CHANNEL": pl.Utf8, "TOLPROMPT": pl.Int64, "TOLUPDATE": pl.Int64, "MONTH": pl.Utf8
    })

def normalize_channel_update(df):
    if "DESC" in df.columns:
        df = df.rename({"DESC": "LINE"})
    if "DATE" in df.columns:
        df = df.rename({"DATE": "REPORT_DATE"})
    for col in df.columns:
        if col != "REPORT_DATE" and df[col].dtype == pl.Object:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))
    if "REPORT_DATE" in df.columns and df["REPORT_DATE"].dtype in [pl.Utf8, pl.Object]:
        df = df.with_columns(
            pl.col("REPORT_DATE").str.to_date(format="%d/%m/%Y", strict=False)
            .fill_null(pl.col("REPORT_DATE").str.to_date(format="%Y-%m-%d", strict=False))
        )
    return df.select(["LINE", "ATM", "EBK", "OTC", "TOTAL", "REPORT_DATE"]).cast({
        "LINE": pl.Utf8, "ATM": pl.Int64, "EBK": pl.Int64, 
        "OTC": pl.Int64, "TOTAL": pl.Int64, "REPORT_DATE": pl.Date
    })

def process_full_summary(df):
    df = df.with_row_index(name="_N_", offset=1).with_columns(
        pl.when(pl.col("_N_") == 1).then(pl.lit("TOTAL PROMPT BASE"))
         .when(pl.col("_N_") == 2).then(pl.lit("TOTAL UPDATED"))
         .otherwise(pl.lit(None)).alias("LINE")
    )
    if "REPORT_DATE" not in df.columns:
        df = df.with_columns(pl.lit(REPTDATE).alias("REPORT_DATE"))
    return normalize_channel_update(df)

def read_or_empty(path, schema, normalizer=None):
    if parquet_exists(path):
        df = pl.read_parquet(path)
        return normalizer(df) if normalizer else df
    return pl.DataFrame(schema=schema)

# =============================================================================
# READ TODAY'S DATA
# =============================================================================
print("\n>>> READING TODAY'S CIS DATA")

schemas = {
    'channel_sum': {'CHANNEL': pl.Utf8, 'TOLPROMPT': pl.Int64, 'TOLUPDATE': pl.Int64, 'MONTH': pl.Utf8},
    'channel_update': {'LINE': pl.Utf8, 'ATM': pl.Int64, 'EBK': pl.Int64, 
                       'OTC': pl.Int64, 'TOTAL': pl.Int64, 'REPORT_DATE': pl.Date},
    'branch': {'BRANCHNO': pl.Int64, 'TOLPROMPT': pl.Int64, 'TOLUPDATE': pl.Int64}
}

today_channel = (normalize_channel_sum(pl.read_parquet(TODAY_CHANNEL_SUM)) 
                 if parquet_exists(TODAY_CHANNEL_SUM) 
                 else pl.DataFrame(schema=schemas['channel_sum']))
print(f"  CHANNEL_SUM: {len(today_channel)} records")

today_update = (process_full_summary(pl.read_parquet(TODAY_CHANNEL_UPDATE)) 
                if parquet_exists(TODAY_CHANNEL_UPDATE) 
                else pl.DataFrame(schema=schemas['channel_update']))
print(f"  CHANNEL_UPDATE: {len(today_update)} records")

# =============================================================================
# PROCESS MONTHLY DATA
# =============================================================================
is_last_month_batch = (CIS_YEAR == LAST_MONTH_YEAR and CIS_MONTH == LAST_MONTH_NUM)

print(f"\n>>> PROCESSING {'LAST' if is_last_month_batch else 'CURRENT'} MONTH")

if is_last_month_batch:
    # Update last month
    last_ch = read_or_empty(LAST_CHANNEL_SUM, schemas['channel_sum'], normalize_channel_sum)
    last_up = read_or_empty(LAST_CHANNEL_UPDATE, schemas['channel_update'], normalize_channel_update)
    
    final_last_ch = pl.concat([last_ch, today_channel], how="vertical")
    final_last_up = pl.concat([last_up, today_update], how="vertical")
    
    final_last_ch.write_parquet(LAST_CHANNEL_SUM)
    final_last_up.write_parquet(LAST_CHANNEL_UPDATE)
    
    print(f"  ✓ LAST MONTH updated: {len(final_last_ch)} / {len(final_last_up)} records")
    
    final_current_ch = read_or_empty(CURRENT_CHANNEL_SUM, schemas['channel_sum'], normalize_channel_sum)
    final_current_up = read_or_empty(CURRENT_CHANNEL_UPDATE, schemas['channel_update'], normalize_channel_update)
else:
    # Update current month
    if CIS_DAY == 1:
        curr_base_ch = read_or_empty(LAST_CHANNEL_SUM, schemas['channel_sum'], normalize_channel_sum)
        curr_base_up = read_or_empty(LAST_CHANNEL_UPDATE, schemas['channel_update'], normalize_channel_update)
        print(f"  First day: copied {len(curr_base_ch)} / {len(curr_base_up)} from last month")
    else:
        curr_base_ch = read_or_empty(CURRENT_CHANNEL_SUM, schemas['channel_sum'], normalize_channel_sum)
        curr_base_up = read_or_empty(CURRENT_CHANNEL_UPDATE, schemas['channel_update'], normalize_channel_update)
    
    final_current_ch = pl.concat([curr_base_ch, today_channel], how="vertical")
    final_current_up = pl.concat([curr_base_up, today_update], how="vertical")
    
    final_current_ch.write_parquet(CURRENT_CHANNEL_SUM)
    final_current_up.write_parquet(CURRENT_CHANNEL_UPDATE)
    
    print(f"  ✓ CURRENT MONTH updated: {len(final_current_ch)} / {len(final_current_up)} records")

# =============================================================================
# PROCESS OTC_DETAIL (BCODE + BRANCH MERGE)
# =============================================================================
print("\n>>> PROCESSING OTC_DETAIL")

bcode_df = read_brcode_lookup(BCODE_LOOKUP).sort("BRANCHNO")
print(f"  BCODE: {len(bcode_df)} active branches")

if parquet_exists(OTC_SUMMARY_SRC):
    branch_src = pl.read_parquet(OTC_SUMMARY_SRC)
    if "BRANCHNO" in branch_src.columns:
        branch_df = branch_src.select([
            pl.col("BRANCHNO").cast(pl.Int64),
            pl.col("PROMPT").alias("TOLPROMPT").cast(pl.Int64),
            pl.col("UPDATED").alias("TOLUPDATE").cast(pl.Int64)
        ]).sort("BRANCHNO")
    else:
        branch_df = pl.DataFrame(schema=schemas['branch'])
else:
    branch_df = pl.DataFrame(schema=schemas['branch'])

print(f"  BRANCH: {len(branch_df)} records")

otc_detail = bcode_df.join(branch_df, on="BRANCHNO", how="left").with_columns([
    pl.col("TOLPROMPT").fill_null(0),
    pl.col("TOLUPDATE").fill_null(0)
])

otc_path = CRMWH_DIR / f"OTC_DETAIL_{REPTMON}{REPTYEAR}.parquet"
otc_detail.write_parquet(otc_path)
print(f"  ✓ OTC_DETAIL: {len(otc_detail)} branches ({otc_detail.filter(pl.col('TOLPROMPT') > 0).height} with data)")

# =============================================================================
# VERIFICATION
# =============================================================================
print("\n>>> VERIFICATION")
con = duckdb.connect()

try:
    if is_last_month_batch:
        con.register('ch', final_last_ch.to_arrow())
        print(f"  LAST MONTH CHANNEL_SUM:")
    else:
        con.register('ch', final_current_ch.to_arrow())
        print(f"  CURRENT MONTH CHANNEL_SUM:")
    
    result = con.execute("""
        SELECT CHANNEL, COUNT(*) as records, SUM(TOLPROMPT) as prompt, SUM(TOLUPDATE) as update
        FROM ch GROUP BY CHANNEL ORDER BY CHANNEL
    """).fetchdf()
    print(result.to_string(index=False))
    
    con.register('otc', otc_detail.to_arrow())
    result = con.execute("""
        SELECT COUNT(*) as branches, SUM(TOLPROMPT) as prompt, SUM(TOLUPDATE) as update
        FROM otc
    """).fetchdf()
    print(f"\n  OTC_DETAIL TOTALS:")
    print(result.to_string(index=False))
except Exception as e:
    print(f"  ⚠ Verification failed: {e}")

con.close()

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 80)
print(f"COMPLETED: {REPTDATE.strftime('%Y-%m-%d')}")
print(f"  Monthly: {CURRENT_CHANNEL_SUM}")
print(f"  OTC:     {otc_path}")
print("=" * 80)
