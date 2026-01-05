import polars as pl
import os
import saspy
from datetime import datetime, timedelta
import pandas as pd

# =============================================================================
# INITIALIZATION
# =============================================================================
sas = saspy.SASsession() if saspy else None

# Monthly job: Process last day of previous month
TODAY = datetime.today()
REPTDATE = TODAY.replace(day=1) - timedelta(days=1)  # Last day of prev month
REPTYEAR = f"{REPTDATE.year % 100:02d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"

# Previous month for accumulation
PREV_DATE = REPTDATE.replace(day=1) - timedelta(days=1)

print(f"MONTHLY JOB: Processing {REPTDATE:%B %Y}")
print(f"Processing: {REPTDATE:%Y-%m-%d} | Accumulating with: {PREV_DATE:%Y-%m}")

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_PATH = "/host/mis/parquet/crm"
CURRENT_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTMON}"
PREV_PATH = f"{BASE_PATH}/year={PREV_DATE.year}/month={PREV_DATE.month:02d}"
os.makedirs(CURRENT_PATH, exist_ok=True)

# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================
def read_bcode():
    """Read BCODE branches from lookup file"""
    branches = []
    try:
        with open("/sasdata/rawdata/lookup/LKP_BRANCH", 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        branchno = int(line[1:4].strip())
                        if branchno:
                            branches.append({"BRANCHNO": branchno})
                    except:
                        continue
        return pl.DataFrame(branches).unique().sort("BRANCHNO") if branches else pl.DataFrame({'BRANCHNO': []})
    except Exception as e:
        print(f"Error reading BCODE: {e}")
        return pl.DataFrame({'BRANCHNO': []})

def read_source_data(file_pattern):
    """Read source Parquet file if exists"""
    path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/{file_pattern}"
    if os.path.exists(path):
        return pl.read_parquet(path)
    print(f"File not found: {path}")
    return None

def accumulate_monthly_data(df_new, dataset_name, schema, date_field="DATE"):
    """Accumulate new data with previous month's data"""
    df_new = df_new.select([pl.col(c).cast(schema[c]) for c in schema.keys()])
    
    # Get existing data from current month
    df_current = pl.DataFrame(schema=schema)
    curr_path = f"{CURRENT_PATH}/{dataset_name}.parquet"
    if os.path.exists(curr_path):
        df_current = pl.read_parquet(curr_path).select([pl.col(c).cast(schema[c]) for c in schema.keys()])
        
        # Remove duplicate for current processing date
        if date_field in df_current.columns:
            current_value = REPTDATE.strftime("%d/%m/%Y")
            if current_value in df_current[date_field].unique().to_list():
                df_current = df_current.filter(pl.col(date_field) != current_value)
    
    # Get previous month data
    df_prev = pl.DataFrame(schema=schema)
    prev_path = f"{PREV_PATH}/{dataset_name}.parquet"
    if os.path.exists(prev_path):
        df_prev = pl.read_parquet(prev_path).select([pl.col(c).cast(schema[c]) for c in schema.keys()])
    
    # Combine all data
    return pl.concat([df_prev, df_current, df_new], how="vertical")

def process_channel_summary():
    """Process channel summary data"""
    print("\n1. CHANNEL SUMMARY")
    df = read_source_data("CIPHONET_ALL_SUMMARY.parquet")
    if df is None:
        print("  No data found")
        return pl.DataFrame()
    
    df_new = df.select([
        pl.col("CHANNEL").str.to_uppercase().alias("CHANNEL"),
        pl.col("PROMPT").alias("TOLPROMPT"),
        pl.col("UPDATED").alias("TOLUPDATE"),
        pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH")
    ])
    
    schema = {
        'CHANNEL': pl.Utf8, 'TOLPROMPT': pl.Int64, 
        'TOLUPDATE': pl.Int64, 'MONTH': pl.Utf8
    }
    
    result = accumulate_monthly_data(df_new, "CHANNEL_SUM", schema, date_field="MONTH")
    print(f"  Processed {len(result):,} records")
    return result

def process_otc_detail():
    """Process OTC detail for all branches"""
    print("\n2. OTC DETAIL")
    bcode_df = read_bcode()
    if len(bcode_df) == 0:
        print("  No BCODE data found")
        return pl.DataFrame()
    
    df = read_source_data("CIPHONET_OTC_SUMMARY.parquet")
    if df is None:
        print("  No OTC data found, creating zero-filled records")
        return bcode_df.with_columns([
            pl.lit(0).alias("TOLPROMPT"),
            pl.lit(0).alias("TOLUPDATE")
        ])
    
    df_clean = df.with_columns(
        pl.col("CHANNEL").cast(pl.Int64).alias("BRANCHNO")
    ).select(["BRANCHNO", "PROMPT", "UPDATED"])
    
    result = bcode_df.join(df_clean, on="BRANCHNO", how="left").with_columns([
        pl.col("PROMPT").fill_null(0).alias("TOLPROMPT"),
        pl.col("UPDATED").fill_null(0).alias("TOLUPDATE")
    ]).drop(["PROMPT", "UPDATED"]).sort("BRANCHNO")
    
    print(f"  Processed {len(result)} branches, TOLPROMPT sum: {result['TOLPROMPT'].sum():,}")
    return result

def process_channel_update():
    """Process channel update data"""
    print("\n3. CHANNEL UPDATE")
    df = read_source_data("CIPHONET_FULL_SUMMARY.parquet")
    if df is None:
        print("  No data found")
        return pl.DataFrame()
    
    if len(df) < 2:
        print(f"  Not enough records (need 2, got {len(df)})")
        return pl.DataFrame()
    
    df_new = df.head(2).with_row_index().with_columns(
        pl.when(pl.col("index") == 0).then(pl.lit("TOTAL PROMPT BASE"))
        .when(pl.col("index") == 1).then(pl.lit("TOTAL UPDATED"))
        .alias("DESC")
    ).drop("index").select([
        "DESC", "ATM", "EBK", "OTC", "TOTAL"
    ]).with_columns(
        pl.lit(REPTDATE.strftime("%d/%m/%Y")).alias("DATE")
    )
    
    schema = {
        'DESC': pl.Utf8, 'ATM': pl.Int64, 'EBK': pl.Int64,
        'OTC': pl.Int64, 'TOTAL': pl.Int64, 'DATE': pl.Utf8
    }
    
    result = accumulate_monthly_data(df_new, "CHANNEL_UPDATE", schema, date_field="DATE")
    print(f"  Processed {len(result):,} records")
    return result

# =============================================================================
# SAS TRANSFER FUNCTIONS
# =============================================================================
def transfer_to_sas(df, lib_name, dataset_name):
    """Transfer data to SAS"""
    if len(df) == 0 or sas is None:
        print(f"  Skipping {dataset_name} - no data or SAS not available")
        return None
    
    try:
        # Assign library
        sas.submit(f"libname {lib_name} '/dwh/crm';")
        
        # Transfer data
        print(f"  Creating {lib_name}.{dataset_name}...")
        sas.dataframe2sasdata(df.to_pandas(), table=dataset_name, libref=lib_name)
        
        # Verify
        try:
            sas_ds = sas.sasdata(dataset_name, libref=lib_name)
            row_count = sas_ds.shape[0] if hasattr(sas_ds, 'shape') else 'unknown'
            print(f"  ✓ {dataset_name} created with {row_count} rows")
            return True
        except:
            print(f"  ⚠ {dataset_name} may not have been created")
            return False
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False

# =============================================================================
# MAIN EXECUTION
# =============================================================================
print("\n" + "=" * 60)
print("MONTHLY PROCESSING STARTED")
print("=" * 60)

# Process data
channel_df = process_channel_summary()
otc_df = process_otc_detail()
update_df = process_channel_update()

# Write output files
print("\n" + "=" * 60)
print("WRITING OUTPUT FILES")
print("=" * 60)

datasets = [
    (channel_df, "CHANNEL_SUM"),
    (update_df, "CHANNEL_UPDATE"),
    (otc_df, "OTC_DETAIL")
]

for df, name in datasets:
    if len(df) > 0:
        path = f"{CURRENT_PATH}/{name}.parquet"
        df.write_parquet(path)
        print(f"✓ {name}: {len(df):,} records → {path}")
    else:
        print(f"✗ {name}: No data to write")

# Transfer to SAS
if sas:
    print("\n" + "=" * 60)
    print("TRANSFERRING TO SAS")
    print("=" * 60)
    
    transfer_to_sas(channel_df, "crm", "channel_sum")
    transfer_to_sas(update_df, "crm", "channel_update")
    transfer_to_sas(otc_df, "crm", f"otc_detail_{REPTYEAR}{REPTMON}")

# Summary
print("\n" + "=" * 60)
print("PROCESS COMPLETED")
print("=" * 60)
print(f"Date: {REPTDATE:%Y-%m-%d}")
print(f"Output: {CURRENT_PATH}")
print("\nRecords processed:")
print(f"  Channel Summary: {len(channel_df):,}")
print(f"  Channel Update:  {len(update_df):,}")
print(f"  OTC Detail:      {len(otc_df):,}")
print("=" * 60)
