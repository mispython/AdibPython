import polars as pl
import os
import saspy
from datetime import datetime, timedelta
import pandas as pd

# =============================================================================
# INITIALIZATION - MATCH SAS LOGIC
# =============================================================================
sas = saspy.SASsession() if saspy else None

# SAS uses TODAY()-1 (not last day of previous month!)
REPTDATE = datetime.today() - timedelta(days=1)
REPTYEAR = f"{REPTDATE.year % 100:02d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"

print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d} (TODAY()-1)")
print(f"REPTYEAR: {REPTYEAR}, REPTMON: {REPTMON}, REPTDAY: {REPTDAY}")

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_PATH = "/host/mis/parquet/crm"
CURRENT_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTMON}"
os.makedirs(CURRENT_PATH, exist_ok=True)

# =============================================================================
# DATA PROCESSING FUNCTIONS - MATCH SAS LOGIC
# =============================================================================
def read_bcode():
    """Read BCODE branches from lookup file - matches SAS INFILE BCODE"""
    branches = []
    try:
        with open("/sasdata/rawdata/lookup/LKP_BRANCH", 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        # SAS: INPUT @002 BRANCHNO 3.
                        branchno = int(line[1:4].strip())
                        if branchno:
                            branches.append({"BRANCHNO": branchno})
                    except:
                        continue
        return pl.DataFrame(branches).unique().sort("BRANCHNO") if branches else pl.DataFrame({'BRANCHNO': []})
    except Exception as e:
        print(f"Error reading BCODE: {e}")
        return pl.DataFrame({'BRANCHNO': []})

def process_channel_summary():
    """Matches SAS EIBMCHNL CHANNEL processing"""
    print("\n1. PROCESSING CHANNEL SUMMARY (EIBMCHNL)")
    
    # Read source data
    path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
    if not os.path.exists(path):
        print(f"  File not found: {path}")
        return pl.DataFrame()
    
    df = pl.read_parquet(path)
    print(f"  Source records: {len(df)}")
    
    # Create CHANNEL dataset - matches SAS DATA CHANNEL
    # SAS: MONTH=PUT(TODAY()-2,MONYY5.)
    month_date = REPTDATE - timedelta(days=1)  # TODAY()-2
    channel_df = df.select([
        pl.col("CHANNEL").str.to_uppercase().alias("CHANNEL"),
        pl.col("PROMPT").alias("TOLPROMPT"),
        pl.col("UPDATED").alias("TOLUPDATE"),
        pl.lit(month_date.strftime("%b%y").upper()).alias("MONTH")
    ])
    
    print(f"  Created CHANNEL with {len(channel_df)} records")
    print(f"  MONTH value: {month_date.strftime('%b%y').upper()}")
    
    # SAS: PROC APPEND DATA=CHANNEL BASE=CRMWH.CHANNEL_SUM FORCE
    # Simple append, no complex accumulation
    base_path = f"{CURRENT_PATH}/CHANNEL_SUM.parquet"
    if os.path.exists(base_path):
        base_df = pl.read_parquet(base_path)
        print(f"  Existing CHANNEL_SUM records: {len(base_df)}")
        result = pl.concat([base_df, channel_df], how="vertical")
    else:
        result = channel_df
    
    print(f"  Final CHANNEL_SUM: {len(result)} records")
    return result

def process_otc_detail():
    """Matches SAS EIBMCHNL OTC_DETAIL processing"""
    print("\n2. PROCESSING OTC DETAIL (EIBMCHNL)")
    
    # Read BCODE - matches SAS DATA BCODE
    bcode_df = read_bcode()
    if len(bcode_df) == 0:
        print("  No BCODE data found")
        return pl.DataFrame()
    
    print(f"  BCODE branches: {len(bcode_df)}")
    
    # Read BRANCH data - matches SAS DATA BRANCH
    path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
    if not os.path.exists(path):
        print(f"  BRANCH file not found: {path}")
        # SAS: IF TOLPROMPT=. THEN DO TOLPROMPT=0;
        return bcode_df.with_columns([
            pl.lit(0).alias("TOLPROMPT"),
            pl.lit(0).alias("TOLUPDATE")
        ])
    
    branch_df = pl.read_parquet(path)
    print(f"  BRANCH records: {len(branch_df)}")
    
    # Convert and prepare BRANCH data
    branch_clean = branch_df.with_columns(
        pl.col("CHANNEL").cast(pl.Int64).alias("BRANCHNO")
    ).select([
        "BRANCHNO",
        pl.col("PROMPT").alias("TOLPROMPT"),
        pl.col("UPDATED").alias("TOLUPDATE")
    ])
    
    # SAS: MERGE BCODE(IN=A) BRANCH(IN=B);BY BRANCHNO; IF A;
    result = bcode_df.join(branch_clean, on="BRANCHNO", how="left").with_columns([
        pl.col("TOLPROMPT").fill_null(0),
        pl.col("TOLUPDATE").fill_null(0)
    ]).sort("BRANCHNO")
    
    print(f"  OTC_DETAIL records: {len(result)}")
    print(f"  TOLPROMPT sum: {result['TOLPROMPT'].sum():,}")
    
    return result

def process_channel_update():
    """Matches SAS EIBMCHN2 processing"""
    print("\n3. PROCESSING CHANNEL UPDATE (EIBMCHN2)")
    
    # Read source data
    path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_FULL_SUMMARY.parquet"
    if not os.path.exists(path):
        print(f"  File not found: {path}")
        return pl.DataFrame()
    
    df = pl.read_parquet(path)
    print(f"  Source records: {len(df)}")
    
    if len(df) < 2:
        print(f"  Not enough records (need 2, got {len(df)})")
        return pl.DataFrame()
    
    # SAS: First 2 records only
    df_head = df.head(2)
    
    # SAS: IF _N_ = 1 THEN DESC='TOTAL PROMPT BASE';
    #      IF _N_ = 2 THEN DESC='TOTAL UPDATED';
    update_df = df_head.with_row_index().with_columns(
        pl.when(pl.col("index") == 0).then(pl.lit("TOTAL PROMPT BASE"))
        .when(pl.col("index") == 1).then(pl.lit("TOTAL UPDATED"))
        .alias("DESC")
    ).drop("index").select([
        "DESC", "ATM", "EBK", "OTC", "TOTAL"
    ]).with_columns(
        pl.lit(REPTDATE.strftime("%d/%m/%Y")).alias("DATE")  # SAS: &RDATE
    )
    
    print(f"  Created UAT with {len(update_df)} records")
    
    # SAS: PROC APPEND DATA=UAT BASE=CHN.CHANNEL_UPDATE FORCE
    # Simple append, no complex accumulation
    base_path = f"{CURRENT_PATH}/CHANNEL_UPDATE.parquet"
    if os.path.exists(base_path):
        base_df = pl.read_parquet(base_path)
        print(f"  Existing CHANNEL_UPDATE records: {len(base_df)}")
        result = pl.concat([base_df, update_df], how="vertical")
    else:
        result = update_df
    
    print(f"  Final CHANNEL_UPDATE: {len(result)} records")
    return result

# =============================================================================
# SAS TRANSFER - MATCHES ORIGINAL LOGIC
# =============================================================================
def transfer_to_sas():
    """Transfer data to SAS matching original program"""
    if sas is None:
        print("SAS session not available")
        return
    
    print("\n" + "=" * 60)
    print("TRANSFERRING TO SAS DATASETS")
    print("=" * 60)
    
    # Create SAS datasets matching original names
    datasets = {
        "channel_sum": channel_df,
        f"otc_detail_{REPTYEAR}{REPTMON}": otc_df,
        "channel_update": update_df
    }
    
    for dataset_name, df in datasets.items():
        if len(df) == 0:
            print(f"  Skipping {dataset_name} - no data")
            continue
        
        try:
            # Assign library
            sas.submit(f"libname crm '/dwh/crm';")
            
            # Transfer data
            print(f"  Creating crm.{dataset_name}...")
            sas.dataframe2sasdata(df.to_pandas(), table=dataset_name, libref='crm')
            print(f"  ✓ crm.{dataset_name} created")
            
        except Exception as e:
            print(f"  ✗ Error creating {dataset_name}: {e}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================
print("\n" + "=" * 60)
print("PROCESSING STARTED - FOLLOWING SAS LOGIC")
print("=" * 60)

# Process data following SAS logic
channel_df = process_channel_summary()
otc_df = process_otc_detail()
update_df = process_channel_update()

# Write output files
print("\n" + "=" * 60)
print("WRITING PARQUET FILES")
print("=" * 60)

if len(channel_df) > 0:
    channel_df.write_parquet(f"{CURRENT_PATH}/CHANNEL_SUM.parquet")
    print(f"✓ CHANNEL_SUM.parquet: {len(channel_df):,} records")

if len(otc_df) > 0:
    otc_df.write_parquet(f"{CURRENT_PATH}/OTC_DETAIL.parquet")
    print(f"✓ OTC_DETAIL.parquet: {len(otc_df):,} records")

if len(update_df) > 0:
    update_df.write_parquet(f"{CURRENT_PATH}/CHANNEL_UPDATE.parquet")
    print(f"✓ CHANNEL_UPDATE.parquet: {len(update_df):,} records")

# Transfer to SAS
transfer_to_sas()

# Summary
print("\n" + "=" * 60)
print("PROCESS COMPLETED")
print("=" * 60)
print(f"Processing Date: {REPTDATE:%Y-%m-%d}")
print(f"Output Path: {CURRENT_PATH}")
print(f"\nRecords Processed:")
print(f"  CHANNEL_SUM: {len(channel_df):,}")
print(f"  OTC_DETAIL:  {len(otc_df):,}")
print(f"  CHANNEL_UPDATE: {len(update_df):,}")
print("=" * 60)
