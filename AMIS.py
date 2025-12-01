import polars as pl
import os
import saspy
from datetime import datetime, timedelta
from pathlib import Path

# =============================================================================
# INITIALIZATION
# =============================================================================
sas = saspy.SASsession() if saspy else None
REPTDATE = datetime.today() - timedelta(days=1)
REPTYEAR = f"{REPTDATE.year % 100:02d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"

print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d}")

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_PATH = "/host/mis/parquet/crm"
CRMWH_PATH = f"{BASE_PATH}/CRMWH"
CURRENT_MONTH_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTMON}"
os.makedirs(CRMWH_PATH, exist_ok=True)
os.makedirs(CURRENT_MONTH_PATH, exist_ok=True)

# =============================================================================
# FUNCTION 1: READ BCODE FILE
# =============================================================================
def read_bcode_file():
    """Read BCODE file - SAS: INPUT @002 BRANCHNO 3."""
    records = []
    try:
        with open("/sasdata/rawdata/lookup/LKP_BRANCH", 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        line_padded = line.ljust(80)
                        branchno_str = line_padded[1:4].strip()
                        
                        if branchno_str:
                            branchno = int(branchno_str)
                            brstatus = line_padded[49:50].strip()
                            
                            if brstatus in ['O', 'A']:
                                records.append({"BRANCHNO": branchno})
                    except:
                        continue
        
        if records:
            df = pl.DataFrame(records).unique().sort("BRANCHNO")
            print(f"  BCODE active branches: {len(df)}")
            print(f"  BRANCHNO range: {df['BRANCHNO'].min()} to {df['BRANCHNO'].max()}")
            print(f"  Sample BRANCHNO: {df['BRANCHNO'].head(10).to_list()}")
            return df
        else:
            return pl.DataFrame()
            
    except Exception as e:
        print(f"  Error reading BCODE: {e}")
        return pl.DataFrame()

# =============================================================================
# FUNCTION 2: PROCESS CHANNEL SUMMARY
# =============================================================================
def process_channel_sum():
    """Process CIPHONET_ALL_SUMMARY for CHANNEL_SUM"""
    try:
        channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
        if not os.path.exists(channel_path):
            print(f"  File not found: {channel_path}")
            return pl.DataFrame()
        
        df = pl.read_parquet(channel_path)
        print(f"  Columns: {df.columns}")
        
        channel_df = df.select([
            pl.col("CHANNEL").str.to_uppercase().alias("CHANNEL"),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
        ]).with_columns(
            pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH")
        )
        
        print(f"  Records: {len(channel_df)}")
        return channel_df
        
    except Exception as e:
        print(f"  Error: {e}")
        return pl.DataFrame()

# =============================================================================
# FUNCTION 3: PROCESS OTC DETAIL (SIMPLIFIED)
# =============================================================================
def process_otc_detail():
    """Process OTC summary and merge with BCODE - SIMPLE VERSION"""
    try:
        # 1. Read BCODE
        bcode_df = read_bcode_file()
        if len(bcode_df) == 0:
            print(f"  No BCODE data")
            return pl.DataFrame()
        
        # 2. Read OTC summary
        otc_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
        if not os.path.exists(otc_path):
            print(f"  OTC file not found: {otc_path}")
            otc_detail = bcode_df.with_columns([
                pl.lit(0).alias("TOLPROMPT"),
                pl.lit(0).alias("TOLUPDATE")
            ])
            return otc_detail
        
        otc_df = pl.read_parquet(otc_path)
        print(f"  OTC columns: {otc_df.columns}")
        print(f"  OTC records: {len(otc_df)}")
        print(f"  Sample CHANNEL: {otc_df['CHANNEL'].head(10).to_list()}")
        
        # 3. Convert CHANNEL to BRANCHNO
        # Remove leading zeros and convert to integer
        otc_clean = otc_df.with_columns(
            pl.col("CHANNEL").str.replace_all("^0+", "").cast(pl.Int64).alias("BRANCHNO")
        ).select([
            pl.col("BRANCHNO"),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
        ])
        
        print(f"  OTC after conversion: {len(otc_clean)} records")
        print(f"  Sample BRANCHNO: {otc_clean['BRANCHNO'].head(10).to_list()}")
        
        # 4. Merge BCODE with OTC
        merged = bcode_df.join(otc_clean, on="BRANCHNO", how="left")
        
        # 5. Fill nulls with 0
        otc_detail = merged.with_columns([
            pl.col("TOLPROMPT").fill_null(0),
            pl.col("TOLUPDATE").fill_null(0)
        ]).sort("BRANCHNO")
        
        print(f"\n  FINAL OTC_DETAIL:")
        print(f"    Total records: {len(otc_detail)}")
        print(f"    TOLPROMPT sum: {otc_detail['TOLPROMPT'].sum():,}")
        print(f"    TOLUPDATE sum: {otc_detail['TOLUPDATE'].sum():,}")
        
        return otc_detail
        
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame()

# =============================================================================
# FUNCTION 4: PROCESS CHANNEL UPDATE (FIXED)
# =============================================================================
def process_channel_update():
    """Process CIPHONET_FULL_SUMMARY for CHANNEL_UPDATE"""
    try:
        update_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_FULL_SUMMARY.parquet"
        if not os.path.exists(update_path):
            print(f"  File not found: {update_path}")
            return pl.DataFrame()
        
        df = pl.read_parquet(update_path)
        print(f"  Columns: {df.columns}")
        print(f"  Total records: {len(df)}")
        
        # Get first 2 rows
        if len(df) >= 2:
            update_df = df.head(2)
            
            # Create DESC column - check what column exists for description
            desc_col = None
            for col in ['LINE', 'DESC', 'DESCRIPTION']:
                if col in update_df.columns:
                    desc_col = col
                    break
            
            if desc_col:
                # Use existing description column
                update_df = update_df.with_columns([
                    pl.col(desc_col).alias("DESC"),
                    pl.col("ATM").cast(pl.Int64),
                    pl.col("EBK").cast(pl.Int64),
                    pl.col("OTC").cast(pl.Int64),
                    pl.col("TOTAL").cast(pl.Int64)
                ])
            else:
                # Create DESC based on row number
                update_df = update_df.with_row_index().with_columns(
                    pl.when(pl.col("index") == 0).then("TOTAL PROMPT BASE")
                     .when(pl.col("index") == 1).then("TOTAL UPDATED")
                     .otherwise("").alias("DESC")
                ).drop("index").select([
                    pl.col("DESC"),
                    pl.col("ATM").cast(pl.Int64),
                    pl.col("EBK").cast(pl.Int64),
                    pl.col("OTC").cast(pl.Int64),
                    pl.col("TOTAL").cast(pl.Int64)
                ])
            
            # Add DATE
            update_df = update_df.with_columns(
                pl.lit(REPTDATE.strftime("%d/%m/%Y")).alias("DATE")
            )
            
            print(f"  Update records: {len(update_df)}")
            return update_df
        else:
            print(f"  Not enough records (need 2, got {len(df)})")
            return pl.DataFrame()
        
    except Exception as e:
        print(f"  Error: {e}")
        return pl.DataFrame()

# =============================================================================
# FUNCTION 5: WRITE TO SAS DATASET
# =============================================================================
def write_sas_dataset(df, output_path, dataset_name):
    """Write DataFrame to SAS dataset"""
    if sas is None:
        print(f"  SAS session not available")
        return False
    
    if len(df) == 0:
        print(f"  DataFrame is empty")
        return False
    
    try:
        os.makedirs(output_path, exist_ok=True)
        
        # Assign library
        lib_result = sas.submit(f"libname SASOUT '{output_path}';")
        if "ERROR" in lib_result["LOG"]:
            print(f"  Library error: {lib_result['LOG'][:200]}")
            return False
        
        # Write using df2sd
        print(f"  Writing {len(df)} records to {dataset_name}...")
        write_result = sas.df2sd(df.to_pandas(), table=dataset_name, libref="SASOUT")
        
        # Check result
        if hasattr(write_result, 'LOG') and "ERROR" in write_result.LOG:
            print(f"  Write error: {write_result.LOG}")
            return False
        
        # Verify
        verify = sas.submit(f"""
            proc sql;
                select count(*) as N from SASOUT.{dataset_name};
            quit;
        """)
        
        # Clear library
        sas.submit("libname SASOUT clear;")
        
        # Check file
        sas_file = f"{output_path}/{dataset_name}.sas7bdat"
        if os.path.exists(sas_file):
            size = os.path.getsize(sas_file)
            print(f"  ✓ SAS file created: {sas_file} ({size:,} bytes)")
            return True
        else:
            print(f"  ✗ SAS file not found: {sas_file}")
            return False
            
    except Exception as e:
        print(f"  SAS error: {e}")
        return False

# =============================================================================
# MAIN PROCESSING
# =============================================================================
print("\n" + "=" * 60)
print(f"PROCESSING FOR {REPTDATE:%Y-%m-%d}")
print("=" * 60)

print("\n>>> 1. CHANNEL SUMMARY (EIBMCHNL)")
channel_df = process_channel_sum()

print("\n>>> 2. CHANNEL UPDATE (EIBMCHN2)")
update_df = process_channel_update()

print("\n>>> 3. OTC DETAIL (EIBMCHNL - MERGE)")
otc_detail = process_otc_detail()

# =============================================================================
# WRITE OUTPUTS
# =============================================================================
print("\n" + "=" * 60)
print("WRITING OUTPUT FILES")
print("=" * 60)

# 1. Write CHANNEL_SUM
if len(channel_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
    channel_df.write_parquet(output_path)
    print(f"✓ 1. CHANNEL_SUM.parquet: {output_path}")

# 2. Write CHANNEL_UPDATE
if len(update_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
    update_df.write_parquet(output_path)
    print(f"✓ 2. CHANNEL_UPDATE.parquet: {output_path}")

# 3. Write OTC_DETAIL to SAS
if len(otc_detail) > 0:
    dataset_name = f"OTC_DETAIL_{REPTMON}{REPTYEAR}"
    sas_path = f"{CURRENT_MONTH_PATH}"
    
    print(f"\nWriting {dataset_name} to SAS...")
    sas_success = write_sas_dataset(otc_detail, sas_path, dataset_name)
    
    if sas_success:
        print(f"✓ 3. {dataset_name}.sas7bdat: {sas_path}/{dataset_name}.sas7bdat")
    else:
        # Fallback to Parquet
        parquet_path = f"{CRMWH_PATH}/{dataset_name}.parquet"
        otc_detail.write_parquet(parquet_path)
        print(f"✓ 3. {dataset_name}.parquet (fallback): {parquet_path}")

# =============================================================================
# FINAL CHECK
# =============================================================================
print("\n" + "=" * 60)
print("FINAL CHECK")
print("=" * 60)

print(f"\nFiles in {CURRENT_MONTH_PATH}:")
if os.path.exists(CURRENT_MONTH_PATH):
    for file in sorted(os.listdir(CURRENT_MONTH_PATH)):
        filepath = os.path.join(CURRENT_MONTH_PATH, file)
        if os.path.isfile(filepath):
            size = os.path.getsize(filepath)
            print(f"  {file} ({size:,} bytes)")

print(f"\n" + "=" * 60)
print(f"COMPLETE")
print("=" * 60)
