import polars as pl
import os
import saspy
from datetime import datetime, timedelta
from pathlib import Path

# =============================================================================
# INITIALIZATION - ALL USING TODAY()-1
# =============================================================================
sas = saspy.SASsession() if saspy else None

# SAS: REPTDATE = TODAY() - 1;
REPTDATE = datetime.today() - timedelta(days=1)
REPTYEAR = f"{REPTDATE.year % 100:02d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"

print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d} (TODAY()-1)")

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_PATH = "/host/mis/parquet/crm"
CURRENT_MONTH_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTMON}"
os.makedirs(CURRENT_MONTH_PATH, exist_ok=True)

# =============================================================================
# FUNCTION 1: READ BCODE FILE - ALL BRANCHES
# =============================================================================
def read_bcode_file():
    """Read ALL BCODE branches (375 total, not 269 active)"""
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
                            # Include ALL branches (375), not filtering by status
                            records.append({"BRANCHNO": branchno})
                    except:
                        continue
        
        if records:
            df = pl.DataFrame(records).unique().sort("BRANCHNO")
            print(f"  BCODE total branches: {len(df)}")
            print(f"  BRANCHNO starts at: {df['BRANCHNO'].min()}")
            return df
        else:
            return pl.DataFrame()
    except Exception as e:
        print(f"  Error reading BCODE: {e}")
        return pl.DataFrame()

# =============================================================================
# FUNCTION 2: PROCESS CHANNEL SUMMARY WITH ACCUMULATION
# =============================================================================
def process_channel_sum():
    """Process CHANNEL summary and append to existing"""
    try:
        channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
        if not os.path.exists(channel_path):
            print(f"  File not found: {channel_path}")
            return pl.DataFrame()
        
        df = pl.read_parquet(channel_path)
        print(f"  Columns: {df.columns}")
        
        # SAS: REPTDATE = TODAY() - 1; MONTH = PUT(REPTDATE, MONYY5.);
        # MONTH = "NOV25" (3-char month + 2-digit year)
        channel_df = df.select([
            pl.col("CHANNEL").str.to_uppercase().alias("CHANNEL"),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE"),
            pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH")  # TODAY()-1
        ])
        
        print(f"  Today's records: {len(channel_df)}")
        print(f"  MONTH: {REPTDATE.strftime('%b%y').upper()}")
        
        # Append to existing data
        existing_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
        if os.path.exists(existing_path):
            existing_df = pl.read_parquet(existing_path)
            print(f"  Existing records: {len(existing_df)}")
            
            # Check if today's data already exists (avoid duplicates)
            existing_dates = existing_df['MONTH'].unique()
            if REPTDATE.strftime("%b%y").upper() in existing_dates:
                print(f"  Today's data already exists, replacing...")
                # Remove today's data if exists
                existing_df = existing_df.filter(
                    pl.col("MONTH") != REPTDATE.strftime("%b%y").upper()
                )
            
            # Append new data
            final_df = pl.concat([existing_df, channel_df], how="vertical")
            print(f"  After append: {len(final_df)} records")
            return final_df
        else:
            print(f"  No existing data, creating new")
            return channel_df
        
    except Exception as e:
        print(f"  Error: {e}")
        return pl.DataFrame()

# =============================================================================
# FUNCTION 3: PROCESS OTC DETAIL - ALL 375 BRANCHES
# =============================================================================
def process_otc_detail():
    """Process OTC detail for ALL 375 BCODE branches"""
    try:
        bcode_df = read_bcode_file()
        if len(bcode_df) == 0:
            print(f"  No BCODE data")
            return pl.DataFrame()
        
        otc_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
        if not os.path.exists(otc_path):
            print(f"  OTC file not found: {otc_path}")
            # Create with zeros for all branches
            otc_detail = bcode_df.with_columns([
                pl.lit(0).alias("TOLPROMPT"),
                pl.lit(0).alias("TOLUPDATE")
            ])
            return otc_detail
        
        otc_df = pl.read_parquet(otc_path)
        print(f"  OTC columns: {otc_df.columns}")
        print(f"  OTC records: {len(otc_df)}")
        
        # Convert CHANNEL to BRANCHNO
        otc_clean = otc_df.with_columns(
            pl.col("CHANNEL").cast(pl.Int64).alias("BRANCHNO")
        ).select([
            pl.col("BRANCHNO"),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
        ])
        
        print(f"  OTC BRANCHNO range: {otc_clean['BRANCHNO'].min()} to {otc_clean['BRANCHNO'].max()}")
        
        # Merge ALL BCODE branches (375) with OTC data
        merged = bcode_df.join(otc_clean, on="BRANCHNO", how="left")
        
        # SAS: IF TOLPROMPT=. THEN DO TOLPROMPT=0;
        otc_detail = merged.with_columns([
            pl.col("TOLPROMPT").fill_null(0),
            pl.col("TOLUPDATE").fill_null(0)
        ]).sort("BRANCHNO")
        
        print(f"\n  FINAL OTC_DETAIL:")
        print(f"    Records: {len(otc_detail)} (should be 375)")
        print(f"    First BRANCHNO: {otc_detail['BRANCHNO'].head(5).to_list()}")
        print(f"    TOLPROMPT sum: {otc_detail['TOLPROMPT'].sum():,}")
        
        return otc_detail
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame()

# =============================================================================
# FUNCTION 4: PROCESS CHANNEL UPDATE WITH ACCUMULATION
# =============================================================================
def process_channel_update():
    """Process CHANNEL update and append to existing"""
    try:
        update_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_FULL_SUMMARY.parquet"
        if not os.path.exists(update_path):
            print(f"  File not found: {update_path}")
            return pl.DataFrame()
        
        df = pl.read_parquet(update_path)
        print(f"  Columns: {df.columns}")
        
        if len(df) >= 2:
            update_df = df.head(2)
            
            # SAS: IF _N_ = 1 THEN DESC='TOTAL PROMPT BASE';
            #      IF _N_ = 2 THEN DESC='TOTAL UPDATED';
            update_df = update_df.with_row_index().with_columns(
                pl.when(pl.col("index") == 0).then("TOTAL PROMPT BASE")
                 .when(pl.col("index") == 1).then("TOTAL UPDATED")
                 .alias("DESC")
            ).drop("index").select([
                pl.col("DESC"),
                pl.col("ATM").cast(pl.Int64),
                pl.col("EBK").cast(pl.Int64),
                pl.col("OTC").cast(pl.Int64),
                pl.col("TOTAL").cast(pl.Int64)
            ])
            
            # Add DATE column using REPTDATE (TODAY()-1)
            update_df = update_df.with_columns(
                pl.lit(REPTDATE.strftime("%d/%m/%Y")).alias("DATE")
            )
            
            print(f"  Today's update records: {len(update_df)}")
            
            # Append to existing data
            existing_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
            if os.path.exists(existing_path):
                existing_df = pl.read_parquet(existing_path)
                print(f"  Existing records: {len(existing_df)}")
                
                # Remove today's date if exists
                existing_df = existing_df.filter(
                    pl.col("DATE") != REPTDATE.strftime("%d/%m/%Y")
                )
                
                # Append new data
                final_df = pl.concat([existing_df, update_df], how="vertical")
                print(f"  After append: {len(final_df)} records")
                return final_df
            else:
                print(f"  No existing data, creating new")
                return update_df
        else:
            print(f"  Not enough records")
            return pl.DataFrame()
    except Exception as e:
        print(f"  Error: {e}")
        return pl.DataFrame()

# =============================================================================
# FUNCTION 5: WRITE OTC_DETAIL SAS DATASET
# =============================================================================
def write_otc_sas_dataset(df, dataset_name):
    """Write OTC_DETAIL to SAS dataset"""
    if sas is None or len(df) == 0:
        return False
    
    try:
        sas_path = f"{CURRENT_MONTH_PATH}"
        os.makedirs(sas_path, exist_ok=True)
        
        # Assign library
        lib_result = sas.submit(f"libname OTCLIB '{sas_path}';")
        if "ERROR" in lib_result["LOG"]:
            print(f"  Library error: {lib_result['LOG'][:200]}")
            return False
        
        # Write dataset
        print(f"  Creating {dataset_name}...")
        write_result = sas.df2sd(df.to_pandas(), table=dataset_name, libref="OTCLIB")
        
        if hasattr(write_result, 'LOG') and "ERROR" in write_result.LOG:
            print(f"  Write error: {write_result.LOG}")
            return False
        
        # Check file
        sas_file = f"{sas_path}/{dataset_name}.sas7bdat"
        if os.path.exists(sas_file):
            size = os.path.getsize(sas_file)
            print(f"  ✓ {dataset_name}.sas7bdat created ({size:,} bytes)")
            return True
        else:
            print(f"  ✗ SAS file not found")
            return False
            
    except Exception as e:
        print(f"  SAS error: {e}")
        return False

# =============================================================================
# MAIN PROCESSING
# =============================================================================
print("\n" + "=" * 80)
print("PROCESSING WITH TODAY()-1 AND ACCUMULATION")
print("=" * 80)

print(f"\n>>> 1. CHANNEL SUMMARY")
channel_df = process_channel_sum()

print(f"\n>>> 2. CHANNEL UPDATE")
update_df = process_channel_update()

print(f"\n>>> 3. OTC DETAIL")
otc_detail = process_otc_detail()

# =============================================================================
# WRITE OUTPUTS
# =============================================================================
print("\n" + "=" * 80)
print("WRITING OUTPUTS")
print("=" * 80)

# 1. Write CHANNEL_SUM
if len(channel_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
    channel_df.write_parquet(output_path)
    print(f"✓ CHANNEL_SUM: {output_path} ({len(channel_df)} records)")
    print(f"  MONTH values: {channel_df['MONTH'].unique().to_list()}")

# 2. Write CHANNEL_UPDATE
if len(update_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
    update_df.write_parquet(output_path)
    print(f"✓ CHANNEL_UPDATE: {output_path} ({len(update_df)} records)")

# 3. Write OTC_DETAIL SAS dataset
if len(otc_detail) > 0:
    dataset_name = f"OTC_DETAIL_{REPTMON}{REPTYEAR}"  # OTC_DETAIL_1125
    print(f"\nCreating OTC_DETAIL SAS dataset...")
    sas_success = write_otc_sas_dataset(otc_detail, dataset_name)
    
    if sas_success:
        print(f"✓ {dataset_name}.sas7bdat created")
    else:
        # Fallback to Parquet
        parquet_path = f"{CURRENT_MONTH_PATH}/{dataset_name}.parquet"
        otc_detail.write_parquet(parquet_path)
        print(f"✓ {dataset_name}.parquet (fallback): {parquet_path}")

# =============================================================================
# VERIFICATION
# =============================================================================
print("\n" + "=" * 80)
print("VERIFICATION")
print("=" * 80)

print(f"\nExpected vs Actual:")
print(f"1. REPTDATE: {REPTDATE:%Y-%m-%d} (TODAY()-1)")
print(f"2. MONTH format: {REPTDATE.strftime('%b%y').upper()} (should be NOV25)")

if len(otc_detail) > 0:
    print(f"\n3. OTC_DETAIL check:")
    print(f"   - Records: {len(otc_detail)} (should be 375)")
    print(f"   - First BRANCHNO: {otc_detail['BRANCHNO'].min()} (should be 2)")
    print(f"   - Last BRANCHNO: {otc_detail['BRANCHNO'].max()}")

print("\n" + "=" * 80)
print("PROCESS COMPLETE")
print("=" * 80)
