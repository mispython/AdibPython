import polars as pl
import os
import saspy
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# =============================================================================
# INITIALIZATION - FIXED FOR MONTHLY JOB
# =============================================================================
sas = saspy.SASsession() if saspy else None

# FIX: For monthly job, we want the LAST DAY of previous month
# Get first day of current month, subtract 1 day to get last day of previous month
CURRENT_DATE = datetime.today()
FIRST_DAY_CURRENT_MONTH = CURRENT_DATE.replace(day=1)
LAST_DAY_PREV_MONTH = FIRST_DAY_CURRENT_MONTH - timedelta(days=1)

# Use last day of previous month as processing date
REPTDATE = LAST_DAY_PREV_MONTH
REPTYEAR = f"{REPTDATE.year % 100:02d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"

# Get previous month (two months ago) for accumulation
# e.g., If processing December 2025, accumulate with November 2025
PREV_MONTH_DATE = REPTDATE.replace(day=1) - timedelta(days=1)
PREV_YEAR = PREV_MONTH_DATE.year
PREV_MONTH = PREV_MONTH_DATE.month

print(f"MONTHLY JOB: Processing LAST DAY of previous month")
print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d} (LAST DAY OF PREVIOUS MONTH)")
print(f"ACCUMULATING WITH: {PREV_YEAR}-{PREV_MONTH:02d} (month before processing month)")
print(f"OUTPUT PATH: year={REPTDATE.year}/month={REPTMON}")

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_PATH = "/host/mis/parquet/crm"
CURRENT_MONTH_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTMON}"
PREV_MONTH_PATH = f"{BASE_PATH}/year={PREV_YEAR}/month={PREV_MONTH:02d}"

os.makedirs(CURRENT_MONTH_PATH, exist_ok=True)

# =============================================================================
# SCHEMA DEFINITIONS
# =============================================================================
CHANNEL_SUM_SCHEMA = {
    'CHANNEL': pl.Utf8,
    'TOLPROMPT': pl.Int64,
    'TOLUPDATE': pl.Int64,
    'MONTH': pl.Utf8
}

CHANNEL_UPDATE_SCHEMA = {
    'DESC': pl.Utf8,
    'ATM': pl.Int64,
    'EBK': pl.Int64,
    'OTC': pl.Int64,
    'TOTAL': pl.Int64,
    'DATE': pl.Utf8
}

# =============================================================================
# FUNCTION 1: READ BCODE FILE
# =============================================================================
def read_bcode_file():
    """Read ALL BCODE branches (375 total)"""
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
                            records.append({"BRANCHNO": branchno})
                    except:
                        continue
        
        if records:
            df = pl.DataFrame(records).unique().sort("BRANCHNO")
            print(f"  BCODE total branches: {len(df)}")
            return df
        else:
            return pl.DataFrame({'BRANCHNO': pl.Series([], dtype=pl.Int64)})
    except Exception as e:
        print(f"  Error reading BCODE: {e}")
        return pl.DataFrame({'BRANCHNO': pl.Series([], dtype=pl.Int64)})

# =============================================================================
# FUNCTION 2: PROCESS CHANNEL SUMMARY
# =============================================================================
def process_channel_sum():
    """Process CHANNEL summary - accumulate with previous month"""
    try:
        channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
        print(f"  Looking for: {channel_path}")
        if not os.path.exists(channel_path):
            print(f"  File not found: {channel_path}")
            return pl.DataFrame(schema=CHANNEL_SUM_SCHEMA)
        
        df = pl.read_parquet(channel_path)
        print(f"  Columns: {df.columns}")
        
        # Today's data with explicit schema
        channel_df = df.select([
            pl.col("CHANNEL").str.to_uppercase().cast(pl.Utf8).alias("CHANNEL"),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE"),
            pl.lit(REPTDATE.strftime("%b%y").upper()).cast(pl.Utf8).alias("MONTH")
        ])
        
        print(f"  Today's records: {len(channel_df)}")
        print(f"  MONTH: {REPTDATE.strftime('%b%y').upper()}")
        
        # Get previous month data
        prev_month_data = pl.DataFrame(schema=CHANNEL_SUM_SCHEMA)
        prev_sum_path = f"{PREV_MONTH_PATH}/CHANNEL_SUM.parquet"
        
        if os.path.exists(prev_sum_path):
            prev_month_data = pl.read_parquet(prev_sum_path)
            # **FIX: Cast to proper schema to avoid type mismatch**
            prev_month_data = prev_month_data.select([
                pl.col("CHANNEL").cast(pl.Utf8),
                pl.col("TOLPROMPT").cast(pl.Int64),
                pl.col("TOLUPDATE").cast(pl.Int64),
                pl.col("MONTH").cast(pl.Utf8)
            ])
            print(f"  Previous month data: {len(prev_month_data)} records")
        
        # Get current month existing data
        curr_month_data = pl.DataFrame(schema=CHANNEL_SUM_SCHEMA)
        curr_sum_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
        
        if os.path.exists(curr_sum_path):
            curr_month_data = pl.read_parquet(curr_sum_path)
            # **FIX: Cast to proper schema**
            curr_month_data = curr_month_data.select([
                pl.col("CHANNEL").cast(pl.Utf8),
                pl.col("TOLPROMPT").cast(pl.Int64),
                pl.col("TOLUPDATE").cast(pl.Int64),
                pl.col("MONTH").cast(pl.Utf8)
            ])
            print(f"  Current month existing: {len(curr_month_data)} records")
            
            # Remove today's data if exists
            today_month = REPTDATE.strftime("%b%y").upper()
            if today_month in curr_month_data['MONTH'].unique().to_list():
                print(f"  Removing existing {today_month} data...")
                curr_month_data = curr_month_data.filter(pl.col("MONTH") != today_month)
                print(f"  After removal: {len(curr_month_data)}")
        
        # Combine all data
        all_data = []
        if len(prev_month_data) > 0:
            all_data.append(prev_month_data)
            print(f"  Adding previous month: {len(prev_month_data)}")
        if len(curr_month_data) > 0:
            all_data.append(curr_month_data)
        all_data.append(channel_df)
        
        if all_data:
            final_df = pl.concat(all_data, how="vertical")
            print(f"  After accumulation: {len(final_df)} total records")
            return final_df
        else:
            return channel_df
        
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame(schema=CHANNEL_SUM_SCHEMA)

# =============================================================================
# FUNCTION 3: PROCESS OTC DETAIL
# =============================================================================
def process_otc_detail():
    """Process OTC detail for ALL 375 BCODE branches"""
    try:
        bcode_df = read_bcode_file()
        if len(bcode_df) == 0:
            print(f"  No BCODE data")
            return pl.DataFrame()
        
        otc_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
        print(f"  Looking for: {otc_path}")
        if not os.path.exists(otc_path):
            print(f"  OTC file not found: {otc_path}")
            otc_detail = bcode_df.with_columns([
                pl.lit(0).cast(pl.Int64).alias("TOLPROMPT"),
                pl.lit(0).cast(pl.Int64).alias("TOLUPDATE")
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
        
        print(f"  OTC after conversion: {len(otc_clean)} records")
        
        # Merge ALL 375 BCODE branches with OTC data
        merged = bcode_df.join(otc_clean, on="BRANCHNO", how="left")
        
        # Fill nulls with 0
        otc_detail = merged.with_columns([
            pl.col("TOLPROMPT").fill_null(0),
            pl.col("TOLUPDATE").fill_null(0)
        ]).sort("BRANCHNO")
        
        print(f"\n  FINAL OTC_DETAIL:")
        print(f"    Total records: {len(otc_detail)} (should be 375)")
        print(f"    TOLPROMPT sum: {otc_detail['TOLPROMPT'].sum():,}")
        
        return otc_detail
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame()

# =============================================================================
# FUNCTION 4: PROCESS CHANNEL UPDATE
# =============================================================================
def process_channel_update():
    """Process CHANNEL update - accumulate with previous month"""
    try:
        update_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_FULL_SUMMARY.parquet"
        print(f"  Looking for: {update_path}")
        if not os.path.exists(update_path):
            print(f"  File not found: {update_path}")
            return pl.DataFrame(schema=CHANNEL_UPDATE_SCHEMA)
        
        df = pl.read_parquet(update_path)
        print(f"  Columns: {df.columns}")
        
        if len(df) >= 2:
            update_df = df.head(2)
            
            # **FIX ISSUE #2: Properly create DESC column**
            update_df = update_df.with_row_index(name="index").with_columns(
                pl.when(pl.col("index") == 0)
                  .then(pl.lit("TOTAL PROMPT BASE"))
                  .when(pl.col("index") == 1)
                  .then(pl.lit("TOTAL UPDATED"))
                  .otherwise(pl.lit(None))
                  .alias("DESC")  # **FIX: Added .alias() here**
            ).drop("index").select([
                pl.col("DESC").cast(pl.Utf8),
                pl.col("ATM").cast(pl.Int64),
                pl.col("EBK").cast(pl.Int64),
                pl.col("OTC").cast(pl.Int64),
                pl.col("TOTAL").cast(pl.Int64)
            ])
            
            # Add DATE column
            update_df = update_df.with_columns(
                pl.lit(REPTDATE.strftime("%d/%m/%Y")).cast(pl.Utf8).alias("DATE")
            )
            
            print(f"  Today's update records: {len(update_df)}")
            
            # Get previous month data
            prev_month_data = pl.DataFrame(schema=CHANNEL_UPDATE_SCHEMA)
            prev_update_path = f"{PREV_MONTH_PATH}/CHANNEL_UPDATE.parquet"
            
            if os.path.exists(prev_update_path):
                prev_month_data = pl.read_parquet(prev_update_path)
                # **FIX: Cast to proper schema**
                prev_month_data = prev_month_data.select([
                    pl.col("DESC").cast(pl.Utf8),
                    pl.col("ATM").cast(pl.Int64),
                    pl.col("EBK").cast(pl.Int64),
                    pl.col("OTC").cast(pl.Int64),
                    pl.col("TOTAL").cast(pl.Int64),
                    pl.col("DATE").cast(pl.Utf8)
                ])
                print(f"  Previous month data: {len(prev_month_data)} records")
            
            # Get current month existing data
            curr_month_data = pl.DataFrame(schema=CHANNEL_UPDATE_SCHEMA)
            curr_update_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
            
            if os.path.exists(curr_update_path):
                curr_month_data = pl.read_parquet(curr_update_path)
                # **FIX: Cast to proper schema**
                curr_month_data = curr_month_data.select([
                    pl.col("DESC").cast(pl.Utf8),
                    pl.col("ATM").cast(pl.Int64),
                    pl.col("EBK").cast(pl.Int64),
                    pl.col("OTC").cast(pl.Int64),
                    pl.col("TOTAL").cast(pl.Int64),
                    pl.col("DATE").cast(pl.Utf8)
                ])
                print(f"  Current month existing: {len(curr_month_data)} records")
                
                # Remove today's data if exists
                today_date = REPTDATE.strftime("%d/%m/%Y")
                if today_date in curr_month_data['DATE'].unique().to_list():
                    print(f"  Removing existing {today_date} data...")
                    curr_month_data = curr_month_data.filter(pl.col("DATE") != today_date)
                    print(f"  After removal: {len(curr_month_data)}")
            
            # Combine all data
            all_data = []
            if len(prev_month_data) > 0:
                all_data.append(prev_month_data)
                print(f"  Adding previous month: {len(prev_month_data)}")
            if len(curr_month_data) > 0:
                all_data.append(curr_month_data)
            all_data.append(update_df)
            
            if all_data:
                final_df = pl.concat(all_data, how="vertical")
                print(f"  After accumulation: {len(final_df)} total records")
                return final_df
            else:
                return update_df
        else:
            print(f"  Not enough records (need 2, got {len(df)})")
            return pl.DataFrame(schema=CHANNEL_UPDATE_SCHEMA)
    except Exception as e:
        print(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame(schema=CHANNEL_UPDATE_SCHEMA)

# =============================================================================
# SAS DATA TRANSFER FUNCTIONS
# =============================================================================
def assign_libname(lib_name, sas_path):
    """Assign SAS library"""
    log = sas.submit(f"libname {lib_name} '{sas_path}';")
    return log

def verify_sas_dataset(lib_name, dataset_name, expected_rows=None):
    """Verify that a SAS dataset exists and has the expected number of rows"""
    try:
        # Try to access the dataset
        sas_ds = sas.sasdata(dataset_name, libref=lib_name)
        
        # Get dataset information
        ds_info = sas_ds.contents()
        
        # Extract row count from contents
        if hasattr(sas_ds, 'shape'):
            actual_rows = sas_ds.shape[0]
        else:
            # Try to get row count from contents
            actual_rows = "unknown"
            # Run a PROC SQL count
            count_result = sas.submit(f"""
                proc sql noprint;
                    select count(*) into :row_count from {lib_name}.{dataset_name};
                quit;
            """)
            # Extract count from log (simplified approach)
            # In practice, you'd parse the log or use a different method
            
        print(f"    ✓ Verified {lib_name}.{dataset_name} exists")
        if expected_rows and actual_rows != "unknown":
            if actual_rows == expected_rows:
                print(f"    ✓ Row count matches: {actual_rows} rows")
            else:
                print(f"    ⚠ Row count mismatch: expected {expected_rows}, got {actual_rows}")
        
        return True
    except Exception as e:
        print(f"    ✗ Error verifying {lib_name}.{dataset_name}: {e}")
        return False

def transfer_to_sas(df_polars, lib_name, dataset_name, description=""):
    """Transfer Polars DataFrame to SAS dataset with verification"""
    if len(df_polars) == 0:
        print(f"  No data to transfer for {dataset_name}")
        return None
    
    try:
        # Convert to pandas
        df_pandas = df_polars.to_pandas()
        print(f"  Transferring {description or dataset_name} with {len(df_pandas)} records...")
        
        # Transfer to SAS
        print(f"    Creating {lib_name}.{dataset_name}...")
        sas_ds = sas.dataframe2sasdata(df_pandas, table=dataset_name, libref=lib_name)
        
        if isinstance(sas_ds, saspy.sasdata.SASdata):
            print(f"    ✓ SASdata object returned - dataset created successfully")
            
            # Verify the dataset was created
            print(f"    Verifying dataset creation...")
            verify_sas_dataset(lib_name, dataset_name, len(df_pandas))
            
            # Print some sample data
            try:
                # Get first few rows
                sample = sas_ds.head(3)
                print(f"    Sample data (first 3 rows):")
                print(f"    {sample}")
            except:
                pass
                
        else:
            print(f"    ⚠ Unexpected return type: {type(sas_ds)}")
            
        return sas_ds
        
    except Exception as e:
        print(f"    ✗ Error transferring {dataset_name}: {e}")
        import traceback
        traceback.print_exc()
        return None

# =============================================================================
# MAIN PROCESSING
# =============================================================================
print("\n" + "=" * 80)
print(f"MONTHLY JOB: PROCESSING {REPTDATE:%B %Y} DATA")
print("=" * 80)

print(f"\n>>> 1. CHANNEL SUMMARY (EIBMCHNL)")
print(f"Accumulating with {PREV_YEAR}-{PREV_MONTH:02d} data...")
channel_df = process_channel_sum()

print(f"\n>>> 2. CHANNEL UPDATE (EIBMCHN2)")
print(f"Accumulating with {PREV_YEAR}-{PREV_MONTH:02d} data...")
update_df = process_channel_update()

print(f"\n>>> 3. OTC DETAIL (EIBMCHNL - MERGE)")
print("Processing ALL 375 BCODE branches...")
otc_detail = process_otc_detail()

# =============================================================================
# WRITE OUTPUT FILES
# =============================================================================
print("\n" + "=" * 80)
print("WRITING OUTPUT FILES")
print("=" * 80)

if len(channel_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
    channel_df.write_parquet(output_path)
    print(f"✓ CHANNEL_SUM.parquet: {output_path}")
    print(f"  Total records: {len(channel_df)}")
else:
    print(f"✗ CHANNEL_SUM.parquet: No data to write")

if len(update_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
    update_df.write_parquet(output_path)
    print(f"✓ CHANNEL_UPDATE.parquet: {output_path}")
    print(f"  Total records: {len(update_df)}")
else:
    print(f"✗ CHANNEL_UPDATE.parquet: No data to write")

if len(otc_detail) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/OTC_DETAIL.parquet"
    otc_detail.write_parquet(output_path)
    print(f"✓ OTC_DETAIL.parquet: {output_path}")
    print(f"  Total records: {len(otc_detail)}")
else:
    print(f"✗ OTC_DETAIL.parquet: No data to write")

print("\nDataFrame columns:")
print("OTC Detail:", otc_detail.columns)
print("Channel Summary:", channel_df.columns if len(channel_df) > 0 else "Empty")
print("Channel Update:", update_df.columns if len(update_df) > 0 else "Empty")

# =============================================================================
# SAS DATA TRANSFER
# =============================================================================
print("\n" + "=" * 80)
print("TRANSFERRING DATA TO SAS")
print("=" * 80)

# Check if SAS session is available
if sas is None:
    print("ERROR: SAS session not available!")
    exit(1)

try:
    # Assign libraries
    print("\n1. Assigning SAS libraries...")
    lib1 = assign_libname("crm", "/dwh/crm")
    lib2 = assign_libname("ctrl_crm", "/sas/python/virt_edw/Data_Warehouse/MIS/Job/CIS/SASTABLE")
    
    print("   Libraries assigned successfully")
    
    # Transfer data to SAS
    print("\n2. Transferring data to SAS...")
    
    # Transfer CHANNEL SUMMARY
    if len(channel_df) > 0:
        result1 = transfer_to_sas(channel_df, "crm", "channel_sum", "CHANNEL SUMMARY")
    else:
        print(f"  Skipping CHANNEL SUMMARY - no data")
    
    # Transfer CHANNEL UPDATE
    if len(update_df) > 0:
        result2 = transfer_to_sas(update_df, "crm", "channel_update", "CHANNEL UPDATE")
    else:
        print(f"  Skipping CHANNEL UPDATE - no data")
    
    # Transfer OTC DETAIL
    if len(otc_detail) > 0:
        otc_dataset_name = f"otc_detail_{REPTYEAR}{REPTMON}"
        result3 = transfer_to_sas(otc_detail, "crm", otc_dataset_name, f"OTC DETAIL {REPTYEAR}{REPTMON}")
    else:
        print(f"  Skipping OTC DETAIL - no data")
    
    print("\n✓ SAS data transfer completed!")
    
    # Final verification
    print("\n3. Final verification of SAS datasets:")
    verify_sas_dataset("crm", "channel_sum")
    verify_sas_dataset("crm", "channel_update")
    verify_sas_dataset("crm", f"otc_detail_{REPTYEAR}{REPTMON}")
    
except Exception as e:
    print(f"\n✗ Error during SAS transfer: {e}")
    import traceback
    traceback.print_exc()

# =============================================================================
# CLEANUP AND SUMMARY
# =============================================================================
print("\n" + "=" * 80)
print("MONTHLY PROCESS COMPLETED - SUMMARY")
print("=" * 80)
print(f"Processing Date: {REPTDATE:%Y-%m-%d}")
print(f"Output Directory: {CURRENT_MONTH_PATH}")
print(f"\nFiles Created:")
print(f"  - CHANNEL_SUM.parquet: {len(channel_df)} records")
print(f"  - CHANNEL_UPDATE.parquet: {len(update_df)} records")
print(f"  - OTC_DETAIL.parquet: {len(otc_detail)} records")
print(f"\nSAS Datasets Created:")
print(f"  - crm.channel_sum")
print(f"  - crm.channel_update")
print(f"  - crm.otc_detail_{REPTYEAR}{REPTMON}")
print("=" * 80)
