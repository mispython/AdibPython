import polars as pl
import os
import saspy
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# =============================================================================
# INITIALIZATION
# =============================================================================
sas = saspy.SASsession() if saspy else None

REPTDATE = datetime.today() - timedelta(days=1)
REPTYEAR = f"{REPTDATE.year % 100:02d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"

# FIX: Proper previous month calculation for year boundary
PREV_MONTH_DATE = REPTDATE.replace(day=1) - timedelta(days=1)
PREV_YEAR = PREV_MONTH_DATE.year
PREV_MONTH = f"{PREV_MONTH_DATE.month:02d}"

print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d} (TODAY()-1)")
print(f"ACCUMULATING WITH: {PREV_YEAR}-{PREV_MONTH} (previous month)")

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_PATH = "/host/mis/parquet/crm"
CURRENT_MONTH_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTMON}"
PREV_MONTH_PATH = f"{BASE_PATH}/year={PREV_YEAR}/month={PREV_MONTH}"

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
        # FIX: Make sure we're using the correct path format
        channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
        print(f"  Looking for file: {channel_path}")
        
        if not os.path.exists(channel_path):
            print(f"  File not found, checking if day directory exists...")
            # Check if the day directory exists
            day_dir = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}"
            if os.path.exists(day_dir):
                print(f"  Day directory exists, listing files:")
                for f in os.listdir(day_dir):
                    print(f"    - {f}")
            
            # Try alternative path - maybe the file has a different name?
            alt_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/ciphonet_all_summary.parquet"
            if os.path.exists(alt_path):
                print(f"  Found file with lowercase name")
                channel_path = alt_path
            else:
                print(f"  File not found at either location")
                return pl.DataFrame(schema=CHANNEL_SUM_SCHEMA)
        
        df = pl.read_parquet(channel_path)
        print(f"  File loaded successfully")
        print(f"  Columns: {df.columns}")
        print(f"  Raw data records: {len(df)}")
        
        # Today's data
        channel_df = df.select([
            pl.col("CHANNEL").str.to_uppercase().cast(pl.Utf8).alias("CHANNEL"),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE"),
            pl.lit(REPTDATE.strftime("%b%y").upper()).cast(pl.Utf8).alias("MONTH")
        ])
        
        print(f"  Today's processed records: {len(channel_df)}")
        print(f"  MONTH value: {REPTDATE.strftime('%b%y').upper()}")
        
        # Get previous month data
        prev_month_data = pl.DataFrame(schema=CHANNEL_SUM_SCHEMA)
        prev_sum_path = f"{PREV_MONTH_PATH}/CHANNEL_SUM.parquet"
        
        if os.path.exists(prev_sum_path):
            print(f"  Loading previous month data from: {prev_sum_path}")
            prev_month_data = pl.read_parquet(prev_sum_path)
            print(f"  Previous month data loaded: {len(prev_month_data)} records")
            
            # Cast to ensure schema consistency
            prev_month_data = prev_month_data.select([
                pl.col("CHANNEL").cast(pl.Utf8),
                pl.col("TOLPROMPT").cast(pl.Int64),
                pl.col("TOLUPDATE").cast(pl.Int64),
                pl.col("MONTH").cast(pl.Utf8)
            ])
        else:
            print(f"  No previous month data found at: {prev_sum_path}")
        
        # Get current month existing data
        curr_month_data = pl.DataFrame(schema=CHANNEL_SUM_SCHEMA)
        curr_sum_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
        
        if os.path.exists(curr_sum_path):
            print(f"  Loading current month existing data from: {curr_sum_path}")
            curr_month_data = pl.read_parquet(curr_sum_path)
            print(f"  Current month existing data loaded: {len(curr_month_data)} records")
            
            # Cast to ensure schema consistency
            curr_month_data = curr_month_data.select([
                pl.col("CHANNEL").cast(pl.Utf8),
                pl.col("TOLPROMPT").cast(pl.Int64),
                pl.col("TOLUPDATE").cast(pl.Int64),
                pl.col("MONTH").cast(pl.Utf8)
            ])
            
            # Remove any existing data for today's MONTH
            today_month = REPTDATE.strftime("%b%y").upper()
            if today_month in curr_month_data['MONTH'].unique().to_list():
                print(f"  Removing existing {today_month} data...")
                before_count = len(curr_month_data)
                curr_month_data = curr_month_data.filter(pl.col("MONTH") != today_month)
                print(f"  Before: {before_count}, After: {len(curr_month_data)} records")
        else:
            print(f"  No existing current month data")
        
        # Combine all data
        all_data = []
        
        if len(prev_month_data) > 0:
            all_data.append(prev_month_data)
            print(f"  Adding previous month: {len(prev_month_data)} records")
        
        if len(curr_month_data) > 0:
            all_data.append(curr_month_data)
            print(f"  Adding current month existing: {len(curr_month_data)} records")
        
        all_data.append(channel_df)
        print(f"  Adding today's new data: {len(channel_df)} records")
        
        if all_data:
            final_df = pl.concat(all_data, how="vertical")
            print(f"\n  FINAL RESULT:")
            print(f"    Total records: {len(final_df)}")
            
            # Show breakdown by MONTH
            month_counts = final_df.group_by("MONTH").agg(pl.count().alias("records")).sort("MONTH")
            print(f"    Breakdown by MONTH:")
            for row in month_counts.iter_rows(named=True):
                print(f"      {row['MONTH']}: {row['records']} records")
            
            return final_df
        else:
            print(f"  No data to combine")
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
        print(f"  Looking for OTC file: {otc_path}")
        
        if not os.path.exists(otc_path):
            print(f"  OTC file not found, checking alternatives...")
            # Try lowercase
            alt_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/ciphonet_otc_summary.parquet"
            if os.path.exists(alt_path):
                print(f"  Found OTC file with lowercase name")
                otc_path = alt_path
            else:
                print(f"  OTC file not found, creating zero-filled data")
                otc_detail = bcode_df.with_columns([
                    pl.lit(0).cast(pl.Int64).alias("TOLPROMPT"),
                    pl.lit(0).cast(pl.Int64).alias("TOLUPDATE")
                ])
                return otc_detail
        
        otc_df = pl.read_parquet(otc_path)
        print(f"  OTC file loaded successfully")
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
        
        # Merge ALL BCODE branches with OTC data
        merged = bcode_df.join(otc_clean, on="BRANCHNO", how="left")
        
        # Fill nulls with 0
        otc_detail = merged.with_columns([
            pl.col("TOLPROMPT").fill_null(0),
            pl.col("TOLUPDATE").fill_null(0)
        ]).sort("BRANCHNO")
        
        print(f"\n  FINAL OTC_DETAIL:")
        print(f"    Total records: {len(otc_detail)}")
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
        print(f"  Looking for update file: {update_path}")
        
        if not os.path.exists(update_path):
            print(f"  File not found, checking alternatives...")
            # Try lowercase
            alt_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/ciphonet_full_summary.parquet"
            if os.path.exists(alt_path):
                print(f"  Found update file with lowercase name")
                update_path = alt_path
            else:
                print(f"  Update file not found")
                return pl.DataFrame(schema=CHANNEL_UPDATE_SCHEMA)
        
        df = pl.read_parquet(update_path)
        print(f"  Update file loaded successfully")
        print(f"  Columns: {df.columns}")
        
        if len(df) >= 2:
            update_df = df.head(2)
            
            update_df = update_df.with_row_index(name="index").with_columns(
                pl.when(pl.col("index") == 0)
                  .then(pl.lit("TOTAL PROMPT BASE"))
                  .when(pl.col("index") == 1)
                  .then(pl.lit("TOTAL UPDATED"))
                  .otherwise(pl.lit(None))
                  .alias("DESC")
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
                print(f"  Loading previous month update data from: {prev_update_path}")
                prev_month_data = pl.read_parquet(prev_update_path)
                print(f"  Previous month data: {len(prev_month_data)} records")
                
                prev_month_data = prev_month_data.select([
                    pl.col("DESC").cast(pl.Utf8),
                    pl.col("ATM").cast(pl.Int64),
                    pl.col("EBK").cast(pl.Int64),
                    pl.col("OTC").cast(pl.Int64),
                    pl.col("TOTAL").cast(pl.Int64),
                    pl.col("DATE").cast(pl.Utf8)
                ])
            
            # Get current month existing data
            curr_month_data = pl.DataFrame(schema=CHANNEL_UPDATE_SCHEMA)
            curr_update_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
            
            if os.path.exists(curr_update_path):
                print(f"  Loading current month existing update data from: {curr_update_path}")
                curr_month_data = pl.read_parquet(curr_update_path)
                print(f"  Current month existing: {len(curr_month_data)} records")
                
                curr_month_data = curr_month_data.select([
                    pl.col("DESC").cast(pl.Utf8),
                    pl.col("ATM").cast(pl.Int64),
                    pl.col("EBK").cast(pl.Int64),
                    pl.col("OTC").cast(pl.Int64),
                    pl.col("TOTAL").cast(pl.Int64),
                    pl.col("DATE").cast(pl.Utf8)
                ])
                
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
# FUNCTION 5: WRITE OTC_DETAIL SAS DATASET
# =============================================================================
def write_otc_sas_dataset(df, dataset_name):
    """Write OTC_DETAIL to SAS dataset"""
    if sas is None or len(df) == 0:
        print(f"  Cannot write SAS dataset: sas={sas}, df len={len(df) if df is not None else 'None'}")
        return False
    
    try:
        sas_path = CURRENT_MONTH_PATH
        os.makedirs(sas_path, exist_ok=True)
        
        # Assign library
        print(f"  Assigning SAS library OTCLIB to {sas_path}")
        lib_result = sas.submit(f"libname OTCLIB '{sas_path}';")
        
        # Check for errors
        if isinstance(lib_result, dict) and "LOG" in lib_result:
            log_text = lib_result["LOG"]
        elif hasattr(lib_result, 'LOG'):
            log_text = lib_result.LOG
        else:
            log_text = str(lib_result)
            
        if "ERROR" in log_text:
            print(f"  Library error in log:")
            print(f"  {log_text[:500]}")
            return False
        
        # Write dataset
        print(f"  Creating {dataset_name}...")
        print(f"  DataFrame type: {type(df)}")
        print(f"  DataFrame columns: {list(df.columns) if df is not None else 'None'}")
        
        if df is not None:
            df_pandas = df.to_pandas()
            print(f"  Pandas DataFrame shape: {df_pandas.shape}")
            
            write_result = sas.df2sd(df_pandas, table=dataset_name, libref="OTCLIB")
            
            # Check for errors
            if hasattr(write_result, 'LOG'):
                log_text = write_result.LOG
                if "ERROR" in log_text:
                    print(f"  Write error in log:")
                    print(f"  {log_text[:500]}")
                    return False
        
        # Check file
        sas_file = f"{sas_path}/{dataset_name}.sas7bdat"
        if os.path.exists(sas_file):
            size = os.path.getsize(sas_file)
            print(f"  ✓ {dataset_name}.sas7bdat created ({size:,} bytes)")
            return True
        else:
            print(f"  ✗ SAS file not found at: {sas_file}")
            print(f"  Listing files in {sas_path}:")
            for f in os.listdir(sas_path):
                print(f"    - {f}")
            return False
            
    except Exception as e:
        print(f"  SAS error: {e}")
        import traceback
        traceback.print_exc()
        return False

# =============================================================================
# SAS DATA TRANSFER FUNCTIONS - FIXED VERSION
# =============================================================================
def assign_libname(lib_name, sas_path):
    """Assign SAS library"""
    print(f"  Assigning {lib_name} to {sas_path}")
    log = sas.submit(f"libname {lib_name} '{sas_path}';")
    return log

def set_data(df_polars, lib_name, ctrl_name, cur_data, prev_data):
    """Transfer polars DataFrame to SAS dataset - FIXED VERSION"""
    try:
        print(f"\n  Transferring data to SAS...")
        print(f"    DataFrame type: {type(df_polars)}")
        print(f"    DataFrame shape: {df_polars.shape if hasattr(df_polars, 'shape') else 'No shape'}")
        
        # FIX: Ensure we have a proper DataFrame
        if df_polars is None or len(df_polars) == 0:
            print(f"    Error: DataFrame is empty or None")
            return None
            
        # Convert to pandas
        df_pandas = df_polars.to_pandas()
        print(f"    Pandas DataFrame shape: {df_pandas.shape}")
        print(f"    Pandas columns: {list(df_pandas.columns)}")
        
        # Write to WORK library first
        print(f"    Writing to work.{cur_data}")
        write_result = sas.df2sd(df_pandas, table=cur_data, libref='work')
        
        if hasattr(write_result, 'LOG') and "ERROR" in write_result.LOG:
            print(f"    Error writing to work: {write_result.LOG[:500]}")
            return write_result
        
        # Get metadata from control dataset
        print(f"    Getting metadata from {ctrl_name}.{prev_data}")
        meta_log = sas.submit(f"""
            proc sql noprint;
               create table work.colmeta as 
                select name, type, length
            from dictionary.columns
            where libname = upcase("{ctrl_name}")  
                 and memname = upcase("{prev_data}");
            quit;
        """)
        
        df_meta = sas.sasdata("colmeta", libref="work").to_df()
        print(f"    Metadata records: {len(df_meta)}")
        
        if len(df_meta) > 0:
            cols = df_meta["name"].dropna().tolist()
            col_list = ", ".join(cols)
            
            casted_cols = []
            for _, row in df_meta.iterrows():
                col = row["name"]
                length = row['length']
                if str(row['type']).strip().lower() == 'char' and pd.notnull(length) and length > 0:
                    casted_cols.append(f"input(trim({col}), ${int(length)}.) as {col}")
                else:
                    casted_cols.append(col)
            
            casted_cols_str = ",\n ".join(casted_cols)
            
            transfer_log = sas.submit(f"""
                proc sql noprint;
                     create table {lib_name}.{cur_data} as
                     select {col_list} from {ctrl_name}.{prev_data}(obs=0)
                     union corr
                     select {casted_cols_str} from work.{cur_data};
                quit;
            """)
        else:
            print(f"    No metadata found, using simple data step")
            transfer_log = sas.submit(f"""
                data {lib_name}.{cur_data};
                    set work.{cur_data};
                run;
            """)
        
        # Check for errors
        if hasattr(transfer_log, 'LOG'):
            if "ERROR" in transfer_log.LOG:
                print(f"    Transfer error: {transfer_log.LOG[:500]}")
            else:
                print(f"    ✓ Transfer complete")
        
        return transfer_log
        
    except Exception as e:
        print(f"    Error in set_data: {e}")
        import traceback
        traceback.print_exc()
        return None

# =============================================================================
# MAIN PROCESSING
# =============================================================================
print("\n" + "=" * 80)
print(f"PROCESSING WITH PREVIOUS MONTH ACCUMULATION")
print("=" * 80)

print(f"\n>>> 1. CHANNEL SUMMARY (EIBMCHNL)")
print(f"Accumulating with {PREV_YEAR}-{PREV_MONTH} data...")
channel_df = process_channel_sum()

print(f"\n>>> 2. CHANNEL UPDATE (EIBMCHN2)")
print(f"Accumulating with {PREV_YEAR}-{PREV_MONTH} data...")
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

if len(update_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
    update_df.write_parquet(output_path)
    print(f"✓ CHANNEL_UPDATE.parquet: {output_path}")
    print(f"  Total records: {len(update_df)}")

if len(otc_detail) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/OTC_DETAIL.parquet"
    otc_detail.write_parquet(output_path)
    print(f"✓ OTC_DETAIL.parquet: {output_path}")
    print(f"  Columns: {otc_detail.columns}")

# =============================================================================
# TRANSFER TO SAS DATASETS
# =============================================================================
print("\n>>> TRANSFERRING TO SAS DATASETS")

if sas:
    try:
        print(f"\n1. Assigning SAS libraries...")
        assign_libname("crm", "/stgsrcsys/host/uat")
        assign_libname("ctrl_crm", "/sas/python/virt_edw/Data_Warehouse/SASTABLE")
        
        # FIX: Define proper dataset names
        if len(channel_df) > 0:
            print(f"\n2. Transferring CHANNEL_SUM...")
            # Use proper control table names
            log1 = set_data(channel_df, "crm", "ctrl_crm", "channel_sum", "channel_sum_ctl")
            if log1:
                print(f"  ✓ CHANNEL_SUM transferred: {len(channel_df)} records")
            else:
                print(f"  ✗ CHANNEL_SUM transfer failed")
        
        if len(update_df) > 0:
            print(f"\n3. Transferring CHANNEL_UPDATE...")
            log2 = set_data(update_df, "crm", "ctrl_crm", "channel_update", "channel_update_ctl")
            if log2:
                print(f"  ✓ CHANNEL_UPDATE transferred: {len(update_df)} records")
            else:
                print(f"  ✗ CHANNEL_UPDATE transfer failed")
        
        if len(otc_detail) > 0:
            print(f"\n4. Transferring OTC_DETAIL...")
            dataset_name = f"OTC_DETAIL_{REPTMON}{REPTYEAR}"
            otc_data = dataset_name  # Use the same name for SAS table
            otc_ctl = "otc_detail_ctl"  # Control table name
            
            print(f"  Dataset name: {dataset_name}")
            print(f"  OTC data name: {otc_data}")
            print(f"  Control table: {otc_ctl}")
            
            # First try direct write
            sas_success = write_otc_sas_dataset(otc_detail, dataset_name)
            
            if not sas_success:
                print(f"  Direct write failed, trying set_data method...")
                log3 = set_data(otc_detail, "crm", "ctrl_crm", otc_data, otc_ctl)
                if log3:
                    print(f"  ✓ OTC_DETAIL transferred via set_data")
                else:
                    print(f"  ✗ OTC_DETAIL transfer failed")
            else:
                print(f"  ✓ OTC_DETAIL created directly")
        
        print(f"\n✓ SAS transfer operations completed")
        
    except Exception as e:
        print(f"\n✗ Error in SAS transfer: {e}")
        import traceback
        traceback.print_exc()
else:
    print(f"\nSAS session not available")

# =============================================================================
# VERIFICATION
# =============================================================================
print("\n" + "=" * 80)
print("VERIFICATION")
print("=" * 80)

print(f"\n1. PROCESSING DETAILS:")
print(f"   - Processing date: {REPTDATE:%Y-%m-%d}")
print(f"   - Previous month: {PREV_YEAR}-{PREV_MONTH}")
print(f"   - Current month path: {CURRENT_MONTH_PATH}")
print(f"   - Previous month path: {PREV_MONTH_PATH}")

if len(channel_df) > 0:
    print(f"\n2. CHANNEL_SUM:")
    print(f"   - Total records: {len(channel_df)}")

if len(update_df) > 0:
    print(f"\n3. CHANNEL_UPDATE:")
    print(f"   - Total records: {len(update_df)}")

if len(otc_detail) > 0:
    print(f"\n4. OTC_DETAIL:")
    print(f"   - Total branches: {len(otc_detail)}")
    print(f"   - First BRANCHNO: {otc_detail['BRANCHNO'].min()}")
    print(f"   - Last BRANCHNO: {otc_detail['BRANCHNO'].max()}")

print("\n" + "=" * 80)
print("PROCESS COMPLETE")
print("=" * 80)
