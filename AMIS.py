import polars as pl
import os
import saspy
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# =============================================================================
# INITIALIZATION - ALL USING TODAY()-1
# =============================================================================
sas = saspy.SASsession() if saspy else None

# SAS: REPTDATE = TODAY() - 1;
REPTDATE = datetime.today() - timedelta(days=1)
REPTYEAR = f"{REPTDATE.year % 100:02d}"
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"

# For accumulation: need to get previous month's data
PREV_MONTH_DATE = REPTDATE.replace(day=1) - timedelta(days=1)
PREV_YEAR = PREV_MONTH_DATE.year
PREV_MONTH = PREV_MONTH_DATE.month

print(f"PROCESSING DATE: {REPTDATE:%Y-%m-%d} (TODAY()-1)")
print(f"ACCUMULATING WITH: {PREV_YEAR}-{PREV_MONTH:02d} (previous month)")

# =============================================================================
# PATH CONFIGURATION
# =============================================================================
BASE_PATH = "/host/mis/parquet/crm"
CURRENT_MONTH_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTMON}"
PREV_MONTH_PATH = f"{BASE_PATH}/year={PREV_YEAR}/month={PREV_MONTH:02d}"

os.makedirs(CURRENT_MONTH_PATH, exist_ok=True)

# =============================================================================
# FUNCTION 1: READ BCODE FILE - ALL 375 BRANCHES
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
                            # Include ALL branches
                            records.append({"BRANCHNO": branchno})
                    except:
                        continue
        
        if records:
            df = pl.DataFrame(records).unique().sort("BRANCHNO")
            print(f"  BCODE total branches: {len(df)}")
            return df
        else:
            return pl.DataFrame()
    except Exception as e:
        print(f"  Error reading BCODE: {e}")
        return pl.DataFrame()

# =============================================================================
# FUNCTION 2: PROCESS CHANNEL SUMMARY WITH PREVIOUS MONTH ACCUMULATION
# =============================================================================
def process_channel_sum():
    """Process CHANNEL summary - accumulate with previous month"""
    try:
        channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
        if not os.path.exists(channel_path):
            print(f"  File not found: {channel_path}")
            return pl.DataFrame()
        
        df = pl.read_parquet(channel_path)
        print(f"  Columns: {df.columns}")
        
        # TODAY()-1 for MONTH calculation
        channel_df = df.select([
            pl.col("CHANNEL").str.to_uppercase().alias("CHANNEL"),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE"),
            pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH")
        ])
        
        print(f"  Today's records: {len(channel_df)}")
        print(f"  MONTH: {REPTDATE.strftime('%b%y').upper()}")
        
        # ==============================================
        # KEY CHANGE: ACCUMULATE WITH PREVIOUS MONTH
        # ==============================================
        
        # 1. First, get previous month's accumulated data
        prev_month_data = pl.DataFrame()
        prev_sum_path = f"{PREV_MONTH_PATH}/CHANNEL_SUM.parquet"
        
        if os.path.exists(prev_sum_path):
            prev_month_data = pl.read_parquet(prev_sum_path)
            print(f"  Previous month data: {len(prev_month_data)} records")
        else:
            print(f"  No previous month data found at: {prev_sum_path}")
        
        # 2. Get current month's existing data (if any)
        curr_sum_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
        curr_month_data = pl.DataFrame()
        
        if os.path.exists(curr_sum_path):
            curr_month_data = pl.read_parquet(curr_sum_path)
            print(f"  Current month existing: {len(curr_month_data)} records")
        
        # 3. Combine: previous month + current month existing + today's new data
        all_data = []
        
        # Add previous month data
        if len(prev_month_data) > 0:
            all_data.append(prev_month_data)
            print(f"  Adding previous month: {len(prev_month_data)}")
        
        # Add current month existing data
        if len(curr_month_data) > 0:
            # Remove today's data if it already exists (deduplication)
            today_month = REPTDATE.strftime("%b%y").upper()
            if today_month in curr_month_data['MONTH'].unique().to_list():
                print(f"  Removing existing {today_month} data from current month...")
                curr_month_data = curr_month_data.filter(pl.col("MONTH") != today_month)
                print(f"  After removal: {len(curr_month_data)}")
            
            all_data.append(curr_month_data)
        
        # Add today's new data
        all_data.append(channel_df)
        
        # Combine all data
        if all_data:
            final_df = pl.concat(all_data, how="vertical")
            print(f"  After accumulation: {len(final_df)} total records")
            return final_df
        else:
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
        return pl.DataFrame()

# =============================================================================
# FUNCTION 4: PROCESS CHANNEL UPDATE WITH PREVIOUS MONTH ACCUMULATION
# =============================================================================
def process_channel_update():
    """Process CHANNEL update - accumulate with previous month"""
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
            
            # Add DATE column using TODAY()-1
            update_df = update_df.with_columns(
                pl.lit(REPTDATE.strftime("%d/%m/%Y")).alias("DATE")
            )
            
            print(f"  Today's update records: {len(update_df)}")
            
            # ==============================================
            # KEY CHANGE: ACCUMULATE WITH PREVIOUS MONTH
            # ==============================================
            
            # 1. Get previous month's accumulated data
            prev_month_data = pl.DataFrame()
            prev_update_path = f"{PREV_MONTH_PATH}/CHANNEL_UPDATE.parquet"
            
            if os.path.exists(prev_update_path):
                prev_month_data = pl.read_parquet(prev_update_path)
                print(f"  Previous month data: {len(prev_month_data)} records")
            else:
                print(f"  No previous month data found at: {prev_update_path}")
            
            # 2. Get current month's existing data (if any)
            curr_update_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
            curr_month_data = pl.DataFrame()
            
            if os.path.exists(curr_update_path):
                curr_month_data = pl.read_parquet(curr_update_path)
                print(f"  Current month existing: {len(curr_month_data)} records")
            
            # 3. Combine: previous month + current month existing + today's new data
            all_data = []
            
            # Add previous month data
            if len(prev_month_data) > 0:
                all_data.append(prev_month_data)
                print(f"  Adding previous month: {len(prev_month_data)}")
            
            # Add current month existing data
            if len(curr_month_data) > 0:
                # Remove today's data if it already exists
                today_date = REPTDATE.strftime("%d/%m/%Y")
                if today_date in curr_month_data['DATE'].unique().to_list():
                    print(f"  Removing existing {today_date} data...")
                    curr_month_data = curr_month_data.filter(pl.col("DATE") != today_date)
                    print(f"  After removal: {len(curr_month_data)}")
                
                all_data.append(curr_month_data)
            
            # Add today's new data
            all_data.append(update_df)
            
            # Combine all data
            if all_data:
                final_df = pl.concat(all_data, how="vertical")
                print(f"  After accumulation: {len(final_df)} total records")
                return final_df
            else:
                return update_df
        else:
            print(f"  Not enough records (need 2, got {len(df)})")
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
# SAS DATA TRANSFER FUNCTIONS
# =============================================================================
def assign_libname(lib_name, sas_path):
    """Assign SAS library to physical path"""
    log = sas.submit(f"libname {lib_name} '{sas_path}';")
    return log

def set_data(df_polars, lib_name, ctrl_name, cur_data, prev_data):
    """Transfer polars DataFrame to SAS dataset with metadata control"""
    df_pandas = df_polars.to_pandas()
    sas.df2sd(df_pandas, table=cur_data, libref='work')
    
    log = sas.submit(f"""
        proc sql noprint;
           create table colmeta as 
            select name, type, length
        from dictionary.columns
        where libname = upcase("{ctrl_name}")  
             and memname = upcase("{prev_data}");
        quit;
    """)
    
    df_meta = sas.sasdata("colmeta", libref="work").to_df()
    
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
        
        log = sas.submit(f"""
            proc sql noprint;
                 create table {lib_name}.{cur_data} as
                 select {col_list} from {ctrl_name}.{prev_data}(obs=0)
                 union corr
                 select {casted_cols_str} from work.{cur_data};
            quit;
        """)
    else:
        log = sas.submit(f"""
            data {lib_name}.{cur_data};
                set work.{cur_data};
            run;
        """)
    
    return log

# =============================================================================
# MAIN PROCESSING
# =============================================================================
print("\n" + "=" * 80)
print(f"PROCESSING WITH PREVIOUS MONTH ACCUMULATION")
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

# 1. Write CHANNEL_SUM (with previous month accumulation)
if len(channel_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
    channel_df.write_parquet(output_path)
    print(f"✓ CHANNEL_SUM.parquet: {output_path}")
    print(f"  Total records after accumulation: {len(channel_df)}")

# 2. Write CHANNEL_UPDATE (with previous month accumulation)
if len(update_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
    update_df.write_parquet(output_path)
    print(f"✓ CHANNEL_UPDATE.parquet: {output_path}")
    print(f"  Total records after accumulation: {len(update_df)}")

# 3. Write OTC_DETAIL Parquet
if len(otc_detail) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/OTC_DETAIL.parquet"
    otc_detail.write_parquet(output_path)
    print(f"✓ OTC_DETAIL.parquet: {output_path}")

# =============================================================================
# TRANSFER TO SAS DATASETS
# =============================================================================
print("\n>>> TRANSFERRING TO SAS DATASETS")

if sas:
    try:
        # Assign libraries
        assign_libname("crm", "/stgsrcsys/host/uat")
        assign_libname("ctrl_crm", "/sas/python/virt_edw/Data_Warehouse/SASTABLE")
        
        # Transfer CHANNEL_SUM
        if len(channel_df) > 0:
            print(f"\nTransferring CHANNEL_SUM to SAS...")
            log1 = set_data(channel_df, "crm", "ctrl_crm", "channel_sum", "channel_sum_ctl")
            print(f"  CHANNEL_SUM transferred: {len(channel_df)} records")
        
        # Transfer CHANNEL_UPDATE
        if len(update_df) > 0:
            print(f"\nTransferring CHANNEL_UPDATE to SAS...")
            log2 = set_data(update_df, "crm", "ctrl_crm", "channel_update", "channel_update_ctl")
            print(f"  CHANNEL_UPDATE transferred: {len(update_df)} records")
        
        # Transfer OTC_DETAIL
        if len(otc_detail) > 0:
            dataset_name = f"OTC_DETAIL_{REPTMON}{REPTYEAR}"
            print(f"\nTransferring OTC_DETAIL to SAS...")
            sas_success = write_otc_sas_dataset(otc_detail, dataset_name)
            if not sas_success:
                log3 = set_data(otc_detail, "crm", "ctrl_crm", dataset_name, "otc_detail_ctl")
            
        print(f"\n✓ SAS datasets created with previous month accumulation")
        
    except Exception as e:
        print(f"\n✗ Error transferring to SAS: {e}")
else:
    print(f"\nSAS session not available")

# =============================================================================
# VERIFICATION
# =============================================================================
print("\n" + "=" * 80)
print("VERIFICATION - WITH PREVIOUS MONTH ACCUMULATION")
print("=" * 80)

print(f"\n1. ACCUMULATION DETAILS:")
print(f"   - Processing date: {REPTDATE:%Y-%m-%d}")
print(f"   - Previous month: {PREV_YEAR}-{PREV_MONTH:02d}")
print(f"   - Previous month path: {PREV_MONTH_PATH}")

if len(channel_df) > 0:
    print(f"\n2. CHANNEL_SUM ACCUMULATION:")
    print(f"   - Total records: {len(channel_df)}")
    unique_months = channel_df['MONTH'].unique().sort()
    print(f"   - Unique MONTH values: {unique_months.to_list()}")
    
    # Count records per month
    month_counts = channel_df.group_by("MONTH").agg(pl.count().alias("records"))
    print(f"   - Records per month:")
    for row in month_counts.iter_rows(named=True):
        print(f"     {row['MONTH']}: {row['records']} records")

if len(update_df) > 0:
    print(f"\n3. CHANNEL_UPDATE ACCUMULATION:")
    print(f"   - Total records: {len(update_df)}")
    # Show unique dates
    unique_dates = update_df['DATE'].unique().sort()
    print(f"   - Number of unique dates: {len(unique_dates)}")
    print(f"   - First 5 dates: {unique_dates.head(5).to_list()}")

if len(otc_detail) > 0:
    print(f"\n4. OTC_DETAIL:")
    print(f"   - Total branches: {len(otc_detail)} (should be 375)")
    print(f"   - First BRANCHNO: {otc_detail['BRANCHNO'].min()} (should be 2)")

print("\n" + "=" * 80)
print("PROCESS COMPLETE - WITH PREVIOUS MONTH ACCUMULATION")
print("=" * 80)
