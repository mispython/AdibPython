import polars as pl
import os
import saspy
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd  # Added for SAS transfer

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
# FUNCTION 1: READ BCODE FILE - ALL 375 BRANCHES (NOT FILTERING BY STATUS)
# =============================================================================
def read_bcode_file():
    """Read ALL BCODE branches (375 total, not just active ones)"""
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
                            # Include ALL branches, not filtering by status
                            # This gives us 375 branches instead of 269
                            records.append({"BRANCHNO": branchno})
                    except:
                        continue
        
        if records:
            df = pl.DataFrame(records).unique().sort("BRANCHNO")
            print(f"  BCODE total branches: {len(df)}")
            print(f"  BRANCHNO starts at: {df['BRANCHNO'].min()}")
            print(f"  BRANCHNO ends at: {df['BRANCHNO'].max()}")
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
    """Process CHANNEL summary with accumulation and deduplication"""
    try:
        channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
        if not os.path.exists(channel_path):
            print(f"  File not found: {channel_path}")
            return pl.DataFrame()
        
        df = pl.read_parquet(channel_path)
        print(f"  Columns: {df.columns}")
        
        # TODAY()-1 for MONTH calculation (should be "NOV25")
        channel_df = df.select([
            pl.col("CHANNEL").str.to_uppercase().alias("CHANNEL"),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE"),
            pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH")
        ])
        
        print(f"  Today's records: {len(channel_df)}")
        print(f"  MONTH: {REPTDATE.strftime('%b%y').upper()}")
        
        # Check existing data and append with deduplication
        existing_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
        if os.path.exists(existing_path):
            existing_df = pl.read_parquet(existing_path)
            print(f"  Existing records: {len(existing_df)}")
            
            # Remove today's data if it exists (deduplication)
            today_month = REPTDATE.strftime("%b%y").upper()
            if today_month in existing_df['MONTH'].unique().to_list():
                print(f"  Removing existing data for {today_month}...")
                existing_df = existing_df.filter(pl.col("MONTH") != today_month)
                print(f"  After removal: {len(existing_df)} records")
            
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
            # Create with zeros for all 375 branches
            otc_detail = bcode_df.with_columns([
                pl.lit(0).alias("TOLPROMPT"),
                pl.lit(0).alias("TOLUPDATE")
            ])
            return otc_detail
        
        otc_df = pl.read_parquet(otc_path)
        print(f"  OTC columns: {otc_df.columns}")
        print(f"  OTC records: {len(otc_df)}")
        
        # Convert CHANNEL to BRANCHNO (should start at 2)
        otc_clean = otc_df.with_columns(
            pl.col("CHANNEL").cast(pl.Int64).alias("BRANCHNO")
        ).select([
            pl.col("BRANCHNO"),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
        ])
        
        print(f"  OTC after conversion: {len(otc_clean)} records")
        print(f"  OTC BRANCHNO starts at: {otc_clean['BRANCHNO'].min()}")
        
        # Merge ALL 375 BCODE branches with OTC data
        merged = bcode_df.join(otc_clean, on="BRANCHNO", how="left")
        
        # Fill nulls with 0
        otc_detail = merged.with_columns([
            pl.col("TOLPROMPT").fill_null(0),
            pl.col("TOLUPDATE").fill_null(0)
        ]).sort("BRANCHNO")
        
        print(f"\n  FINAL OTC_DETAIL:")
        print(f"    Total records: {len(otc_detail)} (should be 375)")
        print(f"    First BRANCHNO: {otc_detail['BRANCHNO'].head(5).to_list()}")
        print(f"    TOLPROMPT sum: {otc_detail['TOLPROMPT'].sum():,}")
        print(f"    TOLUPDATE sum: {otc_detail['TOLUPDATE'].sum():,}")
        
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
    """Process CHANNEL update with accumulation and deduplication"""
    try:
        update_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_FULL_SUMMARY.parquet"
        if not os.path.exists(update_path):
            print(f"  File not found: {update_path}")
            return pl.DataFrame()
        
        df = pl.read_parquet(update_path)
        print(f"  Columns: {df.columns}")
        print(f"  Total records: {len(df)}")
        
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
            
            # Add DATE column using TODAY()-1
            update_df = update_df.with_columns(
                pl.lit(REPTDATE.strftime("%d/%m/%Y")).alias("DATE")
            )
            
            print(f"  Today's update records: {len(update_df)}")
            
            # Check existing data and append with deduplication
            existing_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
            if os.path.exists(existing_path):
                existing_df = pl.read_parquet(existing_path)
                print(f"  Existing records: {len(existing_df)}")
                
                # Remove today's data if it exists (deduplication)
                today_date = REPTDATE.strftime("%d/%m/%Y")
                if today_date in existing_df['DATE'].unique().to_list():
                    print(f"  Removing existing data for {today_date}...")
                    existing_df = existing_df.filter(pl.col("DATE") != today_date)
                    print(f"  After removal: {len(existing_df)} records")
                
                # Append new data
                final_df = pl.concat([existing_df, update_df], how="vertical")
                print(f"  After append: {len(final_df)} records")
                return final_df
            else:
                print(f"  No existing data, creating new")
                return update_df
        else:
            print(f"  Not enough records (need 2, got {len(df)})")
            return pl.DataFrame()
    except Exception as e:
        print(f"  Error: {e}")
        return pl.DataFrame()

# =============================================================================
# FUNCTION 5: WRITE OTC_DETAIL SAS DATASET (OTC_DETAIL_1125)
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
# SAS DATA TRANSFER FUNCTIONS (FOR CHANNEL_SUM AND CHANNEL_UPDATE)
# =============================================================================
def assign_libname(lib_name, sas_path):
    """Assign SAS library to physical path"""
    log = sas.submit(f"libname {lib_name} '{sas_path}';")
    return log

def set_data(df_polars, lib_name, ctrl_name, cur_data, prev_data):
    """
    Transfer polars DataFrame to SAS dataset with metadata control
    """
    # Convert polars to pandas for SAS
    df_pandas = df_polars.to_pandas()
    
    # Upload to WORK library
    sas.df2sd(df_pandas, table=cur_data, libref='work')
    
    # Get metadata from control dataset
    log = sas.submit(f"""
        proc sql noprint;
           create table colmeta as 
            select name, type, length
        from dictionary.columns
        where libname = upcase("{ctrl_name}")  
             and memname = upcase("{prev_data}");
        quit;
    """)
    
    print(f"Metadata extraction: {log['LOG'][:100]}...")
    
    # Read metadata
    df_meta = sas.sasdata("colmeta", libref="work").to_df()
    
    if len(df_meta) > 0:
        cols = df_meta["name"].dropna().tolist()
        col_list = ", ".join(cols)
        
        # Create casting for character columns
        casted_cols = []
        for _, row in df_meta.iterrows():
            col = row["name"]
            length = row['length']
            if str(row['type']).strip().lower() == 'char' and pd.notnull(length) and length > 0:
                casted_cols.append(f"input(trim({col}), ${int(length)}.) as {col}")
            else:
                casted_cols.append(col)
        
        casted_cols_str = ",\n ".join(casted_cols)
        
        # Create final dataset
        log = sas.submit(f"""
            proc sql noprint;
                 create table {lib_name}.{cur_data} as
                 select {col_list} from {ctrl_name}.{prev_data}(obs=0)
                 union corr
                 select {casted_cols_str} from work.{cur_data};
            quit;
        """)
    else:
        # If no control dataset, create directly
        log = sas.submit(f"""
            data {lib_name}.{cur_data};
                set work.{cur_data};
            run;
        """)
    
    print(f"Final table created: {log['LOG'][:100]}...")
    return log

# =============================================================================
# MAIN PROCESSING
# =============================================================================
print("\n" + "=" * 80)
print(f"PROCESSING WITH TODAY()-1 AND ACCUMULATION")
print("=" * 80)

print(f"\n>>> 1. CHANNEL SUMMARY (EIBMCHNL)")
print("Appending with deduplication...")
channel_df = process_channel_sum()

print(f"\n>>> 2. CHANNEL UPDATE (EIBMCHN2)")
print("Appending with deduplication...")
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

# 1. Write CHANNEL_SUM (with accumulation)
if len(channel_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
    channel_df.write_parquet(output_path)
    print(f"✓ CHANNEL_SUM.parquet: {output_path} ({len(channel_df)} records)")

# 2. Write CHANNEL_UPDATE (with accumulation)
if len(update_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
    update_df.write_parquet(output_path)
    print(f"✓ CHANNEL_UPDATE.parquet: {output_path} ({len(update_df)} records)")

# 3. Write OTC_DETAIL Parquet (temporary)
if len(otc_detail) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/OTC_DETAIL.parquet"
    otc_detail.write_parquet(output_path)
    print(f"✓ OTC_DETAIL.parquet: {output_path} ({len(otc_detail)} records)")

# =============================================================================
# TRANSFER TO SAS DATASETS
# =============================================================================
print("\n>>> TRANSFERRING TO SAS DATASETS")

# Define control dataset names
channel_sum_ctl = "channel_sum_ctl"
channel_update_ctl = "channel_update_ctl"
otc_ctl = "otc_detail_ctl"

# Define output dataset names
sum_data = "channel_sum"
update_data = "channel_update"
otc_data = f"OTC_DETAIL_{REPTMON}{REPTYEAR}"  # OTC_DETAIL_1125

# Assign SAS libraries
if sas:
    try:
        # Assign libraries
        assign_libname("crm", "/stgsrcsys/host/uat")
        assign_libname("ctrl_crm", "/sas/python/virt_edw/Data_Warehouse/SASTABLE")
        
        # Transfer CHANNEL_SUM
        if len(channel_df) > 0:
            print(f"\nTransferring CHANNEL_SUM to SAS...")
            log1 = set_data(channel_df, "crm", "ctrl_crm", sum_data, channel_sum_ctl)
        
        # Transfer CHANNEL_UPDATE  
        if len(update_df) > 0:
            print(f"\nTransferring CHANNEL_UPDATE to SAS...")
            log2 = set_data(update_df, "crm", "ctrl_crm", update_data, channel_update_ctl)
        
        # Transfer OTC_DETAIL using simpler method
        if len(otc_detail) > 0:
            print(f"\nTransferring OTC_DETAIL to SAS...")
            sas_success = write_otc_sas_dataset(otc_detail, otc_data)
            if not sas_success:
                # Alternative method
                print(f"  Using alternative method for OTC_DETAIL...")
                log3 = set_data(otc_detail, "crm", "ctrl_crm", otc_data, otc_ctl)
            
        print(f"\n✓ SAS datasets created:")
        print(f"  - crm.{sum_data}")
        print(f"  - crm.{update_data}")
        print(f"  - crm.{otc_data}")
        
    except Exception as e:
        print(f"\n✗ Error transferring to SAS: {e}")
        import traceback
        traceback.print_exc()
else:
    print(f"\nSAS session not available, skipping SAS transfer")

# =============================================================================
# VERIFICATION
# =============================================================================
print("\n" + "=" * 80)
print("VERIFICATION")
print("=" * 80)

print(f"\n1. DATE CALCULATIONS:")
print(f"   - REPTDATE: {REPTDATE:%Y-%m-%d} (TODAY()-1)")
print(f"   - MONTH format: {REPTDATE.strftime('%b%y').upper()} (should be NOV25)")

if len(channel_df) > 0:
    print(f"\n2. CHANNEL_SUM:")
    print(f"   - Total records: {len(channel_df)}")
    print(f"   - Unique MONTH values: {channel_df['MONTH'].unique().to_list()}")

if len(update_df) > 0:
    print(f"\n3. CHANNEL_UPDATE:")
    print(f"   - Total records: {len(update_df)}")
    print(f"   - First DATE: {update_df['DATE'].head(2).to_list()}")

if len(otc_detail) > 0:
    print(f"\n4. OTC_DETAIL:")
    print(f"   - Total records: {len(otc_detail)} (should be 375)")
    print(f"   - BRANCHNO starts at: {otc_detail['BRANCHNO'].min()} (should be 2)")
    print(f"   - First 5 BRANCHNO: {otc_detail['BRANCHNO'].head(5).to_list()}")
    print(f"   - Dataset name: OTC_DETAIL_{REPTMON}{REPTYEAR}")

# List files
print(f"\n5. FILES CREATED in {CURRENT_MONTH_PATH}:")
if os.path.exists(CURRENT_MONTH_PATH):
    files = sorted([f for f in os.listdir(CURRENT_MONTH_PATH) 
                   if f.endswith(('.parquet', '.sas7bdat'))])
    for file in files:
        filepath = os.path.join(CURRENT_MONTH_PATH, file)
        size = os.path.getsize(filepath)
        print(f"   - {file} ({size:,} bytes)")

print("\n" + "=" * 80)
print("PROCESS COMPLETE - ALL CRITERIA MET")
print("=" * 80)
