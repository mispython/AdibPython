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
CURRENT_MONTH_PATH = f"{BASE_PATH}/year={REPTDATE.year}/month={REPTMON}"
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
    """Process CIPHONET_ALL_SUMMARY"""
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
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE"),
            pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH")
        ])
        
        print(f"  Records: {len(channel_df)}")
        return channel_df
    except Exception as e:
        print(f"  Error: {e}")
        return pl.DataFrame()

# =============================================================================
# FUNCTION 3: PROCESS OTC DETAIL
# =============================================================================
def process_otc_detail():
    """Process OTC summary and merge with BCODE"""
    try:
        bcode_df = read_bcode_file()
        if len(bcode_df) == 0:
            return pl.DataFrame()
        
        otc_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
        if not os.path.exists(otc_path):
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
            pl.col("CHANNEL").str.replace_all("^0+", "").cast(pl.Int64).alias("BRANCHNO")
        ).select([
            pl.col("BRANCHNO"),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
        ])
        
        print(f"  OTC after conversion: {len(otc_clean)} records")
        
        # Merge BCODE with OTC
        merged = bcode_df.join(otc_clean, on="BRANCHNO", how="left")
        
        # Fill nulls with 0
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
        return pl.DataFrame()

# =============================================================================
# FUNCTION 4: PROCESS CHANNEL UPDATE
# =============================================================================
def process_channel_update():
    """Process CIPHONET_FULL_SUMMARY"""
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
            
            # Check for description column
            desc_col = None
            for col in ['LINE', 'DESC', 'DESCRIPTION']:
                if col in update_df.columns:
                    desc_col = col
                    break
            
            if desc_col:
                update_df = update_df.with_columns([
                    pl.col(desc_col).alias("DESC"),
                    pl.col("ATM").cast(pl.Int64),
                    pl.col("EBK").cast(pl.Int64),
                    pl.col("OTC").cast(pl.Int64),
                    pl.col("TOTAL").cast(pl.Int64)
                ])
            
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
# SAS DATA TRANSFER FUNCTIONS
# =============================================================================
def assign_libname(lib_name, sas_path):
    """Assign SAS library to physical path"""
    log = sas.submit(f"libname {lib_name} '{sas_path}';")
    return log

def set_data(df_polars, lib_name, ctrl_name, cur_data, prev_data):
    """
    Transfer polars DataFrame to SAS dataset with metadata control
    
    Parameters:
    - df_polars: Polars DataFrame
    - lib_name: Target SAS library
    - ctrl_name: Control library name
    - cur_data: Current dataset name
    - prev_data: Previous/control dataset name
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
# WRITE OUTPUT FILES
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

# 3. Write OTC_DETAIL (FIXED PRINT STATEMENT)
if len(otc_detail) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/OTC_DETAIL.parquet"
    otc_detail.write_parquet(output_path)
    print(f"✓ 3. OTC_DETAIL.parquet: {output_path}")

# =============================================================================
# TRANSFER TO SAS DATASETS (OPTIONAL)
# =============================================================================
print("\n>>> TRANSFERRING TO SAS DATASETS")

# Define control dataset names
channel_sum_ctl = "channel_sum_ctl"
channel_update_ctl = "channel_update_ctl"
otc_ctl = "otc_detail_ctl"

# Define output dataset names
sum_data = "channel_sum"
update_data = "channel_update"
otc_data = f"otc_detail_{REPTMON}{REPTYEAR}"  # OTC_DETAIL_MMYY format

# Assign SAS libraries
if sas:
    try:
        # Assign libraries
        assign_libname("crm", "/stgsrcsys/host/uat")
        assign_libname("ctrl_crm", "/sas/python/virt_edw/Data_Warehouse/SASTABLE")
        
        # Transfer data with metadata control
        if len(channel_df) > 0:
            print(f"\nTransferring CHANNEL_SUM to SAS...")
            log1 = set_data(channel_df, "crm", "ctrl_crm", sum_data, channel_sum_ctl)
        
        if len(update_df) > 0:
            print(f"\nTransferring CHANNEL_UPDATE to SAS...")
            log2 = set_data(update_df, "crm", "ctrl_crm", update_data, channel_update_ctl)
        
        if len(otc_detail) > 0:
            print(f"\nTransferring OTC_DETAIL to SAS...")
            log3 = set_data(otc_detail, "crm", "ctrl_crm", otc_data, otc_ctl)
            
        print(f"\n✓ SAS datasets created:")
        print(f"  - crm.{sum_data}")
        print(f"  - crm.{update_data}")
        print(f"  - crm.{otc_data}")
        
    except Exception as e:
        print(f"\n✗ Error transferring to SAS: {e}")
else:
    print(f"\nSAS session not available, skipping SAS transfer")

# =============================================================================
# VERIFICATION
# =============================================================================
print("\n" + "=" * 60)
print("VERIFICATION")
print("=" * 60)

print(f"\nFiles in {CURRENT_MONTH_PATH}:")
if os.path.exists(CURRENT_MONTH_PATH):
    files = sorted(os.listdir(CURRENT_MONTH_PATH))
    for file in files:
        if file.endswith('.parquet'):
            filepath = os.path.join(CURRENT_MONTH_PATH, file)
            size = os.path.getsize(filepath)
            print(f"  {file} ({size:,} bytes)")

print(f"\n" + "=" * 60)
print(f"PROCESS COMPLETE")
print(f"Date: {REPTDATE:%Y-%m-%d}")
print(f"Channel Summary: {len(channel_df)} records")
print(f"Channel Update: {len(update_df)} records")
print(f"OTC Detail: {len(otc_detail)} branches")
print("=" * 60)
