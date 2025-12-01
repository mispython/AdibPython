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
RDATE = REPTDATE.strftime("%d%m%y")

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
# FUNCTION 1: PROCESS CHANNEL FILE (EIBMCHNL) - CHANNEL_SUM
# =============================================================================
def process_channel_sum():
    """Process CHANNEL file for summary data"""
    try:
        # Read channel summary file (assuming it's already in parquet)
        channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
        if not os.path.exists(channel_path):
            print(f"  Channel file not found: {channel_path}")
            return pl.DataFrame()
        
        # Read and transform
        df = pl.read_parquet(channel_path)
        
        # Select/rename columns to match SAS input
        channel_df = df.select([
            pl.col("CHANNEL").str.to_uppercase().alias("CHANNEL"),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
        ])
        
        # Add MONTH column (MONYY5. format like "NOV25")
        channel_df = channel_df.with_columns(
            pl.lit(REPTDATE.strftime("%b%y").upper()).alias("MONTH")
        )
        
        print(f"  Channel records: {len(channel_df)}")
        return channel_df
        
    except Exception as e:
        print(f"  Error processing channel: {e}")
        return pl.DataFrame()

# =============================================================================
# FUNCTION 2: PROCESS BRANCH AND BCODE (EIBMCHNL) - OTC_DETAIL
# =============================================================================
def process_otc_detail():
    """Process BCODE and BRANCH files for OTC detail"""
    try:
        # Read BCODE file
        bcode_records = []
        with open("/sasdata/rawdata/lookup/LKP_BRANCH", 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        branchno = int(line[1:4].strip())  # Position 2-4
                        bcode_records.append({"BRANCHNO": branchno})
                    except:
                        continue
        
        bcode_df = pl.DataFrame(bcode_records).unique().sort("BRANCHNO")
        print(f"  BCODE branches: {len(bcode_df)}")
        
        # Read BRANCH data
        branch_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
        if os.path.exists(branch_path):
            branch_df = pl.read_parquet(branch_path).select([
                pl.col("BRANCHNO").cast(pl.Int64),
                pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
                pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
            ])
        else:
            branch_df = pl.DataFrame({
                "BRANCHNO": [],
                "TOLPROMPT": [],
                "TOLUPDATE": []
            })
        
        print(f"  BRANCH records: {len(branch_df)}")
        
        # Merge BCODE with BRANCH (SAS: MERGE BCODE(IN=A) BRANCH(IN=B); BY BRANCHNO; IF A;)
        otc_detail = bcode_df.join(branch_df, on="BRANCHNO", how="left").with_columns([
            pl.col("TOLPROMPT").fill_null(0),
            pl.col("TOLUPDATE").fill_null(0)
        ])
        
        return otc_detail
        
    except Exception as e:
        print(f"  Error processing OTC detail: {e}")
        return pl.DataFrame()

# =============================================================================
# FUNCTION 3: PROCESS CHANNEL FILE (EIBMCHN2) - CHANNEL_UPDATE
# =============================================================================
def process_channel_update():
    """Process CHANNEL file for update data (positions based)"""
    try:
        # Assuming we have the full summary file
        update_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_FULL_SUMMARY.parquet"
        if not os.path.exists(update_path):
            print(f"  Update file not found: {update_path}")
            return pl.DataFrame()
        
        df = pl.read_parquet(update_path)
        
        # Select first 2 rows and rename columns
        update_df = df.head(2).select([
            pl.col("ATM").cast(pl.Int64),
            pl.col("EBK").cast(pl.Int64),
            pl.col("OTC").cast(pl.Int64),
            pl.col("TOTAL").cast(pl.Int64)
        ])
        
        # Add DESC column based on row number
        update_df = update_df.with_columns(
            pl.when(pl.col("ATM").is_first()).then("TOTAL PROMPT BASE")
             .otherwise("TOTAL UPDATED").alias("DESC")
        )
        
        # Add DATE column
        update_df = update_df.with_columns(
            pl.lit(REPTDATE.strftime("%d/%m/%Y")).alias("DATE")
        )
        
        # Reorder columns
        update_df = update_df.select(["DESC", "ATM", "EBK", "OTC", "TOTAL", "DATE"])
        
        print(f"  Update records: {len(update_df)}")
        return update_df
        
    except Exception as e:
        print(f"  Error processing update: {e}")
        return pl.DataFrame()

# =============================================================================
# MAIN PROCESSING
# =============================================================================
print("\n>>> RUNNING EIBMCHNL (CHANNEL SUMMARY)")
channel_df = process_channel_sum()
if len(channel_df) > 0:
    # Append to existing data
    existing_sum_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
    if os.path.exists(existing_sum_path):
        existing = pl.read_parquet(existing_sum_path)
        final_channel = pl.concat([existing, channel_df])
    else:
        final_channel = channel_df
    
    final_channel.write_parquet(existing_sum_path)
    print(f"  ✓ CHANNEL_SUM updated: {len(final_channel)} total records")

print("\n>>> RUNNING EIBMCHN2 (CHANNEL UPDATE)")
update_df = process_channel_update()
if len(update_df) > 0:
    # Append to existing data
    existing_update_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
    if os.path.exists(existing_update_path):
        existing = pl.read_parquet(existing_update_path)
        final_update = pl.concat([existing, update_df])
    else:
        final_update = update_df
    
    final_update.write_parquet(existing_update_path)
    print(f"  ✓ CHANNEL_UPDATE updated: {len(final_update)} total records")

print("\n>>> CREATING OTC_DETAIL (MERGE BCODE + BRANCH)")
otc_detail = process_otc_detail()
if len(otc_detail) > 0:
    # Write to SAS dataset
    if sas:
        try:
            # Create SAS dataset
            sas_path = f"{CURRENT_MONTH_PATH}/otc_detail"
            os.makedirs(sas_path, exist_ok=True)
            
            sas.submit(f"libname otc_lib '{sas_path}';")
            sas.df2sd(otc_detail.to_pandas(), table="OTC_DETAIL", libref="otc_lib")
            sas.submit("libname otc_lib clear;")
            
            print(f"  ✓ OTC_DETAIL SAS dataset created: {sas_path}/OTC_DETAIL.sas7bdat")
            print(f"  ✓ Records: {len(otc_detail)}")
            print(f"  ✓ With data: {otc_detail.filter(pl.col('TOLPROMPT') > 0).height}")
            
        except Exception as e:
            print(f"  ✗ SAS write failed: {e}")
            # Fallback to Parquet
            parquet_path = f"{CRMWH_PATH}/OTC_DETAIL_{REPTMON}{REPTYEAR}.parquet"
            otc_detail.write_parquet(parquet_path)
            print(f"  ✓ Saved as Parquet: {parquet_path}")
    else:
        # SAS not available, use Parquet
        parquet_path = f"{CRMWH_PATH}/OTC_DETAIL_{REPTMON}{REPTYEAR}.parquet"
        otc_detail.write_parquet(parquet_path)
        print(f"  ✓ Saved as Parquet: {parquet_path}")

# =============================================================================
# VERIFICATION
# =============================================================================
print("\n>>> VERIFICATION")

if len(channel_df) > 0:
    print(f"\n  CHANNEL_SUM Preview:")
    print(channel_df.head(3).to_pandas().to_string(index=False))

if len(update_df) > 0:
    print(f"\n  CHANNEL_UPDATE Preview:")
    print(update_df.to_pandas().to_string(index=False))

if len(otc_detail) > 0:
    print(f"\n  OTC_DETAIL Summary:")
    print(f"    Total branches: {len(otc_detail)}")
    print(f"    TOLPROMPT sum: {otc_detail['TOLPROMPT'].sum()}")
    print(f"    TOLUPDATE sum: {otc_detail['TOLUPDATE'].sum()}")
    
    # Check file creation
    sas_file = f"{CURRENT_MONTH_PATH}/otc_detail/OTC_DETAIL.sas7bdat"
    if os.path.exists(sas_file):
        print(f"    SAS file created: Yes ({os.path.getsize(sas_file):,} bytes)")
    else:
        print(f"    SAS file created: No")

print("\n" + "=" * 60)
print(f"COMPLETE: {REPTDATE:%Y-%m-%d}")
print(f"Output Month: {REPTDATE.year}-{REPTMON}")
print("=" * 60)
