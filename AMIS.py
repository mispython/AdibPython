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
# FUNCTION 1: PROCESS CHANNEL FILE (EIBMCHNL) - CHANNEL_SUM
# =============================================================================
def process_channel_sum():
    """Process CHANNEL file for summary data"""
    try:
        channel_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_ALL_SUMMARY.parquet"
        if not os.path.exists(channel_path):
            print(f"  Channel file not found: {channel_path}")
            return pl.DataFrame()
        
        df = pl.read_parquet(channel_path)
        print(f"  Channel file columns: {df.columns}")
        
        # Check for BRANCHNO column name variations
        if "BRANCHNO" not in df.columns:
            # Try common variations
            for col in df.columns:
                if "branch" in col.lower() or "brn" in col.lower():
                    df = df.rename({col: "BRANCHNO"})
                    print(f"  Renamed column {col} to BRANCHNO")
                    break
        
        channel_df = df.select([
            pl.col("CHANNEL").str.to_uppercase(),
            pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT"),
            pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE")
        ]).with_columns(
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
                        branchno = int(line[1:4].strip())
                        bcode_records.append({"BRANCHNO": branchno})
                    except:
                        continue
        
        bcode_df = pl.DataFrame(bcode_records).unique().sort("BRANCHNO")
        print(f"  BCODE branches: {len(bcode_df)}")
        
        # Read BRANCH data
        branch_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_OTC_SUMMARY.parquet"
        if os.path.exists(branch_path):
            branch_df = pl.read_parquet(branch_path)
            print(f"  BRANCH file columns: {branch_df.columns}")
            
            # Check for BRANCHNO column
            if "BRANCHNO" not in branch_df.columns:
                print(f"  WARNING: BRANCHNO column not found in {branch_path}")
                print(f"  Available columns: {branch_df.columns}")
                
                # Try to find branch number column
                branch_col = None
                for col in branch_df.columns:
                    if "branch" in col.lower() or "brn" in col.lower():
                        branch_col = col
                        break
                
                if branch_col:
                    branch_df = branch_df.rename({branch_col: "BRANCHNO"})
                    print(f"  Using column '{branch_col}' as BRANCHNO")
                else:
                    # If no branch column, check if data has branch numbers elsewhere
                    print(f"  No branch column found, creating empty branch data")
                    branch_df = pl.DataFrame({
                        "BRANCHNO": [],
                        "PROMPT": [],
                        "UPDATED": []
                    })
            
            # Process branch data
            branch_df = branch_df.select([
                pl.col("BRANCHNO").cast(pl.Int64),
                pl.col("PROMPT").cast(pl.Int64).alias("TOLPROMPT").fill_null(0),
                pl.col("UPDATED").cast(pl.Int64).alias("TOLUPDATE").fill_null(0)
            ])
        else:
            print(f"  BRANCH file not found: {branch_path}")
            branch_df = pl.DataFrame({
                "BRANCHNO": [],
                "TOLPROMPT": [],
                "TOLUPDATE": []
            })
        
        print(f"  BRANCH records: {len(branch_df)}")
        
        # Merge BCODE with BRANCH
        if len(bcode_df) > 0 and len(branch_df) > 0:
            otc_detail = bcode_df.join(branch_df, on="BRANCHNO", how="left")
        elif len(bcode_df) > 0:
            # Only BCODE exists
            otc_detail = bcode_df.with_columns([
                pl.lit(0).alias("TOLPROMPT"),
                pl.lit(0).alias("TOLUPDATE")
            ])
        else:
            otc_detail = pl.DataFrame({
                "BRANCHNO": [],
                "TOLPROMPT": [],
                "TOLUPDATE": []
            })
        
        # Ensure 0 values for nulls
        otc_detail = otc_detail.with_columns([
            pl.col("TOLPROMPT").fill_null(0),
            pl.col("TOLUPDATE").fill_null(0)
        ])
        
        return otc_detail
        
    except Exception as e:
        print(f"  Error processing OTC detail: {str(e)}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame()

# =============================================================================
# FUNCTION 3: PROCESS CHANNEL UPDATE (EIBMCHN2)
# =============================================================================
def process_channel_update():
    """Process update data"""
    try:
        update_path = f"/host/cis/parquet/year={REPTDATE.year}/month={REPTMON}/day={REPTDAY}/CIPHONET_FULL_SUMMARY.parquet"
        if not os.path.exists(update_path):
            print(f"  Update file not found: {update_path}")
            return pl.DataFrame()
        
        df = pl.read_parquet(update_path)
        print(f"  Update file columns: {df.columns}")
        
        # Get first 2 rows
        update_df = df.head(2)
        
        # Map columns - check what we actually have
        available_cols = update_df.columns
        print(f"  Available columns in update: {available_cols}")
        
        # Try to find the required columns
        result_cols = {}
        
        # Look for ATM
        for col in available_cols:
            if col.upper() == "ATM":
                result_cols["ATM"] = pl.col(col).cast(pl.Int64)
                break
        
        # Look for EBK
        for col in available_cols:
            if col.upper() == "EBK":
                result_cols["EBK"] = pl.col(col).cast(pl.Int64)
                break
        
        # Look for OTC
        for col in available_cols:
            if col.upper() == "OTC":
                result_cols["OTC"] = pl.col(col).cast(pl.Int64)
                break
        
        # Look for TOTAL
        for col in available_cols:
            if col.upper() == "TOTAL":
                result_cols["TOTAL"] = pl.col(col).cast(pl.Int64)
                break
        
        if len(result_cols) < 4:
            print(f"  WARNING: Missing required columns. Found: {list(result_cols.keys())}")
            return pl.DataFrame()
        
        update_df = update_df.select(list(result_cols.values()))
        
        # Add DESC and DATE
        update_df = update_df.with_columns([
            pl.when(pl.col("ATM").is_first()).then("TOTAL PROMPT BASE")
             .otherwise("TOTAL UPDATED").alias("DESC"),
            pl.lit(REPTDATE.strftime("%d/%m/%Y")).alias("DATE")
        ])
        
        print(f"  Update records: {len(update_df)}")
        return update_df
        
    except Exception as e:
        print(f"  Error processing update: {e}")
        return pl.DataFrame()

# =============================================================================
# SIMPLE SAS WRITE FUNCTION
# =============================================================================
def write_sas_dataset(df, output_path, dataset_name):
    """Simple function to write DataFrame to SAS dataset"""
    if sas is None:
        print(f"  SAS session not available")
        return False
    
    if len(df) == 0:
        print(f"  DataFrame is empty, nothing to write")
        return False
    
    try:
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)
        
        # Assign SAS library
        lib_log = sas.submit(f"libname SASOUT '{output_path}';")
        if "ERROR" in lib_log["LOG"]:
            print(f"  Failed to assign SAS library: {lib_log['LOG']}")
            return False
        
        # Write to SAS
        print(f"  Writing {len(df)} records to SAS...")
        write_result = sas.df2sd(df.to_pandas(), table=dataset_name, libref="SASOUT")
        
        if hasattr(write_result, 'LOG') and "ERROR" in write_result.LOG:
            print(f"  SAS write error: {write_result.LOG}")
            return False
        
        print(f"  ✓ SAS write completed")
        
        # Verify
        verify_log = sas.submit(f"""
            proc sql;
                select count(*) as N from SASOUT.{dataset_name};
            quit;
        """)
        print(f"  Verification: {verify_log['LOG'][:100]}...")
        
        # Clear library
        sas.submit("libname SASOUT clear;")
        
        # Check if file was created
        sas_file = f"{output_path}/{dataset_name}.sas7bdat"
        if os.path.exists(sas_file):
            size = os.path.getsize(sas_file)
            print(f"  ✓ File created: {sas_file} ({size:,} bytes)")
            return True
        else:
            print(f"  ✗ File not found: {sas_file}")
            return False
            
    except Exception as e:
        print(f"  Error writing SAS dataset: {e}")
        return False

# =============================================================================
# MAIN PROCESSING
# =============================================================================
print("\n>>> PROCESSING EIBMCHNL (CHANNEL SUMMARY)")
channel_df = process_channel_sum()

print("\n>>> PROCESSING EIBMCHN2 (CHANNEL UPDATE)")
update_df = process_channel_update()

print("\n>>> CREATING OTC_DETAIL")
otc_detail = process_otc_detail()

# =============================================================================
# WRITE OUTPUTS
# =============================================================================
if len(channel_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_SUM.parquet"
    channel_df.write_parquet(output_path)
    print(f"\n✓ CHANNEL_SUM written: {output_path}")

if len(update_df) > 0:
    output_path = f"{CURRENT_MONTH_PATH}/CHANNEL_UPDATE.parquet"
    update_df.write_parquet(output_path)
    print(f"✓ CHANNEL_UPDATE written: {output_path}")

if len(otc_detail) > 0:
    print(f"\n✓ OTC_DETAIL DataFrame created: {len(otc_detail)} records")
    
    # Try SAS first
    sas_output_path = f"{CURRENT_MONTH_PATH}/otc_detail"
    sas_success = write_sas_dataset(otc_detail, sas_output_path, "OTC_DETAIL")
    
    if not sas_success:
        # Fallback to Parquet
        print(f"\nFalling back to Parquet format...")
        parquet_path = f"{CRMWH_PATH}/OTC_DETAIL_{REPTMON}{REPTYEAR}.parquet"
        otc_detail.write_parquet(parquet_path)
        print(f"✓ OTC_DETAIL written as Parquet: {parquet_path}")

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 60)
print("PROCESSING SUMMARY")
print("=" * 60)
print(f"Date: {REPTDATE:%Y-%m-%d}")
print(f"Output Month: {REPTDATE.year}-{REPTMON}")
print(f"Channel Summary: {len(channel_df)} records")
print(f"Channel Update: {len(update_df)} records")
print(f"OTC Detail: {len(otc_detail)} branches")

if len(otc_detail) > 0:
    print(f"\nOTC_DETAIL Statistics:")
    print(f"  Total TOLPROMPT: {otc_detail['TOLPROMPT'].sum()}")
    print(f"  Total TOLUPDATE: {otc_detail['TOLUPDATE'].sum()}")
    print(f"  Branches with data: {otc_detail.filter(pl.col('TOLPROMPT') > 0).height}")

print("=" * 60)
