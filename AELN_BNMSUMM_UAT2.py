import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

BASE_INPUT_PATH = Path("/host/cis/parquet/")  # Folder for source files
BASE_OUTPUT_PATH = Path("/pythonITD/mis_dev/sas_migration/CIGR/output")  # Folder for output files
BASE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Helper Functions
def get_report_date_info():
    today = datetime.today()
    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    
    reptmon = f"{last_day_prev_month.month:02d}"
    reptyear = str(last_day_prev_month.year)[-2:]
    
    return last_day_prev_month, reptmon, reptyear

def save_outputs(df, name):
    """Save DataFrame to both Parquet and CSV formats"""
    df.write_parquet(BASE_OUTPUT_PATH / f"{name}.parquet")
    df.write_csv(BASE_OUTPUT_PATH / f"{name}.csv")

# MAIN SCRIPT
if __name__ == "__main__":
    reptdate, reptmon, reptyear = get_report_date_info()
    
    print(f"Processing for report date: {reptdate}, month: {reptmon}, year: {reptyear}")
    
    # Construct file paths with dynamic date
    # Assuming your Parquet files are partitioned by date
    year = reptdate.year
    month = reptdate.month
    day = reptdate.day
    
    # File paths - adjust the pattern based on your actual file structure
    CIS_FILE = BASE_INPUT_PATH / f"year={year}/month={month:02d}/day={day:02d}/CUST_GROUPING_OUT.parquet"
    RCIS_FILE = BASE_INPUT_PATH / f"year={year}/month={month:02d}/day={day:02d}/CUST_GROUPING_RPTCC.parquet"
    
    print(f"CIS File: {CIS_FILE}")
    print(f"RCIS File: {RCIS_FILE}")
    
    # Check if files exist
    if not CIS_FILE.exists():
        print(f"Warning: CIS file not found at {CIS_FILE}")
        # Try alternative path patterns if needed
        CIS_FILE = BASE_INPUT_PATH / f"CUST_GROUPING_OUT.parquet"
    
    if not RCIS_FILE.exists():
        print(f"Warning: RCIS file not found at {RCIS_FILE}")
        # Try alternative path patterns if needed
        RCIS_FILE = BASE_INPUT_PATH / f"CUST_GROUPING_RPTCC.parquet"
    
    try:
        # 1. Read CISBASEL data from Parquet
        print("Reading CISBASEL data...")
        cisbasel_df = pl.read_parquet(CIS_FILE)
        
        # Map columns to match SAS variable names if needed
        # If your Parquet columns have different names, rename them here
        # Example: cisbasel_df = cisbasel_df.rename({"old_name": "GRPING", ...})
        
        # Select and reorder columns to match SAS output structure
        # Adjust based on your actual Parquet schema
        cisbasel_columns = [
            "GRPING", "GROUPNO", "CUSTNO", "FULLNAME", "ACCTNO", 
            "NOTENO", "PRODUCT", "AMTINDC", "BALAMT", "TOTAMT", 
            "RLENCODE", "PRIMSEC"
        ]
        
        # Filter to only include columns that exist in the DataFrame
        available_cols = [col for col in cisbasel_columns if col in cisbasel_df.columns]
        cisbasel_df = cisbasel_df.select(available_cols)
        
        # Ensure correct data types
        cisbasel_df = cisbasel_df.with_columns([
            pl.col("ACCTNO").cast(pl.Int64),
            pl.col("NOTENO").cast(pl.Int64),
            pl.col("BALAMT").cast(pl.Float64),
            pl.col("TOTAMT").cast(pl.Float64),
        ])
        
        save_outputs(cisbasel_df, f"LOAN_CISBASEL{reptmon}{reptyear}")
        print(f"CISBASEL shape: {cisbasel_df.shape}")
        
        # 2. Read CISRPTCC data from Parquet
        print("Reading CISRPTCC data...")
        cisrptcc_df = pl.read_parquet(RCIS_FILE)
        
        # Map columns if needed
        cisrptcc_columns = [
            "GRPING", "C1CUST", "C1TYPE", "C1CODE", "C1DESC",
            "C2CUST", "C2TYPE", "C2CODE", "C2DESC"
        ]
        
        # Filter to only include columns that exist
        available_rptcc_cols = [col for col in cisrptcc_columns if col in cisrptcc_df.columns]
        cisrptcc_df = cisrptcc_df.select(available_rptcc_cols)
        
        # Ensure correct data types
        cisrptcc_df = cisrptcc_df.with_columns([
            pl.col("GRPING").cast(pl.Int64),
        ])
        
        save_outputs(cisrptcc_df, f"LOAN_CISRPTCC{reptmon}{reptyear}")
        print(f"CISRPTCC shape: {cisrptcc_df.shape}")
        
        # Display sample data
        print("\nCISBASEL Sample:")
        print(cisbasel_df.head())
        print("\nCISRPTCC Sample:")
        print(cisrptcc_df.head())
        
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
