import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import pyreadstat  # For writing SAS .sas7bdat files

def calculate_report_dates():
    """Calculate report dates based on current date and week logic"""
    today = datetime.now()
    day = today.day
    
    if 8 <= day <= 14:
        reptdate = datetime(today.year, today.month, 8)
        wk = '1'
        if today.month == 1:
            mm1 = 12
            yy1 = today.year - 1
        else:
            mm1 = today.month - 1
            yy1 = today.year
    elif 15 <= day <= 21:
        reptdate = datetime(today.year, today.month, 15)
        wk = '2'
        mm1 = today.month
        yy1 = today.year
    elif 22 <= day <= 27:
        reptdate = datetime(today.year, today.month, 22)
        wk = '3'
        mm1 = today.month
        yy1 = today.year
    else:
        reptdate = datetime(today.year, today.month, 1) - timedelta(days=1)
        wk = '4'
        mm1 = reptdate.month
        yy1 = reptdate.year
    
    return {
        'reptdate': reptdate,
        'wk': wk,
        'reptyear4': reptdate.year,
        'reptmon': str(reptdate.month).zfill(2),
        'mm1': mm1,
        'yy1': yy1
    }

def parse_date(date_str: str) -> datetime:
    """Parse date string in DD/MM/YYYY format"""
    if not date_str or date_str.strip() == '' or date_str.strip().upper() == 'NULL':
        return None
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            return datetime(int(parts[2]), int(parts[1]), int(parts[0]))
    except:
        return None
    return None

def write_sas_dataset(df: pl.DataFrame, file_path: Path):
    """Write Polars DataFrame to SAS .sas7bdat format"""
    # Convert datetime columns to pandas datetime
    pandas_df = df.to_pandas()
    
    # Handle datetime columns
    for col in pandas_df.columns:
        if pandas_df[col].dtype == 'datetime64[ns]':
            # Convert to pandas datetime with proper format
            pandas_df[col] = pd.to_datetime(pandas_df[col])
    
    # Write to SAS .sas7bdat
    pyreadstat.write_sas7bdat(
        pandas_df,
        str(file_path),
        compression=None
    )

def read_txt_file(file_path: Path, expected_columns: list, skip_rows: int = 5) -> pl.DataFrame:
    """Helper function to read TXT file with pipe delimiter"""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Try to read with proper column names
    try:
        df = pl.read_csv(
            file_path,
            separator='|',
            has_header=False,
            skip_rows=skip_rows,
            truncate_ragged_lines=True,
            ignore_errors=True,
            infer_schema_length=0,
            encoding='utf8'
        )
        
        # Rename columns based on position
        actual_cols = df.columns
        rename_map = {actual_cols[i]: expected_columns[i] for i in range(min(len(actual_cols), len(expected_columns)))}
        df = df.rename(rename_map)
        
        # Add missing columns
        for col in expected_columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        # Return empty DataFrame with expected columns
        return pl.DataFrame({col: [] for col in expected_columns})

def process_pa_data(input_dir: Path, output_dir: Path, reptyear4: int):
    """Process Personal Accident (PA) product data"""
    pa_columns = [
        "POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "DOBX", "GENDER", 
        "AGENTNO", "BRANCH", "ACCTNOX", "RACE", "MARITAL", "INSURED", 
        "ISSUEDTX", "EXPDT", "PREMIUM", "TELNO", "ADDRESS", "TOWN", 
        "POSTCODE", "PROD_CODE", "PROD_DESC", "PROCESS_MTH", "SUBMITDX", 
        "PROPOSALDT", "POLICYNO_OLD", "REGNO_NEW", "AUTO_RENEWAL_IND"
    ]
    
    input_file = input_dir / "LONPAC_PA.txt"
    df = read_txt_file(input_file, pa_columns, skip_rows=5)
    
    # Parse dates
    for date_col in ["ISSUEDTX", "EXPDT", "SUBMITDX", "PROPOSALDT", "DOBX"]:
        if date_col in df.columns:
            df = df.with_columns(
                pl.col(date_col).map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias(date_col.replace('X', ''))
            )
    
    # Filter records
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    # Add derived columns
    df = df.with_columns([
        pl.lit("LONPAC_PA").alias("PRODUCT"),
        pl.col("ACCTNOX").str.slice(13, 5).alias("NOTENO"),
        pl.col("ACCTNOX").str.slice(0, 12).str.replace_all("-", "").alias("ACCTNO")
    ])
    
    # Product dataset
    prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "ACCTNO", "NOTENO", "INSURED", 
                 "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD", "AUTO_RENEWAL_IND"]
    df_prod = df.select(prod_cols)
    
    # Customer dataset
    cust_cols = ["NAME", "POLICYNO", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE", 
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    df_cust = df.select(cust_cols).filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    df_cust = df_cust.with_columns([
        (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
        pl.col("NEWIC").str.replace_all("-", "").alias("IC")
    ])
    
    # Write to SAS .sas7bdat
    write_sas_dataset(df_prod, output_dir / "paprod.sas7bdat")
    write_sas_dataset(df_cust, output_dir / "pacust.sas7bdat")
    
    return df_prod, df_cust

def process_motor_data(input_dir: Path, output_dir: Path, reptyear4: int):
    """Process Motor product data"""
    motor_columns = [
        "POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "CREGNO", "DOBX", 
        "GENDER", "AGENTNO", "BRANCH", "RACE", "MARITAL", "INSURED", 
        "ISSUEDTX", "EXPDT", "PREMIUM", "TELNO", "ADDRESS", "TOWN", 
        "POSTCODE", "PROD_CODE", "PROD_DESC", "PROCESS_MTH", "SUBMITDX", 
        "PROPOSALDT", "POLICYNO_OLD", "REGNO_NEW"
    ]
    
    input_file = input_dir / "LONPAC_MOTOR.txt"
    df = read_txt_file(input_file, motor_columns, skip_rows=5)
    
    # Parse dates
    for date_col in ["ISSUEDTX", "EXPDT", "SUBMITDX", "PROPOSALDT", "DOBX"]:
        if date_col in df.columns:
            df = df.with_columns(
                pl.col(date_col).map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias(date_col.replace('X', ''))
            )
    
    # Filter records
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    # Add derived columns
    df = df.with_columns([
        pl.lit("LONPAC_MOTOR").alias("PRODUCT"),
        pl.col("CREGNO").str.replace_all(" ", "").alias("CREGNO")
    ])
    
    # Product dataset
    prod_cols = ["AGENTNO", "POLICYNO", "CREGNO", "BRANCH", "INSURED",
                 "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
    df_prod = df.select(prod_cols)
    
    # Customer dataset
    cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    df_cust = df.select(cust_cols).filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    df_cust = df_cust.with_columns([
        (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
        pl.col("NEWIC").str.replace_all("-", "").alias("IC")
    ])
    
    # Write to SAS .sas7bdat
    write_sas_dataset(df_prod, output_dir / "motorprod.sas7bdat")
    write_sas_dataset(df_cust, output_dir / "motorcust.sas7bdat")
    
    return df_prod, df_cust

def process_misc_data(input_dir: Path, output_dir: Path, reptyear4: int):
    """Process Miscellaneous product data"""
    misc_columns = [
        "POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "DOBX", "GENDER", 
        "AGENTNO", "BRANCH", "RACE", "INSURED", "ISSUEDTX", "EXPDT", 
        "PREMIUM", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "PROD_CODE", 
        "PROD_DESC", "PROCESS_MTH", "SUBMITDX", "PROPOSALDT", 
        "POLICYNO_OLD", "REGNO_NEW"
    ]
    
    input_file = input_dir / "LONPAC_MISC.txt"
    df = read_txt_file(input_file, misc_columns, skip_rows=5)
    
    # Parse dates
    for date_col in ["ISSUEDTX", "EXPDT", "SUBMITDX", "PROPOSALDT", "DOBX"]:
        if date_col in df.columns:
            df = df.with_columns(
                pl.col(date_col).map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias(date_col.replace('X', ''))
            )
    
    # Filter records
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    # Add derived columns
    df = df.with_columns(pl.lit("LONPAC_MOTOR").alias("PRODUCT"))
    
    # Product dataset
    prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "INSURED",
                 "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
    df_prod = df.select(prod_cols)
    
    # Customer dataset
    cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    df_cust = df.select(cust_cols).filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    df_cust = df_cust.with_columns([
        (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
        pl.col("NEWIC").str.replace_all("-", "").alias("IC")
    ])
    
    # Write to SAS .sas7bdat
    write_sas_dataset(df_prod, output_dir / "miscprod.sas7bdat")
    write_sas_dataset(df_cust, output_dir / "misccust.sas7bdat")
    
    return df_prod, df_cust

def process_fire_data(input_dir: Path, output_dir: Path, reptyear4: int):
    """Process Fire product data"""
    fire_columns = [
        "POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "DOBX", "GENDER", 
        "AGENTNO", "BRANCH", "ACCTNOX", "RACE", "MARITAL", "INSURED", 
        "ISSUEDTX", "EXPDT", "PREMIUM", "TELNO", "ADDRESS", "TOWN", 
        "POSTCODE", "PROD_CODE", "PROD_DESC", "PROCESS_MTH", "SUBMITDX", 
        "PROPOSALDT", "POLICYNO_OLD", "REGNO_NEW", "CCOLLNO", 
        "AUTO_DEBIT_IND", "AUTO_RENEWAL_IND", "PROP_INS_ADDRESS", "TOT_STOREY"
    ]
    
    input_file = input_dir / "LONPAC_FIRE.txt"
    df = read_txt_file(input_file, fire_columns, skip_rows=5)
    
    # Parse dates
    for date_col in ["ISSUEDTX", "EXPDT", "SUBMITDX", "PROPOSALDT", "DOBX"]:
        if date_col in df.columns:
            df = df.with_columns(
                pl.col(date_col).map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias(date_col.replace('X', ''))
            )
    
    # Filter records
    df = df.filter(
        (pl.col("POLICYNO") != "F.ENDT.MAS......") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "") &
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "")
    )
    
    # Add derived columns
    df = df.with_columns([
        pl.lit("LONPAC_FIRE").alias("PRODUCT"),
        pl.col("ACCTNOX").str.slice(13, 5).alias("NOTENO"),
        pl.col("ACCTNOX").str.slice(0, 12).str.replace_all("-", "").alias("ACCTNO")
    ])
    
    # Product dataset
    prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "ACCTNO", "NOTENO", "INSURED",
                 "ISSUEDT", "EXPIRYDT", "PRODUCT", "PREMIUM", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD",
                 "CCOLLNO", "AUTO_DEBIT_IND", "AUTO_RENEWAL_IND", "PROP_INS_ADDRESS", "TOT_STOREY"]
    df_prod = df.select(prod_cols)
    
    # Customer dataset
    cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    df_cust = df.select(cust_cols).filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    df_cust = df_cust.with_columns([
        (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
        pl.col("NEWIC").str.replace_all("-", "").alias("IC")
    ])
    
    # Write to SAS .sas7bdat
    write_sas_dataset(df_prod, output_dir / "fireprod.sas7bdat")
    write_sas_dataset(df_cust, output_dir / "firecust.sas7bdat")
    
    return df_prod, df_cust

def process_hire_data(input_dir: Path, output_dir: Path, file_name: str, output_prefix: str, reptyear4: int):
    """Process Hire Purchase data (both HIRE and NHIRE)"""
    hire_columns = [
        "POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "CREGNO", "DOBX", 
        "GENDER", "AGENTNO", "RACE", "MARITAL", "INSURED", "ISSUEDTX", 
        "EXPDT", "PREMIUM", "TELNO", "ADDRESS", "TOWN", "POSTCODE", 
        "PROD_CODE", "PROD_DESC", "PROCESS_MTH", "SUBMITDX", "PROPOSALDT", 
        "POLICYNO_OLD", "REGNO_NEW"
    ]
    
    input_file = input_dir / f"{file_name}.txt"
    df = read_txt_file(input_file, hire_columns, skip_rows=8)  # FIRSTOBS=9 in SAS
    
    # Parse dates
    for date_col in ["ISSUEDTX", "EXPDT", "SUBMITDX", "PROPOSALDT", "DOBX"]:
        if date_col in df.columns:
            df = df.with_columns(
                pl.col(date_col).map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias(date_col.replace('X', ''))
            )
    
    # Filter records
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    # Add derived columns
    df = df.with_columns([
        pl.lit("LONPAC_HP").alias("PRODUCT"),
        pl.col("CREGNO").str.replace_all(" ", "").alias("CARREG"),
        pl.col("CREGNO").str.replace_all(" ", "").alias("CREGNO")
    ])
    
    # Product dataset
    prod_cols = ["AGENTNO", "POLICYNO", "CREGNO", "INSURED", "ISSUEDT", "EXPIRYDT",
                 "PREMIUM", "CARREG", "PRODUCT", "PROD_CODE",
                 "PROD_DESC", "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
    df_prod = df.select(prod_cols)
    
    # Customer dataset
    cust_cols = ["NAME", "POLICYNO", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    df_cust = df.select(cust_cols).filter(
        (pl.col("POLICYNO") != "F.ENDT.MAS......") &
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    df_cust = df_cust.with_columns([
        (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
        pl.col("NEWIC").str.replace_all("-", "").alias("IC")
    ])
    
    # Write to SAS .sas7bdat
    write_sas_dataset(df_prod, output_dir / f"{output_prefix}prod.sas7bdat")
    write_sas_dataset(df_cust, output_dir / f"{output_prefix}cust.sas7bdat")
    
    return df_prod, df_cust

def merge_all_data(output_dir: Path):
    """Merge all customer and product data"""
    # Read all customer datasets
    pacust = pl.read_parquet(output_dir / "pacust.parquet") if (output_dir / "pacust.parquet").exists() else None
    motorcust = pl.read_parquet(output_dir / "motorcust.parquet") if (output_dir / "motorcust.parquet").exists() else None
    firecust = pl.read_parquet(output_dir / "firecust.parquet") if (output_dir / "firecust.parquet").exists() else None
    hirecust = pl.read_parquet(output_dir / "hirecust.parquet") if (output_dir / "hirecust.parquet").exists() else None
    nonhirecust = pl.read_parquet(output_dir / "nonhirecust.parquet") if (output_dir / "nonhirecust.parquet").exists() else None
    
    # Concatenate all customer datasets
    all_cust_dfs = [df for df in [pacust, motorcust, firecust, hirecust, nonhirecust] if df is not None]
    if all_cust_dfs:
        all_cust = pl.concat(all_cust_dfs, how="diagonal")
        all_cust = all_cust.filter(
            (pl.col("POLICYNO").is_not_null()) & 
            (pl.col("POLICYNO") != "") &
            (pl.col("POLICYNO") != "Policy issued by")
        ).unique(subset=["POLICYNO"])
        
        # Write merged customer dataset
        write_sas_dataset(all_cust, output_dir / "cust.sas7bdat")
    
    # Read all product datasets
    paprod = pl.read_parquet(output_dir / "paprod.parquet") if (output_dir / "paprod.parquet").exists() else None
    motorprod = pl.read_parquet(output_dir / "motorprod.parquet") if (output_dir / "motorprod.parquet").exists() else None
    fireprod = pl.read_parquet(output_dir / "fireprod.parquet") if (output_dir / "fireprod.parquet").exists() else None
    
    # Concatenate product datasets (only PA, MOTOR, and FIRE as per SAS)
    all_prod_dfs = [df for df in [paprod, motorprod, fireprod] if df is not None]
    if all_prod_dfs:
        all_prod = pl.concat(all_prod_dfs, how="diagonal")
        all_prod = all_prod.filter(
            (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
        ).unique(subset=["POLICYNO"])
        
        # Write merged product dataset
        write_sas_dataset(all_prod, output_dir / "prod.sas7bdat")

def process_lonpac_data(input_dir: str, output_dir: str):
    """Main function to process all LONPAC data"""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Calculate report dates
    dates = calculate_report_dates()
    reptyear4 = dates['reptyear4']
    
    print("Starting LONPAC data processing...")
    print(f"Reading from: {input_path}")
    print(f"Writing to: {output_path}")
    
    try:
        print("Processing PA data...")
        process_pa_data(input_path, output_path, reptyear4)
        
        print("Processing Motor data...")
        process_motor_data(input_path, output_path, reptyear4)
        
        print("Processing Misc data...")
        process_misc_data(input_path, output_path, reptyear4)
        
        print("Processing Fire data...")
        process_fire_data(input_path, output_path, reptyear4)
        
        print("Processing Hire Purchase data...")
        process_hire_data(input_path, output_path, "LONPAC_HIRE", "hire", reptyear4)
        
        print("Processing Non-Hire Purchase data...")
        process_hire_data(input_path, output_path, "LONPAC_NONHIRE", "nonhire", reptyear4)
        
        print("Merging all datasets...")
        merge_all_data(output_path)
        
        print("Processing complete!")
        print(f"Output files are in SAS .sas7bdat format at: {output_path}")
        
    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()

# Usage
if __name__ == "__main__":
    process_lonpac_data(
        input_dir="/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLNPC",
        output_dir="/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMLNPC"
    )
