import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

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
    if not date_str or date_str.strip() == '':
        return None
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            return datetime(int(parts[2]), int(parts[1]), int(parts[0]))
    except:
        return None
    return None

def process_pa_data(input_dir: Path, output_dir: Path, reptyear4: int):
    """Process Personal Accident (PA) product data"""
    # Check if input is parquet or text
    input_file = input_dir / "LONPAC_PA"
    if input_file.with_suffix('.parquet').exists():
        df = pl.read_parquet(input_file.with_suffix('.parquet'))
    else:
        df = pl.read_csv(
            input_file,
            separator='|',
            skip_rows=5,  # FIRSTOBS=6 in SAS
            has_header=True,
            truncate_ragged_lines=True,
            ignore_errors=True
        )
    
    df = df.with_columns([
        pl.col("ISSUEDTX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("ISSUEDT"),
        pl.col("EXPDT").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("EXPIRYDT"),
        pl.col("SUBMITDX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("SUBMITDT"),
        pl.col("PROPOSALDT").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("PROPOSAL_DT"),
        pl.col("DOBX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("DOB"),
    ])
    
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    df = df.with_columns([
        pl.lit("LONPAC_PA").alias("PRODUCT"),
        pl.col("ACCTNOX").str.slice(13, 5).alias("NOTENO"),
        pl.col("ACCTNOX").str.slice(0, 12).str.replace_all("-", "").alias("ACCTNO")
    ])
    
    prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "ACCTNO", "NOTENO", "INSURED", 
                 "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD", "AUTO_RENEWAL_IND"]
    
    cust_cols = ["NAME", "POLICYNO", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE", 
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    
    df_prod = df.select(prod_cols)
    df_cust = df.select(cust_cols).filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    df_cust = df_cust.with_columns([
        (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
        pl.col("NEWIC").str.replace_all("-", "").alias("IC")
    ])
    
    df_prod.write_parquet(output_dir / "lonpac_paprod.parquet")
    df_cust.write_parquet(output_dir / "lonpac_pacust.parquet")
    
    return df_prod, df_cust

def process_motor_data(input_dir: Path, output_dir: Path, reptyear4: int):
    """Process Motor product data"""
    input_file = input_dir / "LONPAC_MOTOR"
    if input_file.with_suffix('.parquet').exists():
        df = pl.read_parquet(input_file.with_suffix('.parquet'))
    else:
        df = pl.read_csv(
            input_file,
            separator='|',
            skip_rows=5,
            has_header=True,
            truncate_ragged_lines=True,
            ignore_errors=True
        )
    
    df = df.with_columns([
        pl.col("ISSUEDTX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("ISSUEDT"),
        pl.col("EXPDT").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("EXPIRYDT"),
        pl.col("SUBMITDX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("SUBMITDT"),
        pl.col("PROPOSALDT").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("PROPOSAL_DT"),
        pl.col("DOBX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("DOB"),
    ])
    
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    df = df.with_columns([
        pl.lit("LONPAC_MOTOR").alias("PRODUCT"),
        pl.col("CREGNO").str.replace_all(" ", "").alias("CREGNO")
    ])
    
    prod_cols = ["AGENTNO", "POLICYNO", "CREGNO", "BRANCH", "INSURED",
                 "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
    
    cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    
    df_prod = df.select(prod_cols)
    df_cust = df.select(cust_cols).filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    df_cust = df_cust.with_columns([
        (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
        pl.col("NEWIC").str.replace_all("-", "").alias("IC")
    ])
    
    df_prod.write_parquet(output_dir / "lonpac_motorprod.parquet")
    df_cust.write_parquet(output_dir / "lonpac_motorcust.parquet")
    
    return df_prod, df_cust

def process_misc_data(input_dir: Path, output_dir: Path, reptyear4: int):
    """Process Miscellaneous product data"""
    input_file = input_dir / "LONPAC_MISC"
    if input_file.with_suffix('.parquet').exists():
        df = pl.read_parquet(input_file.with_suffix('.parquet'))
    else:
        df = pl.read_csv(
            input_file,
            separator='|',
            skip_rows=5,
            has_header=True,
            truncate_ragged_lines=True,
            ignore_errors=True
        )
    
    df = df.with_columns([
        pl.col("ISSUEDTX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("ISSUEDT"),
        pl.col("EXPDT").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("EXPIRYDT"),
        pl.col("SUBMITDX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("SUBMITDT"),
        pl.col("PROPOSALDT").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("PROPOSAL_DT"),
        pl.col("DOBX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("DOB"),
    ])
    
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    df = df.with_columns(pl.lit("LONPAC_MOTOR").alias("PRODUCT"))
    
    prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "INSURED",
                 "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
    
    cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    
    df_prod = df.select(prod_cols)
    df_cust = df.select(cust_cols).filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    df_cust = df_cust.with_columns([
        (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
        pl.col("NEWIC").str.replace_all("-", "").alias("IC")
    ])
    
    return df_prod, df_cust

def process_fire_data(input_dir: Path, output_dir: Path, reptyear4: int):
    """Process Fire product data"""
    input_file = input_dir / "LONPAC_FIRE"
    if input_file.with_suffix('.parquet').exists():
        df = pl.read_parquet(input_file.with_suffix('.parquet'))
    else:
        df = pl.read_csv(
            input_file,
            separator='|',
            skip_rows=5,
            has_header=True,
            truncate_ragged_lines=True,
            ignore_errors=True
        )
    
    df = df.with_columns([
        pl.col("ISSUEDTX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("ISSUEDT"),
        pl.col("EXPDT").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("EXPIRYDT"),
        pl.col("SUBMITDX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("SUBMITDT"),
        pl.col("PROPOSALDT").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("PROPOSAL_DT"),
        pl.col("DOBX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("DOB"),
    ])
    
    df = df.filter(
        (pl.col("POLICYNO") != "F.ENDT.MAS......") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "") &
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "")
    )
    
    df = df.with_columns([
        pl.lit("LONPAC_FIRE").alias("PRODUCT"),
        pl.col("ACCTNOX").str.slice(13, 5).alias("NOTENO"),
        pl.col("ACCTNOX").str.slice(0, 12).str.replace_all("-", "").alias("ACCTNO")
    ])
    
    prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "ACCTNO", "NOTENO", "INSURED",
                 "ISSUEDT", "EXPIRYDT", "PRODUCT", "PREMIUM", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD",
                 "CCOLLNO", "AUTO_DEBIT_IND", "AUTO_RENEWAL_IND", "PROP_INS_ADDRESS", "TOT_STOREY"]
    
    cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    
    df_prod = df.select(prod_cols)
    df_cust = df.select(cust_cols).filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    df_cust = df_cust.with_columns([
        (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
        pl.col("NEWIC").str.replace_all("-", "").alias("IC")
    ])
    
    df_prod.write_parquet(output_dir / "lonpac_fireprod.parquet")
    df_cust.write_parquet(output_dir / "lonpac_firecust.parquet")
    
    return df_prod, df_cust

def process_hire_data(input_dir: Path, output_dir: Path, file_name: str, output_prefix: str, reptyear4: int):
    """Process Hire Purchase data (both HIRE and NHIRE)"""
    input_file = input_dir / file_name
    if input_file.with_suffix('.parquet').exists():
        df = pl.read_parquet(input_file.with_suffix('.parquet'))
    else:
        df = pl.read_csv(
            input_file,
            separator='|',
            skip_rows=8,  # FIRSTOBS=9 in SAS (different from other files!)
            has_header=True,
            truncate_ragged_lines=True,
            ignore_errors=True
        )
    
    df = df.with_columns([
        pl.col("ISSUEDTX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("ISSUEDT"),
        pl.col("EXPDT").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("EXPIRYDT"),
        pl.col("SUBMITDX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("SUBMITDT"),
        pl.col("PROPOSALDT").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("PROPOSAL_DT"),
        pl.col("DOBX").map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias("DOB"),
    ])
    
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    df = df.with_columns([
        pl.lit("LONPAC_HP").alias("PRODUCT"),
        pl.col("CREGNO").str.replace_all(" ", "").alias("CARREG"),
        pl.col("CREGNO").str.replace_all(" ", "").alias("CREGNO")
    ])
    
    prod_cols = ["AGENTNO", "POLICYNO", "CREGNO", "INSURED", "ISSUEDT", "EXPIRYDT",
                 "PREMIUM", "AGENTNO", "CARREG", "CREGNO", "PRODUCT", "PROD_CODE",
                 "PROD_DESC", "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
    
    cust_cols = ["NAME", "POLICYNO", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    
    df_prod = df.select(prod_cols)
    df_cust = df.select(cust_cols).filter(
        (pl.col("POLICYNO") != "F.ENDT.MAS......") &
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    df_cust = df_cust.with_columns([
        (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE"),
        pl.col("NEWIC").str.replace_all("-", "").alias("IC")
    ])
    
    df_prod.write_parquet(output_dir / f"lonpac_{output_prefix}prod.parquet")
    df_cust.write_parquet(output_dir / f"lonpac_{output_prefix}cust.parquet")
    
    return df_prod, df_cust

def merge_all_data(output_dir: Path):
    """Merge all customer and product data"""
    pacust = pl.read_parquet(output_dir / "lonpac_pacust.parquet")
    
    motorcust = pl.read_parquet(output_dir / "lonpac_motorcust.parquet")
    misccust_path = output_dir / "lonpac_misccust.parquet"
    if misccust_path.exists():
        misccust = pl.read_parquet(misccust_path)
        motorcust = pl.concat([motorcust, misccust])
    
    firecust = pl.read_parquet(output_dir / "lonpac_firecust.parquet")
    hirecust = pl.read_parquet(output_dir / "lonpac_hirecust.parquet")
    nonhirecust = pl.read_parquet(output_dir / "lonpac_nonhirecust.parquet")
    
    all_cust = pl.concat([pacust, motorcust, firecust, hirecust, nonhirecust], how="diagonal")
    all_cust = all_cust.filter(
        (pl.col("POLICYNO").is_not_null()) & 
        (pl.col("POLICYNO") != "") &
        (pl.col("POLICYNO") != "Policy issued by")
    ).unique(subset=["POLICYNO"])
    
    all_cust.write_parquet(output_dir / "lonpac_cust.parquet")
    
    paprod = pl.read_parquet(output_dir / "lonpac_paprod.parquet")
    motorprod = pl.read_parquet(output_dir / "lonpac_motorprod.parquet")
    miscprod_path = output_dir / "lonpac_miscprod.parquet"
    if miscprod_path.exists():
        miscprod = pl.read_parquet(miscprod_path)
        motorprod = pl.concat([motorprod, miscprod])
    
    fireprod = pl.read_parquet(output_dir / "lonpac_fireprod.parquet")
    
    # SAS only merges PA, MOTOR, and FIRE into PROD (NOT hire/nonhire)
    all_prod = pl.concat([paprod, motorprod, fireprod], how="diagonal")
    all_prod = all_prod.filter(
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    ).unique(subset=["POLICYNO"])
    
    all_prod.write_parquet(output_dir / "lonpac_prod.parquet")
    
    return all_cust, all_prod

def process_lonpac_data(input_dir: str, output_dir: str):
    """Main function to process all LONPAC data"""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Calculate report dates
    dates = calculate_report_dates()
    reptyear4 = dates['reptyear4']
    
    print("Starting LONPAC data processing...")
    
    print("Processing PA data...")
    process_pa_data(input_path, output_path, reptyear4)
    
    print("Processing Motor data...")
    process_motor_data(input_path, output_path, reptyear4)
    
    print("Processing Misc data...")
    misc_prod, misc_cust = process_misc_data(input_path, output_path, reptyear4)
    misc_prod.write_parquet(output_path / "lonpac_miscprod.parquet")
    misc_cust.write_parquet(output_path / "lonpac_misccust.parquet")
    
    print("Processing Fire data...")
    process_fire_data(input_path, output_path, reptyear4)
    
    print("Processing Hire Purchase data...")
    process_hire_data(input_path, output_path, "LONPAC_HIRE", "hire", reptyear4)
    
    print("Processing Non-Hire Purchase data...")
    process_hire_data(input_path, output_path, "LONPAC_NONHIRE", "nonhire", reptyear4)
    
    print("Merging all datasets...")
    merge_all_data(output_path)
    
    print("Processing complete!")

# Usage
if __name__ == "__main__":
    process_lonpac_data(
        input_dir="/sasdata/rawdata/lonpac/",
        output_dir="/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/output"
    )
