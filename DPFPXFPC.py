import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import saspy

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
        date_str = date_str.strip()
        # Handle multiple date formats
        for sep in ['/', '-', '.']:
            if sep in date_str:
                parts = date_str.split(sep)
                if len(parts) == 3:
                    # Try to determine format
                    if len(parts[0]) == 4:  # YYYY/MM/DD
                        return datetime(int(parts[0]), int(parts[1]), int(parts[2]))
                    else:  # DD/MM/YYYY
                        return datetime(int(parts[2]), int(parts[1]), int(parts[0]))
        return None
    except:
        return None

def read_txt_file_robust(file_path: Path, expected_columns: list, skip_rows: int = 5) -> pl.DataFrame:
    """Robust helper function to read TXT file with pipe delimiter"""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    try:
        # Read file line by line to handle malformed data better
        lines = []
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            # Skip header rows
            for _ in range(skip_rows):
                next(f, None)
            
            # Read the rest of the lines
            for line in f:
                line = line.strip()
                if line:  # Skip empty lines
                    # Handle quoted fields with pipe delimiter
                    parts = []
                    current = []
                    in_quotes = False
                    for char in line:
                        if char == '"' and not in_quotes:
                            in_quotes = True
                            current.append(char)
                        elif char == '"' and in_quotes:
                            in_quotes = False
                            current.append(char)
                        elif char == '|' and not in_quotes:
                            parts.append(''.join(current))
                            current = []
                        else:
                            current.append(char)
                    parts.append(''.join(current))
                    lines.append(parts)
        
        # Convert to polars DataFrame
        if not lines:
            return pl.DataFrame({col: [] for col in expected_columns})
        
        # Determine max columns
        max_cols = max(len(line) for line in lines)
        
        # Pad lines to max columns
        padded_lines = [line + [''] * (max_cols - len(line)) for line in lines]
        
        # Create DataFrame
        df = pl.DataFrame(padded_lines)
        
        # Rename columns based on position
        actual_cols = df.columns
        rename_map = {actual_cols[i]: expected_columns[i] for i in range(min(len(actual_cols), len(expected_columns)))}
        df = df.rename(rename_map)
        
        # Add missing columns
        for col in expected_columns:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        
        # Remove any extra columns
        df = df.select(expected_columns)
        
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pl.DataFrame({col: [] for col in expected_columns})

def write_sas_dataset_saspy(df: pl.DataFrame, libref: str, table_name: str, sas_session):
    """Write Polars DataFrame to SAS .sas7bdat using saspy"""
    if df.is_empty():
        print(f"Warning: DataFrame for {table_name} is empty, skipping...")
        return
    
    # Convert to pandas
    pandas_df = df.to_pandas()
    
    # Convert datetime columns to string for SAS compatibility
    for col in pandas_df.columns:
        if pd.api.types.is_datetime64_any_dtype(pandas_df[col]):
            pandas_df[col] = pandas_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        elif pandas_df[col].dtype == 'object':
            # Convert strings to proper format
            pandas_df[col] = pandas_df[col].astype(str)
    
    # Write to SAS using df2sd
    try:
        sas_session.df2sd(pandas_df, table=table_name, libref=libref)
        print(f"Successfully wrote {table_name} to {libref}")
    except Exception as e:
        print(f"Error writing {table_name}: {e}")
        # Try alternative method using SAS code
        try:
            # Use SAS code to create dataset
            sas_code = f"""
            data {libref}.{table_name};
            set work.{table_name};
            run;
            """
            sas_session.submit(sas_code)
            print(f"Successfully wrote {table_name} to {libref} using SAS code")
        except Exception as e2:
            print(f"Alternative method also failed: {e2}")

def safe_str_replace(df: pl.DataFrame, col_name: str, old: str, new: str) -> pl.DataFrame:
    """Safely replace string in a column if it exists"""
    if col_name in df.columns:
        return df.with_columns(
            pl.col(col_name).str.replace_all(old, new).alias(col_name)
        )
    return df

def safe_str_slice(df: pl.DataFrame, col_name: str, start: int, length: int) -> pl.DataFrame:
    """Safely slice string in a column if it exists"""
    if col_name in df.columns:
        return df.with_columns(
            pl.col(col_name).str.slice(start, length).alias(col_name)
        )
    return df

def process_pa_data(input_dir: Path, output_dir: Path, reptyear4: int, sas_session, libref: str):
    """Process Personal Accident (PA) product data"""
    print("  Reading PA data...")
    pa_columns = [
        "POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "DOBX", "GENDER", 
        "AGENTNO", "BRANCH", "ACCTNOX", "RACE", "MARITAL", "INSURED", 
        "ISSUEDTX", "EXPDT", "PREMIUM", "TELNO", "ADDRESS", "TOWN", 
        "POSTCODE", "PROD_CODE", "PROD_DESC", "PROCESS_MTH", "SUBMITDX", 
        "PROPOSALDT", "POLICYNO_OLD", "REGNO_NEW", "AUTO_RENEWAL_IND"
    ]
    
    input_file = input_dir / "LONPAC_PA.txt"
    df = read_txt_file_robust(input_file, pa_columns, skip_rows=5)
    
    print(f"  PA data: {df.height} rows read")
    
    if df.height == 0:
        print("  No data to process")
        return
    
    # Parse dates
    for col in ["ISSUEDTX", "EXPDT", "SUBMITDX", "PROPOSALDT", "DOBX"]:
        if col in df.columns:
            new_col = col.replace('X', '') if col.endswith('X') else col + '_DT'
            df = df.with_columns(
                pl.col(col).map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias(new_col)
            )
    
    # Filter records
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    # Add derived columns
    df = df.with_columns(pl.lit("LONPAC_PA").alias("PRODUCT"))
    
    if "ACCTNOX" in df.columns:
        df = df.with_columns([
            pl.col("ACCTNOX").str.slice(13, 5).alias("NOTENO"),
            pl.col("ACCTNOX").str.slice(0, 12).str.replace_all("-", "").alias("ACCTNO")
        ])
    
    # Product dataset
    prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "ACCTNO", "NOTENO", "INSURED", 
                 "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD", "AUTO_RENEWAL_IND"]
    
    available_cols = [col for col in prod_cols if col in df.columns]
    df_prod = df.select(available_cols)
    
    # Customer dataset
    cust_cols = ["NAME", "POLICYNO", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE", 
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    
    df_cust = df.select([col for col in cust_cols if col in df.columns])
    df_cust = df_cust.filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    if "DOB" in df_cust.columns:
        df_cust = df_cust.with_columns(
            (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE")
        )
    
    if "NEWIC" in df_cust.columns:
        df_cust = df_cust.with_columns(
            pl.col("NEWIC").str.replace_all("-", "").alias("IC")
        )
    
    # Write to SAS
    print("  Writing PA product data...")
    write_sas_dataset_saspy(df_prod, libref, "PAPROD", sas_session)
    print("  Writing PA customer data...")
    write_sas_dataset_saspy(df_cust, libref, "PACUST", sas_session)
    
    return df_prod, df_cust

def process_motor_data(input_dir: Path, output_dir: Path, reptyear4: int, sas_session, libref: str):
    """Process Motor product data"""
    print("  Reading Motor data...")
    motor_columns = [
        "POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "CREGNO", "DOBX", 
        "GENDER", "AGENTNO", "BRANCH", "RACE", "MARITAL", "INSURED", 
        "ISSUEDTX", "EXPDT", "PREMIUM", "TELNO", "ADDRESS", "TOWN", 
        "POSTCODE", "PROD_CODE", "PROD_DESC", "PROCESS_MTH", "SUBMITDX", 
        "PROPOSALDT", "POLICYNO_OLD", "REGNO_NEW"
    ]
    
    input_file = input_dir / "LONPAC_MOTOR.txt"
    df = read_txt_file_robust(input_file, motor_columns, skip_rows=5)
    print(f"  Motor data: {df.height} rows read")
    
    if df.height == 0:
        print("  No data to process")
        return
    
    # Parse dates
    for col in ["ISSUEDTX", "EXPDT", "SUBMITDX", "PROPOSALDT", "DOBX"]:
        if col in df.columns:
            new_col = col.replace('X', '') if col.endswith('X') else col + '_DT'
            df = df.with_columns(
                pl.col(col).map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias(new_col)
            )
    
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    df = df.with_columns(pl.lit("LONPAC_MOTOR").alias("PRODUCT"))
    
    if "CREGNO" in df.columns:
        df = df.with_columns(
            pl.col("CREGNO").str.replace_all(" ", "").alias("CREGNO")
        )
    
    prod_cols = ["AGENTNO", "POLICYNO", "CREGNO", "BRANCH", "INSURED",
                 "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
    
    available_cols = [col for col in prod_cols if col in df.columns]
    df_prod = df.select(available_cols)
    
    cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    
    df_cust = df.select([col for col in cust_cols if col in df.columns])
    df_cust = df_cust.filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    if "DOB" in df_cust.columns:
        df_cust = df_cust.with_columns(
            (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE")
        )
    
    if "NEWIC" in df_cust.columns:
        df_cust = df_cust.with_columns(
            pl.col("NEWIC").str.replace_all("-", "").alias("IC")
        )
    
    print("  Writing Motor product data...")
    write_sas_dataset_saspy(df_prod, libref, "MOTORPROD", sas_session)
    print("  Writing Motor customer data...")
    write_sas_dataset_saspy(df_cust, libref, "MOTORCUST", sas_session)
    
    return df_prod, df_cust

def process_misc_data(input_dir: Path, output_dir: Path, reptyear4: int, sas_session, libref: str):
    """Process Miscellaneous product data"""
    print("  Reading Misc data...")
    misc_columns = [
        "POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "DOBX", "GENDER", 
        "AGENTNO", "BRANCH", "RACE", "INSURED", "ISSUEDTX", "EXPDT", 
        "PREMIUM", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "PROD_CODE", 
        "PROD_DESC", "PROCESS_MTH", "SUBMITDX", "PROPOSALDT", 
        "POLICYNO_OLD", "REGNO_NEW"
    ]
    
    input_file = input_dir / "LONPAC_MISC.txt"
    df = read_txt_file_robust(input_file, misc_columns, skip_rows=5)
    print(f"  Misc data: {df.height} rows read")
    
    if df.height == 0:
        print("  No data to process")
        return
    
    # Parse dates
    for col in ["ISSUEDTX", "EXPDT", "SUBMITDX", "PROPOSALDT", "DOBX"]:
        if col in df.columns:
            new_col = col.replace('X', '') if col.endswith('X') else col + '_DT'
            df = df.with_columns(
                pl.col(col).map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias(new_col)
            )
    
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    df = df.with_columns(pl.lit("LONPAC_MOTOR").alias("PRODUCT"))
    
    prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "INSURED",
                 "ISSUEDT", "EXPIRYDT", "PREMIUM", "PRODUCT", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
    
    available_cols = [col for col in prod_cols if col in df.columns]
    df_prod = df.select(available_cols)
    
    cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    
    df_cust = df.select([col for col in cust_cols if col in df.columns])
    df_cust = df_cust.filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    if "DOB" in df_cust.columns:
        df_cust = df_cust.with_columns(
            (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE")
        )
    
    if "NEWIC" in df_cust.columns:
        df_cust = df_cust.with_columns(
            pl.col("NEWIC").str.replace_all("-", "").alias("IC")
        )
    
    print("  Writing Misc product data...")
    write_sas_dataset_saspy(df_prod, libref, "MISCPROD", sas_session)
    print("  Writing Misc customer data...")
    write_sas_dataset_saspy(df_cust, libref, "MISCCUST", sas_session)
    
    return df_prod, df_cust

def process_fire_data(input_dir: Path, output_dir: Path, reptyear4: int, sas_session, libref: str):
    """Process Fire product data"""
    print("  Reading Fire data...")
    fire_columns = [
        "POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "DOBX", "GENDER", 
        "AGENTNO", "BRANCH", "ACCTNOX", "RACE", "MARITAL", "INSURED", 
        "ISSUEDTX", "EXPDT", "PREMIUM", "TELNO", "ADDRESS", "TOWN", 
        "POSTCODE", "PROD_CODE", "PROD_DESC", "PROCESS_MTH", "SUBMITDX", 
        "PROPOSALDT", "POLICYNO_OLD", "REGNO_NEW", "CCOLLNO", 
        "AUTO_DEBIT_IND", "AUTO_RENEWAL_IND", "PROP_INS_ADDRESS", "TOT_STOREY"
    ]
    
    input_file = input_dir / "LONPAC_FIRE.txt"
    df = read_txt_file_robust(input_file, fire_columns, skip_rows=5)
    print(f"  Fire data: {df.height} rows read")
    
    if df.height == 0:
        print("  No data to process")
        return
    
    # Parse dates
    for col in ["ISSUEDTX", "EXPDT", "SUBMITDX", "PROPOSALDT", "DOBX"]:
        if col in df.columns:
            new_col = col.replace('X', '') if col.endswith('X') else col + '_DT'
            df = df.with_columns(
                pl.col(col).map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias(new_col)
            )
    
    df = df.filter(
        (pl.col("POLICYNO") != "F.ENDT.MAS......") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "") &
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "")
    )
    
    df = df.with_columns(pl.lit("LONPAC_FIRE").alias("PRODUCT"))
    
    if "ACCTNOX" in df.columns:
        df = df.with_columns([
            pl.col("ACCTNOX").str.slice(13, 5).alias("NOTENO"),
            pl.col("ACCTNOX").str.slice(0, 12).str.replace_all("-", "").alias("ACCTNO")
        ])
    
    prod_cols = ["AGENTNO", "POLICYNO", "BRANCH", "ACCTNO", "NOTENO", "INSURED",
                 "ISSUEDT", "EXPIRYDT", "PRODUCT", "PREMIUM", "PROD_CODE", "PROD_DESC",
                 "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD",
                 "CCOLLNO", "AUTO_DEBIT_IND", "AUTO_RENEWAL_IND", "PROP_INS_ADDRESS", "TOT_STOREY"]
    
    available_cols = [col for col in prod_cols if col in df.columns]
    df_prod = df.select(available_cols)
    
    cust_cols = ["POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    
    df_cust = df.select([col for col in cust_cols if col in df.columns])
    df_cust = df_cust.filter(
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    if "DOB" in df_cust.columns:
        df_cust = df_cust.with_columns(
            (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE")
        )
    
    if "NEWIC" in df_cust.columns:
        df_cust = df_cust.with_columns(
            pl.col("NEWIC").str.replace_all("-", "").alias("IC")
        )
    
    print("  Writing Fire product data...")
    write_sas_dataset_saspy(df_prod, libref, "FIREPROD", sas_session)
    print("  Writing Fire customer data...")
    write_sas_dataset_saspy(df_cust, libref, "FIRECUST", sas_session)
    
    return df_prod, df_cust

def process_hire_data(input_dir: Path, output_dir: Path, file_name: str, output_prefix: str, reptyear4: int, sas_session, libref: str):
    """Process Hire Purchase data (both HIRE and NHIRE)"""
    print(f"  Reading {file_name} data...")
    hire_columns = [
        "POLICYNO", "NAME", "NEWIC", "OLDIC", "REGNO", "CREGNO", "DOBX", 
        "GENDER", "AGENTNO", "RACE", "MARITAL", "INSURED", "ISSUEDTX", 
        "EXPDT", "PREMIUM", "TELNO", "ADDRESS", "TOWN", "POSTCODE", 
        "PROD_CODE", "PROD_DESC", "PROCESS_MTH", "SUBMITDX", "PROPOSALDT", 
        "POLICYNO_OLD", "REGNO_NEW"
    ]
    
    input_file = input_dir / f"{file_name}.txt"
    df = read_txt_file_robust(input_file, hire_columns, skip_rows=8)  # FIRSTOBS=9 in SAS
    print(f"  {file_name} data: {df.height} rows read")
    
    if df.height == 0:
        print("  No data to process")
        return
    
    # Parse dates
    for col in ["ISSUEDTX", "EXPDT", "SUBMITDX", "PROPOSALDT", "DOBX"]:
        if col in df.columns:
            new_col = col.replace('X', '') if col.endswith('X') else col + '_DT'
            df = df.with_columns(
                pl.col(col).map_elements(lambda x: parse_date(x), return_dtype=pl.Datetime).alias(new_col)
            )
    
    df = df.filter(
        (pl.col("AGENTNO").is_not_null()) & (pl.col("AGENTNO") != "") &
        (pl.col("POLICYNO").is_not_null()) & (pl.col("POLICYNO") != "")
    )
    
    df = df.with_columns(pl.lit("LONPAC_HP").alias("PRODUCT"))
    
    if "CREGNO" in df.columns:
        df = df.with_columns([
            pl.col("CREGNO").str.replace_all(" ", "").alias("CARREG"),
            pl.col("CREGNO").str.replace_all(" ", "").alias("CREGNO")
        ])
    
    prod_cols = ["AGENTNO", "POLICYNO", "CREGNO", "INSURED", "ISSUEDT", "EXPIRYDT",
                 "PREMIUM", "CARREG", "PRODUCT", "PROD_CODE",
                 "PROD_DESC", "PROCESS_MTH", "SUBMITDT", "PROPOSAL_DT", "POLICYNO_OLD"]
    
    available_cols = [col for col in prod_cols if col in df.columns]
    df_prod = df.select(available_cols)
    
    cust_cols = ["NAME", "POLICYNO", "NEWIC", "OLDIC", "REGNO", "GENDER", "RACE",
                 "MARITAL", "DOB", "TELNO", "ADDRESS", "TOWN", "POSTCODE", "REGNO_NEW"]
    
    df_cust = df.select([col for col in cust_cols if col in df.columns])
    df_cust = df_cust.filter(
        (pl.col("POLICYNO") != "F.ENDT.MAS......") &
        (pl.col("NAME").is_not_null()) & (pl.col("NAME") != "")
    )
    
    if "DOB" in df_cust.columns:
        df_cust = df_cust.with_columns(
            (pl.lit(reptyear4) - pl.col("DOB").dt.year()).alias("AGE")
        )
    
    if "NEWIC" in df_cust.columns:
        df_cust = df_cust.with_columns(
            pl.col("NEWIC").str.replace_all("-", "").alias("IC")
        )
    
    # Determine table names
    prod_table = f"{output_prefix.upper()}PROD"
    cust_table = f"{output_prefix.upper()}CUST"
    
    print(f"  Writing {file_name} product data...")
    write_sas_dataset_saspy(df_prod, libref, prod_table, sas_session)
    print(f"  Writing {file_name} customer data...")
    write_sas_dataset_saspy(df_cust, libref, cust_table, sas_session)
    
    return df_prod, df_cust

def merge_all_data(sas_session, libref: str):
    """Merge all customer and product data using SAS"""
    print("Merging all datasets...")
    
    # Merge customer data
    sas_code = f"""
    proc sort data={libref}.PACUST; by POLICYNO; run;
    proc sort data={libref}.MOTORCUST; by POLICYNO; run;
    proc sort data={libref}.FIRECUST; by POLICYNO; run;
    proc sort data={libref}.HIRECUST; by POLICYNO; run;
    proc sort data={libref}.NONHIRECUST; by POLICYNO; run;
    
    data {libref}.CUST;
        merge {libref}.PACUST 
              {libref}.MOTORCUST 
              {libref}.FIRECUST 
              {libref}.HIRECUST 
              {libref}.NONHIRECUST;
        by POLICYNO;
        if POLICYNO = '' then delete;
        if POLICYNO = 'Policy issued by' then delete;
    run;
    
    proc sort data={libref}.PAPROD; by POLICYNO; run;
    proc sort data={libref}.MOTORPROD; by POLICYNO; run;
    proc sort data={libref}.FIREPROD; by POLICYNO; run;
    
    data {libref}.PROD;
        format PRODUCT $15.;
        merge {libref}.PAPROD 
              {libref}.MOTORPROD 
              {libref}.FIREPROD;
        by POLICYNO;
        if POLICYNO = '' then delete;
    run;
    """
    
    try:
        result = sas_session.submit(sas_code)
        if result['LOG']:
            print("SAS merge completed successfully")
        else:
            print("Warning: SAS merge may not have completed properly")
    except Exception as e:
        print(f"Error during merge: {e}")

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
    
    # Initialize SAS session
    print("Initializing SAS session...")
    sas = saspy.SASsession()
    
    # Define libref
    libref = "LONPAC"
    
    try:
        # Assign libref
        print(f"Assigning libref {libref}...")
        sas.submit(f"libname {libref} '{output_path}';")
        
        print("Processing PA data...")
        process_pa_data(input_path, output_path, reptyear4, sas, libref)
        
        print("Processing Motor data...")
        process_motor_data(input_path, output_path, reptyear4, sas, libref)
        
        print("Processing Misc data...")
        process_misc_data(input_path, output_path, reptyear4, sas, libref)
        
        print("Processing Fire data...")
        process_fire_data(input_path, output_path, reptyear4, sas, libref)
        
        print("Processing Hire Purchase data...")
        process_hire_data(input_path, output_path, "LONPAC_HIRE", "hire", reptyear4, sas, libref)
        
        print("Processing Non-Hire Purchase data...")
        process_hire_data(input_path, output_path, "LONPAC_NONHIRE", "nonhire", reptyear4, sas, libref)
        
        # Merge all datasets
        merge_all_data(sas, libref)
        
        print("Processing complete!")
        print(f"Output files are in SAS .sas7bdat format at: {output_path}")
        
    except Exception as e:
        print(f"Error during processing: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up SAS session
        print("Cleaning up SAS session...")
        try:
            sas.disconnect()
        except:
            pass

# Usage
if __name__ == "__main__":
    process_lonpac_data(
        input_dir="/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLNPC",
        output_dir="/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMLNPC"
    )
