import polars as pl
import duckdb
from pathlib import Path
import datetime
from datetime import date
import re

# ------------------------------------------------------------
# REPTDATE Calculation (equivalent to SAS DATA REPTDATE)
# ------------------------------------------------------------
def calculate_reptdate():
    """Calculate reporting date and related variables"""
    today = date.today()
    day_of_month = today.day
    
    if 8 <= day_of_month <= 14:
        reptdate = date(today.year, today.month, 8)
        wk = '1'
    elif 15 <= day_of_month <= 21:
        reptdate = date(today.year, today.month, 15)
        wk = '2'
    elif 22 <= day_of_month <= 27:
        reptdate = date(today.year, today.month, 22)
        wk = '3'
    else:
        # First day of current month minus 1 day = last day of previous month
        if today.month == 1:
            reptdate = date(today.year - 1, 12, 31)
        else:
            import calendar
            last_day_prev = calendar.monthrange(today.year, today.month - 1)[1]
            reptdate = date(today.year, today.month - 1, last_day_prev)
        wk = '4'
    
    # Create macro-like variables (Python equivalents)
    NOWK = wk
    RDATE = reptdate.strftime('%d%m%y')
    REPTMON = f"{reptdate.month:02d}"
    REPTYEAR = str(reptdate.year)[-2:]  # Last 2 digits of year
    
    return {
        'NOWK': NOWK,
        'RDATE': RDATE,
        'REPTMON': REPTMON,
        'REPTYEAR': REPTYEAR,
        'reptdate': reptdate
    }

# Run the calculation
macros = calculate_reptdate()
NOWK = macros['NOWK']
RDATE = macros['RDATE']
REPTMON = macros['REPTMON']
REPTYEAR = macros['REPTYEAR']
reptdate = macros['reptdate']

print(f"NOWK: {NOWK}, RDATE: {RDATE}, REPTMON: {REPTMON}, REPTYEAR: {REPTYEAR}")

# ------------------------------------------------------------
# BRH Data Processing
# ------------------------------------------------------------
def process_brh(brh_file_path):
    """Process BRH file - equivalent to SAS DATA BRH"""
    # Read with DuckDB for fixed-width format
    conn = duckdb.connect()
    
    # Create table from fixed-width file
    conn.execute(f"""
        CREATE OR REPLACE TABLE brh_raw AS 
        SELECT * FROM read_csv(
            '{brh_file_path}',
            delim='|',  # Using pipe as dummy delimiter for fixed-width
            header=False,
            all_varchar=True
        )
    """)
    
    # Parse fixed-width columns using DuckDB
    brh_df = conn.execute("""
        WITH parsed AS (
            SELECT 
                SUBSTR(column0, 2, 3)::INTEGER as BRANCH,
                SUBSTR(column0, 6, 3) as BRCD,
                SUBSTR(column0, 50, 1) as BRSTAT
            FROM brh_raw
        )
        SELECT BRANCH, BRCD
        FROM parsed
        WHERE BRSTAT != 'C' OR BRSTAT IS NULL
    """).pl()
    
    conn.close()
    return brh_df

# ------------------------------------------------------------
# Date Extraction Functions (ELDSDT1 and ELDSDT2 equivalents)
# ------------------------------------------------------------
def extract_date_from_file(file_path, line_num=1, pos_start=53):
    """Extract date from fixed position in file - equivalent to ELDSDT1/ELDSDT2"""
    with open(file_path, 'r') as f:
        lines = f.readlines()
        if len(lines) >= line_num:
            line = lines[line_num - 1]
            # Assuming format: positions 53-54: DD, 56-57: MM, 59-62: YYYY
            dd = int(line[pos_start-1:pos_start+1])
            mm = int(line[pos_start+3:pos_start+5])
            yy = int(line[pos_start+7:pos_start+11])
            return date(yy, mm, dd)
    return None

def process_elds_files(elds_file1, elds_file2):
    """Process ELDSDT1 and ELDSDT2 equivalent"""
    # Extract dates
    eldsdt1 = extract_date_from_file(elds_file1, 1, 53)
    eldsdt2 = extract_date_from_file(elds_file2, 1, 53)
    
    # Create ELDSDT1 macro variable
    ELDSDT1 = eldsdt1.strftime('%d%m%y') if eldsdt1 else None
    
    # Create ELDSDT2 macro variable
    ELDSDT2 = eldsdt2.strftime('%d%m%y') if eldsdt2 else None
    
    return {
        'ELDSDT1': ELDSDT1,
        'ELDSDT2': ELDSDT2,
        'eldsdt1': eldsdt1,
        'eldsdt2': eldsdt2
    }

# ------------------------------------------------------------
# ELN1 Data Processing
# ------------------------------------------------------------
def process_eln1(eln1_file_path):
    """Process ELN1 data - equivalent to SAS DATA ELN1"""
    # Define fixed-width column positions
    col_specs = [
        (0, 13, "AANO"),
        (79, 82, "BRCD"),
        (86, 96, "FACCODE"),
        (99, 129, "FACILI"),
        (132, 147, "AMOUNTX"),
        (150, 158, "BNMEFF"),
        (161, 361, "APPRIC"),
        (364, 379, "AMTAPPLY"),
        (382, 582, "AVPRIC"),
        (585, 593, "PRICING"),
        (596, 608, "NEWIC"),
        (611, 614, "CPARTY"),
        (617, 632, "LNTYPE"),
        (635, 650, "GINCOME"),
        (650, 665, "SPAAMT"),
        (665, 765, "CPRELAT"),
        (765, 865, "CPRELAS"),
        (889, 939, "CPSTAFF"),
        (939, 942, "CPDITOR"),
        (942, 947, "CPSTFID"),
        (947, 958, "CPBRHO"),
        (991, 1016, "STATUS")
    ]
    
    # Read with Polars using fixed-width reader
    df = pl.read_csv(
        eln1_file_path,
        skip_rows=1,  # FIRSTOBS=2 in SAS
        has_header=False,
        infer_schema_length=0,
        encoding='utf8'
    )
    
    # Parse columns using string operations
    df = df.with_columns([
        pl.col("column_1").str.slice(start, end-start).str.strip().alias(name)
        for (start, end, name) in col_specs
    ]).drop("column_1")
    
    # Convert numeric columns
    numeric_cols = ["AMOUNTX", "AMTAPPLY", "GINCOME", "SPAAMT"]
    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col)
                .str.replace_all(",", "")
                .str.replace_all(" ", "")
                .cast(pl.Float64)
                .alias(col)
            )
    
    # Convert to uppercase where needed
    uppercase_cols = ["AANO", "APPRIC", "AVPRIC", "LNTYPE", "CPRELAT", 
                     "CPRELAS", "CPSTAFF", "CPDITOR", "CPSTFID", "CPBRHO", "STATUS"]
    for col in uppercase_cols:
        if col in df.columns:
            df = df.with_columns(pl.col(col).str.to_uppercase())
    
    return df

# ------------------------------------------------------------
# ELN2 Data Processing
# ------------------------------------------------------------
def process_eln2(eln2_file_path):
    """Process ELN2 data - equivalent to SAS DATA ELN2"""
    # Define fixed-width column positions
    col_specs = [
        (0, 13, "AANO"),
        (16, 18, "DD"),
        (19, 21, "MM"),
        (22, 26, "YY"),
        (29, 44, "FELIMIT"),
        (47, 62, "TRLIMIT"),
        (65, 69, "CUSTCODE"),
        (72, 76, "SECTOR"),
        (79, 83, "PCODCRIS"),
        (86, 90, "PCODFISS"),
        (93, 96, "SMESIZE"),
        (99, 103, "NOEMPLO"),
        (106, 117, "TURNOVER"),
        (120, 122, "SDD"),
        (123, 125, "SMM"),
        (126, 130, "SYY"),
        (133, 135, "BDD"),
        (136, 138, "BMM"),
        (139, 143, "BYY"),
        (146, 148, "RDD"),
        (149, 151, "RMM"),
        (152, 156, "RYY"),
        (159, 161, "IDD"),
        (162, 164, "IMM"),
        (165, 169, "IYY"),
        (172, 174, "LDD"),
        (175, 177, "LMM"),
        (178, 182, "LYY"),
        (185, 187, "ADD"),
        (188, 190, "AMM"),
        (191, 195, "AYY"),
        (198, 200, "PDD"),
        (201, 203, "PMM"),
        (204, 208, "PYY"),
        (211, 271, "APVBY"),
        (274, 276, "P2DD"),
        (277, 279, "P2MM"),
        (280, 284, "P2YY"),
        (287, 347, "APVBY2"),
        (347, 372, "APVDES1"),
        (372, 397, "APVDES2"),
        (397, 597, "REASONS"),
        (597, 606, "ICREASON"),
        (621, 623, "DD4"),
        (624, 626, "MM4"),
        (627, 631, "YY4"),
        (634, 694, "SMENAME1"),
        (694, 754, "SMENAME2"),
        (754, 757, "TRANBR"),
        (757, 760, "TRANBRNO"),
        (760, 764, "TRANREG"),
        (779, 780, "ADVANCES"),
        (783, 786, "PRODUCT"),
        (789, 792, "STATE"),
        (795, 810, "EXSTLMT"),
        (813, 828, "CHNGLMT"),
        (831, 832, "GREENTCO"),
        (835, 836, "BIOTCO"),
        (839, 840, "SMEIP"),
        (843, 844, "SME1INCR"),
        (847, 848, "SMEMSC"),
        (851, 852, "STRUPCO_2YR"),
        (855, 880, "STATUS"),
        (883, 885, "CTRY_INCORP"),
        (907, 909, "STRUPCO_3YR"),
        (912, 914, "HDD"),
        (915, 917, "HMM"),
        (918, 922, "HYY"),
        (925, 1075, "NAME"),
        (1078, 1080, "LN_UTILISE_LOCAT_CD"),
        (1083, 1095, "NEW_BUSS_REG_ID"),
        (1098, 1103, "CLIMATE_PRIN_TAXONOMY_CLASS"),
        (1106, 1109, "SOURCE_INCOME_CURRENCY_CD"),
        (1112, 1122, "GRP_ANNL_SALES_FINANCIAL_DT"),
        (1125, 1150, "GRP_ANNL_SALES_AMT")
    ]
    
    # Read with Polars
    df = pl.read_csv(
        eln2_file_path,
        skip_rows=1,  # FIRSTOBS=2 in SAS
        has_header=False,
        infer_schema_length=0,
        encoding='utf8'
    )
    
    # Parse columns
    df = df.with_columns([
        pl.col("column_1").str.slice(start, end-start).str.strip().alias(name)
        for (start, end, name) in col_specs
    ]).drop("column_1")
    
    # Convert numeric columns
    numeric_cols = ["FELIMIT", "TRLIMIT", "CUSTCODE", "SECTOR", "PCODCRIS", 
                   "PCODFISS", "SMESIZE", "NOEMPLO", "TURNOVER", "EXSTLMT", 
                   "CHNGLMT", "TRANBRNO", "NEW_BUSS_REG_ID", "GRP_ANNL_SALES_AMT"]
    
    for col in numeric_cols:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col)
                .str.replace_all(",", "")
                .str.replace_all(" ", "")
                .cast(pl.Float64)
                .alias(col)
            )
    
    # Add AMOUNT column
    df = df.with_columns(pl.col("CHNGLMT").alias("AMOUNT"))
    
    # Create date columns using DuckDB for complex date handling
    conn = duckdb.connect()
    
    # Register DataFrame with DuckDB
    conn.register("eln2_temp", df)
    
    # Execute date conversions
    result = conn.execute("""
        SELECT *,
            -- Create date columns
            CASE 
                WHEN AMM != '' AND ADD != '' AND AYY != '' 
                THEN make_date(CAST(AYY AS INTEGER), CAST(AMM AS INTEGER), CAST(ADD AS INTEGER))
                ELSE NULL 
            END as AADATE,
            
            CASE 
                WHEN SMM != '' AND SDD != '' AND SYY != '' 
                THEN make_date(CAST(SYY AS INTEGER), CAST(SMM AS INTEGER), CAST(SDD AS INTEGER))
                ELSE NULL 
            END as SBDATE,
            
            CASE 
                WHEN RMM != '' AND RDD != '' AND RYY != '' 
                THEN make_date(CAST(RYY AS INTEGER), CAST(RMM AS INTEGER), CAST(RDD AS INTEGER))
                ELSE NULL 
            END as DPDATE,
            
            CASE 
                WHEN IMM != '' AND IDD != '' AND IYY != '' 
                THEN make_date(CAST(IYY AS INTEGER), CAST(IMM AS INTEGER), CAST(IDD AS INTEGER))
                ELSE NULL 
            END as IDDATE,
            
            CASE 
                WHEN LMM != '' AND LDD != '' AND LYY != '' 
                THEN make_date(CAST(LYY AS INTEGER), CAST(LMM AS INTEGER), CAST(LDD AS INTEGER))
                ELSE NULL 
            END as LODATE,
            
            CASE 
                WHEN MM4 != '' AND DD4 != '' AND YY4 != '' 
                THEN make_date(CAST(YY4 AS INTEGER), CAST(MM4 AS INTEGER), CAST(DD4 AS INTEGER))
                ELSE NULL 
            END as CMDATE,
            
            CASE 
                WHEN PMM != '' AND PDD != '' AND PYY != '' 
                THEN make_date(CAST(PYY AS INTEGER), CAST(PMM AS INTEGER), CAST(PDD AS INTEGER))
                ELSE NULL 
            END as APVDTE1,
            
            CASE 
                WHEN P2MM != '' AND P2DD != '' AND P2YY != '' 
                THEN make_date(CAST(P2YY AS INTEGER), CAST(P2MM AS INTEGER), CAST(P2DD AS INTEGER))
                ELSE NULL 
            END as APVDTE2,
            
            CASE 
                WHEN BMM != '' AND BDD != '' AND BYY != '' 
                THEN make_date(CAST(BYY AS INTEGER), CAST(BMM AS INTEGER), CAST(BDD AS INTEGER))
                ELSE NULL 
            END as BR_FULL_DOC_RECEIVE_DT,
            
            CASE 
                WHEN HMM != '' AND HDD != '' AND HYY != '' 
                THEN make_date(CAST(HYY AS INTEGER), CAST(HMM AS INTEGER), CAST(HDD AS INTEGER))
                ELSE NULL 
            END as HOE_FULL_DOC_RECEIVE_DT
            
        FROM eln2_temp
    """).pl()
    
    conn.close()
    
    # Drop original date component columns
    drop_cols = ["MM", "DD", "YY", "SMM", "SDD", "SYY", "RMM", "RDD", "RYY",
                "IMM", "IDD", "IYY", "P2MM", "P2DD", "P2YY", "PMM", "PDD", "PYY",
                "DD4", "MM4", "YY4", "LMM", "LDD", "LYY", "AMM", "ADD", "AYY",
                "BMM", "BDD", "BYY", "HMM", "HDD", "HYY"]
    
    result = result.drop([col for col in drop_cols if col in result.columns])
    
    # Convert to uppercase where needed
    uppercase_cols = ["AANO", "APVBY", "APVBY2", "APVDES1", "APVDES2", "REASONS",
                     "ICREASON", "SMENAME1", "SMENAME2", "TRANBR", "TRANREG",
                     "ADVANCES", "PRODUCT", "STATE", "GREENTCO", "BIOTCO", "SMEIP",
                     "SME1INCR", "SMEMSC", "STRUPCO_2YR", "STATUS", "CTRY_INCORP",
                     "STRUPCO_3YR", "NAME", "LN_UTILISE_LOCAT_CD", 
                     "CLIMATE_PRIN_TAXONOMY_CLASS", "SOURCE_INCOME_CURRENCY_CD"]
    
    for col in uppercase_cols:
        if col in result.columns:
            result = result.with_columns(pl.col(col).str.to_uppercase())
    
    return result

# ------------------------------------------------------------
# Main Processing Pipeline
# ------------------------------------------------------------
def main_pipeline(brh_path, elds1_path, elds2_path, eln1_path, eln2_path, output_dir):
    """Main processing pipeline equivalent to the SAS program"""
    
    # 1. Calculate reporting dates
    macros = calculate_reptdate()
    NOWK = macros['NOWK']
    REPTMON = macros['REPTMON']
    REPTYEAR = macros['REPTYEAR']
    
    # 2. Process BRH data
    print("Processing BRH data...")
    brh_df = process_brh(brh_path)
    
    # 3. Process ELDS dates
    print("Processing ELDS dates...")
    elds_dates = process_elds_files(elds1_path, elds2_path)
    
    # 4. Process ELN1 data
    print("Processing ELN1 data...")
    eln1_df = process_eln1(eln1_path)
    
    # 5. Process ELN2 data
    print("Processing ELN2 data...")
    eln2_df = process_eln2(eln2_path)
    
    # 6. Merge ELN1 and ELN2 (equivalent to SAS DATA SIBC)
    print("Merging ELN1 and ELN2...")
    
    # Sort both dataframes
    eln1_sorted = eln1_df.sort(["AANO", "STATUS"])
    eln2_sorted = eln2_df.sort(["AANO", "STATUS"])
    
    # Merge on AANO and STATUS
    sibc_df = eln1_sorted.join(eln2_sorted, on=["AANO", "STATUS"], how="outer")
    
    # Apply conditional logic for AMOUNT
    sibc_df = sibc_df.with_columns(
        pl.when(
            (pl.col("STATUS") == "APPLIED") & 
            (pl.col("AMOUNT").is_null() | (pl.col("AMOUNT") == 0))
        )
        .then(pl.col("AMOUNTX"))
        .otherwise(pl.col("AMOUNT"))
        .alias("AMOUNT")
    )
    
    # Drop AMOUNTX and CHNGLMT
    sibc_df = sibc_df.drop(["AMOUNTX", "CHNGLMT"])
    
    # 7. Remove duplicates (equivalent to PROC SORT NODUPKEY)
    sibc_df = sibc_df.unique(subset=["AANO", "STATUS"])
    
    # 8. Sort by BRCD for merge
    sibc_df = sibc_df.sort("BRCD")
    brh_df_sorted = brh_df.sort("BRCD")
    
    # 9. Merge with BRH data
    print("Merging with BRH data...")
    sibc_final = sibc_df.join(brh_df_sorted, on="BRCD", how="inner")
    
    # Drop BRCD column
    sibc_final = sibc_final.drop("BRCD")
    
    # 10. Save output files (equivalent to ELDS1 and ELDS2 datasets)
    print("Saving output files...")
    output_filename = f"SIBC{REPTMON}{REPTYEAR}{NOWK}"
    
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Save to Parquet (equivalent to SAS dataset)
    sibc_final.write_parquet(output_path / f"{output_filename}.parquet")
    
    # Also save to CSV for compatibility
    sibc_final.write_csv(output_path / f"{output_filename}.csv")
    
    print(f"Processing complete. Files saved with prefix: {output_filename}")
    
    return sibc_final

# ------------------------------------------------------------
# Execution Example
# ------------------------------------------------------------
if __name__ == "__main__":
    # Define file paths (update these with your actual paths)
    base_path = Path("C:/Your/Data/Path/")
    
    brh_path = base_path / "BRH.txt"
    elds1_path = base_path / "ELDSTXT.txt"
    elds2_path = base_path / "ELDSTX2.txt"
    eln1_path = base_path / "ELDSTXT.txt"  # Same as elds1 but different processing
    eln2_path = base_path / "ELDSTX2.txt"  # Same as elds2 but different processing
    
    output_dir = base_path / "output"
    
    # Run the pipeline
    try:
        result_df = main_pipeline(
            brh_path=brh_path,
            elds1_path=elds1_path,
            elds2_path=elds2_path,
            eln1_path=eln1_path,
            eln2_path=eln2_path,
            output_dir=output_dir
        )
        
        # Print summary statistics
        print("\nProcessing Summary:")
        print(f"Number of records: {len(result_df)}")
        print(f"Columns: {result_df.columns}")
        
        # Show first few rows
        print("\nFirst 5 rows:")
        print(result_df.head(5))
        
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
    except Exception as e:
        print(f"Error during processing: {e}")
