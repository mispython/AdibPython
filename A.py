import re
import pprint
from collections import defaultdict
import os

FOLDER_NAME = 'DETICA'

def generate_layout_file(field_layout_file, output_dir='.'):
    if not os.path.exists(field_layout_file):
        raise FileNotFoundError(f"Field Config File not found in {field_layout_file}")
    
    try:
        with open(field_layout_file, 'r', encoding='utf-8') as f:
            sas_input_code = f.read()

        lrecl_pattern = re.compile(r'lrecl\s*=\s*(\d+)', re.IGNORECASE)
        lrecl_match = lrecl_pattern.search(sas_input_code)
        if not lrecl_match:
            raise ValueError("Could not find LRECL in the code")
        lrecl = int(lrecl_match.group(1))

        layout = {}
        input_line_pattern = re.compile(r'@\s*(\d+)\s+([A-Za-z0-9_]+)\s+([$\w\.]+)')
        print(input_line_pattern)
        range_line_pattern = re.compile(r'^\s*([A-Za-z0-9_]+)\s*(\$)?[ ]*(\d+)-(\d+)')
        input_lines = sas_input_code.split('input')[1].split(";")[0]

        for line in input_lines.strip().splitlines():
            line = line.strip()
            if not line:
                continue
            match = input_line_pattern.match(line)
            print(match)
            if match:
                start_pos, field_name, informat_str = match.groups()
                start_pos = int(start_pos)

                spec = {}
                byte_length = None
                clean_informat = informat_str.strip('$.').upper()

                float_match = re.match(r'(\d+)\.(\d+)', clean_informat)
                pd_match = re.match(r'S370FPD(\d+)(?:\.(\d+))?', clean_informat)
                ebcdic_match = re.match(r'EBCDIC(\d+)', clean_informat)
                integer_match = re.match(r'(\d+)\.', informat_str.upper())
                string_match = re.match(r'\$(\d+)\.', informat_str.upper())
                if pd_match:
                    spec['type'] = 'packed'
                    byte_length =  int(pd_match.group(1))
                    decimals = pd_match.group(2)
                    if decimals:
                        spec['decimals'] = int(decimals)
                elif ebcdic_match:
                    spec['type'] = 'ebcdic'
                    byte_length =  int(ebcdic_match.group(1))
                elif float_match:
                    spec['type'] = 'float'
                    byte_length = int(float_match.group(1))
                    spec['decimals'] = int(float_match.group(2))
                elif integer_match:
                    spec['type'] = 'integer'
                    byte_length = int(clean_informat)
                elif string_match:
                    spec['type'] = 'string'
                    byte_length = int(clean_informat)
                elif re.match(r'YYMMDD(\d+)', clean_informat):
                    spec['type'] = 'integer'
                    byte_length = int(re.match(r'YYMMDD(\d+)', clean_informat).group(1))

                if byte_length is None:
                    print(f"Could not parse informat '{informat_str}' for field '{field_name}'. Skiping")     
                    continue

                spec['slice'] = slice(start_pos - 1, start_pos - 1 + byte_length)
                layout[field_name] = spec
            elif match := range_line_pattern.match(line):
                field_name, dollar_sign, start_pos_str, end_pos_str = match.groups()
                start_pos = int(start_pos_str)
                end_pos = int(end_pos_str)

                spec = {}
                byte_length = end_pos - start_pos - 1

                if dollar_sign:
                    spec['type'] = 'string'
                else:
                    spec['type'] = 'integer'

                spec['slice'] = slice(start_pos - 1, end_pos)
                layout[field_name] = spec
            else:
                print(f"Skipping line: {line} does not match defined INPUT format")
                

        output_filename = f"{os.path.basename(field_layout_file).rsplit('.' , 1)[0]}_layout.py"
        output_layout_path = os.path.join(output_dir, output_filename)

        formatted_layout_string = pprint.pformat(layout, sort_dicts=False)

        with open(output_layout_path, 'w', encoding='utf-8') as f:
            f.write(f"LRECL = {lrecl}\n\n")
            f.write(f"LAYOUT = {formatted_layout_string}\n")
        print(f"Successfully generated layout file to {output_layout_path}")
    except Exception as e:
        print(f"Error generating layout file: {e}")


FIELD_LAYOUT_PATH = f"/host/mis/config/{FOLDER_NAME}"
LAYOUT_OUTPUT_DIR = f"/host/mis/config/{FOLDER_NAME}/output"

if not os.path.exists(LAYOUT_OUTPUT_DIR):
    os.makedirs(LAYOUT_OUTPUT_DIR)
    
if __name__ == "__main__":
    try:
        for filename in os.listdir(FIELD_LAYOUT_PATH):
            if filename.endswith(".txt"):
                file_path = os.path.join(FIELD_LAYOUT_PATH, filename)
                try:
                    generate_layout_file(file_path, output_dir=LAYOUT_OUTPUT_DIR)
                except Exception as e:
                    print(f"Error generating layout {filename}: {e}")
    except FileNotFoundError as e:
        print(f"Error path not found: {e}")
    print(f"Completed generating layout file.")
















# ELN_BNMSUMM_PROCESSOR.py

import polars as pl
import pyreadstat
import os
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# BASE PATHS
BASE_INPUT_PATH = Path("Data_Warehouse/MIS/Job/ELDS/input")
ELDS_DATA_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/output")
ELDS_DATA_PATH.mkdir(parents=True, exist_ok=True)

# DATETIME CALCULATIONS
REPTDATE = datetime.today() - timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)
FILE_DT = REPTDATE.strftime('%Y%m%d')

SAS_ORIGIN = datetime(1960, 1, 1)
RDATE = (REPTDATE - SAS_ORIGIN).days

# SOURCE FILE PATHS
BNMSUMM1 = BASE_INPUT_PATH / f"bnmsummary1_{FILE_DT}.csv"
BNMSUMM2 = BASE_INPUT_PATH / f"bnmsummary2_{FILE_DT}.csv"

# OUTPUT PATHS
OUTPUT_DATA_PATH = ELDS_DATA_PATH / f"year={REPTDATE.year}" / f"month={REPTDATE.month:02d}" / f"day={REPTDATE.day:02d}"
OUTPUT_DATA_PATH.mkdir(parents=True, exist_ok=True)

print(f"Processing date: {REPTDATE}")
print(f"Input SUMM1: {BNMSUMM1}")
print(f"Input SUMM2: {BNMSUMM2}")
print(f"Output path: {OUTPUT_DATA_PATH}")

def extract_maano_sequence(maano_str):
    """Extract sequence number from MAANO string like 'AKH/000709/25'"""
    if maano_str is None:
        return None
    try:
        # Extract positions 2-9 and convert to integer
        # For 'AKH/000709/25' -> 'KH/00070' -> remove non-digits -> '00070' -> 70
        extracted = maano_str[1:9]  # positions 2-9 (0-indexed: 1-8)
        # Remove non-digit characters and convert to integer
        numeric_part = ''.join(filter(str.isdigit, extracted))
        return int(numeric_part) if numeric_part else None
    except:
        return None

# ============================================================================
# PROCESS SUMM1
# ============================================================================
print("\nProcessing SUMM1...")

SUMM1 = pl.read_csv(
    BNMSUMM1,
    separator=",",
    has_header=False,
    ignore_errors=True,
    new_columns=[
        "MAANO", "STAGE", "APPLICATION", "DTECOMPLETE", "AANO", "APPKEY", 
        "PRIORITY_SECTOR", "DSRISS3", "FIN_CONCEPT", "LN_UTILISE_LOCAT_CD", 
        "SPECIALFUND", "ASSET_PURCH_AMT", "PURPOSE_LOAN", "STRUPCO_3YR", 
        "FACICODE", "AMTAPPLY", "AMOUNT", "APPTYPE", "REJREASON", "DATEXT", 
        "ACCTNO", "EIR", "EREQNO", "REFIN_FLG", "STATUS", "CCPT_TAG", "CIR", 
        "PRICING_TYPE", "LU_ADD1", "LU_ADD2", "LU_ADD3", "LU_ADD4", 
        "LU_TOWN_CITY", "LU_POSTCODE", "LU_STATE_CD", "LU_COUNTRY_CD", 
        "PROP_STATUS", "DTCOMPLETE", "LU_SOURCE"
    ]
)

SUMM1 = SUMM1.slice(1, SUMM1.height - 1)

SUMM1 = SUMM1.with_columns([
    pl.lit("Y").alias("INDINTERIM"),
    pl.lit(RDATE).alias("DATE")
])

SUMM1 = SUMM1.with_row_index(name="_N_", offset=1)

# FIXED: Use the correct MAANO extraction logic
SUMM1 = SUMM1.with_columns(
    pl.col("MAANO")
      .map_elements(extract_maano_sequence, return_dtype=pl.Int64)
      .alias("MAANO_SEQ")
)

# FIXED: Proper validation - check if MAANO_SEQ matches (_N_ - 1)
invalid_rows = SUMM1.filter(
    (pl.col("MAANO_SEQ").is_not_null()) &
    (pl.col("MAANO_SEQ") != (pl.col("_N_") - 1))
)

if invalid_rows.height > 0:
    print(f"ERROR: {invalid_rows.height} invalid rows found in SUMM1")
    print("First 10 invalid rows:")
    print(invalid_rows.select(["_N_", "MAANO", "MAANO_SEQ"]).head(10))
    raise SystemExit(77)

print(f"SUMM1 CSV records: {SUMM1.height}")

# READ EHP SAS FILE
print("Reading SUMM1_EHP from SAS...")
EHP_SRC = '/stgsrcsys/host/uat/tbc/intg_app_ehp_fs_dwh_bnmsummary1.sas7bdat'

if os.path.exists(EHP_SRC):
    SUMM1_EHP_DF, meta_dp = pyreadstat.read_sas7bdat(EHP_SRC)
    SUMM1_EHP = pl.from_pandas(SUMM1_EHP_DF)
    
    print(f"SUMM1_EHP original shape: {SUMM1_EHP.shape}")
    
    # FIXED: Ensure SUMM1_EHP has the same columns as SUMM1
    # Get the common columns between both DataFrames
    common_columns = list(set(SUMM1.columns) & set(SUMM1_EHP.columns))
    
    # Select only common columns from both DataFrames
    SUMM1_common = SUMM1.select(common_columns)
    SUMM1_EHP_common = SUMM1_EHP.select(common_columns)
    
    # Add the additional columns to SUMM1_EHP
    SUMM1_EHP_common = SUMM1_EHP_common.with_columns([
        pl.lit("Y").alias("INDINTERIM"),
        pl.lit(RDATE).alias("DATE")
    ])
    
    # Add row index starting from where SUMM1 ends
    start_index = SUMM1_common.height + 1
    SUMM1_EHP_common = SUMM1_EHP_common.with_row_index(name="_N_", offset=start_index)
    
    # Apply MAANO validation to EHP data
    SUMM1_EHP_common = SUMM1_EHP_common.with_columns(
        pl.col("MAANO")
          .map_elements(extract_maano_sequence, return_dtype=pl.Int64)
          .alias("MAANO_SEQ")
    )
    
    invalid_rows_ehp = SUMM1_EHP_common.filter(
        (pl.col("MAANO_SEQ").is_not_null()) &
        (pl.col("MAANO_SEQ") != (pl.col("_N_") - 1))
    )
    
    if invalid_rows_ehp.height > 0:
        print(f"ERROR: {invalid_rows_ehp.height} invalid rows found in SUMM1_EHP")
        print("First 10 invalid rows:")
        print(invalid_rows_ehp.select(["_N_", "MAANO", "MAANO_SEQ"]).head(10))
        raise SystemExit(77)
    
    # Combine the data
    SUMM1 = pl.concat([SUMM1_common, SUMM1_EHP_common])
    print(f"SUMM1_EHP records: {SUMM1_EHP_common.height}")
    print(f"Combined SUMM1 total: {SUMM1.height}")
else:
    print(f"WARNING: EHP file not found: {EHP_SRC}")

# LOAD PREVIOUS BASE
print("Loading previous SUMM1...")
prev_summ1_path = ELDS_DATA_PATH / f"year={PREVDATE.year}" / f"month={PREVDATE.month:02d}" / f"day={PREVDATE.day:02d}" / "SUMM1.parquet"

if prev_summ1_path.exists():
    # Use duckdb to read the parquet file
    con = duckdb.connect()
    PREV_SUMM1_DF = con.execute(f"SELECT * FROM read_parquet('{prev_summ1_path}')").df()
    PREV_SUMM1 = pl.from_pandas(PREV_SUMM1_DF)
    
    # Ensure previous data has same columns as current SUMM1
    common_columns_prev = list(set(SUMM1.columns) & set(PREV_SUMM1.columns))
    PREV_SUMM1 = PREV_SUMM1.select(common_columns_prev)
    SUMM1_prev_ready = SUMM1.select(common_columns_prev)
    
    ELDS_SUMM1 = pl.concat([PREV_SUMM1, SUMM1_prev_ready])
    print(f"Combined with previous: {ELDS_SUMM1.height} total records")
else:
    print("No previous SUMM1 found, using current only")
    ELDS_SUMM1 = SUMM1

# Remove temporary columns before writing
ELDS_SUMM1 = ELDS_SUMM1.drop(["_N_", "MAANO_SEQ"])

# WRITE TO CURRENT BASE using duckdb (as in original)
print(f"Writing SUMM1 to {OUTPUT_DATA_PATH / 'SUMM1.parquet'}...")
con = duckdb.connect()
con.register('ELDS_SUMM1', ELDS_SUMM1.to_pandas())
con.execute(f"COPY ELDS_SUMM1 TO '{OUTPUT_DATA_PATH}/SUMM1.parquet' (FORMAT PARQUET)")

# ============================================================================
# PROCESS SUMM2
# ============================================================================
print("\nProcessing SUMM2...")

SUMM2 = pl.read_csv(
    BNMSUMM2,
    separator=",",
    has_header=False,
    ignore_errors=True,
    new_columns=[
        "MAANO", "STAGE", "APPLICATION", "DTECOMPLETE", "IDNO", "ENTKEY", 
        "APPLNAME", "COUNTRY", "DBIRTH", "ENTITY_TYPE", "CORP_STATUS_CD", 
        "INDUSTRIAL_STATUS", "RESIDENCY_STATUS_CD", "ANNSUBTSALARY", 
        "GENDER", "OCCUPATION", "EMPNAME", "EMPLOY_SECTOR_CD", "EMPLOY_TYPE_CD", 
        "POSTCODE", "STATE_CD", "COUNTRY_CD", "ROLE", "ICPP", "MARRIED", 
        "CUSTOMER_CODE", "CISNUMBER", "NO_OF_EMPLOYEE", "ANNUAL_TURNOVER",         
        "SMESIZE", "RACE", "INDUSTRIAL_SECTOR_CD", "OCCUPAT_MASCO_CD", 
        "EREQNO", "DSRISS3", "DTCOMPLETE"
    ]
)

SUMM2 = SUMM2.slice(1, SUMM2.height - 1)

SUMM2 = SUMM2.with_columns([
    pl.lit("Y").alias("INDINTERIM"),
    pl.lit(RDATE).alias("DATE")
])

SUMM2 = SUMM2.with_row_index(name="_N_", offset=1)

SUMM2 = SUMM2.with_columns(
    pl.col("MAANO")
      .map_elements(extract_maano_sequence, return_dtype=pl.Int64)
      .alias("MAANO_SEQ")
)

invalid_rows_summ2 = SUMM2.filter(
    (pl.col("MAANO_SEQ").is_not_null()) &
    (pl.col("MAANO_SEQ") != (pl.col("_N_") - 1))
)

if invalid_rows_summ2.height > 0:
    print(f"ERROR: {invalid_rows_summ2.height} invalid rows found in SUMM2")
    print("First 10 invalid rows:")
    print(invalid_rows_summ2.select(["_N_", "MAANO", "MAANO_SEQ"]).head(10))
    raise SystemExit(77)

print(f"SUMM2 CSV records: {SUMM2.height}")

# READ EHP2 SAS FILE
print("Reading SUMM2_EHP from SAS...")
EHP2_SRC = '/stgsrcsys/host/uat/tbc/intg_app_ehp_fs_dwh_bnmsummary2.sas7bdat'

if os.path.exists(EHP2_SRC):
    SUMM2_EHP_DF, meta_dp = pyreadstat.read_sas7bdat(EHP2_SRC)
    SUMM2_EHP = pl.from_pandas(SUMM2_EHP_DF)
    
    print(f"SUMM2_EHP original shape: {SUMM2_EHP.shape}")
    
    SUMM2_EHP = SUMM2_EHP.slice(1, SUMM2_EHP.height - 1)
    
    # FIXED: Ensure column alignment for SUMM2
    common_columns_summ2 = list(set(SUMM2.columns) & set(SUMM2_EHP.columns))
    
    SUMM2_common = SUMM2.select(common_columns_summ2)
    SUMM2_EHP_common = SUMM2_EHP.select(common_columns_summ2)
    
    SUMM2_EHP_common = SUMM2_EHP_common.with_columns([
        pl.lit("Y").alias("INDINTERIM"),
        pl.lit(RDATE).alias("DATE")
    ])
    
    start_index_summ2 = SUMM2_common.height + 1
    SUMM2_EHP_common = SUMM2_EHP_common.with_row_index(name="_N_", offset=start_index_summ2)
    
    SUMM2_EHP_common = SUMM2_EHP_common.with_columns(
        pl.col("MAANO")
          .map_elements(extract_maano_sequence, return_dtype=pl.Int64)
          .alias("MAANO_SEQ")
    )
    
    invalid_rows_ehp2 = SUMM2_EHP_common.filter(
        (pl.col("MAANO_SEQ").is_not_null()) &
        (pl.col("MAANO_SEQ") != (pl.col("_N_") - 1))
    )
    
    if invalid_rows_ehp2.height > 0:
        print(f"ERROR: {invalid_rows_ehp2.height} invalid rows found in SUMM2_EHP")
        print("First 10 invalid rows:")
        print(invalid_rows_ehp2.select(["_N_", "MAANO", "MAANO_SEQ"]).head(10))
        raise SystemExit(77)
    
    SUMM2 = pl.concat([SUMM2_common, SUMM2_EHP_common])
    print(f"SUMM2_EHP records: {SUMM2_EHP_common.height}")
    print(f"Combined SUMM2 total: {SUMM2.height}")
else:
    print(f"WARNING: EHP file not found: {EHP2_SRC}")

# LOAD PREVIOUS BASE
print("Loading previous SUMM2...")
prev_summ2_path = ELDS_DATA_PATH / f"year={PREVDATE.year}" / f"month={PREVDATE.month:02d}" / f"day={PREVDATE.day:02d}" / "SUMM2.parquet"

if prev_summ2_path.exists():
    PREV_SUMM2_DF = con.execute(f"SELECT * FROM read_parquet('{prev_summ2_path}')").df()
    PREV_SUMM2 = pl.from_pandas(PREV_SUMM2_DF)
    
    common_columns_prev2 = list(set(SUMM2.columns) & set(PREV_SUMM2.columns))
    PREV_SUMM2 = PREV_SUMM2.select(common_columns_prev2)
    SUMM2_prev_ready = SUMM2.select(common_columns_prev2)
    
    ELDS_SUMM2 = pl.concat([PREV_SUMM2, SUMM2_prev_ready])
    print(f"Combined with previous: {ELDS_SUMM2.height} total records")
else:
    print("No previous SUMM2 found, using current only")
    ELDS_SUMM2 = SUMM2

# Remove temporary columns before writing
ELDS_SUMM2 = ELDS_SUMM2.drop(["_N_", "MAANO_SEQ"])

# WRITE TO CURRENT BASE
print(f"Writing SUMM2 to {OUTPUT_DATA_PATH / 'SUMM2.parquet'}...")
con.register('ELDS_SUMM2', ELDS_SUMM2.to_pandas())
con.execute(f"COPY ELDS_SUMM2 TO '{OUTPUT_DATA_PATH}/SUMM2.parquet' (FORMAT PARQUET)")

print("\n" + "="*80)
print("PROCESSING COMPLETE!")
print("="*80)
print(f"SUMM1 total records: {ELDS_SUMM1.height}")
print(f"SUMM2 total records: {ELDS_SUMM2.height}")
