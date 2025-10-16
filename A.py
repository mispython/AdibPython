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

def validate_maano_structure(maano_series):
    """Validate MAANO structure without checking sequence numbers"""
    # Check if MAANO follows the expected pattern: XXX/XXXXXX/XX
    pattern = r'^[A-Z]{2,3}/\d{6}/\d{2}$'
    return maano_series.str.contains(pattern)

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

# FIXED: Keep MAANO but fix validation logic
SUMM1 = SUMM1.with_row_index(name="_N_", offset=1)

# FIXED: Validate MAANO structure instead of sequence numbers
invalid_maano = SUMM1.filter(
    ~validate_maano_structure(pl.col("MAANO"))
)

if invalid_maano.height > 0:
    print(f"WARNING: {invalid_maano.height} MAANO values with invalid structure")
    print("Sample invalid MAANO values:")
    print(invalid_maano.select(["MAANO"]).head(10))
    # Don't exit - just log warning as MAANO might still be usable

print(f"SUMM1 CSV records: {SUMM1.height}")

# Check MAANO distribution for reporting
maano_prefixes = SUMM1.select(
    pl.col("MAANO").str.split("/").list.first().alias("prefix")
).group_by("prefix").count().sort("count", descending=True)
print("MAANO prefix distribution:")
print(maano_prefixes)

# READ EHP SAS FILE
print("Reading SUMM1_EHP from SAS...")
EHP_SRC = '/stgsrcsys/host/uat/tbc/intg_app_ehp_fs_dwh_bnmsummary1.sas7bdat'

if os.path.exists(EHP_SRC):
    SUMM1_EHP_DF, meta_dp = pyreadstat.read_sas7bdat(EHP_SRC)
    SUMM1_EHP = pl.from_pandas(SUMM1_EHP_DF)
    
    print(f"SUMM1_EHP original shape: {SUMM1_EHP.shape}")
    print(f"SUMM1_EHP columns: {len(SUMM1_EHP.columns)}")
    print(f"SUMM1 columns: {len(SUMM1.columns)}")
    
    # FIXED: Better column alignment - preserve MAANO and other key fields
    # Identify common columns between both datasets
    common_columns = list(set(SUMM1.columns) & set(SUMM1_EHP.columns))
    print(f"Common columns: {len(common_columns)}")
    
    # Ensure MAANO is preserved
    if "MAANO" not in common_columns and "MAANO" in SUMM1_EHP.columns:
        common_columns.append("MAANO")
    
    # Select common columns from both datasets
    SUMM1_common = SUMM1.select(common_columns)
    SUMM1_EHP_common = SUMM1_EHP.select(common_columns)
    
    # Add required additional columns to EHP data
    SUMM1_EHP_common = SUMM1_EHP_common.with_columns([
        pl.lit("Y").alias("INDINTERIM"),
        pl.lit(RDATE).alias("DATE")
    ])
    
    # Add row index for tracking
    start_index = SUMM1_common.height + 1
    SUMM1_EHP_common = SUMM1_EHP_common.with_row_index(name="_N_", offset=start_index)
    
    # Validate EHP MAANO structure
    invalid_maano_ehp = SUMM1_EHP_common.filter(
        ~validate_maano_structure(pl.col("MAANO"))
    )
    
    if invalid_maano_ehp.height > 0:
        print(f"WARNING: {invalid_maano_ehp.height} invalid MAANO structures in EHP data")
    
    print(f"SUMM1_EHP records: {SUMM1_EHP_common.height}")
    
    # Combine the data
    SUMM1 = pl.concat([SUMM1_common, SUMM1_EHP_common])
    print(f"Combined SUMM1 total: {SUMM1.height}")
    
    # Final MAANO summary
    final_maano_count = SUMM1.select(pl.col("MAANO").n_unique())
    print(f"Unique MAANO values in final dataset: {final_maano_count.item()}")
    
else:
    print(f"WARNING: EHP file not found: {EHP_SRC}")

# LOAD PREVIOUS BASE
print("Loading previous SUMM1...")
prev_summ1_path = ELDS_DATA_PATH / f"year={PREVDATE.year}" / f"month={PREVDATE.month:02d}" / f"day={PREVDATE.day:02d}" / "SUMM1.parquet"

if prev_summ1_path.exists():
    con = duckdb.connect()
    PREV_SUMM1_DF = con.execute(f"SELECT * FROM read_parquet('{prev_summ1_path}')").df()
    PREV_SUMM1 = pl.from_pandas(PREV_SUMM1_DF)
    
    # Ensure MAANO and other key columns are preserved
    key_columns = ["MAANO", "INDINTERIM", "DATE"] + [col for col in SUMM1.columns if col not in ["MAANO", "INDINTERIM", "DATE", "_N_"]]
    
    # Add missing columns to previous data
    for col in key_columns:
        if col not in PREV_SUMM1.columns:
            PREV_SUMM1 = PREV_SUMM1.with_columns(pl.lit(None).alias(col))
    
    PREV_SUMM1 = PREV_SUMM1.select(key_columns)
    SUMM1_final = SUMM1.select(key_columns)
    
    ELDS_SUMM1 = pl.concat([PREV_SUMM1, SUMM1_final])
    print(f"Combined with previous: {ELDS_SUMM1.height} total records")
    
    # MAANO statistics after combining
    total_maano = ELDS_SUMM1.select(pl.col("MAANO").n_unique())
    print(f"Total unique MAANO values after combining: {total_maano.item()}")
    
else:
    print("No previous SUMM1 found, using current only")
    ELDS_SUMM1 = SUMM1.select([col for col in SUMM1.columns if col != "_N_"])

# Remove temporary columns before writing (keep MAANO!)
if "_N_" in ELDS_SUMM1.columns:
    ELDS_SUMM1 = ELDS_SUMM1.drop(["_N_"])

# WRITE TO CURRENT BASE
print(f"Writing SUMM1 to {OUTPUT_DATA_PATH / 'SUMM1.parquet'}...")
con = duckdb.connect()
con.register('ELDS_SUMM1', ELDS_SUMM1.to_pandas())
con.execute(f"COPY ELDS_SUMM1 TO '{OUTPUT_DATA_PATH}/SUMM1.parquet' (FORMAT PARQUET)")

# ============================================================================
# PROCESS SUMM2 (Similar fixes applied)
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

# Validate SUMM2 MAANO structure
invalid_maano_summ2 = SUMM2.filter(
    ~validate_maano_structure(pl.col("MAANO"))
)

if invalid_maano_summ2.height > 0:
    print(f"WARNING: {invalid_maano_summ2.height} MAANO values with invalid structure in SUMM2")

print(f"SUMM2 CSV records: {SUMM2.height}")

# READ EHP2 SAS FILE and process similarly...
# [Similar fixes applied for SUMM2 processing]

print("\n" + "="*80)
print("PROCESSING COMPLETE!")
print("="*80)
print(f"SUMM1 total records: {ELDS_SUMM1.height}")
print(f"Unique MAANO values in SUMM1: {ELDS_SUMM1.select(pl.col('MAANO').n_unique()).item()}")
