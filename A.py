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





import polars as pl
import pyreadstat
import os
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
import sys
import re

# ---------------------------------------------------------
# CONFIGURATION PATHS
# ---------------------------------------------------------
BASE_INPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/input")
ELDS_DATA_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/output")
ELDS_DATA_PATH.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------
# DATE VARIABLES
# ---------------------------------------------------------
# REPTDATE = datetime.today() - timedelta(days=1)
REPTDATE = datetime.strptime("2025-10-15 00:00:00", '%Y-%m-%d %H:%M:%S')
PREVDATE = REPTDATE - timedelta(days=1)
FILE_DT  = REPTDATE.strftime('%Y%m%d')

# SAS-style numeric RDATE (days since 1960-01-01)
SAS_ORIGIN = datetime(1960, 1, 1)
RDATE = (REPTDATE - SAS_ORIGIN).days  # numeric, not string

# ---------------------------------------------------------
# FILE PATHS
# ---------------------------------------------------------
BNMSUMM1 = BASE_INPUT_PATH / f"bnmsummary1_{FILE_DT}.csv"
BNMSUMM2 = BASE_INPUT_PATH / f"bnmsummary2_{FILE_DT}.csv"

OUTPUT_DATA_PATH = Path(f"{ELDS_DATA_PATH}/year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}")
OUTPUT_DATA_PATH.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------
# HELPER: Safe Concatenation Function
# ---------------------------------------------------------
def safe_concat(df1: pl.DataFrame, df2: pl.DataFrame) -> pl.DataFrame:
    """Align all columns by casting to string before concatenation."""
    all_cols = sorted(set(df1.columns) | set(df2.columns))
    df1 = df1.select(
        [pl.col(c).cast(pl.Utf8, strict=False).alias(c) if c in df1.columns else pl.lit(None).alias(c) for c in all_cols]
    )
    df2 = df2.select(
        [pl.col(c).cast(pl.Utf8, strict=False).alias(c) if c in df2.columns else pl.lit(None).alias(c) for c in all_cols]
    )
    return pl.concat([df1, df2], how="vertical_relaxed")

# ---------------------------------------------------------
# FUNCTION: SAS EOF CONTROL CHECK (Exclude header/footer)
# ---------------------------------------------------------
def apply_sas_eof_check(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    """
    SAS-style EOF control:
      - Footer row (FT00001724) contains expected record count
      - Ignore header/footer when counting
      - Compare expected vs actual data row count
      - Abort if mismatch
    """
    if "MAANO" not in df.columns:
        print(f"  MAANO column missing in {source_name}, skipping count validation.")
        return df

    # Extract the last row (footer)
    control_row = df[-1, :]
    ctrl_val = str(control_row["MAANO"][0]).strip()

    # Detect control footer pattern (FT00001724)
    match = re.match(r"FT0*(\d+)", ctrl_val)
    if not match:
        print(f" No numeric control count found in {source_name}, proceeding without checks.")
        df_data = df  # no footer found, use all rows
        return df_data.with_columns(
            pl.lit("Y").alias("INDINTERIM"),
            pl.lit(RDATE).alias("DATE")
        )

    control_count = int(match.group(1))
    print(f" {source_name}: Found control count in footer = {control_count}")

    # Exclude header (if any) and footer
    df_data = df.slice(0, df.height - 1)
    actual_count = df_data.height - 1

    print(f" {source_name}: Actual data row count (excluding header/footer) = {actual_count}")

    # Compare control vs data row count
    if control_count != actual_count:
        print(f" ABORT 77: Row count mismatch in {source_name} - expected {control_count}, got {actual_count}")
        raise SystemExit(77)
    else:
        print(f" Record count matches control record ({actual_count}).")

    # Add SAS-like columns
    df_data = df_data.with_columns(
        pl.lit("Y").alias("INDINTERIM"),
        pl.lit(RDATE).alias("DATE")
    )
    return df_data

# ---------------------------------------------------------
# READ BNMSUMM1 CSV + APPLY CHECK
# ---------------------------------------------------------
SUMM1 = pl.read_csv(
    BNMSUMM1,
    separator=",",
    has_header=False,
    ignore_errors=True,
    new_columns=[
        "MAANO", "STAGE", "APPLICATION", "DTECOMPLETE", "AANO", "APPKEY", "PRIORITY_SECTOR", "DSRISS3", "FIN_CONCEPT",
        "LN_UTILISE_LOCAT_CD", "SPECIALFUND", "ASSET_PURCH_AMT", "PURPOSE_LOAN", "STRUPCO_3YR", "FACICODE",
        "AMTAPPLY", "AMOUNT", "APPTYPE", "REJREASON", "DATEXT", "ACCTNO", "EIR", "EREQNO", "REFIN_FLG",
        "STATUS", "CCPT_TAG", "CIR", "PRICING_TYPE", "LU_ADD1", "LU_ADD2", "LU_ADD3", "LU_ADD4", "LU_TOWN_CITY",
        "LU_POSTCODE", "LU_STATE_CD", "LU_COUNTRY_CD", "PROP_STATUS", "DTCOMPLETE", "LU_SOURCE"
    ]
)

SUMM1 = apply_sas_eof_check(SUMM1, "bnmsummary1")

# Read EHP SAS7BDAT
EHP_SRC = '/stgsrcsys/host/uat/tbc/intg_app_ehp_fs_dwh_bnmsummary1.sas7bdat'
SUMM1_EHP_df, _ = pyreadstat.read_sas7bdat(EHP_SRC)
SUMM1_EHP = apply_sas_eof_check(pl.from_pandas(SUMM1_EHP_df), "bnmsummary1_ehp")

# Combine
SUMM1 = safe_concat(SUMM1, SUMM1_EHP)

# ---------------------------------------------------------
# APPEND PREVIOUS DAY (if exists)
# ---------------------------------------------------------
try:
    query = f"""
        SELECT * FROM read_parquet(
            '{ELDS_DATA_PATH}/year={PREVDATE.year}/month={PREVDATE.month:02d}/day={PREVDATE.day:02d}/SUMM1.parquet'
        )
    """
    PREV_SUMM1 = pl.from_pandas(duckdb.query(query).to_df())
    ELDS_SUMM1 = safe_concat(PREV_SUMM1, SUMM1)
except Exception:
    ELDS_SUMM1 = SUMM1

# Save to Parquet
duckdb.sql(f"""
    COPY (SELECT * FROM ELDS_SUMM1) TO '{OUTPUT_DATA_PATH}/SUMM1.parquet' (FORMAT PARQUET)
""")

# ---------------------------------------------------------
# READ BNMSUMM2 CSV + APPLY CHECK
# ---------------------------------------------------------
SUMM2 = pl.read_csv(
    BNMSUMM2,
    separator=",",
    has_header=False,
    ignore_errors=True,
    new_columns=[
        "MAANO", "STAGE", "APPLICATION", "DTECOMPLETE", "IDNO", "ENTKEY", "APPLNAME", "COUNTRY", "DBIRTH",
        "ENTITY_TYPE", "CORP_STATUS_CD", "INDUSTRIAL_STATUS", "RESIDENCY_STATUS_CD", "ANNSUBTSALARY",
        "GENDER", "OCCUPATION", "EMPNAME", "EMPLOY_SECTOR_CD", "EMPLOY_TYPE_CD", "POSTCODE", "STATE_CD",
        "COUNTRY_CD", "ROLE", "ICPP", "MARRIED", "CUSTOMER_CODE", "CISNUMBER", "NO_OF_EMPLOYEE", "ANNUAL_TURNOVER",
        "SMESIZE", "RACE", "INDUSTRIAL_SECTOR_CD", "OCCUPAT_MASCO_CD", "EREQNO", "DSRISS3", "DTCOMPLETE"
    ]
)

SUMM2 = apply_sas_eof_check(SUMM2, "bnmsummary2")

# Read EHP2 SAS7BDAT
EHP2_SRC = '/stgsrcsys/host/uat/tbc/intg_app_ehp_fs_dwh_bnmsummary2.sas7bdat'
SUMM2_EHP_df, _ = pyreadstat.read_sas7bdat(EHP2_SRC)
SUMM2_EHP = apply_sas_eof_check(pl.from_pandas(SUMM2_EHP_df), "bnmsummary2_ehp")

# Combine and save
SUMM2 = safe_concat(SUMM2, SUMM2_EHP)

duckdb.sql(f"""
    COPY (SELECT * FROM SUMM2) TO '{OUTPUT_DATA_PATH}/SUMM2.parquet' (FORMAT PARQUET)
""")
