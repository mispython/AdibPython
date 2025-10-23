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
    truncate_ragged_lines=True,
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
    truncate_ragged_lines=True,
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





 Table A is for rerun purpose handling, can ignore in python as the program will recreate the base in duckdb format 

Your table B code wrong, if the prevdate base not found should aborted the program instead of create exception as dataset need to be cumulative. 

table a: 
  %MACRO PROCESS1;
%IF %SYSFUNC(EXIST(ELDS.SUMM1&REPTYEAR&REPTMON&REPTDAY)) %THEN %DO;
   DATA ELDS.SUMM1&REPTYEAR&REPTMON&REPTDAY;
      SET ELDS.SUMM1&REPTYEAR&REPTMON&REPTDAY;
      IF DATE = &RDATE THEN DELETE;
   RUN;
%END;
%MEND;
%PROCESS1;

DATA ELDS.SUMM1&REPTYEAR&REPTMON&REPTDAY;
   SET PELDS.SUMM1&REPTYEAR1&REPTMON1&REPTDAY1 SUMM1;
RUN

table b: 
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
