import polars as pl
import pyreadstat
import os
import duckdb
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------
# CONFIGURATION PATHS
# ---------------------------------------------------------
# BASE_INPUT_PATH = Path("/host/mis/input/ELDS/SOURCE") # Folder for ELDS source files
BASE_INPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/input")
# ELDS_DATA_PATH = Path("/host/mis/parquet/ELDS") # Folder for output files
ELDS_DATA_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/output")
ELDS_DATA_PATH.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------
# DATE VARIABLES
# ---------------------------------------------------------
REPTDATE = datetime.today() - timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)
FILE_DT  = REPTDATE.strftime('%Y%m%d') 

SAS_ORIGIN = datetime(1960,1,1)
RDATE = (REPTDATE - SAS_ORIGIN).days

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
    # Align schemas by converting all to string before concatenation
    df1 = df1.select([pl.col(c).cast(pl.Utf8, strict=False).alias(c) for c in df1.columns])
    df2 = df2.select([pl.col(c).cast(pl.Utf8, strict=False).alias(c) for c in df2.columns])
    return pl.concat([df1, df2], how="diagonal")

# ---------------------------------------------------------
# READ BNMSUMM1 CSV
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
SUMM1 = SUMM1.slice(1, SUMM1.height - 1)

SUMM1 = (
    SUMM1
    .with_columns(
        pl.lit("Y").alias("INDINTERIM"),
        pl.lit(str(RDATE)).alias("DATE")  # Cast to string early to avoid schema conflict
    )
    .with_row_index(name="_N_", offset=1)
    .with_columns(
        pl.col("MAANO").cast(pl.Utf8).str.slice(2, 6).alias("MAANO_SUB")
    )
)

# ---------------------------------------------------------
# READ SAS7BDAT (EHP SOURCE)
# ---------------------------------------------------------
EHP_SRC = '/stgsrcsys/host/uat/tbc/intg_app_ehp_fs_dwh_bnmsummary1.sas7bdat'
SUMM1_EHP_df, meta_dp = pyreadstat.read_sas7bdat(EHP_SRC)
SUMM1_EHP = pl.from_pandas(SUMM1_EHP_df)

SUMM1_EHP = (
    SUMM1_EHP
    .with_columns(
        pl.lit("Y").alias("INDINTERIM"),
        pl.lit(str(RDATE)).alias("DATE")
    )
    .with_row_index(name="_N_", offset=1)
)
if "MAANO" in SUMM1_EHP.columns:
    SUMM1_EHP = SUMM1_EHP.with_columns(
        pl.col("MAANO").cast(pl.Utf8).str.slice(2, 6).alias("MAANO_SUB")
    )

# FIX: Safe concat that keeps all columns and prevents schema errors
SUMM1 = safe_concat(SUMM1, SUMM1_EHP)

# ---------------------------------------------------------
# APPEND WITH PREVIOUS DAY PARQUET
# ---------------------------------------------------------
query = f"""
SELECT *
FROM read_parquet('{ELDS_DATA_PATH}/year={PREVDATE.year}/month={PREVDATE.month:02d}/day={PREVDATE.day:02d}/SUMM1.parquet')
"""
try:
    PREV_SUMM1 = pl.from_pandas(duckdb.query(query).to_df())
    ELDS_SUMM1 = safe_concat(PREV_SUMM1, SUMM1)
except Exception:
    ELDS_SUMM1 = SUMM1  # Fallback if previous parquet missing

# ---------------------------------------------------------
# SAVE TO PARQUET
# ---------------------------------------------------------
duckdb.sql(f"""
    COPY (SELECT * FROM ELDS_SUMM1) TO '{OUTPUT_DATA_PATH}/SUMM1.parquet' (FORMAT PARQUET)
""")

# ---------------------------------------------------------
# READ BNMSUMM2 CSV
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
SUMM2 = SUMM2.slice(1, SUMM2.height - 1)

SUMM2 = (
    SUMM2
    .with_columns(
        pl.lit("Y").alias("INDINTERIM"),
        pl.lit(str(RDATE)).alias("DATE")
    )
    .with_row_index(name="_N_", offset=1)
)

if "MAANO" in SUMM2.columns:
    SUMM2 = SUMM2.with_columns(
        pl.col("MAANO").cast(pl.Utf8).str.slice(2, 6).alias("MAANO_SUB")
    )

# ---------------------------------------------------------
# READ AND CONCAT EHP2 SAS FILE (KEEP ALL COLUMNS)
# ---------------------------------------------------------
EHP2_SRC = '/stgsrcsys/host/uat/tbc/intg_app_ehp_fs_dwh_bnmsummary2.sas7bdat'
SUMM2_EHP_df, meta_dp2 = pyreadstat.read_sas7bdat(EHP2_SRC)
SUMM2_EHP = pl.from_pandas(SUMM2_EHP_df)

SUMM2_EHP = (
    SUMM2_EHP
    .with_columns(
        pl.lit("Y").alias("INDINTERIM"),
        pl.lit(str(RDATE)).alias("DATE")
    )
    .with_row_index(name="_N_", offset=1)
)
if "MAANO" in SUMM2_EHP.columns:
    SUMM2_EHP = SUMM2_EHP.with_columns(
        pl.col("MAANO").cast(pl.Utf8).str.slice(2, 6).alias("MAANO_SUB")
    )

SUMM2 = safe_concat(SUMM2, SUMM2_EHP)

# ---------------------------------------------------------
# SAVE SUMM2 TO PARQUET
# ---------------------------------------------------------
duckdb.sql(f"""
    COPY (SELECT * FROM SUMM2) TO '{OUTPUT_DATA_PATH}/SUMM2.parquet' (FORMAT PARQUET)
""")
