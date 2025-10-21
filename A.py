import polars as pl
import pyreadstat
import os
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
from sas7bdat import SAS7BDAT

# BASE_INPUT_PATH = Path("/host/mis/input/ELDS/SOURCE") # Folder for ELDS source files
BASE_INPUT_PATH = Path("Data_Warehouse/MIS/Job/ELDS/input")
# ELDS_DATA_PATH = Path("/host/mis/parquet/ELDS") # Folder for output files
ELDS_DATA_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/output")
ELDS_DATA_PATH.mkdir(parents=True, exist_ok=True)

# datetime
REPTDATE = datetime.today() - timedelta(days=1)
PREVDATE = REPTDATE - timedelta(days=1)
FILE_DT  = REPTDATE.strftime('%Y%m%d') 

SAS_ORIGIN = datetime(1960,1,1)
RDATE = (REPTDATE- SAS_ORIGIN).days

# Source File paths
BNMSUMM1 = BASE_INPUT_PATH / f"bnmsummary1_{FILE_DT}.csv"
BNMSUMM2 = BASE_INPUT_PATH / f"bnmsummary2_{FILE_DT}.csv"

# output paths
OUTPUT_DATA_PATH = Path("{ELDS_DATA_PATH}/year={REPTDATE.year}/month={REPTDATE.month:02d}/day={REPTDATE.day:02d}")
OUTPUT_DATA_PATH.mkdir(parents=True, exist_ok=True)

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
SUMM1 = SUMM1.slice(1, SUMM1.height -1)

SUMM1 = SUMM1.with_columns(
    pl.lit("Y").alias("INDINTERIM"),
    pl.lit (RDATE).cast(pl.Date).alias("DATE")
)

SUMM1 = SUMM1.with_row_count(name="_N_", offset=1)

SUMM1 = SUMM1.with_columns(
    pl.col("MAANO").str.slice(2,8).alias("MAANO_SUB")
)

invalid_rows = SUMM1.filter(pl.col("MAANO_SUB") != (pl.col("_N_")-1))
if invalid_rows.height > 0:
    raise SystemExit(77)

# Read deposit sas file
# EHP_SRC = '/sas/eHP/intermediate/intg_app_ehp_fs_dwh_bnmsummary1.sas7bdat'
EHP_SRC = '/stgsrcsys/host/uat/tbc/intg_app_ehp_fs_dwh_bnmsummary1.sas7bdat'
SUMM1_EHP, meta_dp = pyreadstat.read_sas7bdat(EHP_SRC)

SUMM1_EHP = SUMM1_EHP.with_columns(
    pl.lit("Y").alias("INDINTERIM"),
    pl.lit (RDATE).alias("DATE")
)

SUMM1_EHP = SUMM1_EHP.with_row_count(name="_N_", offset=1)

SUMM1_EHP = SUMM1_EHP.with_columns(
    pl.col("MAANO").str.slice(2,8).alias("MAANO_SUB")
)

invalid_rows = SUMM1_EHP.filter(pl.col("MAANO_SUB") != (pl.col("_N_")-1))
if invalid_rows.height > 0:
    raise SystemExit(77)

SUMM1 = pl.concat(SUMM1, SUMM1_EHP)

# previous base for concact
query = """
SELECT *
FROM read_parquet('{ELDS_DATA_PATH}/year={PREVDATE.year}/month={PREVDATE.month:02d}/day={PREVDATE.day:02d}/SUMM1.parquet')
"""
PREV_SUMM1 = duckdb.query(query).to_df()

ELDS_SUMM1= pl.concat([PREV_SUMM1, SUMM1])

# write to current base
duckdb.sql(""""
    COPY ELDS_SUMM1 TO '{OUTPUT_DATA_PATH}/SUMM1.parquet' (FORMAT PARQUET)
""")

SUMM2 = pl.read_csv(
    BNMSUMM2,
    separator=",",
    has_header=False,
    ignore_errors=True,
    new_columns=[
        "MAANO", "STAGE", "APPLICATION", "DTECOMPLETE", "IDNO", "ENTKEY", "APPLNAME", "COUNTRY", "DBIRTH",
        "ENTITY_TYPE", "CORP_STATUS_CD", "INDUSTRIAL_STATUS", "RESIDENCY_STATUS_CD", "ANNSUBTSALARY ", 
        "GENDER","OCCUPATION", "EMPNAME", "EMPLOY_SECTOR_CD", "EMPLOY_TYPE_CD", "POSTCODE", "STATE_CD", 
        "COUNTRY_CD", "ROLE", "ICPP", "MARRIED", "CUSTOMER_CODE", "CISNUMBER", "NO_OF_EMPLOYEE", "ANNUAL_TURNOVER",         
        "SMESIZE", "RACE", "INDUSTRIAL_SECTOR_CD", "OCCUPAT_MASCO_CD", "EREQNO", "DSRISS3", "DTCOMPLETE"
    ]
)

SUMM2 = SUMM2.slice(1, SUMM2.height -1)

SUMM2 = SUMM2.with_columns(
    pl.lit("Y").alias("INDINTERIM"),
    pl.lit (RDATE).alias("DATE")
)

SUMM2 = SUMM2.with_row_count(name="_N_", offset=1)

SUMM2 = SUMM2.with_columns(
    pl.col("MAANO").str.slice(2,8).alias("MAANO_SUB")
)

invalid_rows = SUMM2.filter(pl.col("MAANO_SUB") != (pl.col("_N_")-1))
if invalid_rows.height > 0:
    raise SystemExit(77)

# EHP2_SRC = '/sas/eHP/intermediate/intg_app_ehp_fs_dwh_bnmsummary2.sas7bdat'
EHP2_SRC = '/stgsrcsys/host/uat/tbc/intg_app_ehp_fs_dwh_bnmsummary2.sas7bdat'
SUMM2_EHP, meta_dp = pyreadstat.read_sas7bdat(EHP2_SRC)

SUMM2_EHP = SUMM2_EHP.slice(1, SUMM2_EHP.height -1)

SUMM2_EHP = SUMM2_EHP.with_columns(
    pl.lit("Y").alias("INDINTERIM"),
    pl.lit (RDATE).alias("DATE")
)

SUMM2_EHP = SUMM2_EHP.with_row_count(name="_N_", offset=1)

SUMM2_EHP = SUMM2_EHP.with_columns(
    pl.col("MAANO").str.slice(2,8).alias("MAANO_SUB")
)

invalid_rows = SUMM2_EHP.filter(pl.col("MAANO_SUB") != (pl.col("_N_")-1))
if invalid_rows.height > 0:
    raise SystemExit(77)

SUMM2 = pl.concat(SUMM2, SUMM2_EHP)

# previous base for concact
query = """
SELECT *
FROM read_parquet('{ELDS_DATA_PATH}/year={PREVDATE.year}/month={PREVDATE.month:02d}/day={PREVDATE.day:02d}/SUMM2.parquet')
"""
PREV_SUMM2 = duckdb.query(query).to_df()

ELDS_SUMM2= pl.concat([PREV_SUMM2, SUMM2])

# write to current base
duckdb.sql(""""
    COPY ELDS_SUMM2 TO '{OUTPUT_DATA_PATH}/SUMM2.parquet' (FORMAT PARQUET)
""")



