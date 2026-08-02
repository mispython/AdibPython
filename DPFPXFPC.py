from __future__ import annotations
from pathlib import Path
from datetime import date, datetime, timedelta
import polars as pl
import pyarrow.parquet as pq
import pyreadstat
import saspy

BASE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWCRMA")

# ---- paths ----
DEPO_SAV   = BASE / "conv" / "saving.sas7bdat"
DEPO_CUR   = BASE / "conv" / "current.sas7bdat"
DEPO_FD    = BASE / "conv" / "fd.sas7bdat"

IDEPO_SAV  = BASE / "islamic" / "saving.sas7bdat"
IDEPO_CUR  = BASE / "islamic" / "current.sas7bdat"
IDEPO_FD   = BASE / "islamic" / "fd.sas7bdat"

CISCA_DEP  = BASE / "cisca" / "deposit.sas7bdat"
CISDP_DEP  = BASE / "cisdp" / "deposit.sas7bdat"

LN_NOTE    = BASE / "conv" / "lnnote.sas7bdat"
ILN_NOTE   = BASE / "islamic" / "lnnote.sas7bdat"

OUT_BEP    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBWCRMA")
OUT_BEP.mkdir(parents=True, exist_ok=True)

# ---- helpers ----
def read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read SAS7BDAT file and convert to Polars DataFrame"""
    df, meta = pyreadstat.read_sas7bdat(str(path))
    return pl.from_pandas(df)

def write_sas7bdat(df: pl.DataFrame, path: Path):
    """Write DataFrame to SAS7BDAT format using saspy"""
    sas = saspy.SASsession()
    # Convert to pandas for SAS transfer
    pdf = df.to_pandas()
    # Upload to SAS
    sas.df2sd(pdf, table='temp_table')
    # Export as sas7bdat
    sas.submit(f"""
        PROC EXPORT DATA=temp_table
            OUTFILE="{path}"
            DBMS=SAS7BDAT REPLACE;
        RUN;
    """)
    sas.endsas()

def write_textfile(df: pl.DataFrame, path: Path, delimiter: str = '|'):
    """Write DataFrame to delimited text file"""
    df.write_csv(path, separator=delimiter)

def z11_first8_to_mmddyyyy_date(n) -> date | None:
    if n is None:
        return None
    try:
        s = str(int(n)).zfill(11)[:8]  # MMDDYYYY
        return date(int(s[4:8]), int(s[0:2]), int(s[2:4]))
    except Exception:
        return None

def write_parquet(df: pl.DataFrame, path: Path):
    pq.write_table(df.to_arrow(), path)

# Common columns needed for deposit processing
COMMON_COLS = ["BRANCH","ACCTNO","MTDAVBAL","PRODUCT","OPENDT","OPENIND","CLOSEDT","CURBAL",
               "AVGAMT","INACTIVE","FORBAL","FORATE","CURCODE"]

# ---- REPTDATE using datetime.now() - 1 day ----
NOW = datetime.now()
YESTERDAY = NOW - timedelta(days=1)
REPTDATE = YESTERDAY.date()
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON  = f"{REPTDATE.month:02d}"
RDATE    = f"{REPTDATE.day:02d}{REPTDATE.month:02d}{REPTDATE.year%100:02d}"
NOWK     = "1" if REPTDATE.day == 8 else "2" if REPTDATE.day == 15 else "3" if REPTDATE.day == 22 else "4"

# ---- SAVING (DEPO + IDEPO) ----
SAVING_DEPO = read_sas7bdat(DEPO_SAV)
SAVING_IDEPO = read_sas7bdat(IDEPO_SAV)

# Find common columns between the two DataFrames
common_saving_cols = [col for col in COMMON_COLS if col in SAVING_DEPO.columns and col in SAVING_IDEPO.columns]

# Select only common columns
SAVING_DEPO = SAVING_DEPO.select(common_saving_cols)
SAVING_IDEPO = SAVING_IDEPO.select(common_saving_cols)

SAVING = pl.concat([SAVING_DEPO, SAVING_IDEPO], how="vertical", rechunk=True)
SAVING = SAVING.unique(subset=["ACCTNO"], keep="first")

# ---- CURRENT (DEPO + IDEPO) ----
CURRENT_DEPO = read_sas7bdat(DEPO_CUR)
CURRENT_IDEPO = read_sas7bdat(IDEPO_CUR)

# Find common columns between the two DataFrames
common_current_cols = [col for col in COMMON_COLS if col in CURRENT_DEPO.columns and col in CURRENT_IDEPO.columns]

# Select only common columns
CURRENT_DEPO = CURRENT_DEPO.select(common_current_cols)
CURRENT_IDEPO = CURRENT_IDEPO.select(common_current_cols)

CURRENT = pl.concat([CURRENT_DEPO, CURRENT_IDEPO], how="vertical", rechunk=True)
CURRENT = CURRENT.unique(subset=["ACCTNO"], keep="first")

# ---- FD (DEPO + IDEPO) ----
FD_DEPO = read_sas7bdat(DEPO_FD)
FD_IDEPO = read_sas7bdat(IDEPO_FD)

# Find common columns between the two DataFrames
common_fd_cols = [col for col in COMMON_COLS if col in FD_DEPO.columns and col in FD_IDEPO.columns]

# Select only common columns
FD_DEPO = FD_DEPO.select(common_fd_cols)
FD_IDEPO = FD_IDEPO.select(common_fd_cols)

FD = pl.concat([FD_DEPO, FD_IDEPO], how="vertical", rechunk=True)
FD = FD.unique(subset=["ACCTNO"], keep="first")

# ---- CIS filters (SECCUST='901') ----
CISCA = (read_sas7bdat(CISCA_DEP)
           .filter(pl.col("SECCUST") == "901")
           .select(["ACCTNO","CUSTNAM1","NEWIC","OLDIC"])
           .unique(subset=["ACCTNO"], keep="first"))
CISDP = (read_sas7bdat(CISDP_DEP)
           .filter(pl.col("SECCUST") == "901")
           .select(["ACCTNO","CUSTNAM1","NEWIC","OLDIC"])
           .unique(subset=["ACCTNO"], keep="first"))

# ---- merges like SAS (IF A;) ----
SAVING  = SAVING.join(CISDP, on="ACCTNO", how="inner")
CURRENT = CURRENT.join(CISCA, on="ACCTNO", how="inner")
FD      = FD.join(CISDP, on="ACCTNO", how="inner")

# Find common columns across all three deposit types for final union
common_deposit_cols = list(set(SAVING.columns) & set(CURRENT.columns) & set(FD.columns))

# Ensure key columns are included
key_cols = ["ACCTNO","CUSTNAM1","NEWIC","OLDIC","BRANCH","MTDAVBAL","PRODUCT",
            "OPENDT","OPENIND","CLOSEDT","CURBAL","AVGAMT","INACTIVE"]
common_deposit_cols = [col for col in key_cols if col in common_deposit_cols] + \
                      [col for col in common_deposit_cols if col not in key_cols]

# Select common columns for each DataFrame
SAVING = SAVING.select(common_deposit_cols)
CURRENT = CURRENT.select(common_deposit_cols)
FD = FD.select(common_deposit_cols)

# ---- union -> DEPOSIT ----
DEPOSIT = pl.concat([SAVING, CURRENT, FD], how="vertical", rechunk=True)

# NRICCIS selection
bad_ic = {"","00000000000","0","-"}
DEPOSIT = DEPOSIT.with_columns([
    pl.when(~pl.col("NEWIC").is_in(bad_ic)).then(pl.col("NEWIC"))
      .when(~pl.col("OLDIC").is_in(bad_ic)).then(pl.col("OLDIC"))
      .otherwise(pl.lit("")).alias("NRICCIS")
])

# OPENDT/CLOSEDT numeric -> date via Z11 first8 (MMDDYYYY)
DEPOSIT = DEPOSIT.with_columns([
    pl.col("OPENDT").map_elements(z11_first8_to_mmddyyyy_date).alias("OPENDT"),
    pl.col("CLOSEDT").map_elements(z11_first8_to_mmddyyyy_date).alias("CLOSEDT"),
]).unique(subset=["ACCTNO"], keep="first").sort("ACCTNO")

# ---- EXTCRMA creation (since we don't have CRMA_PATH) ----
# Create EXTCRMA from DEPOSIT data
EXTCRMA = DEPOSIT.select([
    "ACCTNO","CUSTNAM1","NRICCIS","BRANCH","MTDAVBAL","PRODUCT",
    "OPENDT","OPENIND","CLOSEDT","CURBAL","AVGAMT","INACTIVE"
]).with_columns([
    pl.lit("M").alias("MATCHIND"),  # Since all are matched deposits
    pl.col("NRICCIS").alias("NRICNO"),
    pl.lit(None).cast(pl.Int64).alias("CNTIC"),
    pl.lit(None).cast(pl.Int64).alias("CNTAC"),
    pl.lit(None).cast(pl.Utf8).alias("AANO"),
    pl.when((pl.col("INACTIVE") == ""))
      .then(pl.lit("A")).otherwise(pl.col("INACTIVE")).alias("INACTIVE"),
]).sort("ACCTNO")

# ---- match with LOAN ----
LOAN = pl.concat([read_sas7bdat(LN_NOTE), read_sas7bdat(ILN_NOTE)], how="vertical", rechunk=True) \
         .filter(pl.col("VINNO") != "") \
         .select(pl.col("VINNO").alias("AANO"), pl.col("ESCRACCT"))
EXTCRMA = EXTCRMA.join(LOAN, on="AANO", how="left")

# derive open/close parts, rounding to cents*100, PRODTYPE
def prodtype_from_acctno(v: int | None) -> str | None:
    if v is None: return None
    if (4000000000 <= v <= 4999999999) or (5000000000 <= v <= 5999999999) or (6000000000 <= v <= 6589999999) or (6600000000 <= v <= 6999999999): return "SA"
    if (3000000000 <= v <= 3589999999) or (3600000000 <= v <= 3999999999): return "CA"
    if (1000000000 <= v <= 1589999999) or (1600000000 <= v <= 1999999999) or (7000000000 <= v <= 7999999999): return "FD"
    if (1590000000 <= v <= 1599999999) or (1689999999 <= v <= 1699999999) or (1789999999 <= v <= 1799999999): return "FCYFD"
    if (3590000000 <= v <= 3599999999) or (3790000000 <= v <= 3799999999): return "FCYCA"
    if (6590000000 <= v <= 6599999999): return "GIA"
    return None

EXTCRMA = EXTCRMA.with_columns([
    pl.when(pl.col("OPENDT").is_not_null()).then(pl.col("OPENDT").dt.day()).otherwise(pl.lit(0)).alias("OPENDD"),
    pl.when(pl.col("OPENDT").is_not_null()).then(pl.col("OPENDT").dt.month()).otherwise(pl.lit(0)).alias("OPENMM"),
    pl.when(pl.col("OPENDT").is_not_null()).then(pl.col("OPENDT").dt.year()).otherwise(pl.lit(0)).alias("OPENYY"),
    pl.when(pl.col("CLOSEDT").is_not_null()).then(pl.col("CLOSEDT").dt.day()).otherwise(pl.lit(0)).alias("CLOSEDD"),
    pl.when(pl.col("CLOSEDT").is_not_null()).then(pl.col("CLOSEDT").dt.month()).otherwise(pl.lit(0)).alias("CLOSEMM"),
    pl.when(pl.col("CLOSEDT").is_not_null()).then(pl.col("CLOSEDT").dt.year()).otherwise(pl.lit(0)).alias("CLOSEYY"),
    (pl.col("MTDAVBAL").round(2) * 100).alias("MTDAVBAL"),
    (pl.col("AVGAMT").round(2)   * 100).alias("AVGAMT"),
    (pl.col("CURBAL").round(2)   * 100).alias("CURBAL"),
]).with_columns([
    pl.col("ACCTNO").map_elements(prodtype_from_acctno).alias("PRODTYPE")
]).sort("ACCTNO")

# ---- write outputs ----
# 1) EXTCRMA dataset
base_name = f"EXTCRMA{REPTMON}{NOWK}"
# Text file (pipe-delimited)
write_textfile(EXTCRMA, OUT_BEP / f"{base_name}.txt")
# SAS7BDAT file
write_sas7bdat(EXTCRMA, OUT_BEP / f"{base_name}.sas7bdat")
# Parquet file
write_parquet(EXTCRMA, OUT_BEP / f"{base_name}.parquet")

# 2) EXTMIS dataset
EXTMIS = EXTCRMA.select([
    "NRICNO","CNTIC","ACCTNO","CNTAC","AANO","MATCHIND",
    "MTDAVBAL","PRODUCT","PRODTYPE","OPENYY","OPENMM","OPENDD",
    "OPENIND","INACTIVE","CLOSEYY","CLOSEMM","CLOSEDD",
    "CURBAL","AVGAMT","BRANCH","ESCRACCT","CUSTNAM1","NRICCIS"
])

base_name_mis = f"EXTMIS{REPTMON}{NOWK}"
# Text file (pipe-delimited)
write_textfile(EXTMIS, OUT_BEP / f"{base_name_mis}.txt")
# SAS7BDAT file
write_sas7bdat(EXTMIS, OUT_BEP / f"{base_name_mis}.sas7bdat")
# Parquet file
write_parquet(EXTMIS, OUT_BEP / f"{base_name_mis}.parquet")
