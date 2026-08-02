from __future__ import annotations
from pathlib import Path
from datetime import date
import polars as pl
import pyarrow.parquet as pq

BASE = Path("Data_Warehouse")

# ---- paths ----
DEPO_RPT   = BASE / "SAP.PBB.MNITB" / "REPTDATE.parquet"
DEPO_SAV   = BASE / "SAP.PBB.MNITB" / "SAVING.parquet"
DEPO_CUR   = BASE / "SAP.PBB.MNITB" / "CURRENT.parquet"
DEPO_FD    = BASE / "SAP.PBB.MNITB" / "FD.parquet"

IDEPO_SAV  = BASE / "SAP.PIBB.MNITB" / "SAVING.parquet"
IDEPO_CUR  = BASE / "SAP.PIBB.MNITB" / "CURRENT.parquet"
IDEPO_FD   = BASE / "SAP.PIBB.MNITB" / "FD.parquet"

CISCA_DEP  = BASE / "SAP.PBB.CISBEXT.DP" / "DEPOSIT.parquet"
CISDP_DEP  = BASE / "SAP.PBB.CRM.CISBEXT" / "DEPOSIT.parquet"

LN_NOTE    = BASE / "SAP.PBB.MNILN" / "LNNOTE.parquet"
ILN_NOTE   = BASE / "SAP.PIBB.MNILN" / "LNNOTE.parquet"

CRMA_PATH  = BASE / "SAP.PBB.CRMA2MIS.TEXT.parquet"
FOFMT_PATH = BASE / "SAP.PBB.FCYCA" / "FOFMT.parquet"  # PROC FORMAT CNTLOUT (must contain $FORATE.)

OUT_BEP    = BASE / "SAP.PBB.BEP.SASDATA"
OUT_BEP.mkdir(parents=True, exist_ok=True)

# ---- helpers ----
def ddmmyy8_to_date(s: str) -> date:
    dd, mm, yy2 = int(s[0:2]), int(s[2:4]), int(s[4:6])
    yy = 1900 + yy2 if yy2 >= 50 else 2000 + yy2
    return date(yy, mm, dd)

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

# ---- OPTIONS/REPTDATE/NOWK ----
RPT = pl.read_parquet(DEPO_RPT).with_columns(pl.col("REPTDATE").cast(pl.Date))
REPTDATE = RPT.select("REPTDATE").row(0)[0]
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON  = f"{REPTDATE.month:02d}"
RDATE    = f"{REPTDATE.day:02d}{REPTDATE.month:02d}{REPTDATE.year%100:02d}"
NOWK     = "1" if REPTDATE.day == 8 else "2" if REPTDATE.day == 15 else "3" if REPTDATE.day == 22 else "4"

# ---- EXTCRMA (from CRMA) ----
EXTCRMA = pl.read_parquet(CRMA_PATH).select(
    pl.col("NRICNO").cast(pl.Utf8),
    pl.col("CNTIC").cast(pl.Int64),
    pl.col("ACCTNO").cast(pl.Int64),
    pl.col("CNTAC").cast(pl.Int64),
    pl.col("AANO").cast(pl.Utf8),
).sort("ACCTNO")

# ---- PROC FORMAT: $FORATE. via CNTLOUT ----
FOFMT = pl.read_parquet(FOFMT_PATH)
FORATE_MAP = (
    FOFMT.filter(pl.col("FMTNAME") == "$FORATE")
         .select(pl.col("START").alias("CURCODE"), pl.col("LABEL").alias("FORATE_LABEL"))
         .with_columns(pl.col("CURCODE").cast(pl.Utf8),
                       pl.col("FORATE_LABEL").str.replace_all(",", "").cast(pl.Float64).alias("FORATE"))
         .select("CURCODE", "FORATE")
)

# ---- SAVING (DEPO + IDEPO) ----
SAVING = pl.concat([pl.read_parquet(DEPO_SAV), pl.read_parquet(IDEPO_SAV)], how="vertical", rechunk=True)
SAVING = SAVING.join(FORATE_MAP, on="CURCODE", how="left").with_columns([
    pl.when(pl.col("CURCODE") != "MYR").then(pl.col("FORATE")).otherwise(pl.lit(1.0)).alias("FORATE"),
    pl.when(pl.col("CURCODE") != "MYR").then(pl.col("CURBAL")).otherwise(pl.lit(None)).alias("FORBAL"),
]).with_columns([
    pl.when(pl.col("CURCODE") != "MYR").then(pl.col("CURBAL") * pl.col("FORATE")).otherwise(pl.col("CURBAL")).alias("CURBAL"),
    pl.when((pl.col("CURCODE") != "MYR") & (pl.col("CURCODE") != "XAU"))
      .then(pl.col("MTDAVBAL") * pl.col("FORATE")).otherwise(pl.col("MTDAVBAL")).alias("MTDAVBAL"),
]).select([
    "BRANCH","ACCTNO","MTDAVBAL","PRODUCT","OPENDT","OPENIND","CLOSEDT","CURBAL",
    "AVGAMT","INACTIVE","FORBAL","FORATE","CURCODE"
]).unique(subset=["ACCTNO"], keep="first")

# ---- CURRENT (DEPO + IDEPO) ----
CURRENT = pl.concat([pl.read_parquet(DEPO_CUR), pl.read_parquet(IDEPO_CUR)], how="vertical", rechunk=True)
CURRENT = CURRENT.join(FORATE_MAP, on="CURCODE", how="left").with_columns([
    pl.when(pl.col("CURCODE") != "MYR").then(pl.col("FORATE")).otherwise(pl.lit(1.0)).alias("FORATE"),
    pl.lit(None).alias("FORBAL"),  # ensure column exists like SAS KEEP
]).with_columns([
    pl.when(pl.col("CURCODE") != "MYR").then(pl.col("MTDAVBAL") * pl.col("FORATE")).otherwise(pl.col("MTDAVBAL")).alias("MTDAVBAL"),
]).select([
    "BRANCH","ACCTNO","MTDAVBAL","PRODUCT","OPENDT","OPENIND","CLOSEDT","CURBAL",
    "AVGAMT","INACTIVE","FORBAL","FORATE","CURCODE"
]).unique(subset=["ACCTNO"], keep="first")

# ---- FD (DEPO + IDEPO) ----
FD = pl.concat([pl.read_parquet(DEPO_FD), pl.read_parquet(IDEPO_FD)], how="vertical", rechunk=True)
FD = FD.join(FORATE_MAP, on="CURCODE", how="left").with_columns([
    pl.when(pl.col("CURCODE") != "MYR").then(pl.col("FORATE")).otherwise(pl.lit(1.0)).alias("FORATE"),
    pl.lit(None).alias("FORBAL"),  # ensure column exists like SAS KEEP
]).with_columns([
    pl.when(pl.col("CURCODE") != "MYR").then(pl.col("MTDAVBAL") * pl.col("FORATE")).otherwise(pl.col("MTDAVBAL")).alias("MTDAVBAL"),
]).select([
    "BRANCH","ACCTNO","MTDAVBAL","PRODUCT","OPENDT","OPENIND","CLOSEDT","CURBAL",
    "AVGAMT","INACTIVE","FORBAL","FORATE","CURCODE"
]).unique(subset=["ACCTNO"], keep="first")

# ---- CIS filters (SECCUST='901') ----
CISCA = (pl.read_parquet(CISCA_DEP)
           .filter(pl.col("SECCUST") == "901")
           .select(["ACCTNO","CUSTNAM1","NEWIC","OLDIC"])
           .unique(subset=["ACCTNO"], keep="first"))
CISDP = (pl.read_parquet(CISDP_DEP)
           .filter(pl.col("SECCUST") == "901")
           .select(["ACCTNO","CUSTNAM1","NEWIC","OLDIC"])
           .unique(subset=["ACCTNO"], keep="first"))

# ---- merges like SAS (IF A;)
SAVING  = SAVING.join(CISDP, on="ACCTNO", how="inner")
CURRENT = CURRENT.join(CISCA, on="ACCTNO", how="inner")
FD      = FD.join(CISDP, on="ACCTNO", how="inner")

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

# ---- join to EXTCRMA ----
EXTCRMA = EXTCRMA.join(DEPOSIT, on="ACCTNO", how="left").with_columns([
    pl.when(pl.all_horizontal(pl.col("ACCTNO").is_not_null(), pl.col("BRANCH").is_not_null()))
      .then(pl.lit("M")).otherwise(pl.lit("F")).alias("MATCHIND"),
    pl.when((pl.col("INACTIVE") == "") & (pl.col("MATCHIND") == "M"))
      .then(pl.lit("A")).otherwise(pl.col("INACTIVE")).alias("INACTIVE"),
]).sort("AANO")

# ---- match with LOAN ----
LOAN = pl.concat([pl.read_parquet(LN_NOTE), pl.read_parquet(ILN_NOTE)], how="vertical", rechunk=True) \
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
# 1) EXTCRMA dataset (since SAS also creates BEP.EXTCRMA&REPTMON&NOWK)
write_parquet(EXTCRMA, OUT_BEP / f"EXTCRMA{REPTMON}{NOWK}.parquet")

# 2) EXTMIS (fields listed in PUT @...; include CLOSEDD now)
EXTMIS = EXTCRMA.select([
    "NRICNO","CNTIC","ACCTNO","CNTAC","AANO","MATCHIND",
    "MTDAVBAL","PRODUCT","PRODTYPE","OPENYY","OPENMM","OPENDD",
    "OPENIND","INACTIVE","CLOSEYY","CLOSEMM","CLOSEDD",
    "CURBAL","AVGAMT","BRANCH","ESCRACCT","CUSTNAM1","NRICCIS"
])
write_parquet(EXTMIS, OUT_BEP / f"EXTMIS{REPTMON}{NOWK}.parquet")

remove the reptdate.parquet. use datetime timedelta - 1 instead.

all inputs are in sas7bdat. use pyreadstat to read.
output in textfile (and sas7bdat write with saspy and parquet if needed)
