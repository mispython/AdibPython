from __future__ import annotations

from pathlib import Path
from datetime import date
import polars as pl
import duckdb  # noqa: F401 (explicit import per your stack)
import pyarrow as pa  # noqa: F401
import pyarrow.parquet as pq  # noqa: F401


# =========================
# Paths
# =========================
BASE_INPUT  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRCGCS")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# ---- Inputs (Parquet) mirroring SAS libs/members ----
MNITB_CURRENT  = BASE_INPUT / "intg_dp_acct_current_m{reptmon}.sas7bdat"    # SAS: MNITB.CURRENT
MNILN_LNNOTE   = BASE_INPUT / "enrh_ln_note_m{reptmon}.sas7bdat"     # SAS: MNILN.LNNOTE

CRFTABL        = BASE_INPUT / "crftabl.txt"   # SAS: SAP.PBB.BTRADE.CRFTABL
BTRSA_SASDATA_DIR = BASE_INPUT / "btmast{reptmon}{nowk}{reptyear}.sas7bdat"                           # We'll form MAST file dynamically below

COLL_PARQUET   = BASE_INPUT / "LCCRISEX_{yyyy}{mm}{dd}"
DESC_PARQUET   = BASE_INPUT / "LCCRISEX.DESC_{yyyy}{mm}{dd}"

# ---- Output ----
OUT_DIR  = BASE_OUTPUT / "EXCP"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "npgsexcp.sas7bdat"


# =========================
# Helper(s)
# =========================
def sas_days_to_date(days: int) -> date:
    origin = date(1960, 1, 1)
    return origin.fromordinal(origin.toordinal() + int(days))


# =========================
# 1) Macro vars from MNILN.REPTDATE
# =========================
rept = pl.read_parquet(MNILN_REPTDATE)
if rept.height != 1:
    raise ValueError("MNILN.REPTDATE must have exactly one row.")

val = rept.item(0, "REPTDATE")
if isinstance(val, date):
    REPTDATE = val
elif isinstance(val, (int, float)):
    REPTDATE = sas_days_to_date(int(val))
else:
    REPTDATE = date.fromisoformat(str(val))

REPTMON   = f"{REPTDATE.month:02d}"         # PUT(MM, Z2.)
REPTYEAR2 = f"{REPTDATE.year % 100:02d}"    # PUT(REPTDATE, YEAR2.)
REPTDAY   = f"{REPTDATE.day:02d}"           # PUT(DAY(REPTDATE), Z2.)

# BTRSA.MAST&REPTDAY&REPTMON dataset → "BTRADE.SASDATA.MASTDDMM.parquet"
MAST_FILE = BTRSA_SASDATA_DIR / f"BTRADE.SASDATA.MAST{REPTDAY}{REPTMON}.parquet"


# =========================
# 2) CRFT from CRFTABL, filter & map SCH, keep SCH=='   '
# =========================
# SAS INPUT fields expected in Parquet: RECTYP1, TFID, SUBACCT, PREIND, CENSUST, ACCTNO
crft = (
    pl.read_parquet(CRFTABL)
    .filter(pl.col("RECTYP1") != "1")  # IF RECTYP1='1' THEN DELETE; else process
    .select([
        "TFID", "SUBACCT", "PREIND", "CENSUST", "ACCTNO"
    ])
    .with_columns([
        pl.lit("   ").alias("SCH")
    ])
    .with_columns([
        pl.when(pl.col("CENSUST") == 3).then("P51")
         .when(pl.col("CENSUST") == 4).then("P72")
         .when(pl.col("CENSUST") == 5).then("P65")
         .otherwise(pl.col("SCH"))
         .alias("SCH")
    ])
    # Keep only unmapped ('   ') as in SAS: "IF SCH EQ '   ';"
    .filter(pl.col("SCH") == "   ")
)

# NODUPKEY BY ACCTNO CENSUST SUBACCT
crft = crft.unique(subset=["ACCTNO", "CENSUST", "SUBACCT"], keep="first")

# Merge with MAST (BTRSA.MAST&REPTDAY&REPTMON) by ACCTNO; IF A AND B
if not MAST_FILE.exists():
    raise FileNotFoundError(f"Expected MAST file not found: {MAST_FILE}")
mast = pl.read_parquet(MAST_FILE).select(["ACCTNO"]).unique(subset=["ACCTNO"], keep="first")

crft = crft.join(mast, on="ACCTNO", how="inner")
crft = crft.filter(pl.col("ACCTNO") > 0).with_columns([
    pl.lit(0).alias("NOTENO"),
    pl.lit(0).alias("PRODUCT"),
])

# NODUPKEY BY ACCTNO SUBACCT
crft = crft.unique(subset=["ACCTNO", "SUBACCT"], keep="first")

# KEEP ACCTNO CENSUST PRODUCT NOTENO
crft = crft.select(["ACCTNO", "CENSUST", "PRODUCT", "NOTENO"])


# =========================
# 3) CA from MNITB.CURRENT (map→SCH; keep SCH=='   ')
# =========================
ca = (
    pl.read_parquet(MNITB_CURRENT)
    .select(["ACCTNO", "CENSUST", "PRODUCT"])
    .with_columns([
        pl.lit(0).alias("NOTENO"),
        pl.lit("   ").alias("SCH")
    ])
    .with_columns([
        pl.when((pl.col("PRODUCT") == 112) & (pl.col("CENSUST") == 301)).then("P70")
         .when((pl.col("PRODUCT") == 112) & (pl.col("CENSUST") == 300)).then("P51")
         .when((pl.col("PRODUCT") == 112) & (pl.col("CENSUST") == 302)).then("P72")
         .when((pl.col("PRODUCT") == 114) & (pl.col("CENSUST") == 303)).then("P72")
         .when((pl.col("PRODUCT") == 108) & (pl.col("CENSUST") == 304)).then("P75")
         .otherwise(pl.col("SCH"))
         .alias("SCH")
    ])
    .filter(pl.col("SCH") == "   ")  # keep only unmapped
    .select(["ACCTNO", "CENSUST", "PRODUCT", "NOTENO"])
)


# =========================
# 4) LN from MNILN.LNNOTE (map→SCH; keep SCH=='   ')
# =========================
ln = (
    pl.read_parquet(MNILN_LNNOTE)
    .select(["ACCTNO", "NOTENO", "LOANTYPE", "CENSUS"])
    .with_columns([
        pl.col("LOANTYPE").alias("PRODUCT"),
        pl.col("CENSUS").alias("CENSUST"),
        pl.lit("   ").alias("SCH"),
    ])
    .with_columns([
        pl.when((pl.col("LOANTYPE") == 510) & (pl.col("CENSUS").is_in([5.12, 5.13]))).then("P70")
         .when((pl.col("LOANTYPE") == 532) & (pl.col("CENSUS") == 3.00)).then("P51")
         .when((pl.col("LOANTYPE") == 524) & (pl.col("CENSUS") == 5.16)).then("P72")
         .when((pl.col("LOANTYPE") == 527) & (pl.col("CENSUS") == 5.17)).then("P72")
         .when((pl.col("LOANTYPE") == 531) & (pl.col("CENSUS") == 5.00)).then("P63")
         .when((pl.col("LOANTYPE") == 533) & (pl.col("CENSUS") == 533.01)).then("P64")
         .when((pl.col("LOANTYPE") == 533) & (pl.col("CENSUS") == 533.00)).then("P65")
         .otherwise(pl.col("SCH"))
         .alias("SCH")
    ])
    .filter(pl.col("SCH") == "   ")  # keep only unmapped
    .select(["ACCTNO", "NOTENO", "PRODUCT", "CENSUST"])
)


# =========================
# 5) COLL/DESC merge, filter DESC CENSUS range, then BY ACCTNO
# =========================
coll = pl.read_parquet(COLL_PARQUET).select(["CCOLLNO", "ACCTNO"])
desc = pl.read_parquet(DESC_PARQUET).select(["CCOLLNO", "CINSTCL", "NATGUAR", "CENSUS"])

# Filter DESC census range: (51000000 <= CENSUS <= 1099999999)
desc = desc.filter((pl.col("CENSUS") >= 51000000) & (pl.col("CENSUS") <= 1099999999))

# IF A AND B -> inner join on CCOLLNO
coll = coll.join(desc, on="CCOLLNO", how="inner")
# Equivalent of PROC SORT BY ACCTNO is not required for correctness but harmless:
# coll = coll.sort(by=["ACCTNO"])


# =========================
# 6) AAA = SET CA LN CRFT; sort BY ACCTNO
# =========================
aaa = pl.concat(
    [
        ca.select(["ACCTNO", "CENSUST", "PRODUCT", "NOTENO"]),
        ln.select(["ACCTNO", "CENSUST", "PRODUCT", "NOTENO"]),
        crft.select(["ACCTNO", "CENSUST", "PRODUCT", "NOTENO"]),
    ],
    how="vertical",
    rechunk=True
).sort(by=["ACCTNO"])


# =========================
# 7) EXCP.NPGSEXCP = MERGE AAA(IN=A) COLL(IN=B) BY ACCTNO; IF A AND B
# =========================
excp = aaa.join(coll, on="ACCTNO", how="inner")

# Write output
excp.write_parquet(OUT_FILE, use_pyarrow=True)
print(f"Wrote {OUT_FILE}")


all inputs are in sas7bdat sas dataset and need to be in all lowercase.
use pyreadstat to read.
for both LCCRISEX AND LCCRISEX.DESC, both are in flat file which have binary. so need to convert to temp parquet first for readable
remove reptdate, use datetime timedelta - 1 instead. 
output in sas7bdat. 
write out using saspy
