from __future__ import annotations

from pathlib import Path
from datetime import date, datetime
import polars as pl
import duckdb  # noqa: F401
import pyarrow as pa  # noqa: F401
import pyarrow.parquet as pq  # noqa: F401


# =========================
# Paths (adjust as needed)
# =========================
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLTRRF")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# Inputs
LOAN_LNNOTE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat")
LOAN_LNCOMM   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat")

CISLN_LOAN    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/loan.sas7bdat")

COLL_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831")
DESC_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831")
MICR_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLTRRF/BOPESS.txt")

NPGS_TRRF_IN  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLTRRF/trrf.sas7bdat")               # SAS: NPGS.TRRF (prior file for merge)

# Output
OUT_DIR  = BASE_OUTPUT
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = None  # set after REPTMON known


# =========================
# Helpers
# =========================
def sas_days_to_date(days: int) -> date:
    origin = date(1960, 1, 1)
    return origin.fromordinal(origin.toordinal() + int(days))


def parse_mmddyy8_from_z11_prefix_to_date(x) -> date | None:
    """Emulates INPUT(SUBSTR(PUT(x,Z11.),1,8),MMDDYY8.)"""
    if x is None:
        return None
    try:
        xi = int(x)
        if xi <= 0:
            return None
        s = f"{xi:011d}"[:8]
        try:
            return datetime.strptime(s, "%m%d%Y").date()
        except Exception:
            return datetime.strptime(s, "%m%d%y").date()
    except Exception:
        return None


def month_end_of(d: date) -> date:
    # SAS-style month-end (leap if mod 4 == 0)
    if d.month in (1, 3, 5, 7, 8, 10, 12):
        last = 31
    elif d.month in (4, 6, 9, 11):
        last = 30
    else:
        last = 29 if (d.year % 4 == 0) else 28
    return date(d.year, d.month, last)


def month_end_str(d: date | None) -> str:
    if d is None:
        return "          "
    e = month_end_of(d)
    return f"{e.day:02d}/{e.month:02d}/{e.year:04d}"


# =========================
# Macro-like vars from LOAN.REPTDATE
# =========================
rept = pl.read_parquet(LOAN_REPTDATE)
if rept.height != 1:
    raise ValueError("MNILN.REPTDATE must have exactly one row.")

val = rept.item(0, "REPTDATE")
if isinstance(val, date):
    REPTDATE = val
elif isinstance(val, (int, float)):
    REPTDATE = sas_days_to_date(int(val))
else:
    REPTDATE = date.fromisoformat(str(val))

REPTMON  = f"{REPTDATE.month:02d}"
REPTDAY  = f"{REPTDATE.day:02d}"
REPTYEAR = f"{REPTDATE.year:04d}"
SDATE_INT = (REPTDATE - date(1960, 1, 1)).days
OUT_FILE  = OUT_DIR / f"LNTRRF{REPTMON}.parquet"


# =========================
# LOAN0 / LOAN1 from LNNOTE; keep LOANTYPE=575 & CENSUS=575.09
# =========================
lnnote = pl.read_parquet(LOAN_LNNOTE)
loan_base = (
    lnnote
    .with_columns([
        pl.col("LOANTYPE").alias("PRODUCT"),
        pl.col("CENSUS").alias("CENSUST"),
    ])
    .filter((pl.col("LOANTYPE") == 575) & (pl.col("CENSUS") == 575.09))
)

loan1 = loan_base.filter(pl.col("COMMNO") > 0)
loan0 = loan_base.filter(~(pl.col("COMMNO") > 0))

# COMM: NETPROC = CORGAMT - INTAMT
lncomm = pl.read_parquet(LOAN_LNCOMM)
comm = (
    lncomm
    .with_columns([
        pl.when(pl.col("CORGAMT").is_null()).then(0.00).otherwise(pl.col("CORGAMT")).alias("CORGAMT"),
        pl.when(pl.col("INTAMT").is_null()).then(0.00).otherwise(pl.col("INTAMT")).alias("INTAMT"),
    ])
    .with_columns((pl.col("CORGAMT") - pl.col("INTAMT")).alias("NETPROC"))
    .select(["ACCTNO", "COMMNO", "NETPROC"])
    .sort(by=["ACCTNO", "COMMNO"])
)

loan1 = loan1.join(comm, on=["ACCTNO", "COMMNO"], how="inner")
loan  = pl.concat([loan0, loan1], how="vertical", rechunk=True)


# =========================
# Derive ISSUED, NODAYS, ARREARS (NDAYS.), NPLDATE
# =========================
loan = loan.with_columns([
    pl.when(pl.col("ISSUEDT").is_not_null() & (pl.col("ISSUEDT") > 0))
      .then(pl.col("ISSUEDT").cast(pl.Int64)
            .map_elements(parse_mmddyy8_from_z11_prefix_to_date, return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("ISSUED")
])

loan = loan.with_columns([
    pl.when((pl.col("BLDATE") > 0) & (pl.lit(SDATE_INT) > pl.col("BLDATE")))
      .then(pl.lit(SDATE_INT) - pl.col("BLDATE"))
      .otherwise(0)
      .alias("NODAYS")
])

# NDAYS. mapping from CNTLOUT
cntl = pl.read_parquet(PBBLNFMT_CNTLOUT)
ndays_map = (
    cntl.filter(pl.col("FMTNAME").str.to_uppercase() == "NDAYS")
        .select(
            pl.col("START").cast(pl.Int64).alias("START"),
            pl.col("END").cast(pl.Int64).alias("END"),
            pl.col("LABEL").cast(pl.Int64).alias("LABEL")
        )
)

def ndays_informat(n: int) -> int:
    m = ndays_map.filter((pl.lit(n) >= pl.col("START")) & (pl.lit(n) <= pl.col("END")))
    return int(m.item(0, "LABEL")) if m.height > 0 else 0

loan = loan.with_columns([
    pl.col("NODAYS").map_elements(lambda x: ndays_informat(int(x) if x is not None else 0), return_dtype=pl.Int64).alias("ARREARS")
])

# ARREARS==24 → ROUND((NODAYS/365)*12)
loan = loan.with_columns([
    pl.when(pl.col("ARREARS") == 24)
      .then((pl.col("NODAYS").cast(pl.Float64) / 365.0 * 12.0).round(0).cast(pl.Int64))
      .otherwise(pl.col("ARREARS"))
      .alias("ARREARS")
])

# NPLDATE when NODAYS > 89 → month-end of (BLDATE+90)
loan = loan.with_columns([
    pl.when(pl.col("NODAYS") > 89)
      .then(pl.col("BLDATE").cast(pl.Int64)
            .map_elements(lambda d: month_end_of(sas_days_to_date(int(d) + 90)) if d is not None else None,
                          return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("NPLDATE")
])

loan = loan.unique(subset=["ACCTNO", "NOTENO"], keep="first").sort(by=["ACCTNO", "NOTENO"])


# =========================
# CISLN (SECCUST='901') merge by ACCTNO, NODUPKEY
# =========================
cisln = (
    pl.read_parquet(CISLN_LOAN)
      .filter(pl.col("SECCUST") == "901")
      .select(["ACCTNO", "NEWIC", "CUSTNAME", *([c for c in ["NAME"] if c in pl.read_parquet(CISLN_LOAN).columns])])
      .unique(subset=["ACCTNO"], keep="first")
)
loan = loan.join(cisln, on="ACCTNO", how="left")


# =========================
# COLL/DESC with CGCGUR filter and SCH mapping (080→7Q, 090→8Q)
# =========================
coll = pl.read_parquet(COLL_PARQUET).select(["CCOLLNO", "ACCTNO", "NOTENO"])
desc = (
    pl.read_parquet(DESC_PARQUET)
      .select(["CCOLLNO", "CINSTCL", "NATGUAR", "CGCGUR", "CENSUS", "TRANCHE"])
      .filter(pl.col("CGCGUR").is_in(["080", "090"]))
      .with_columns([
          pl.lit("    ").alias("SCH")
      ])
      .with_columns([
          pl.when(pl.col("CGCGUR") == "080").then("7Q")
           .when(pl.col("CGCGUR") == "090").then("8Q")
           .otherwise(pl.col("SCH"))
           .alias("SCH")
      ])
)
coll = coll.join(desc, on="CCOLLNO", how="inner")
coll = coll.filter((pl.col("CINSTCL") == "18") & (pl.col("NATGUAR") == "06"))
coll = coll.sort(by=["ACCTNO", "NOTENO"])

# NPGS = LOAN ⋈ COLL by ACCTNO, NOTENO (inner)
npgs = loan.join(coll, on=["ACCTNO", "NOTENO"], how="inner")

# =========================
# MICR by PENDBRH
# =========================
micr = pl.read_parquet(MICR_PARQUET).select(["PENDBRH", "MICRCD"])
npgs = npgs.join(micr, on="PENDBRH", how="left")

# =========================
# CVAR02 from SCH, keep only non-blank CVAR02
# =========================
npgs = npgs.with_columns([pl.lit("   ").alias("CVAR02")]).with_columns([
    pl.when(pl.col("SCH") == "7Q").then("7Q")
     .when(pl.col("SCH") == "8Q").then("8Q")
     .otherwise(pl.col("CVAR02"))
     .alias("CVAR02")
])
npgs = npgs.filter(pl.col("CVAR02") != "   ")


# =========================
# Final CVAR fields, NORMDT, flags
# =========================
NORMDT = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

npgs = npgs.with_columns([
    pl.col("CENSUS").alias("CVAR01"),
    pl.col("NEWIC").alias("CVAR03"),
    pl.when(pl.col("CUSTNAME") == "  ").then(pl.col("NAME")).otherwise(pl.col("CUSTNAME")).alias("CVAR04"),
    pl.col("ISSUED").alias("CVAR05"),
    pl.col("ACCTNO").alias("CVAR06"),
    pl.lit("FL").alias("CVAR07"),
    pl.col("NETPROC").alias("CVAR08"),
    pl.col("BALANCE").alias("CVAR09"),
    pl.lit(0.00).alias("CVAR10"),
    pl.col("ARREARS").alias("CVAR11"),
    pl.lit("   ").alias("CVAR12"),
    pl.col("NPLDATE").map_elements(
        lambda d: f"{d.day:02d}/{d.month:02d}/{d.year:04d}" if d is not None else "          ",
        return_dtype=pl.Utf8
    ).alias("CVAR13"),
    pl.lit("0233").alias("CVAR14"),
    pl.col("MICRCD").alias("CVAR15"),
    pl.col("PENDBRH").alias("BRANCH"),
    pl.lit("TL").alias("CVAR16"),
    pl.col("CURBAL").alias("CVAR17"),
    pl.lit(NORMDT).alias("NORMDT"),
])

# IF ARREARS GE 3 AND NPLDATE > 0 THEN  CVAR12='NPL'
npgs = npgs.with_columns([
    pl.when((pl.col("ARREARS") >= 3) & pl.col("NPLDATE").is_not_null())
      .then(pl.lit("NPL"))
      .otherwise(pl.col("CVAR12"))
      .alias("CVAR12")
])

# PROC SORT; BY CVAR06 CVAR01
npgs = npgs.sort(by=["CVAR06", "CVAR01"])

# Read prior NPGS.TRRF as NPLA (if present) and merge by CVAR06,CVAR01
if NPGS_TRRF_IN.exists():
    npla = pl.read_parquet(NPGS_TRRF_IN).sort(by=["CVAR06", "CVAR01"])
    # Only keep the columns we need from NPLA for the subsequent logic (STATUS, NDATE)
    keep_npla = [c for c in ["CVAR06", "CVAR01", "STATUS", "NDATE"] if c in npla.columns]
    npla = npla.select(keep_npla) if keep_npla else npla
    npgs = npgs.join(npla, on=["CVAR06", "CVAR01"], how="left")
else:
    # If missing, create placeholders to keep logic intact
    for c in ["STATUS", "NDATE"]:
        if c not in npgs.columns:
            npgs = npgs.with_columns(pl.lit(None).alias(c))

# Apply post-merge CVAR13 logic using STATUS/NDATE/NORMDT
def cvar13_update(row):
    cv12 = row.get("CVAR12")
    status = (row.get("STATUS") or "").strip() if row.get("STATUS") is not None else ""
    ndate  = row.get("NDATE") or "          "
    normdt = row.get("NORMDT") or "          "
    cur13  = row.get("CVAR13") or "          "

    if cv12 == "NPL":
        if status == "NPL":
            return ndate
        return cur13
    else:  # CVAR12 == '   '
        if status == "NPL":
            return normdt
        if (status == "   " or status == "") and ndate != "          ":
            return ndate
        return cur13

npgs = npgs.with_columns([
    pl.struct(["CVAR12", "STATUS", "NDATE", "NORMDT", "CVAR13"]).map_elements(
        cvar13_update, return_dtype=pl.Utf8
    ).alias("CVAR13")
])

# Final sort BY CVAR01
npgs = npgs.sort(by=["CVAR01"])

# Ensure passthrough columns exist for KEEP
for c in ["COSTCTR", "BALANCE", "CURBAL", "ACCRUAL", "TRANCHE", "CGCGUR",
          "CENSUST", "PRODUCT", "NATGUAR", "CINSTCL", "SCH"]:
    if c not in npgs.columns:
        npgs = npgs.with_columns(pl.lit(None).alias(c))

keep_cols = [
    "CVAR01","CVAR02","CVAR03","CVAR04","CVAR05","CVAR06","CVAR07",
    "CVAR08","CVAR09","CVAR10","CVAR11","CVAR12","CVAR13","CVAR14",
    "COSTCTR","BALANCE","CURBAL","ACCRUAL","TRANCHE","CGCGUR",
    "BRANCH","CVAR15","CENSUST","PRODUCT","NATGUAR","CINSTCL","SCH",
    "CVAR16","CVAR17"
]

out = npgs.select(keep_cols)

# =========================
# Output: NPGS.LNTRRF&REPTMON
# =========================
out.write_parquet(OUT_FILE, use_pyarrow=True)
print(f"Wrote {OUT_FILE}")


for loannote and loancomm, need to add filter "WHERE ENTITY_CD != 'PIBB'" (conventional)
all inputs are in sas7bdat sas dataset.
use pyreadstat to read.
remove reptdate, use datetime timedelta - 1 instead. 
output in sas7bdat. 
write out using saspy
