from __future__ import annotations

from pathlib import Path
from datetime import date, datetime
import polars as pl
import duckdb  # noqa: F401 (explicit import as requested)
import pyarrow as pa  # noqa: F401
import pyarrow.parquet as pq  # noqa: F401


# =========================
# Paths (adjust to your env)
# =========================
BASE_INPUT  = Path("parquet_input")
BASE_OUTPUT = Path("parquet_output")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# ---- Input parquet tables (mirror SAS libs/members) ----
# LOAN / LOANI libraries
        # SAS: LOAN.REPTDATE (SAP.PBB.MNILN)
LOAN_LNNOTE   = BASE_INPUT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat"            # SAS: LOAN.LNNOTE
LOAN_LNCOMM   = BASE_INPUT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat"            # SAS: LOAN.LNCOMM

LOANI_LNNOTE  = BASE_INPUT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat"       # SAS: LOANI.LNNOTE (SAP.PIBB.MNILN)
LOANI_LNCOMM  = BASE_INPUT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat"       # SAS: LOANI.LNCOMM

# CISLN
CISLN_LOAN    = BASE_INPUT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/loan.sas7bdat"         # SAS: CISLN.LOAN -> (ACCTNO, NEWIC, CUSTNAME, SECCUST, NAME?)

# COLL / DESC (from fixed-width)
COLL_PARQUET  = BASE_INPUT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831"       # CCOLLNO, ACCTNO, NOTENO
DESC_PARQUET  = BASE_INPUT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831"  # CCOLLNO, CINSTCL, NATGUAR, CENSUS, TRANCHE

# MICR (different source than earlier jobs)
MICR_PARQUET  = BASE_INPUT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/BOPESS.txt"    # PENDBRH, MICRCD

# Historical NPL status file referenced as NPGS.SMEZ
NPGS_SMEZ     = BASE_INPUT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/smez.sas7bdat"                      # expects CVAR06, CVAR01, STATUS, NDATE




# =========================
# Helper functions
# =========================
def sas_days_to_date(days: int) -> date:
    origin = date(1960, 1, 1)
    return origin.fromordinal(origin.toordinal() + int(days))


def ddmmyy8_string(d: date) -> str:
    # SAS DDMMYY8. => dd/mm/yy
    return d.strftime("%d/%m/%y")


def parse_mmddyy8_from_z11_prefix_to_date(x) -> date | None:
    """
    Emulate: INPUT(SUBSTR(PUT(x, Z11.), 1, 8), MMDDYY8.)
    Returns python date or None if invalid/zero.
    """
    if x is None:
        return None
    try:
        xi = int(x)
        if xi <= 0:
            return None
        s = f"{xi:011d}"[:8]  # first 8 chars
        # Prefer MMDDYYYY. If that fails, try MMDDYY.
        try:
            return datetime.strptime(s, "%m%d%Y").date()
        except Exception:
            return datetime.strptime(s, "%m%d%y").date()
    except Exception:
        return None


def month_end_of(d: date) -> date:
    # SAS rule in this job (leap if mod 4 == 0)
    if d.month in (1, 3, 5, 7, 8, 10, 12):
        last = 31
    elif d.month in (4, 6, 9, 11):
        last = 30
    else:
        last = 29 if (d.year % 4 == 0) else 28
    return date(d.year, d.month, last)


def month_end_str(d: date | None) -> str:
    if d is None:
        return "          "  # 10 spaces
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
SDATE     = f"{SDATE_INT:05d}"  # if you need the Z5 string elsewhere


# =========================
# Build LOAN0 / LOAN1 from LOANI.LNNOTE ∪ LOAN.LNNOTE
# =========================
loani_ln = pl.read_parquet(LOANI_LNNOTE)
loan_ln  = pl.read_parquet(LOAN_LNNOTE)

loan_base = (
    pl.concat([loani_ln, loan_ln], how="vertical", rechunk=True)
    .with_columns([
        pl.col("LOANTYPE").alias("PRODUCT"),
        pl.col("CENSUS").alias("CENSUST"),
        # SCH init and mapping logic
        pl.lit("    ").alias("SCH")
    ])
)

# Apply SCH mapping rules
loan_base = loan_base.with_columns([
    pl.when(pl.col("LOANTYPE") == 163).then("P94")
     .when((pl.col("LOANTYPE") == 512) & (pl.col("CENSUS") == 512.01)).then("P93")
     .when((pl.col("LOANTYPE") == 574) & (pl.col("CENSUS") == 574.02)).then("P93")
     .when((pl.col("LOANTYPE") == 512) & (pl.col("CENSUS") == 512.00)).then("P101")
     .otherwise(pl.col("SCH"))
     .alias("SCH")
])

# Keep only rows where SCH != '    '
loan_base = loan_base.filter(pl.col("SCH") != "    ")

# Split by COMMNO
loan1 = loan_base.filter(pl.col("COMMNO") > 0)
loan0 = loan_base.filter(~(pl.col("COMMNO") > 0))

# =========================
# COMM from both libs; compute NETPROC = CORGAMT - INTAMT
# =========================
loani_comm = pl.read_parquet(LOANI_LNCOMM)
loan_comm  = pl.read_parquet(LOAN_LNCOMM)

comm = (
    pl.concat([loani_comm, loan_comm], how="vertical", rechunk=True)
    .with_columns([
        pl.when(pl.col("CORGAMT").is_null()).then(0.00).otherwise(pl.col("CORGAMT")).alias("CORGAMT"),
        pl.when(pl.col("INTAMT").is_null()).then(0.00).otherwise(pl.col("INTAMT")).alias("INTAMT"),
    ])
    .with_columns([
        (pl.col("CORGAMT") - pl.col("INTAMT")).alias("NETPROC")
    ])
    .select("ACCTNO", "COMMNO", "NETPROC")
)

# Merge LOAN1 with COMM on ACCTNO, COMMNO (inner)
loan1 = loan1.join(comm, on=["ACCTNO", "COMMNO"], how="inner")

# Union back
loan = pl.concat([loan0, loan1], how="vertical", rechunk=True)

# =========================
# Derive ISSUED, NODAYS, ARREARS, NPLDATE
# =========================
# ISSUED: from ISSUEDT via Z11 prefix -> MMDDYY8.
loan = loan.with_columns([
    pl.when(pl.col("ISSUEDT").is_not_null() & (pl.col("ISSUEDT") > 0))
      .then(pl.col("ISSUEDT").cast(pl.Int64)
            .map_elements(parse_mmddyy8_from_z11_prefix_to_date, return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("ISSUED")
])

# NODAYS: if BLDATE > 0 and SDATE > BLDATE then SDATE - BLDATE, else 0
loan = loan.with_columns([
    pl.when((pl.col("BLDATE") > 0) & (pl.lit(SDATE_INT) > pl.col("BLDATE")))
      .then(pl.lit(SDATE_INT) - pl.col("BLDATE"))
      .otherwise(0)
      .alias("NODAYS")
])

# ARREARS via NDAYS. informat (non-equi mapping)
cntl = pl.read_parquet(PBBLNFMT_CNTLOUT)
ndays_map = (
    cntl.filter(pl.col("FMTNAME").str.to_uppercase() == "NDAYS")
        .select(
            pl.col("START").cast(pl.Int64).alias("START"),
            pl.col("END").cast(pl.Int64).alias("END"),
            pl.col("LABEL").cast(pl.Int64).alias("LABEL")
        )
)

def ndays_informat(nodays: int) -> int:
    if nodays is None:
        return 0
    m = ndays_map.filter(
        (pl.lit(nodays) >= pl.col("START")) & (pl.lit(nodays) <= pl.col("END"))
    )
    return int(m.item(0, "LABEL")) if m.height > 0 else 0

loan = loan.with_columns([
    pl.col("NODAYS").map_elements(lambda x: ndays_informat(int(x) if x is not None else 0), return_dtype=pl.Int64).alias("ARREARS")
])

# Special case ARREARS==24 -> ROUND((NODAYS/365)*12)
loan = loan.with_columns([
    pl.when(pl.col("ARREARS") == 24)
      .then((pl.col("NODAYS").cast(pl.Float64) / 365.0 * 12.0).round(0).cast(pl.Int64))
      .otherwise(pl.col("ARREARS"))
      .alias("ARREARS")
])

# NPLDATE when NODAYS > 89 -> set to month-end of (BLDATE+90)
loan = loan.with_columns([
    pl.when(pl.col("NODAYS") > 89)
      .then(pl.col("BLDATE").cast(pl.Int64)
            .map_elements(lambda d: month_end_of(sas_days_to_date(int(d) + 90)) if d is not None else None,
                          return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("NPLDATE")
])

# Deduplicate LOAN by ACCTNO, NOTENO (NODUPKEY)
loan = loan.unique(subset=["ACCTNO", "NOTENO"], keep="first")

# =========================
# Merge CISLN (SECCUST = '901')
# =========================
cisln = (
    pl.read_parquet(CISLN_LOAN)
      .filter(pl.col("SECCUST") == "901")
      .select(["ACCTNO", "NEWIC", "CUSTNAME", *([c for c in ["NAME"] if c in pl.read_parquet(CISLN_LOAN).columns])])
      .unique(subset=["ACCTNO"], keep="first")
)
loan = loan.join(cisln, on="ACCTNO", how="left")

# =========================
# COLL/DESC merge with filter (CINSTCL='18' AND NATGUAR='06')
# =========================
coll = pl.read_parquet(COLL_PARQUET).select(["CCOLLNO", "ACCTNO", "NOTENO"])
desc = pl.read_parquet(DESC_PARQUET).select(["CCOLLNO", "CINSTCL", "NATGUAR", "CENSUS", "TRANCHE"])
coll = coll.join(desc, on="CCOLLNO", how="inner")
coll = coll.filter((pl.col("CINSTCL") == "18") & (pl.col("NATGUAR") == "06"))

# BY ACCTNO NOTENO inner merge to NPGS
npgs = loan.join(coll, on=["ACCTNO", "NOTENO"], how="inner")

# =========================
# MICR merge by PENDBRH (MICR file has PENDBRH,MICRCD)
# =========================
micr = pl.read_parquet(MICR_PARQUET).select(["PENDBRH", "MICRCD"])
npgs = npgs.join(micr, on="PENDBRH", how="left")

# =========================
# CVAR02 mapping from SCH, then filter non-blank
# =========================
npgs = npgs.with_columns([
    pl.lit("   ").alias("CVAR02")
]).with_columns([
    pl.when(pl.col("SCH") == "P93").then("93")
     .when(pl.col("SCH") == "P94").then("94")
     .when(pl.col("SCH") == "P101").then("101")
     .otherwise(pl.col("CVAR02"))
     .alias("CVAR02")
])

# Keep only rows with CVAR02 != '   '
npgs = npgs.filter(pl.col("CVAR02") != "   ")

# =========================
# Final CVAR fields + NORMDT and NPL flags
# =========================
npgs = npgs.with_columns([
    pl.col("CENSUS").alias("CVAR01"),
    pl.col("NEWIC").alias("CVAR03"),
    # CVAR04 fallback: if '  ' then NAME
    pl.when(pl.col("CUSTNAME") == "  ").then(pl.col("NAME")).otherwise(pl.col("CUSTNAME")).alias("CVAR04"),
    pl.col("ISSUED").alias("CVAR05"),
    pl.col("ACCTNO").alias("CVAR06"),
    pl.lit("FL").alias("CVAR07"),
    pl.col("NETPROC").alias("CVAR08"),
    pl.col("BALANCE").alias("CVAR09"),
    pl.lit(0.00).alias("CVAR10"),
    pl.col("ARREARS").alias("CVAR11"),
    pl.lit("   ").alias("CVAR12"),
    pl.col("NPLDATE").map_elements(lambda d: f"{d.day:02d}/{d.month:02d}/{d.year:04d}" if d is not None else "          ",
                                   return_dtype=pl.Utf8).alias("CVAR13"),
    pl.lit("0233").alias("CVAR14"),
    pl.col("MICRCD").alias("CVAR15"),
    pl.col("PENDBRH").alias("BRANCH"),
    pl.lit("TL").alias("CVAR16"),
    pl.col("CURBAL").alias("CVAR17"),
])

# NORMDT = REPTDAY/REPTMON/REPTYEAR
NORMDT = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"
npgs = npgs.with_columns([pl.lit(NORMDT).alias("NORMDT")])

# IF ARREARS GE 3 AND NPLDATE > 0 THEN CVAR12='NPL'
npgs = npgs.with_columns([
    pl.when((pl.col("ARREARS") >= 3) & pl.col("NPLDATE").is_not_null())
      .then(pl.lit("NPL"))
      .otherwise(pl.col("CVAR12"))
      .alias("CVAR12")
])

# =========================
# Merge with NPGS.SMEZ (NPLA) by CVAR06, CVAR01 for final CVAR13 adjustments
# =========================
# Sort like SAS prior to merge (not necessary for join correctness, but kept for parity)
npgs = npgs.sort(by=["CVAR06", "CVAR01"])

if NPGS_SMEZ.exists():
    npla = pl.read_parquet(NPGS_SMEZ).sort(by=["CVAR06", "CVAR01"])
    # Expect fields: CVAR06, CVAR01, STATUS, NDATE
    npgs = npgs.join(npla.select(["CVAR06", "CVAR01", "STATUS", "NDATE"]), on=["CVAR06", "CVAR01"], how="left")
else:
    # If historical file not available, create empty columns to preserve logic
    npgs = npgs.with_columns([
        pl.lit(None).alias("STATUS"),
        pl.lit("          ").alias("NDATE")
    ])

# Now apply SAS logic to adjust CVAR13 using STATUS/NDATE and NORMDT
def adjust_cvar13(row):
    c12   = row.get("CVAR12") or "   "
    stat  = row.get("STATUS") or "   "
    ndate = row.get("NDATE")  or "          "
    c13   = row.get("CVAR13") or "          "
    normdt= row.get("NORMDT") or "          "

    if c12 == "NPL":
        if stat == "NPL":
            return ndate
        return c13
    else:
        if stat == "NPL":
            return normdt
        if stat == "   " and ndate != "          ":
            return ndate
        return c13

npgs = npgs.with_columns([
    pl.struct(["CVAR12", "STATUS", "NDATE", "CVAR13", "NORMDT"]).map_elements(adjust_cvar13, return_dtype=pl.Utf8).alias("CVAR13")
])

# =========================
# Final sort & KEEP list
# =========================
npgs = npgs.sort(by=["CVAR01"])

# Ensure passthrough columns exist (if absent upstream, create as nulls)
for c in ["COSTCTR", "BALANCE", "CURBAL", "ACCRUAL", "TRANCHE", "SCH"]:
    if c not in npgs.columns:
        npgs = npgs.with_columns(pl.lit(None).alias(c))

keep_cols = [
    "CVAR01","CVAR02","CVAR03","CVAR04","CVAR05","CVAR06","CVAR07",
    "CVAR08","CVAR09","CVAR10","CVAR11","CVAR12","CVAR13","CVAR14",
    "COSTCTR","BALANCE","CURBAL","ACCRUAL","TRANCHE",
    "BRANCH","CVAR15","CENSUST","PRODUCT","NATGUAR","CINSTCL","SCH",
    "CVAR16","CVAR17"
]

out = npgs.select(keep_cols)

# =========================
# Output: NPGS.LNSMEZ&REPTMON (Parquet)
# =========================
out_dir = BASE_OUTPUT / "NPGS"
out_dir.mkdir(parents=True, exist_ok=True)
out_file = out_dir / f"LNSMEZ{REPTMON}.parquet"
out.write_parquet(out_file, use_pyarrow=True)
print(f"Wrote {out_file}")


remove the base input as every datasets from differnet path
for loan, need to add filter "WHERE ENTITY_CD != 'PIBB'" (conventional)
for iloan, need to add filter of "WHERE ENTITY_CD = 'PIBB'" (islamic)
all inputs are in sas7bdat sas dataset and need to be in all lowercase.
use pyreadstat to read.
remove reptdate, use datetime timedelta - 1 instead. 
output in sas7bdat. 
write out using saspy
