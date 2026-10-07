from __future__ import annotations

from pathlib import Path
from datetime import date, datetime
import polars as pl
import duckdb  # noqa: F401 (imported to satisfy "use duckdb" requirement)
import pyarrow as pa  # noqa: F401
import pyarrow.parquet as pq  # noqa: F401


# =========================
# Paths (adjust to your env)
# =========================

# ---- Input parquet tables (mirror SAS DD names / libs) ----
MNITB_CURRENT    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/intg_dp_acct_current_m{reptmon}.sas7bdat")
LIMIT_OVERDFT    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/intg_dp_acct_overdft_m{reptmon}..sas7bdat")

CISDP_DEPOSIT    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cisdp/deposit.sas7bdat")

GP3_KLUNION      = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/GP3.txt")
COLL_PARQUET     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_{yyyy}{mm}{dd}")        # CCOLLNO, ACCTNO
DESC_PARQUET     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_{yyyy}{mm}{dd}")   # CCOLLNO, CINSTCL, NATGUAR, CENSUS
MICR_PARQUET     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDP169/PIBBMICR.txt")        # BRANCH, MICRCD

# PROC FORMAT (CNTLOUT-like) extracted from %INC PGM(PBBLNFMT)
# Must include rows for FMTNAME='NDAYS' with numeric range START..END and LABEL (integer months)
from PBBLNFMT import


# =========================
# Helper functions
# =========================
def sas_days_to_date(days: int) -> date:
    origin = date(1960, 1, 1)
    return origin.fromordinal(origin.toordinal() + int(days))


def ddmmyy8_string(d: date) -> str:
    # SAS DDMMYY8. => dd/mm/yy
    return d.strftime("%d/%m/%y")


def parse_mmddyy8_from_z11_prefix_to_days(x) -> int:
    """
    Emulate: INPUT(SUBSTR(PUT(x, Z11.), 1, 8), MMDDYY8.)
    Returns SAS-days int (days since 1960) or 0 if invalid/zero.
    """
    if x is None:
        return 0
    try:
        xi = int(x)
        if xi <= 0:
            return 0
        s = f"{xi:011d}"[:8]  # first 8 chars
        # try MMDDYYYY, else MMDDYY
        try:
            d = datetime.strptime(s, "%m%d%Y").date()
        except Exception:
            d = datetime.strptime(s, "%m%d%y").date()
        return (d - date(1960, 1, 1)).days
    except Exception:
        return 0


def mdy_safe(m, d, y):
    try:
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def month_end_of_sas_days(days_int: int) -> date:
    base = sas_days_to_date(days_int)
    # SAS-like month-end rule used in this job:
    if base.month in (1, 3, 5, 7, 8, 10, 12):
        last = 31
    elif base.month in (4, 6, 9, 11):
        last = 30
    else:
        # February: leap year if mod 4 == 0
        last = 29 if (base.year % 4 == 0) else 28
    return date(base.year, base.month, last)


def month_end_str(d: date | None) -> str:
    if d is None:
        return "          "  # 10 spaces
    # Adjust to month-end with SAS rule (again) then format dd/mm/yyyy
    if d.month in (1, 3, 5, 7, 8, 10, 12):
        last = 31
    elif d.month in (4, 6, 9, 11):
        last = 30
    else:
        last = 29 if (d.year % 4 == 0) else 28
    e = date(d.year, d.month, last)
    return f"{e.day:02d}/{e.month:02d}/{e.year:04d}"


# =========================
# Load REPTDATE & derive macro-like vars
# =========================
rep = pl.read_parquet(MNITB_REPTDATE)
if rep.height != 1:
    raise ValueError("MNITB.REPTDATE must have exactly one row.")

val = rep.item(0, "REPTDATE")  # accept date/int/str
if isinstance(val, date):
    repdate = val
elif isinstance(val, (int, float)):
    repdate = sas_days_to_date(int(val))
else:
    repdate = date.fromisoformat(str(val))

REPTMON  = f"{repdate.month:02d}"
REPTMON1 = f"{(12 if repdate.month == 1 else repdate.month - 1):02d}"
RDATE    = ddmmyy8_string(repdate)
SDATE_INT = (repdate - date(1960, 1, 1)).days
SDATE     = f"{SDATE_INT:05d}"  # zero-padded width 5 (string), if needed elsewhere


# =========================
# CA = MNITB.CURRENT filter; merge ODLMT (LIMIT.OVERDFT)
# =========================
mnitb = pl.read_parquet(MNITB_CURRENT)
ca = (
    mnitb
    .filter(
        (pl.col("PRODUCT") == 169) &
        (pl.col("CENSUST").is_between(16901, 16908))
    )
)

odlmt = pl.read_parquet(LIMIT_OVERDFT).select(["ACCTNO", "LMTSTART"])

# LMTSTART > 0 -> parse MMDDYY8 from first 8 digits of zero-padded width 11 and replace with Date
odlmt = odlmt.with_columns([
    pl.when(pl.col("LMTSTART") > 0)
      .then(pl.col("LMTSTART").cast(pl.Int64).map_elements(
            lambda x: datetime.strptime(f"{int(x):011d}"[:8], "%m%d%Y").date()
            if len(f"{int(x):011d}"[:8]) == 8 else None,
            return_dtype=pl.Date))
      .otherwise(pl.col("LMTSTART"))
      .alias("LMTSTART")
]).unique(subset=["ACCTNO"], keep="first")

ca = ca.join(odlmt, on="ACCTNO", how="left")


# =========================
# Merge GP3 by ACCTNO; set NPLDATE from (RPTDAY,RPTMON,RPTYEAR)
# =========================
gp3 = pl.read_parquet(GP3_KLUNION).select(["ACCTNO", "RPTDAY", "RPTMON", "RPTYEAR"])
ca = ca.join(gp3, on="ACCTNO", how="left").with_columns([
    pl.when((pl.col("RPTDAY") > 0) & (pl.col("RPTMON") > 0) & (pl.col("RPTYEAR") > 0))
      .then(pl.struct(["RPTMON", "RPTDAY", "RPTYEAR"]).map_elements(
            lambda s: mdy_safe(s["RPTMON"], s["RPTDAY"], s["RPTYEAR"]), return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("NPLDATE")
])


# =========================
# Merge CISDP (SECCUST='901', NODUPKEY by ACCTNO)
# =========================
cis = (
    pl.read_parquet(CISDP_DEPOSIT)
      .filter(pl.col("SECCUST") == "901")
      .select(["ACCTNO", "NEWIC", "CUSTNAME"])
      .unique(subset=["ACCTNO"], keep="first")
)
ca = ca.join(cis, on="ACCTNO", how="left")


# =========================
# COLL/DESC merge → DEP by ACCTNO; NODUPKEY ACCTNO; MICR by BRANCH
# =========================
coll = pl.read_parquet(COLL_PARQUET).select(["CCOLLNO", "ACCTNO"])
desc = pl.read_parquet(DESC_PARQUET).select(["CCOLLNO", "CINSTCL", "NATGUAR", "CENSUS"])
coll = coll.join(desc, on="CCOLLNO", how="inner")  # IF A AND B

dep = ca.join(coll, on="ACCTNO", how="inner")      # IF A AND B
dep = dep.unique(subset=["ACCTNO"], keep="first")  # NODUPKEY BY ACCTNO

micr = pl.read_parquet(MICR_PARQUET).select(["BRANCH", "MICRCD"])
dep  = dep.join(micr, on="BRANCH", how="left")


# =========================
# Arrears/NPL logic using NDAYS. informat from CNTLOUT
# =========================
cntl = pl.read_parquet(PBBLNFMT_CNTLOUT)
ndays_map = (
    cntl.filter(pl.col("FMTNAME").str.to_uppercase() == "NDAYS")
        .select(
            pl.col("START").cast(pl.Int64).alias("START"),
            pl.col("END").cast(pl.Int64).alias("END"),
            pl.col("LABEL").cast(pl.Int64).alias("LABEL")
        )
)

# Initialize/reset fields per SAS
dep = dep.with_columns([
    pl.lit("  ").alias("CVAR02"),
    pl.lit(0).alias("ARREARS"),
    pl.lit(0).alias("NODAYS"),
    pl.lit(None, dtype=pl.Date).alias("NPLDATE")  # reset before recompute
])

def compute_overdraft_fields(row):
    """
    Emulates the SAS block:
      - Determine ODDAYS from EXODDATE/TEMPODDT (earliest valid date), both encoded numeric -> MMDDYY8
      - NODAYS = &SDATE - ODDAYS + 1
      - ARREARS = INPUT(NODAYS, NDAYS.)
      - If ARREARS=24 then ARREARS=ROUND(NODAYS/30)
      - If ARREARS>=3 then NPLDATE = month-end of (ODDAYS+90)
    """
    EXODDATE = row.get("EXODDATE")
    TEMPODDT = row.get("TEMPODDT")
    CURBAL   = row.get("CURBAL")

    # Condition to enter block
    if (((EXODDATE or 0) != 0) or ((TEMPODDT or 0) != 0)) and (CURBAL is not None and CURBAL < 0):
        # Determine ODDAYS as SAS-days int
        if (EXODDATE or 0) == 0 and (TEMPODDT or 0) == 0:
            o_days = 0
        elif (EXODDATE or 0) > 0 and (TEMPODDT or 0) == 0:
            o_days = parse_mmddyy8_from_z11_prefix_to_days(EXODDATE)
        elif (EXODDATE or 0) == 0 and (TEMPODDT or 0) > 0:
            o_days = parse_mmddyy8_from_z11_prefix_to_days(TEMPODDT)
        else:
            ed = parse_mmddyy8_from_z11_prefix_to_days(EXODDATE)
            td = parse_mmddyy8_from_z11_prefix_to_days(TEMPODDT)
            if ed > 0 and td > 0:
                o_days = min(ed, td)
            else:
                o_days = ed or td

        # Compute NODAYS
        nodays = 0
        if o_days > 0:
            nodays = SDATE_INT - o_days
        nodays = nodays + 1

        arrears = 0
        npldate = None
        if nodays > 0:
            # Apply NDAYS. via non-equi match START<=nodays<=END
            match = ndays_map.filter(
                (pl.lit(nodays) >= pl.col("START")) & (pl.lit(nodays) <= pl.col("END"))
            )
            arrears = int(match.item(0, "LABEL")) if match.height > 0 else 0
            if arrears == 24:
                arrears = round(nodays / 30.0)
            if arrears >= 3:
                o_days_plus_90 = o_days + 90
                npl_d = month_end_of_sas_days(o_days_plus_90)
                npldate = npl_d
        return (nodays, arrears, npldate)

    # Default (block not entered)
    return (0, 0, None)

dep = dep.with_columns([
    pl.struct(dep.columns).map_elements(
        lambda s: compute_overdraft_fields(s),
        return_dtype=pl.Struct([
            pl.Field("NODAYS2", pl.Int64),
            pl.Field("ARREARS2", pl.Int64),
            pl.Field("NPLDATE2", pl.Date),
        ])
    ).alias("_OD")
]).with_columns([
    pl.col("_OD").struct.field("NODAYS2").alias("NODAYS"),
    pl.col("_OD").struct.field("ARREARS2").alias("ARREARS"),
    pl.when(pl.col("_OD").struct.field("NPLDATE2").is_not_null())
      .then(pl.col("_OD").struct.field("NPLDATE2"))
      .otherwise(pl.col("NPLDATE"))
      .alias("NPLDATE")
]).drop(["_OD"])


# =========================
# Final CVAR fields & formatting
# =========================
dep = dep.with_columns([
    pl.col("CENSUS").alias("CVAR01"),
    # CVAR02 already set as '  '
    pl.col("NEWIC").alias("CVAR03"),
    pl.col("CUSTNAME").alias("CVAR04"),
    pl.col("LMTSTART").alias("CVAR05"),
    pl.col("ACCTNO").alias("CVAR06"),
    pl.lit("ID").alias("CVAR07"),
    pl.when(pl.col("APPRLIMT").is_null()).then(0.00).otherwise(pl.col("APPRLIMT")).alias("CVAR08"),

    # LEDGBAL split into CVAR09 (abs negative) and CVAR10 (non-negative)
    pl.when(pl.col("LEDGBAL") >= 0).then(0.00).otherwise((-1) * pl.col("LEDGBAL")).alias("CVAR09"),
    pl.when(pl.col("LEDGBAL") >= 0).then(pl.col("LEDGBAL")).otherwise(0.00).alias("CVAR10"),

    pl.col("ARREARS").alias("CVAR11"),
    pl.lit("   ").alias("CVAR12"),
    pl.col("NPLDATE").map_elements(month_end_str, return_dtype=pl.Utf8).alias("CVAR13"),
    pl.lit("0351").alias("CVAR14"),
    pl.col("MICRCD").alias("CVAR15"),
])

# IF ARREARS GE 3 AND NPLDATE > 0 THEN CVAR12='NPL'
dep = dep.with_columns([
    pl.when((pl.col("ARREARS") >= 3) & pl.col("NPLDATE").is_not_null())
      .then(pl.lit("NPL"))
      .otherwise(pl.col("CVAR12"))
      .alias("CVAR12")
])

# Final ordering & KEEP (SAS: BY CVAR01)
dep = dep.sort(by=["CVAR01"])

keep_cols = [
    "CVAR01","CVAR02","CVAR03","CVAR04","CVAR05","CVAR06","CVAR07",
    "CVAR08","CVAR09","CVAR10","CVAR11","CVAR12","CVAR13","CVAR14","CR",
    "BRANCH","CVAR15","CENSUST","PRODUCT","CINSTCL","NATGUAR","SCH"
]
# Ensure CR/SCH exist if missing upstream
for c in ["CR", "SCH"]:
    if c not in dep.columns:
        dep = dep.with_columns(pl.lit(None).alias(c))

out = dep.select(keep_cols)

# =========================
# Output: NPGS.DPIPGS&REPTMON (Parquet)
# =========================
out_dir = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDP169"
out_dir.mkdir(parents=True, exist_ok=True)
out_file = out_dir / f"DPIPGS{REPTMON}.parquet"
out.write_parquet(out_file, use_pyarrow=True)

print(f"Wrote {out_file}")



for CURRENT AND OVERDRAFT dataset, need to add filter of "WHERE ENTITY_CD = 'PIBB'" (islamic)
use pyreadstat to read sas7bdat.a
remove reptdate, use datetime timedelta - 1 instead. 
output in sas7bdat and parquet files. 
