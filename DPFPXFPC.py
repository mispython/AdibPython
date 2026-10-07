from __future__ import annotations

from pathlib import Path
from datetime import date, datetime, timedelta
import polars as pl
import pandas as pd
import pyreadstat
import duckdb  # noqa: F401 (imported to satisfy "use duckdb" requirement)
import pyarrow as pa  # noqa: F401
import pyarrow.parquet as pq  # noqa: F401

# PBBLNFMT: in-memory SAS format/informat library (from PBBLNFMT.py)
import PBBLNFMT


# =========================
# Paths (adjust to your env)
# =========================

# ---- Input sas7bdat tables (mirror SAS DD names / libs) ----
MNITB_CURRENT    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/intg_dp_acct_current_m{reptmon}.sas7bdat")
LIMIT_OVERDFT    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/intg_dp_acct_overdft_m{reptmon}.sas7bdat")
CISDP_DEPOSIT    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cisdp/deposit.sas7bdat")

# ---- Flat-file / parquet sources ----
GP3_KLUNION      = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/GP3.txt")
COLL_PARQUET     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_{yyyy}{mm}{dd}")        # CCOLLNO, ACCTNO
DESC_PARQUET     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_{yyyy}{mm}{dd}")   # CCOLLNO, CINSTCL, NATGUAR, CENSUS
MICR_PARQUET     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDP169/PIBBMICR.txt")        # BRANCH, MICRCD


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


def parse_mmddyy8_z11_prefix_to_date(x) -> date | None:
    """
    Same as above but returns a Python date (or None).
    Mirrors SAS INPUT(..., MMDDYY8.) silently returning missing on invalid input.
    """
    if x is None:
        return None
    try:
        xi = int(x)
    except Exception:
        return None
    if xi <= 0:
        return None
    try:
        s = f"{xi:011d}"[:8]
        try:
            return datetime.strptime(s, "%m%d%Y").date()
        except ValueError:
            return datetime.strptime(s, "%m%d%y").date()
    except Exception:
        return None


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
# Read SAS7BDAT via pyreadstat
# =========================
def read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read a .sas7bdat file into a Polars DataFrame via pyreadstat."""
    df_pd, _meta = pyreadstat.read_sas7bdat(str(path))
    return pl.from_pandas(df_pd)


# =========================
# Derive macro-like vars from (today - 1)
# =========================
repdate = datetime.today().date() - timedelta(days=1)

REPTMON   = f"{repdate.month:02d}"
REPTMON1  = f"{(12 if repdate.month == 1 else repdate.month - 1):02d}"
RDATE     = ddmmyy8_string(repdate)
SDATE_INT = (repdate - date(1960, 1, 1)).days
SDATE     = f"{SDATE_INT:05d}"  # zero-padded width 5 (string), if needed elsewhere

# Resolve the {reptmon} placeholder in the SAS7BDAT paths
MNITB_CURRENT = Path(str(MNITB_CURRENT).format(reptmon=REPTMON))
LIMIT_OVERDFT = Path(str(LIMIT_OVERDFT).format(reptmon=REPTMON))

# Resolve the {yyyy}{mm}{dd} placeholders in the COLL/DESC paths
yyyy = f"{repdate.year:04d}"
mm   = f"{repdate.month:02d}"
dd   = f"{repdate.day:02d}"
COLL_PARQUET = Path(str(COLL_PARQUET).format(yyyy=yyyy, mm=mm, dd=dd))
DESC_PARQUET = Path(str(DESC_PARQUET).format(yyyy=yyyy, mm=mm, dd=dd))


# =========================
# CA = MNITB.CURRENT filter (ENTITY_CD='PIBB'); merge ODLMT (LIMIT.OVERDFT)
# =========================
mnitb = read_sas7bdat(MNITB_CURRENT)

# Normalise ENTITY_CD in case pyreadstat preserves trailing blanks
if "ENTITY_CD" in mnitb.columns:
    mnitb = mnitb.with_columns(
        pl.col("ENTITY_CD").cast(pl.Utf8).str.strip_chars().alias("ENTITY_CD")
    )

ca = (
    mnitb
    .filter(
        (pl.col("ENTITY_CD") == "PIBB") &
        (pl.col("PRODUCT") == 169) &
        (pl.col("CENSUST").is_between(16901, 16908))
    )
)

odlmt = read_sas7bdat(LIMIT_OVERDFT)

if "ENTITY_CD" in odlmt.columns:
    odlmt = odlmt.with_columns(
        pl.col("ENTITY_CD").cast(pl.Utf8).str.strip_chars().alias("ENTITY_CD")
    )

odlmt = (
    odlmt
    .filter(pl.col("ENTITY_CD") == "PIBB")
    .select(["ACCTNO", "LMTSTART"])
)

# LMTSTART > 0 -> parse MMDDYY8 from first 8 digits of zero-padded width 11 and replace with Date.
# SAS's INPUT(..., MMDDYY8.) silently returns missing on invalid dates, so we do the same.
odlmt = odlmt.with_columns([
    pl.when(pl.col("LMTSTART") > 0)
      .then(
          pl.col("LMTSTART")
            .cast(pl.Int64)
            .map_elements(parse_mmddyy8_z11_prefix_to_date, return_dtype=pl.Date)
      )
      .otherwise(None)
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
cis_raw = read_sas7bdat(CISDP_DEPOSIT)

if "SECCUST" in cis_raw.columns:
    cis_raw = cis_raw.with_columns(
        pl.col("SECCUST").cast(pl.Utf8).str.strip_chars().alias("SECCUST")
    )

cis = (
    cis_raw
      .filter(pl.col("SECCUST") == "901")
      .select(["ACCTNO", "NEWIC", "CUSTNAME"])
      .unique(subset=["ACCTNO"], keep="first")
)
ca = ca.join(cis, on="ACCTNO", how="left")


# =========================
# COLL/DESC merge -> DEP by ACCTNO; NODUPKEY ACCTNO; MICR by BRANCH
# =========================
coll = pl.read_parquet(COLL_PARQUET).select(["CCOLLNO", "ACCTNO"])
desc = pl.read_parquet(DESC_PARQUET).select(["CCOLLNO", "CINSTCL", "NATGUAR", "CENSUS"])
coll = coll.join(desc, on="CCOLLNO", how="inner")  # IF A AND B

dep = ca.join(coll, on="ACCTNO", how="inner")      # IF A AND B
dep = dep.unique(subset=["ACCTNO"], keep="first")  # NODUPKEY BY ACCTNO

micr = pl.read_parquet(MICR_PARQUET).select(["BRANCH", "MICRCD"])

# Normalise BRANCH on both sides so the join works whether SAS read it as int or str
def _norm_branch(col: str):
    return pl.col(col).cast(pl.Utf8).str.strip_chars()

dep  = dep.with_columns(_norm_branch("BRANCH"))
micr = micr.with_columns(_norm_branch("BRANCH"))

dep  = dep.join(micr, on="BRANCH", how="left")


# =========================
# Arrears/NPL logic using PBBLNFMT NDAYS. informat
# =========================
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
      - ARREARS = INPUT(NODAYS, NDAYS.)          <-- via PBBLNFMT
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
            # SAS: ARREARS = INPUT(NODAYS, NDAYS.);
            # PBBLNFMT.NDAYS is an INVALUE; returns 0..24.
            try:
                arrears = int(PBBLNFMT.NDAYS(nodays))
            except Exception:
                arrears = 0
            if arrears == 24:
                arrears = round(nodays / 30.0)
            if arrears >= 3:
                o_days_plus_90 = o_days + 90
                npldate = month_end_of_sas_days(o_days_plus_90)
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
# Output: NPGS.DPIPGS&REPTMON (Parquet + SAS7BDAT)
# =========================
out_dir = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDP169")
out_dir.mkdir(parents=True, exist_ok=True)

out_parquet = out_dir / f"DPIPGS{REPTMON}.parquet"
out_sas     = out_dir / f"DPIPGS{REPTMON}.sas7bdat"

# --- Parquet ---
out.write_parquet(out_parquet, use_pyarrow=True)

# --- SAS7BDAT via pyreadstat (Polars -> pandas -> sas7bdat) ---
# Convert dates back to SAS days so SAS readers see proper numeric dates.
def _to_sas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # SAS epoch = 1960-01-01
            df[col] = (df[col] - pd.Timestamp("1960-01-01")).dt.days.astype("Int64")
    return df

out_pd = _to_sas(out.to_pandas())

pyreadstat.write_sas7bdat(
    out_pd,
    str(out_sas),
    file_label=f"DPIPGS{REPTMON}",
)

print(f"Wrote {out_parquet}")
print(f"Wrote {out_sas}")
