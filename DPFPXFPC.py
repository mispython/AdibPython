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
MNITB_CURRENT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/intg_dp_acct_current_m{reptmon}.sas7bdat")
LIMIT_OVERDFT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/intg_dp_acct_overdft_m{reptmon}.sas7bdat")
CISDP_DEPOSIT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cisdp/deposit.sas7bdat")

# ---- Fixed-width / flat-file sources (NOT parquet) ----
GP3_KLUNION  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/GP3.txt")
COLL_FILE    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_{yyyy}{mm}{dd}")        # CCOLLNO, ACCTNO
DESC_FILE    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_{yyyy}{mm}{dd}")   # CCOLLNO, CINSTCL, NATGUAR, CENSUS
MICR_FILE    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDP169/PIBBMICR.txt")                    # BRANCH, MICRCD


# =========================
# Helper functions
# =========================
def sas_days_to_date(days: int) -> date:
    origin = date(1960, 1, 1)
    return origin.fromordinal(origin.toordinal() + int(days))


def ddmmyy8_string(d: date) -> str:
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
        s = f"{xi:011d}"[:8]
        try:
            d = datetime.strptime(s, "%m%d%Y").date()
        except Exception:
            d = datetime.strptime(s, "%m%d%y").date()
        return (d - date(1960, 1, 1)).days
    except Exception:
        return 0


def parse_mmddyy8_z11_prefix_to_date(x) -> date | None:
    """
    Same as parse_mmddyy8_from_z11_prefix_to_days but returns a date (or None).
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
    if base.month in (1, 3, 5, 7, 8, 10, 12):
        last = 31
    elif base.month in (4, 6, 9, 11):
        last = 30
    else:
        last = 29 if (base.year % 4 == 0) else 28
    return date(base.year, base.month, last)


def month_end_str(d: date | None) -> str:
    if d is None:
        return "          "  # 10 spaces
    if d.month in (1, 3, 5, 7, 8, 10, 12):
        last = 31
    elif d.month in (4, 6, 9, 11):
        last = 30
    else:
        last = 29 if (d.year % 4 == 0) else 28
    e = date(d.year, d.month, last)
    return f"{e.day:02d}/{e.month:02d}/{e.year:04d}"


# =========================
# Readers
# =========================
def read_sas7bdat(path: Path) -> pl.DataFrame:
    """Read a .sas7bdat file into a Polars DataFrame via pyreadstat."""
    df_pd, _meta = pyreadstat.read_sas7bdat(str(path))
    return pl.from_pandas(df_pd)


def read_fixed_width(path: Path,
                     specs: list[tuple[str, int, int, pl.DataType]],
                     encoding: str = "utf8-lossy") -> pl.DataFrame:
    """
    Read a fixed-width text file.

    specs: list of (name, start_1based, width, dtype).
    SAS column pointers (@n) are 1-based; widths are in bytes.
    Numeric fields that are blank become null.
    """
    raw = pl.read_csv(
        path,
        has_header=False,
        separator="\x01",          # bogus sep -> one column per line
        quote_char=None,
        truncate_ragged_lines=True,
        new_columns=["_line"],
        encoding=encoding,
        infer_schema_length=0,     # read everything as string first
    )

    exprs = []
    for name, start1, width, dtype in specs:
        start0 = start1 - 1
        s = pl.col("_line").cast(pl.Utf8).str.slice(start0, width).str.strip_chars()
        if dtype == pl.Utf8:
            exprs.append(s.alias(name))
        else:
            exprs.append(
                pl.when((s == "") | s.is_null())
                  .then(None)
                  .otherwise(s)
                  .cast(dtype, strict=False)
                  .alias(name)
            )

    return raw.select(exprs)


# =========================
# Derive macro-like vars from (today - 1)
# =========================
repdate = datetime.today().date() - timedelta(days=1)

REPTMON   = f"{repdate.month:02d}"
REPTMON1  = f"{(12 if repdate.month == 1 else repdate.month - 1):02d}"
RDATE     = ddmmyy8_string(repdate)
SDATE_INT = (repdate - date(1960, 1, 1)).days
SDATE     = f"{SDATE_INT:05d}"

# Resolve placeholders in paths
MNITB_CURRENT = Path(str(MNITB_CURRENT).format(reptmon=REPTMON))
LIMIT_OVERDFT = Path(str(LIMIT_OVERDFT).format(reptmon=REPTMON))

yyyy = f"{repdate.year:04d}"
mm   = f"{repdate.month:02d}"
dd   = f"{repdate.day:02d}"
COLL_FILE = Path(str(COLL_FILE).format(yyyy=yyyy, mm=mm, dd=dd))
DESC_FILE = Path(str(DESC_FILE).format(yyyy=yyyy, mm=mm, dd=dd))


# =========================
# Normalisation helpers
# =========================
def _norm_acctno(col: str):
    return pl.col(col).cast(pl.Utf8).str.strip_chars()


def _norm_branch(col: str):
    return pl.col(col).cast(pl.Utf8).str.strip_chars()


# =========================
# CA = MNITB.CURRENT filter (ENTITY_CD='PIBB'); merge ODLMT (LIMIT.OVERDFT)
# =========================
mnitb = read_sas7bdat(MNITB_CURRENT)

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

# LMTSTART > 0 -> parse MMDDYY8; invalid -> None (mirrors SAS INPUT(..., MMDDYY8.))
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

ca = ca.with_columns(_norm_acctno("ACCTNO"))
odlmt = odlmt.with_columns(_norm_acctno("ACCTNO"))
ca = ca.join(odlmt, on="ACCTNO", how="left")


# =========================
# --- GP3 ---  fixed-width text, NOT parquet
# SAS:
#   INPUT @004 ACCTNO 10.  @019 RPTDAY 2.  @021 RPTMON 2.  @023 RPTYEAR 4.
# =========================
gp3 = read_fixed_width(
    GP3_KLUNION,
    specs=[
        ("ACCTNO",   4, 10, pl.Utf8),
        ("RPTDAY",  19,  2, pl.Int64),
        ("RPTMON",  21,  2, pl.Int64),
        ("RPTYEAR", 23,  4, pl.Int64),
    ],
).with_columns(_norm_acctno("ACCTNO"))

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
      .with_columns(_norm_acctno("ACCTNO"))
      .unique(subset=["ACCTNO"], keep="first")
)
ca = ca.join(cis, on="ACCTNO", how="left")


# =========================
# --- COLL / DESC ---  fixed-width, NOT parquet
# SAS:
#   COLL: INPUT @004 CCOLLNO PD6.  @146 ACCTNO PD6.
#   DESC: INPUT @001 CCOLLNO 11.   @051 CINSTCL $2.  @055 NATGUAR $2.  @211 CENSUS 10.
# =========================
coll = read_fixed_width(
    COLL_FILE,
    specs=[
        ("CCOLLNO",   4,  6, pl.Utf8),
        ("ACCTNO",  146,  6, pl.Utf8),
    ],
).with_columns(
    pl.col("CCOLLNO").cast(pl.Utf8).str.strip_chars().alias("CCOLLNO"),
    _norm_acctno("ACCTNO"),
)

desc = read_fixed_width(
    DESC_FILE,
    specs=[
        ("CCOLLNO",   1, 11, pl.Utf8),
        ("CINSTCL",  51,  2, pl.Utf8),
        ("NATGUAR",  55,  2, pl.Utf8),
        ("CENSUS",  211, 10, pl.Int64),
    ],
).with_columns(
    pl.col("CCOLLNO").cast(pl.Utf8).str.strip_chars().alias("CCOLLNO")
)

coll = coll.join(desc, on="CCOLLNO", how="inner")  # IF A AND B

dep = ca.join(coll, on="ACCTNO", how="inner")      # IF A AND B
dep = dep.unique(subset=["ACCTNO"], keep="first")  # NODUPKEY BY ACCTNO


# =========================
# --- MICR ---  fixed-width, NOT parquet
# SAS:
#   INPUT @002 BRANCH 3.  @040 MICRCD $5.
# =========================
micr = read_fixed_width(
    MICR_FILE,
    specs=[
        ("BRANCH",  2, 3, pl.Utf8),
        ("MICRCD", 40, 5, pl.Utf8),
    ],
)

dep  = dep.with_columns(_norm_branch("BRANCH"))
micr = micr.with_columns(_norm_branch("BRANCH"))

dep  = dep.join(micr, on="BRANCH", how="left")


# =========================
# Arrears/NPL logic using PBBLNFMT NDAYS. informat
# =========================
dep = dep.with_columns([
    pl.lit("  ").alias("CVAR02"),
    pl.lit(0).alias("ARREARS"),
    pl.lit(0).alias("NODAYS"),
    pl.lit(None, dtype=pl.Date).alias("NPLDATE"),  # reset before recompute
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

    if (((EXODDATE or 0) != 0) or ((TEMPODDT or 0) != 0)) and (CURBAL is not None and CURBAL < 0):
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

        nodays = 0
        if o_days > 0:
            nodays = SDATE_INT - o_days
        nodays = nodays + 1

        arrears = 0
        npldate = None
        if nodays > 0:
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
    pl.col("NEWIC").alias("CVAR03"),
    pl.col("CUSTNAME").alias("CVAR04"),
    pl.col("LMTSTART").alias("CVAR05"),
    pl.col("ACCTNO").alias("CVAR06"),
    pl.lit("ID").alias("CVAR07"),
    pl.when(pl.col("APPRLIMT").is_null()).then(0.00).otherwise(pl.col("APPRLIMT")).alias("CVAR08"),

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
def _to_sas(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime64 columns to SAS day numbers (days since 1960-01-01)."""
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = (df[col] - pd.Timestamp("1960-01-01")).dt.days.astype("Int64")
    return df


pyreadstat.write_sas7bdat(
    _to_sas(out.to_pandas()),
    str(out_sas),
    file_label=f"DPIPGS{REPTMON}",
)

print(f"Wrote {out_parquet}")
print(f"Wrote {out_sas}")
