from __future__ import annotations

from pathlib import Path
from datetime import date, datetime, timedelta
import time
import polars as pl
import pandas as pd
import pyreadstat
import duckdb  # noqa: F401
import pyarrow as pa  # noqa: F401
import pyarrow.parquet as pq  # noqa: F401

import PBBLNFMT


# =========================
# Diagnostics
# =========================
_T0 = time.perf_counter()
def stage(msg: str) -> None:
    global _T0
    now = time.perf_counter()
    print(f"[{now - _T0:8.2f}s] {msg}", flush=True)
    _T0 = now


# =========================
# Paths (adjust to your env)
# =========================
MNITB_CURRENT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/intg_dp_acct_current_m{reptmon}.sas7bdat")
LIMIT_OVERDFT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/intg_dp_acct_overdft_m{reptmon}.sas7bdat")
CISDP_DEPOSIT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cisdp/deposit.sas7bdat")

GP3_KLUNION = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/GP3.txt")
COLL_FILE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_{yyyy}{mm}{dd}")
DESC_FILE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_{yyyy}{mm}{dd}")
MICR_FILE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDP169/PIBBMICR.txt")


# =========================
# Helpers
# =========================
def sas_days_to_date(days: int) -> date:
    return date(1960, 1, 1).fromordinal(date(1960, 1, 1).toordinal() + int(days))


def ddmmyy8_string(d: date) -> str:
    return d.strftime("%d/%m/%y")


def _parse_mmddyy8_str(s: str | None) -> int:
    if not s:
        return 0
    try:
        try:
            d = datetime.strptime(s, "%m%d%Y").date()
        except ValueError:
            d = datetime.strptime(s, "%m%d%y").date()
        return (d - date(1960, 1, 1)).days
    except Exception:
        return 0


def parse_mmddyy8_z11_prefix_to_date(x) -> date | None:
    """SAS: INPUT(SUBSTR(PUT(x, Z11.), 1, 8), MMDDYY8.)"""
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


def parse_sas_or_mmddyy_to_sas_days(x) -> int:
    """
    Decode a SAS date field that may be stored either as:
      - a raw SAS day number (days since 1960-01-01), e.g. 17685
      - an MMDDYY-encoded integer, e.g. 12252024 or 122524

    Returns SAS days (int), or 0 if the value is invalid/zero.
    Uses magnitude: SAS days are typically 0..40000 (1960..2070).
    MMDDYY-encoded values are >= 1e5 (6-digit MMDDYY) or 1e7 (8-digit MMDDYYYY).
    """
    if x is None:
        return 0
    try:
        xi = int(x)
    except Exception:
        return 0
    if xi <= 0:
        return 0
    # Looks like an MMDDYY-encoded int?
    if xi >= 100000:              # 6+ digits -> treat as MMDDYY / MMDDYYYY
        return _parse_mmddyy8_str(f"{xi:011d}"[:8])
    # Otherwise assume already SAS days
    return xi


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
        return "          "
    if d.month in (1, 3, 5, 7, 8, 10, 12):
        last = 31
    elif d.month in (4, 6, 9, 11):
        last = 30
    else:
        last = 29 if (d.year % 4 == 0) else 28
    e = date(d.year, d.month, last)
    return f"{e.day:02d}/{e.month:02d}/{e.year:04d}"


def _month_end_from_sas_days(days: int | None) -> date | None:
    if days is None or int(days) <= 0:
        return None
    return month_end_of_sas_days(int(days) + 90)


def _ndays_lookup(n: int) -> int:
    try:
        return int(PBBLNFMT.NDAYS(n))
    except Exception:
        return 0


def _norm_acctno(col: str):
    return pl.col(col).cast(pl.Utf8).str.strip_chars()


def _norm_branch(col: str):
    return pl.col(col).cast(pl.Utf8).str.strip_chars()


# =========================
# Readers
# =========================
def read_sas7bdat(path: Path) -> pl.DataFrame:
    df_pd, _meta = pyreadstat.read_sas7bdat(str(path))
    return pl.from_pandas(df_pd)


def read_fixed_width(path: Path,
                     specs: list[tuple[str, int, int, pl.DataType]],
                     encoding: str = "utf-8") -> pl.DataFrame:
    """
    Fixed-width reader: whole-file read + vectorised str.slice.
    specs: (name, start_1based, width, dtype)
    """
    raw_bytes = path.read_bytes()
    text = raw_bytes.decode(encoding, errors="replace")
    lines = text.splitlines()

    base = pl.DataFrame({"_line": lines})

    exprs = []
    for name, start1, width, dtype in specs:
        start0 = start1 - 1
        sl = pl.col("_line").cast(pl.Utf8).str.slice(start0, width).str.strip_chars()
        if dtype == pl.Utf8:
            exprs.append(sl.alias(name))
        else:
            exprs.append(
                pl.when((sl == "") | sl.is_null())
                  .then(None)
                  .otherwise(sl)
                  .cast(dtype, strict=False)
                  .alias(name)
            )

    return base.select(exprs)


# =========================
# Derive macro-like vars from (today - 1)
# =========================
repdate = datetime.today().date() - timedelta(days=1)

REPTMON   = f"{repdate.month:02d}"
REPTMON1  = f"{(12 if repdate.month == 1 else repdate.month - 1):02d}"
RDATE     = ddmmyy8_string(repdate)
SDATE_INT = (repdate - date(1960, 1, 1)).days
SDATE     = f"{SDATE_INT:05d}"

MNITB_CURRENT = Path(str(MNITB_CURRENT).format(reptmon=REPTMON))
LIMIT_OVERDFT = Path(str(LIMIT_OVERDFT).format(reptmon=REPTMON))

yyyy = f"{repdate.year:04d}"
mm   = f"{repdate.month:02d}"
dd   = f"{repdate.day:02d}"
COLL_FILE = Path(str(COLL_FILE).format(yyyy=yyyy, mm=mm, dd=dd))
DESC_FILE = Path(str(DESC_FILE).format(yyyy=yyyy, mm=mm, dd=dd))

stage("computed date macros")


# =========================
# CA = MNITB.CURRENT filter
#   SAS: IF PRODUCT=169 AND (16901<=CENSUST<=16908);
# =========================
mnitb = read_sas7bdat(MNITB_CURRENT)
stage(f"read MNITB_CURRENT ({mnitb.height} rows)")

# Normalise ENTITY_CD / PRODUCT / CENSUST defensively
if "ENTITY_CD" in mnitb.columns:
    mnitb = mnitb.with_columns(
        pl.col("ENTITY_CD").cast(pl.Utf8).str.strip_chars().alias("ENTITY_CD")
    )
if "PRODUCT" in mnitb.columns:
    mnitb = mnitb.with_columns(
        pl.col("PRODUCT").cast(pl.Int64, strict=False).alias("PRODUCT")
    )
if "CENSUST" in mnitb.columns:
    mnitb = mnitb.with_columns(
        pl.col("CENSUST").cast(pl.Int64, strict=False).alias("CENSUST")
    )

# SAS original filter — literal
ca = (
    mnitb
    .filter(
        (pl.col("PRODUCT") == 169) &
        (pl.col("CENSUST").is_between(16901, 16908))
    )
)
stage(f"filtered CA ({ca.height} rows)")

if ca.height == 0:
    # SAS semantics against the current raw extract yields 0 rows.
    # Point MNITB_CURRENT at the MNITB.CURRENT *view's* output to fix this.
    raise SystemExit(
        "CA is empty. The SAS filter (PRODUCT=169 AND 16901<=CENSUST<=16908) "
        "produces 0 rows in this raw extract — the view remaps these codes upstream. "
        "Point MNITB_CURRENT at MNITB.CURRENT's output (or apply the view's SQL first)."
    )


# =========================
# Merge LIMIT.OVERDFT (ODLMT) — filter ENTITY_CD='PIBB'
#   SAS original:
#     IF LMTSTART > 0 THEN LMTSTART = INPUT(SUBSTR(PUT(LMTSTART,Z11.),1,8),MMDDYY8.);
#   NOTE: in the raw extract, LMTSTART is stored as SAS days (e.g. 17685),
#         so the SAS PUT/SUBSTR/INPUT dance silently produces missing.
#         We honour the intent by decoding SAS days here.
# =========================
odlmt = read_sas7bdat(LIMIT_OVERDFT)
stage(f"read LIMIT_OVERDFT ({odlmt.height} rows)")

if "ENTITY_CD" in odlmt.columns:
    odlmt = odlmt.with_columns(
        pl.col("ENTITY_CD").cast(pl.Utf8).str.strip_chars().alias("ENTITY_CD")
    )

odlmt = (
    odlmt
    .filter(pl.col("ENTITY_CD") == "PIBB")
    .select(["ACCTNO", "LMTSTART"])
)

odlmt = odlmt.with_columns([
    pl.when(pl.col("LMTSTART") > 0)
      .then(
          pl.col("LMTSTART")
            .cast(pl.Int64)
            .map_elements(sas_days_to_date, return_dtype=pl.Date)
      )
      .otherwise(None)
      .alias("LMTSTART")
]).unique(subset=["ACCTNO"], keep="first")
stage(f"parsed ODLMT ({odlmt.height} rows)")

ca = ca.with_columns(_norm_acctno("ACCTNO"))
odlmt = odlmt.with_columns(_norm_acctno("ACCTNO"))
ca = ca.join(odlmt, on="ACCTNO", how="left")
stage(f"joined ODLMT ({ca.height} rows)")


# =========================
# GP3 fixed-width  (SAS: INPUT @004 ACCTNO 10. @019 RPTDAY 2. @021 RPTMON 2. @023 RPTYEAR 4.)
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
stage(f"read GP3 ({gp3.height} rows)")

ca = ca.join(gp3, on="ACCTNO", how="left").with_columns([
    pl.when((pl.col("RPTDAY") > 0) & (pl.col("RPTMON") > 0) & (pl.col("RPTYEAR") > 0))
      .then(pl.struct(["RPTMON", "RPTDAY", "RPTYEAR"]).map_elements(
            lambda s: mdy_safe(s["RPTMON"], s["RPTDAY"], s["RPTYEAR"]), return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("NPLDATE")
])
stage(f"joined GP3 ({ca.height} rows)")


# =========================
# CISDP (SECCUST='901', NODUPKEY by ACCTNO)
# =========================
cis_raw = read_sas7bdat(CISDP_DEPOSIT)
stage(f"read CISDP_DEPOSIT ({cis_raw.height} rows)")

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
stage(f"filtered CISDP ({cis.height} rows)")

ca = ca.join(cis, on="ACCTNO", how="left")
stage(f"joined CISDP ({ca.height} rows)")


# =========================
# COLL / DESC fixed-width
#   COLL: INPUT @004 CCOLLNO PD6. @146 ACCTNO PD6.
#   DESC: INPUT @001 CCOLLNO 11. @051 CINSTCL $2. @055 NATGUAR $2. @211 CENSUS 10.
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
stage(f"read COLL ({coll.height} rows)")

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
stage(f"read DESC ({desc.height} rows)")

coll = coll.join(desc, on="CCOLLNO", how="inner")
stage(f"joined COLL+DESC ({coll.height} rows)")

dep = ca.join(coll, on="ACCTNO", how="inner")
stage(f"joined CA+COLL ({dep.height} rows)")

dep = dep.unique(subset=["ACCTNO"], keep="first")
stage(f"unique by ACCTNO ({dep.height} rows)")


# =========================
# MICR fixed-width  (SAS: INPUT @002 BRANCH 3. @040 MICRCD $5.)
# =========================
micr = read_fixed_width(
    MICR_FILE,
    specs=[
        ("BRANCH",  2, 3, pl.Utf8),
        ("MICRCD", 40, 5, pl.Utf8),
    ],
)
stage(f"read MICR ({micr.height} rows)")

dep  = dep.with_columns(_norm_branch("BRANCH"))
micr = micr.with_columns(_norm_branch("BRANCH"))
dep  = dep.join(micr, on="BRANCH", how="left")
stage(f"joined MICR ({dep.height} rows)")


# =========================
# Arrears/NPL logic — VECTORISED
#   SAS: ARREARS=0; NODAYS=0; NPLDATE=0;
#        IF (EXODDATE NE 0 OR TEMPODDT NE 0) AND (CURBAL LT 0) THEN DO; ... END;
#   NOTE: EXODDATE / TEMPODDT appear to be SAS days in the raw extract,
#         so we auto-detect (SAS days vs MMDDYY-encoded) per value.
# =========================
dep = dep.with_columns([
    pl.lit("  ").alias("CVAR02"),
    pl.lit(0, dtype=pl.Int64).alias("ARREARS"),
    pl.lit(0, dtype=pl.Int64).alias("NODAYS"),
])


def _mmddyy8_or_sasdays_to_days_expr(col: str) -> pl.Expr:
    """Return SAS days for a column that may be stored as SAS days or MMDDYY-encoded int."""
    return (
        pl.when(pl.col(col).is_not_null() & (pl.col(col) > 0))
          .then(
              pl.col(col)
                .cast(pl.Int64)
                .map_elements(parse_sas_or_mmddyy_to_sas_days, return_dtype=pl.Int64)
          )
          .otherwise(pl.lit(0, dtype=pl.Int64))
          .alias(col + "_DAYS")
    )


dep = dep.with_columns([
    _mmddyy8_or_sasdays_to_days_expr("EXODDATE"),
    _mmddyy8_or_sasdays_to_days_expr("TEMPODDT"),
])
stage("decoded EXODDATE/TEMPODDT")

enter = (
    ((pl.col("EXODDATE_DAYS") != 0) | (pl.col("TEMPODDT_DAYS") != 0))
    & pl.col("CURBAL").is_not_null()
    & (pl.col("CURBAL") < 0)
)

ed = pl.col("EXODDATE_DAYS")
td = pl.col("TEMPODDT_DAYS")
o_days = (
    pl.when((ed > 0) & (td > 0)).then(pl.min_horizontal(ed, td))
      .when(ed > 0).then(ed)
      .when(td > 0).then(td)
      .otherwise(pl.lit(0, dtype=pl.Int64))
)

nodays = (
    pl.when(o_days > 0)
      .then(pl.lit(SDATE_INT, dtype=pl.Int64) - o_days + 1)
      .otherwise(pl.lit(1, dtype=pl.Int64))
)

arrears_raw = (
    pl.when(enter & (nodays > 0))
      .then(nodays.map_elements(_ndays_lookup, return_dtype=pl.Int64))
      .otherwise(pl.lit(0, dtype=pl.Int64))
)

arrears = (
    pl.when(arrears_raw == 24)
      .then((nodays.cast(pl.Float64) / 30.0).round().cast(pl.Int64))
      .otherwise(arrears_raw)
)

npldate_new = (
    pl.when(enter & (arrears >= 3) & (o_days > 0))
      .then(o_days.map_elements(_month_end_from_sas_days, return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
)

dep = dep.with_columns([
    pl.when(enter).then(nodays).otherwise(pl.lit(0, dtype=pl.Int64)).alias("NODAYS"),
    pl.when(enter).then(arrears).otherwise(pl.lit(0, dtype=pl.Int64)).alias("ARREARS"),
    pl.when(npldate_new.is_not_null())
      .then(npldate_new)
      .otherwise(pl.col("NPLDATE"))
      .alias("NPLDATE"),
]).drop(["EXODDATE_DAYS", "TEMPODDT_DAYS"])
stage("vectorised overdraft loop done")


# =========================
# Final CVARs
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

dep = dep.with_columns([
    pl.when((pl.col("ARREARS") >= 3) & pl.col("NPLDATE").is_not_null())
      .then(pl.lit("NPL"))
      .otherwise(pl.col("CVAR12"))
      .alias("CVAR12")
])
stage("final CVARs done")

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
stage(f"selected keep cols ({out.height} rows)")


# =========================
# Output: Parquet + SAS7BDAT
# =========================
out_dir = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDP169")
out_dir.mkdir(parents=True, exist_ok=True)

out_parquet = out_dir / f"DPIPGS{REPTMON}.parquet"
out_sas     = out_dir / f"DPIPGS{REPTMON}.sas7bdat"

out.write_parquet(out_parquet, use_pyarrow=True)
stage(f"wrote parquet -> {out_parquet}")


def _to_sas(df: pd.DataFrame) -> pd.DataFrame:
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
stage(f"wrote sas7bdat -> {out_sas}")
