"""
diag.py — identify the correct columns and values for the
          EIBDP169 / DPIPGS population filter.
"""

from __future__ import annotations
from pathlib import Path
from datetime import date, datetime, timedelta
import time
import polars as pl
import pyreadstat


# =========================
# Paths (same as main job)
# =========================
MNITB_CURRENT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/intg_dp_acct_current_m{reptmon}.sas7bdat")
LIMIT_OVERDFT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/intg_dp_acct_overdft_m{reptmon}.sas7bdat")


# =========================
# Date macros (same as main job)
# =========================
repdate = datetime.today().date() - timedelta(days=1)
REPTMON = f"{repdate.month:02d}"
MNITB_CURRENT = Path(str(MNITB_CURRENT).format(reptmon=REPTMON))
LIMIT_OVERDFT = Path(str(LIMIT_OVERDFT).format(reptmon=REPTMON))


# =========================
# Helpers
# =========================
def _t0():  return time.perf_counter()
def _stage(t, msg):  print(f"[{time.perf_counter()-t:8.2f}s] {msg}", flush=True)


def read_sas7bdat(path: Path) -> pl.DataFrame:
    df_pd, _meta = pyreadstat.read_sas7bdat(str(path))
    return pl.from_pandas(df_pd)


# =========================
# 1. Load MNITB.CURRENT
# =========================
t = _t0()
mnitb = read_sas7bdat(MNITB_CURRENT)
_stage(t, f"read MNITB_CURRENT ({mnitb.height} rows, {len(mnitb.columns)} cols)")

print()
print("=" * 72)
print("A. PIBB population count")
print("=" * 72)

# Normalise ENTITY_CD once
if "ENTITY_CD" in mnitb.columns:
    mnitb = mnitb.with_columns(
        pl.col("ENTITY_CD").cast(pl.Utf8).str.strip_chars().alias("ENTITY_CD")
    )

pib = mnitb.filter(pl.col("ENTITY_CD") == "PIBB")
print(f"rows with ENTITY_CD == 'PIBB': {pib.height}")
print(f"total rows                     : {mnitb.height}")


print()
print("=" * 72)
print("B. Which columns contain values in 16901..16908  (PIBB only)")
print("=" * 72)

CENSUS_LO, CENSUS_HI = 16901, 16908

# Scan every column; try casting to Int64 and matching the range
for c in mnitb.columns:
    try:
        s = pib.select(pl.col(c).cast(pl.Int64, strict=False)).to_series()
        hits = ((s >= CENSUS_LO) & (s <= CENSUS_HI)).sum()
        if hits and hits > 0:
            # show a few distinct matching values
            sample = (
                pib.filter(pl.col(c).cast(pl.Int64, strict=False).is_between(CENSUS_LO, CENSUS_HI))
                   .select(pl.col(c).unique().sort().head(20))
                   .to_series().to_list()
            )
            print(f"  {c:35s} hits={hits:>8d}   sample distinct={sample}")
    except Exception:
        pass


print()
print("=" * 72)
print("C. Which columns contain the value 169  (PIBB only)")
print("=" * 72)

for c in mnitb.columns:
    # Only try numeric-friendly columns
    try:
        s = pib.select(pl.col(c).cast(pl.Int64, strict=False)).to_series()
        hits = (s == 169).sum()
        if hits and hits > 0:
            print(f"  {c:35s} hits={hits:>8d}")
    except Exception:
        pass


print()
print("=" * 72)
print("D. Distinct values of every plausible 'code' column  (PIBB only)")
print("=" * 72)

# Look at the usual suspects individually
SUSPECTS = [
    "PRODUCT", "CENSUST", "SECTOR", "PURPOSE", "DEPTYPE", "CUSTCODE",
    "USER2", "USER3", "SERVICE", "ODSTAT", "TRACKCD", "ACCPROF",
    "RISKCODE", "ORGCODE", "ORGTYPE", "CURCODE", "INTCYCODE",
    "CURRCODE", "CUSTCODE", "STATCD", "DEPTYPE", "PRODUCT",
    "INDUSTRIAL_SECTOR_CD", "REPAY_TYPE_CD", "STMT_CYCLE",
    "PB_ENTERPRISE_PACKAGE_CD", "BONUTYPE", "TRACKCD",
]

seen = set()
for c in SUSPECTS:
    if c in mnitb.columns and c not in seen:
        seen.add(c)
        try:
            uniq = (
                pib.select(pl.col(c).unique().sort().head(40))
                   .to_series().to_list()
            )
            print(f"  {c:35s} n_unique_total={pib.select(pl.col(c).n_unique()).item()}  sample={uniq}")
        except Exception as e:
            print(f"  {c:35s} <error: {e}>")


print()
print("=" * 72)
print("E. Combined hits: CENSUST in range grouped by PRODUCT  (PIBB only)")
print("=" * 72)

# Even though CENSUST in the extract doesn't contain 16901..16908, this
# shows what PRODUCT codes go with whatever CENSUST range DOES exist,
# as a sanity check on the coding scheme.
try:
    grp = (
        pib
        .group_by("CENSUST")
        .agg(pl.len().alias("n"))
        .sort("CENSUST")
    )
    print(grp.head(50))
except Exception as e:
    print("  <error:", e, ">")


print()
print("=" * 72)
print("F. LIMIT.OVERDFT — ENTITY_CD and key columns")
print("=" * 72)

t = _t0()
odlmt = read_sas7bdat(LIMIT_OVERDFT)
_stage(t, f"read LIMIT_OVERDFT ({odlmt.height} rows)")

if "ENTITY_CD" in odlmt.columns:
    odlmt = odlmt.with_columns(
        pl.col("ENTITY_CD").cast(pl.Utf8).str.strip_chars().alias("ENTITY_CD")
    )

pib_o = odlmt.filter(pl.col("ENTITY_CD") == "PIBB")
print(f"LIMIT.OVERDFT:  PIBB rows = {pib_o.height}  /  total = {odlmt.height}")

if "LMTSTART" in odlmt.columns:
    # Sample of LMTSTART to confirm the MMDDYY8 encoding
    sample = (
        pib_o.select(pl.col("LMTSTART").unique().sort().head(20))
             .to_series().to_list()
    )
    print(f"LMTSTART distinct sample (PIBB): {sample}")

    # How many rows have LMTSTART > 0?
    gt0 = pib_o.filter(pl.col("LMTSTART") > 0).height
    print(f"LMTSTART > 0 rows: {gt0}")

    # How many parse as valid MMDDYY8?
    from datetime import datetime as _dt
    def _parse(x):
        try:
            xi = int(x)
            if xi <= 0: return None
            s = f"{xi:011d}"[:8]
            try:
                return _dt.strptime(s, "%m%d%Y").date()
            except ValueError:
                return _dt.strptime(s, "%m%d%y").date()
        except Exception:
            return None

    ok = pib_o.filter(pl.col("LMTSTART") > 0).select(
        pl.col("LMTSTART").map_elements(_parse, return_dtype=pl.Date).is_not_null().sum()
    ).item()
    print(f"LMTSTART > 0 that parse as valid MMDDYY8: {ok}")


print()
print("=" * 72)
print("DONE")
print("=" * 72)
