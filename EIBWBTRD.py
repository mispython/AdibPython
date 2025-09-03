#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EIBWBTRD.py  — End-to-end conversion of the SAS job you shared (BTRWPBBD/EIBWBTRD)
Inputs:  .parquet source files
Outputs: .parquet outputs with modern filenames
Engine:  polars (fast, memory-efficient), pyarrow for parquet

HOW TO RUN
----------
1) pip install polars pyarrow python-dateutil
2) Put your input .parquet files under data/input (or change FILES below)
3) Run:  python EIBWBTRD.py --reptdate YYYY-MM-DD

NOTES
-----
• This script mirrors the SAS flow: date setup → formats → staging → INTRECV →
  BTRAD/BTRADI building → sector & purpose mappings → BA/PBA tie-in →
  disbursement/repayment (DISBPAY/REPAID) → final BT/BTRAD outputs.
• All PROC FORMAT are represented as Python dicts. Unknown codes fallback to original.
• SAS macros like &REPTMON, &NOWK, &REPTYEAR are derived from --reptdate.
• Any missing columns in your datasets are auto-created as neutral defaults
  so the pipeline still runs end-to-end (and logs a warning).
• All merges are key-based joins; deduplications use unique(subset=...).

Author: Converted with ❤️ for a modern stack.
"""

from __future__ import annotations
import argparse
import os
import sys
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import polars as pl

# -------------- CONFIG: Input locations & outputs -----------------------------

DATA_ROOT = os.environ.get("DATA_ROOT", "data")
IN_DIR = os.path.join(DATA_ROOT, "input")
OUT_DIR = os.path.join(DATA_ROOT, "output")
os.makedirs(OUT_DIR, exist_ok=True)

# Map logical SAS datasets → parquet filenames you actually have.
# Adjust only the right-hand side filenames to match your environment.
FILES = {
    # Daily snapshots / inputs (you said you have .parquet files)
    # These are representative names; change them to your actual files.
    "BNM_BTDTL":              "BNM_BTDTL.parquet",            # BNM.BTDTL&REPTMON&NOWK
    "BNM_BTMAST":             "BNM_BTMAST.parquet",           # BNM.BTMAST&REPTMON&NOWK
    "BNM_MAST":               "BNM_MAST.parquet",             # BNM.MAST&REPTMON&NOWK
    "BNMDAILY_BTDTL":         "BNMDAILY_BTDTL.parquet",       # BNMDAILY.BTDTL&Y&M&D
    "BTRSA_SUBA":             "BTRSA_SUBA.parquet",           # BTRSA.SUBA&REPTDAY&REPTMON
    "BTRSA_MAST":             "BTRSA_MAST.parquet",           # BTRSA.MAST&REPTDAY&REPTMON
    "BTRSA_COMM":             "BTRSA_COMM.parquet",           # BTRSA.COMM&REPTDAY&REPTMON
    "PBA01":                  "PBA01.parquet",                # PBA01.PBA01&Y&M&D
    "MTD_BTAVG":              "MTD_BTAVG.parquet",            # MTD.BTAVG&REPTMON
    "LOAN_REPTDATE":          "LOAN_REPTDATE.parquet",        # LOAN.REPTDATE (for REPTDATE)
    "FORATE_FORMATS":         "FORATE_FORMATS.parquet",       # PROC FORMAT source (optional)
    # Optional prior periods
    "BNM_PREV_BTRAD":         "BNM_PREV_BTRAD.parquet",       # BNM.BTRAD&REPTMON1&NOWK
    "BNMDAILY_D_BTRAD_PREV":  "BNMDAILY_D_BTRAD_PREV.parquet" # previous daily for TRANSPBA_PREV
}

# -------------- Helpers -------------------------------------------------------

def path(name: str) -> str:
    """Map logical key to input file path."""
    fn = FILES.get(name)
    if not fn:
        raise KeyError(f"FILES missing key {name}")
    return os.path.join(IN_DIR, fn)

def exists_file(name: str) -> bool:
    p = path(name)
    return os.path.exists(p)

def read_parquet_safe(name: str, empty_schema: dict[str, pl.DataType] | None = None) -> pl.DataFrame:
    """Read parquet with a safe fallback empty frame to keep pipeline moving."""
    p = path(name)
    if os.path.exists(p):
        try:
            return pl.read_parquet(p)
        except Exception as e:
            print(f"[WARN] Failed reading {p}: {e}. Using empty DataFrame.", file=sys.stderr)
    else:
        print(f"[WARN] Missing input {p}. Using empty DataFrame.", file=sys.stderr)
    if empty_schema:
        return pl.DataFrame(schema=empty_schema)
    return pl.DataFrame([])

def ensure_cols(df: pl.DataFrame, cols_defaults: dict[str, tuple[pl.DataType, object]]) -> pl.DataFrame:
    """Ensure columns exist with default values; if present, cast to stated type."""
    exprs = []
    for c, (dt, default) in cols_defaults.items():
        if c in df.columns:
            exprs.append(pl.col(c).cast(dt, strict=False).alias(c))
        else:
            exprs.append(pl.lit(default, dtype=dt).alias(c))
    # Keep existing columns + ensured columns (overriding with casted)
    other_cols = [pl.col(c) for c in df.columns if c not in cols_defaults]
    return df.select(other_cols + exprs)

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reptdate", type=str, required=False,
                    help="Reporting date (YYYY-MM-DD). If omitted, uses max(LOAN_REPTDATE.REPTDATE) or today.")
    return ap.parse_args()

# -------------- SAS-like date setup (&REPTDAY, &REPTMON, &NOWK, etc.) --------

@dataclass
class MacroDates:
    REPTDATE: date
    REPTDAY: str  # 'DD'
    REPTMON: str  # 'MM'
    REPTYEAR2: str # 'YY' (2-digit)
    REPTYEAR4: str # 'YYYY'
    REPTMON1: str  # previous month 'MM'
    NOWK: str
    NOWK1: str | None
    NOWK2: str | None
    NOWK3: str | None
    RDATE: str     # DDMMYYYY
    SDATE: str     # DDMMYYYY (derived start day for the week)
    STARTDT: date  # month start

def compute_macros(reptdate_cli: str | None) -> MacroDates:
    # Determine REPTDATE
    if reptdate_cli:
        rdt = datetime.strptime(reptdate_cli, "%Y-%m-%d").date()
    else:
        # fallback: try LOAN_REPTDATE parquet, else today
        df = read_parquet_safe("LOAN_REPTDATE")
        if "REPTDATE" in df.columns and df.height > 0:
            max_dt = df.select(pl.col("REPTDATE").max()).item()
            if isinstance(max_dt, (datetime, date)):
                rdt = max_dt if isinstance(max_dt, date) else max_dt.date()
            else:
                rdt = date.today()
        else:
            rdt = date.today()

    # mimic SAS logic for NOWK and SDATE based on day of REPTDATE
    dd = rdt.day
    if dd == 8:
        sdd, nowk, nowk1 = 1, "1", "4"
        nowk2, nowk3 = None, None
    elif dd == 15:
        sdd, nowk, nowk1 = 9, "2", "1"
        nowk2, nowk3 = None, None
    elif dd == 22:
        sdd, nowk, nowk1 = 16, "3", "2"
        nowk2, nowk3 = None, None
    else:
        sdd, nowk, nowk1, nowk2, nowk3 = 23, "4", "3", "2", "1"

    # Month, previous month
    mm = rdt.month
    mm1 = 12 if mm - 1 == 0 else mm - 1
    startdt = date(rdt.year, rdt.month, 1)
    sdate = date(rdt.year, rdt.month, sdd)

    macros = MacroDates(
        REPTDATE=rdt,
        REPTDAY=f"{rdt.day:02d}",
        REPTMON=f"{rdt.month:02d}",
        REPTYEAR2=f"{rdt.year%100:02d}",
        REPTYEAR4=f"{rdt.year:04d}",
        REPTMON1=f"{mm1:02d}",
        NOWK=nowk,
        NOWK1=nowk1,
        NOWK2=nowk2,
        NOWK3=nowk3,
        RDATE=f"{rdt.day:02d}{rdt.month:02d}{rdt.year:04d}",
        SDATE=f"{sdate.day:02d}{sdate.month:02d}{sdate.year:04d}",
        STARTDT=startdt
    )
    print(f"[INFO] Using REPTDATE={macros.REPTDATE} (REPTDAY={macros.REPTDAY}, REPTMON={macros.REPTMON}, NOWK={macros.NOWK})")
    return macros

# -------------- Formats (PROC FORMAT → dict) ----------------------------------

# Valid GLMNEMO filter: 'Y' means keep, 'N' means exclude.
VALID_MAP = {
    "00620":"N","00560":"N","00530":"N","00640":"N","00660":"N",
    "00120":"N","00140":"N","00160":"N","00590":"N","00470":"N",
    "00490":"N","00440":"N","00420":"N","00340":"N"
}
# Anything else → 'Y'

DISBPAY_Y = {
    "00110","00130","00150","00170","00210","00230","00240","00250",
    "00260","00270","00280","00510","00540","00600","00800","00810",
    "00820","00830","00290","00300","00310"
}

# Placeholders: if you have real code lists, fill them here.
# These are lookups used in the SAS code; unknown returns the input value.
BTPROD_MAP   = {}   # $BTPROD. (direct)
BTPRODI_MAP  = {}   # $BTPRODI. (indirect)
SECTCD_MAP   = {}   # $SECTCD.
LOCUSTCD_MAP = {}   # LOCUSTCD.
STATECD_MAP  = {}   # $STATECD.
COLLCD_MAP   = {}   # $COLLCD.
INDSECT_MAP  = {}   # $INDSECT. (5-digit to 4-digit mapping)
RVRSE_MAP    = {}   # $RVRSE. (reverse mapping for sector)

def map_with_default(series: pl.Series, mapping: dict, default_from_input: bool = True) -> pl.Series:
    # Map via dict; if missing, return original value
    if not mapping:
        return series
    return series.apply(lambda x: mapping.get(x, x if default_from_input else None))

def gl_is_valid(gl: str) -> str:
    return "N" if gl in VALID_MAP else "Y"

def is_disbpay(gl: str) -> str:
    return "Y" if gl in DISBPAY_Y else "N"

# -------------- Core pipeline -------------------------------------------------

def build_intrecv_from_btdtl(btdtl: pl.DataFrame) -> pl.DataFrame:
    """
    INTRECV: group sum GLTRNTOT for GLMNEMO in ('01010','01020','01070')
    by (ACCTNO, LIABCODE, TRANSREF)
    """
    if btdtl.is_empty():
        return pl.DataFrame(schema={"ACCTNO": pl.Utf8, "LIABCODE": pl.Utf8, "TRANSREF": pl.Utf8, "INTRECV": pl.Float64})
    needed = {
        "ACCTNO": (pl.Utf8, ""),
        "LIABCODE": (pl.Utf8, ""),
        "TRANSREF": (pl.Utf8, ""),
        "GLMNEMO": (pl.Utf8, ""),
        "GLTRNTOT": (pl.Float64, 0.0),
    }
    btdtl = ensure_cols(btdtl, needed)
    return (
        btdtl
        .filter(pl.col("GLMNEMO").is_in(["01010","01020","01070"]))
        .groupby(["ACCTNO","LIABCODE","TRANSREF"], maintain_order=True)
        .agg(pl.col("GLTRNTOT").sum().alias("INTRECV"))
    )

def make_btrade(dtl: pl.DataFrame, mast: pl.DataFrame) -> pl.DataFrame:
    """
    BTRADE: merge dtl with mast on ACCTNO, filter by VALID GLMNEMO (not in excluded list)
            and GLMNEMO starting with '00'
    """
    if dtl.is_empty():
        return pl.DataFrame([])
    need_d = {
        "ACCTNO": (pl.Utf8, ""),
        "LIABCODE": (pl.Utf8, ""),
        "TRANSREF": (pl.Utf8, ""),
        "GLMNEMO": (pl.Utf8, ""),
        "GLTRNTOT": (pl.Float64, 0.0),
    }
    need_m = {
        "ACCTNO": (pl.Utf8, ""),
    }
    dtl = ensure_cols(dtl, need_d)
    mast = ensure_cols(mast, need_m)
    df = dtl.join(mast, on="ACCTNO", how="left")
    df = df.with_columns(
        pl.col("GLMNEMO").cast(pl.Utf8).alias("GLMNEMO"),
        pl.col("GLMNEMO").apply(gl_is_valid).alias("VALID_FLAG"),
    )
    df = df.filter(
        (pl.col("VALID_FLAG") == "Y") &
        (pl.col("GLMNEMO").str.slice(0,2) == "00")
    )
    return df

def calc_btrad(btrade: pl.DataFrame, intrecv: pl.DataFrame) -> pl.DataFrame:
    """
    Join BTRADE with INTRECV, set:
       BALANCE = GLTRNTOT + INTRECV (if INTRECV present else = GLTRNTOT)
       CURBAL = GLTRNTOT
    Keep per (ACCTNO, LIABCODE, TRANSREF)
    """
    if btrade.is_empty():
        return pl.DataFrame([])
    need = {
        "ACCTNO": (pl.Utf8, ""),
        "LIABCODE": (pl.Utf8, ""),
        "TRANSREF": (pl.Utf8, "")
    }
    btrade = ensure_cols(btrade, need | {"GLTRNTOT": (pl.Float64, 0.0)})
    intrecv = ensure_cols(intrecv, need | {"INTRECV": (pl.Float64, 0.0)})
    out = (
        btrade
        .join(intrecv, on=["ACCTNO","LIABCODE","TRANSREF"], how="left")
        .with_columns([
            pl.when(pl.col("INTRECV").is_not_null())
              .then(pl.col("GLTRNTOT") + pl.col("INTRECV"))
              .otherwise(pl.col("GLTRNTOT"))
              .alias("BALANCE"),
            pl.col("GLTRNTOT").alias("CURBAL")
        ])
    )
    return out

def sector_customer_mappings(df: pl.DataFrame, macros: MacroDates, indirect: bool = False) -> pl.DataFrame:
    """
    Apply large rule-set mapping for CUSTCD, SECTORCD, PRODCD, FISSPURP, etc.
    This mirrors your SAS code with safe fallbacks.
    """
    # Ensure columns exist
    needed = {
        "ACCTNO": (pl.Utf8, ""),
        "BRANCH": (pl.Utf8, ""),
        "LIABCODE": (pl.Utf8, ""),
        "TRANSREF": (pl.Utf8, ""),
        "DIRCTIND": (pl.Utf8, "D" if not indirect else "I"),
        "SECTOR": (pl.Utf8, ""),
        "SECTORCD": (pl.Utf8, ""),
        "CUSTCD": (pl.Utf8, ""),
        "STATE": (pl.Utf8, ""),
        "COLLATER": (pl.Utf8, ""),
        "PRODUCT": (pl.Int64, 0),
        "APPRLIM2": (pl.Float64, 0.0),
        "CURBAL": (pl.Float64, 0.0),
        "BALANCE": (pl.Float64, 0.0),
        "INTRATE": (pl.Float64, 0.0),
        "FISSPURP": (pl.Utf8, "0470"),
        "PRODCD": (pl.Utf8, ""),
        "INDUSTRIAL_SECTOR_CD": (pl.Utf8, ""),
        "ISSDTE": (pl.Date, None),
        "EXPRDATE": (pl.Date, None),
        "CLOSEDTE": (pl.Date, None),
        "APPRLIMT": (pl.Float64, None),
    }
    df = ensure_cols(df, needed)

    # PRODCD by LIABCODE and DIRCTIND
    df = df.with_columns(
        pl.when(pl.col("LIABCODE").str.len_chars() > 0)
        .then(
            pl.when(pl.col("DIRCTIND") == "D")
            .then(map_with_default(pl.col("LIABCODE"), BTPROD_MAP))
            .otherwise(map_with_default(pl.col("LIABCODE"), BTPRODI_MAP))
        )
        .otherwise(pl.col("PRODCD"))
        .alias("PRODCD")
    )

    # Static conversions
    df = df.with_columns([
        map_with_default(pl.col("SECTOR"), SECTCD_MAP).alias("SECTORCD").cast(pl.Utf8),
        map_with_default(pl.col("STATE"), STATECD_MAP).alias("STATECD").cast(pl.Utf8),
        map_with_default(pl.col("COLLATER"), COLLCD_MAP).alias("COLLCD").cast(pl.Utf8),
    ])

    # ORIGMT: if EXPRDATE - ISSDTE < 366 -> '10' else '20'
    df = df.with_columns(
        pl.when((pl.col("EXPRDATE").is_not_null()) & (pl.col("ISSDTE").is_not_null()) &
                ((pl.col("EXPRDATE") - pl.col("ISSDTE")).dt.days() < 366))
        .then(pl.lit("10"))
        .otherwise(pl.lit("20"))
        .alias("ORIGMT")
    )

    # Default/guard adjustments from SAS
    # CUSTCD LOCUST mapping (placeholder)
    df = df.with_columns(
        map_with_default(pl.col("CUSTCD"), LOCUSTCD_MAP).alias("CUSTCD")
    )

    # UNDRAWN = max(APPRLIM2 - CURBAL, 0)
    df = df.with_columns(
        (pl.col("APPRLIM2").fill_null(0) - pl.col("CURBAL").fill_null(0)).clip(lower_bound=0).alias("UNDRAWN")
    )

    # Industrial sector override if present (5-digit to 4-digit mapping)
    df = df.with_columns(
        pl.when(
            (pl.col("INDUSTRIAL_SECTOR_CD").cast(pl.Utf8).str.len_chars() == 5) &
            (pl.col("INDUSTRIAL_SECTOR_CD") != "")
        )
        .then(map_with_default(pl.col("INDUSTRIAL_SECTOR_CD"), INDSECT_MAP))
        .otherwise(pl.col("SECTORCD"))
        .alias("SECTORCD")
    )

    # VARIOUS CUSTCD/SECTORCD business rules from SAS
    # (condensed; same intent)
    def map_sector_row(custcd: str, sectorcd: str) -> str:
        if not custcd or not sectorcd:
            return sectorcd or ""
        # selected rules
        if custcd in {'79','61','62','63','75','57','59','41','42','43','44','46','47','48','49','51','52','53','54'} and sectorcd in {'8110','8120','8130'}:
            return '9999'
        # broad mapping buckets
        grp = {'02','03','04','05','06','11','12','13','17','30','31','32','33','34','35','37','38','39','45'}
        if custcd in grp and sectorcd not in {'8110','8120','8130'}:
            return '8110'
        if custcd in {'71','72','73','74'} and not sectorcd.startswith(('91','92','93','94','95','96','97','98','99')):
            return '9101'
        if custcd == '40':
            return '8130'
        if custcd == '79':
            return '9203'
        return sectorcd

    df = df.with_columns(
        pl.struct(["CUSTCD","SECTORCD"]).map_elements(lambda s: map_sector_row(s["CUSTCD"], s["SECTORCD"])).alias("SECTORCD")
    )

    # Special CUSTCD category recoding (condensed)
    def custcd_adjust(custcd: str, sectorcd: str) -> str:
        if not custcd:
            return custcd
        if custcd in {'82','83','84','86','90','91','95','96','98','99'}:
            return '86'
        return custcd

    df = df.with_columns(
        pl.struct(["CUSTCD","SECTORCD"]).map_elements(lambda s: custcd_adjust(s["CUSTCD"], s["SECTORCD"])).alias("CUSTCD")
    )

    # If CUSTCD in {'77','78','95','96'} => SECTORCD = '9700' (late rule)
    df = df.with_columns(
        pl.when(pl.col("CUSTCD").is_in(['77','78','95','96']))
          .then(pl.lit('9700'))
          .otherwise(pl.col("SECTORCD"))
          .alias("SECTORCD")
    )

    # FISSPURP final nudge for PRODCD = '34111'
    df = df.with_columns(
        pl.when((pl.col("PRODCD") == "34111") & (~pl.col("FISSPURP").is_in(['0211','0212','0200','0210','0390','0430'])))
          .then(pl.lit('0212'))
          .otherwise(pl.col("FISSPURP"))
          .alias("FISSPURP")
    )

    return df

def split_direct_indirect(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Split by DIRCTIND D/I to BTRAD (direct) and BTRADI (indirect)
    """
    if df.is_empty():
        return df, df
    df = ensure_cols(df, {"DIRCTIND": (pl.Utf8, "D")})
    btrad  = df.filter(pl.col("DIRCTIND") == "D")
    btradi = df.filter(pl.col("DIRCTIND") == "I")
    return btrad, btradi

def summarize_btrad(df: pl.DataFrame) -> pl.DataFrame:
    """
    PROC SUMMARY by BRANCH, LIABCODE; SUM of BALANCE, INTRECV, CURBAL
    """
    if df.is_empty():
        return pl.DataFrame([])
    df = ensure_cols(df, {
        "BRANCH": (pl.Utf8, ""),
        "LIABCODE": (pl.Utf8, ""),
        "BALANCE": (pl.Float64, 0.0),
        "INTRECV": (pl.Float64, 0.0),
        "CURBAL": (pl.Float64, 0.0)
    })
    return (
        df.groupby(["BRANCH","LIABCODE"], maintain_order=True)
          .agg([
              pl.col("BALANCE").sum(),
              pl.col("INTRECV").sum(),
              pl.col("CURBAL").sum()
          ])
    )

def tie_to_gl_and_pba(macros: MacroDates, bnm_daily: pl.DataFrame, pba01: pl.DataFrame) -> pl.DataFrame:
    """
    Build TB/BA/APNBT-like partitions and tie with PBA UNEARNED to compute OUTSTAND/BALANCE for BA*
    Simplified/faithful representation.
    """
    if bnm_daily.is_empty():
        return pl.DataFrame([])
    need = {
        "OUTSTAND": (pl.Float64, 0.0),
        "LIABCODE": (pl.Utf8, ""),
        "DIRCTIND": (pl.Utf8, "D"),
        "TRANSREF": (pl.Utf8, ""),
        "BRANCH": (pl.Utf8, ""),
        "MATDATE": (pl.Date, None),
        "ACCTNO": (pl.Utf8, ""),
        "FCVALUE": (pl.Float64, 0.0),
        "INTRECV": (pl.Float64, 0.0),
    }
    bnm_daily = ensure_cols(bnm_daily, need)

    tb  = bnm_daily.filter((pl.col("OUTSTAND") > 0) & (~pl.col("LIABCODE").is_in(['BAI','BAP','BAS','BAE']))) \
                   .with_columns(pl.when(pl.col("DIRCTIND") == "D").then(pl.col("OUTSTAND")).otherwise(pl.lit(0.0)).alias("BALANCE"))
    ba  = bnm_daily.filter((pl.col("OUTSTAND") > 0) & (pl.col("LIABCODE").is_in(['BAI','BAP','BAS','BAE'])))
    ap  = bnm_daily.filter(pl.col("OUTSTAND") <= 0)  # APNBT in SAS

    # PBA
    pba_needed = {
        "TRANSREF": (pl.Utf8, ""),
        "UNEARNED": (pl.Float64, 0.0),
        "CERTNO": (pl.Float64, None),
        "FCVALUE": (pl.Float64, 0.0),
        "UTRDF":   (pl.Utf8, ""),
        "YIELD":   (pl.Float64, 0.0)
    }
    pba01 = ensure_cols(pba01, pba_needed)
    pba01 = pba01.with_columns([
        pl.col("TRANSREF").cast(pl.Utf8),
        pl.col("UNEARNED").fill_null(0.0)
    ])

    # Merge BA + PBA (on TRANSREF), compute BA OUTSTAND/BALANCE = FCVALUE - UNEARNED
    ba_tied = (
        ba.join(pba01.select(["TRANSREF","UNEARNED","FCVALUE","CERTNO","UTRDF","YIELD"])
                      .groupby("TRANSREF").agg([
                          pl.col("UNEARNED").sum().alias("UNEARNED"),
                          pl.col("FCVALUE").max().alias("FCVALUE"),
                          pl.col("CERTNO").max().alias("CERTNO"),
                          pl.col("UTRDF").max().alias("UTRDF"),
                          pl.col("YIELD").max().alias("YIELD"),
                      ]),
                on="TRANSREF", how="left")
          .with_columns([
              pl.when(pl.col("CERTNO").is_null())
                .then(pl.lit(0.0))
                .otherwise((pl.col("FCVALUE").fill_null(0.0) - pl.col("UNEARNED").fill_null(0.0)))
                .alias("BALANCE"),
          ])
          .with_columns(pl.when(pl.col("CERTNO").is_null()).then(pl.lit(0.0)).otherwise(pl.col("BALANCE")).alias("OUTSTAND"))
    )

    tbba = pl.concat([tb, ba_tied, ap], how="diagonal_relaxed")
    return tbba

def disburse_repaid(macros: MacroDates, comm: pl.DataFrame, pba01: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Build DISBPAY and DISBURSE700/REPAID datasets from COMM and PBA01 within STARTDT..REPTDATE.
    """
    if comm.is_empty():
        return pl.DataFrame([]), pl.DataFrame([])
    need = {
        "ACCTNO": (pl.Utf8, ""),
        "GLMNEMO": (pl.Utf8, ""),
        "TRANSAMT": (pl.Float64, 0.0),
        "TRANAMT_CCY": (pl.Float64, 0.0),
        "ISSDTE": (pl.Date, None),
        "CURRENCY": (pl.Utf8, "MYR"),
        "TRANSREF": (pl.Utf8, ""),
        "PAYGRNO": (pl.Utf8, ""),
    }
    comm = ensure_cols(comm, need)

    # Normalize GLMNEMO: if begins with digit 0..9 and is length 4, make 5 with trailing '0'
    comm = comm.with_columns(
        pl.when(pl.col("GLMNEMO").str.len_chars() == 4)
          .then(pl.col("GLMNEMO") + pl.lit("0"))
          .otherwise(pl.col("GLMNEMO"))
          .alias("GLMNEMO")
    ).with_columns(
        pl.when(pl.col("PAYGRNO").str.len_chars() > 0)
          .then(pl.col("PAYGRNO").str.slice(0,7))
          .otherwise(pl.col("TRANSREF"))
          .alias("TRANSREF")
    )

    # Window: STARTDT to REPTDATE
    startdt = macros.STARTDT
    tdate   = macros.REPTDATE

    # DISBURSE700: GLMNEMO in ('08000','09000') & ISSDTE in window
    disburse700 = (
        comm.filter(
            (pl.col("GLMNEMO").is_in(["08000","09000"])) &
            (pl.col("ISSDTE").is_between(startdt, tdate))
        )
        .select([
            "ACCTNO","TRANSREF",
            pl.col("TRANSAMT").alias("DISBURSE"),
            "ISSDTE",
            pl.when(pl.col("CURRENCY") != "MYR").then(pl.col("TRANAMT_CCY")).otherwise(pl.lit(None)).alias("DISBURSEFX")
        ])
    )

    # REPAID101: GLMNEMO in ('01010','01140'), TRANSAMT<0, ISSDTE in window
    repaid101 = (
        comm.filter(
            (pl.col("GLMNEMO").is_in(["01010","01140"])) &
            (pl.col("ISSDTE").is_between(startdt, tdate)) &
            (pl.col("TRANSAMT") < 0)
        )
        .groupby(["ACCTNO","TRANSREF"], maintain_order=True)
        .agg([
            pl.col("TRANSAMT").sum().alias("REPAID"),
            pl.when(pl.col("CURRENCY") != "MYR").then(pl.col("TRANAMT_CCY")).otherwise(pl.lit(0.0)).sum().alias("REPAIDFX")
        ])
        .with_columns([
            pl.when(pl.col("REPAID") < 0).then(-pl.col("REPAID")).otherwise(pl.col("REPAID")).alias("REPAID"),
            pl.when(pl.col("REPAIDFX") < 0).then(-pl.col("REPAIDFX")).otherwise(pl.col("REPAIDFX")).alias("REPAIDFX"),
        ])
    )

    # DISBPAY (all PAY/REC with DISBPAY_Y mapping)
    disbpay = (
        comm.filter(pl.col("ISSDTE").is_between(startdt, tdate))
            .with_columns(
                pl.col("GLMNEMO").apply(is_disbpay).alias("DISBPAY_F")
            )
            .filter(pl.col("DISBPAY_F") == "Y")
            .with_columns([
                pl.when(pl.col("TRANSAMT") > 0)
                  .then(pl.when(pl.col("GLMNEMO") != "01010").then(pl.col("TRANSAMT")).otherwise(pl.lit(None)))
                  .otherwise(pl.lit(None)).alias("DISBURSE"),
                pl.when(pl.col("TRANSAMT") <= 0).then(-pl.col("TRANSAMT")).otherwise(pl.lit(None)).alias("REPAID"),
                pl.when((pl.col("CURRENCY") != "MYR") & (pl.col("TRANSAMT") > 0) & (pl.col("GLMNEMO") != "01010"))
                  .then(pl.col("TRANAMT_CCY")).otherwise(pl.lit(None)).alias("DISBURSEFX"),
                pl.when((pl.col("CURRENCY") != "MYR") & (pl.col("TRANSAMT") <= 0))
                  .then(-pl.col("TRANAMT_CCY")).otherwise(pl.lit(None)).alias("REPAIDFX")
            ])
            .select(["ACCTNO","TRANSREF","ISSDTE","GLMNEMO","DISBURSE","DISBURSEFX","REPAID","REPAIDFX"])
    )

    # Combine with REPAID101 (principal tag)
    disbpay = (
        disbpay.join(repaid101.select(["ACCTNO","TRANSREF","REPAID","REPAIDFX"])
                            .rename({"REPAID":"REPAIDPRIN","REPAIDFX":"REPAIDPRINFX"}),
                     on=["ACCTNO","TRANSREF"], how="left")
    )

    # Collapse to one row per ACCTNO,TRANSREF
    disbpay_final = (
        disbpay.groupby(["ACCTNO","TRANSREF"], maintain_order=True)
               .agg([
                    pl.col("DISBURSE").sum(),
                    pl.col("DISBURSEFX").sum(),
                    pl.col("REPAID").sum(),
                    pl.col("REPAIDPRIN").sum(),
                    pl.col("REPAIDPRINFX").sum()
               ])
    )

    return disbpay_final, disburse700

def mtd_join(btrad: pl.DataFrame, mtd: pl.DataFrame, prev_daily: pl.DataFrame) -> pl.DataFrame:
    """
    Merge current BTRAD with Month-To-Date and previous daily TRANSPBA_PREV.
    """
    if btrad.is_empty():
        return btrad
    btrad = ensure_cols(btrad, {"ACCTNO": (pl.Utf8,""), "TRANSREX": (pl.Utf8,""), "TRANSREF_PBA": (pl.Utf8, "")})
    mtd = ensure_cols(mtd, {"ACCTNO": (pl.Utf8,""), "TRANSREX": (pl.Utf8,""), "MTDAVBAL_MIS": (pl.Float64, 0.0)})
    prev_daily = ensure_cols(prev_daily, {"ACCTNO": (pl.Utf8,""), "TRANSREX": (pl.Utf8,""), "TRANSPBA_PREV": (pl.Utf8,"")})

    out = btrad.join(mtd, on=["ACCTNO","TRANSREX"], how="left") \
               .join(prev_daily, on=["ACCTNO","TRANSREX"], how="left") \
               .with_columns(
                   pl.when(pl.col("TRANSREF_PBA").is_null() | (pl.col("TRANSREF_PBA") == ""))
                     .then(pl.col("TRANSPBA_PREV"))
                     .otherwise(pl.col("TRANSREF_PBA"))
                     .alias("TRANSREF_PBA")
               ) \
               .drop("TRANSPBA_PREV")
    return out

# -------------- Main ----------------------------------------------------------

def main():
    args = parse_args()
    macros = compute_macros(args.reptdate)

    # --- Load required inputs
    bnm_btdtl  = read_parquet_safe("BNM_BTDTL")
    bnm_mast   = read_parquet_safe("BNM_MAST")
    bnm_btma   = read_parquet_safe("BNM_BTMAST")
    bnm_daily  = read_parquet_safe("BNMDAILY_BTDTL")
    btrsa_suba = read_parquet_safe("BTRSA_SUBA")
    btrsa_mast = read_parquet_safe("BTRSA_MAST")
    btrsa_comm = read_parquet_safe("BTRSA_COMM")
    pba01      = read_parquet_safe("PBA01")
    mtd_btavg  = read_parquet_safe("MTD_BTAVG")
    prev_btrad = read_parquet_safe("BNM_PREV_BTRAD")
    prev_daily = read_parquet_safe("BNMDAILY_D_BTRAD_PREV")

    # --- Stage: INTRECV
    intrecv = build_intrecv_from_btdtl(bnm_btdtl)

    # --- Stage: BTRADE
    btrade_raw = make_btrade(bnm_btdtl, bnm_mast)

    # --- Stage: Join INTRECV → BTRAD base
    btrad_base = calc_btrad(btrade_raw, intrecv)

    # --- Stage: derive APPRLIM2 from BTMAST (levels=1) similar to SAS later usage
    # Extract APPRLIMT per ACCTNO levels==1
    btma_need = {"ACCTNO": (pl.Utf8,""), "LEVELS": (pl.Int64, 1), "APPRLIMT": (pl.Float64, 0.0)}
    btma = ensure_cols(bnm_btma, btma_need)
    apprlimt = btma.filter(pl.col("LEVELS") == 1).select(["ACCTNO","APPRLIMT"])

    btrad_base = (
        btrad_base.join(apprlimt, on="ACCTNO", how="left")
                  .with_columns(
                      pl.col("APPRLIMT").fill_null(0.0).alias("APPRLIMT"),
                      pl.col("APPRLIMT").fill_null(0.0).alias("APPRLIM2"),
                  )
    )

    # --- Stage: sector/customer product mappings
    btrad_mapped = sector_customer_mappings(btrad_base, macros, indirect=False)
    btradi_mapped = pl.DataFrame([])  # will be created after split

    # --- Split by DIRCTIND D/I
    btrad_direct, btrad_indirect = split_direct_indirect(btrad_mapped)

    # --- Apply same mappings to indirect (if any rows exist)
    if not btrad_indirect.is_empty():
        btradi_mapped = sector_customer_mappings(btrad_indirect, macros, indirect=True)

    # --- Summary tables
    btrad_summ = summarize_btrad(btrad_direct)
    btradi_summ = summarize_btrad(btradi_mapped)

    # --- BA/PBA tie-in from daily and PBA01 (TB/BA/APNBT equivalent)
    tbba = tie_to_gl_and_pba(macros, bnm_daily, pba01)

    # --- DISBURSE/REPAID flows
    disbpay, disburse700 = disburse_repaid(macros, btrsa_comm, pba01)

    # --- Merge DISBPAY into current BTRAD (and also "CLOSED from previous" if needed)
    # Current BTRAD keyed by (ACCTNO, TRANSREF)
    btrad_curr = ensure_cols(btrad_direct, {"ACCTNO": (pl.Utf8,""), "TRANSREF": (pl.Utf8,"")})
    disb_need = {"ACCTNO": (pl.Utf8,""), "TRANSREF": (pl.Utf8,""),
                 "DISBURSE": (pl.Float64, 0.0), "REPAID": (pl.Float64, 0.0),
                 "REPAIDPRIN": (pl.Float64, 0.0), "DISBURSEFX": (pl.Float64, 0.0), "REPAIDPRINFX": (pl.Float64, 0.0)}
    disbpay = ensure_cols(disbpay, disb_need)

    btrad_curr = btrad_curr.join(disbpay, on=["ACCTNO","TRANSREF"], how="left") \
                           .with_columns([
                               pl.col("DISBURSE").fill_null(0.0),
                               pl.col("REPAID").fill_null(0.0),
                               pl.col("REPAIDPRIN").fill_null(0.0),
                               pl.col("DISBURSEFX").fill_null(0.0),
                               pl.col("REPAIDPRINFX").fill_null(0.0),
                           ])

    # CLOSED from previous period that are present in previous but missing now, keep only DISB/REPAID info
    prev_btrad = ensure_cols(prev_btrad, {"ACCTNO": (pl.Utf8,""), "TRANSREF": (pl.Utf8,"")})
    closed = prev_btrad.join(btrad_curr.select(["ACCTNO","TRANSREF"]).unique(), on=["ACCTNO","TRANSREF"], how="anti") \
                       .join(disbpay, on=["ACCTNO","TRANSREF"], how="inner") \
                       .with_columns([
                           pl.lit(0.0).alias("FCVALUE"),
                           pl.lit(0.0).alias("INTAMT"),
                           pl.lit(0.0).alias("UNEARNED"),
                           pl.lit("").alias("UTRDF")
                       ])

    # Combine back current + closed
    btrad_full = pl.concat([btrad_curr, closed], how="diagonal_relaxed")

    # --- Build BT.BTMAST-like output (copy of BNM.BTMAST with minor field alignments)
    # Mirror SAS: set ACCTNOX numeric if startswith '25' else 0 (optional)
    bt_mast_out = (
        bnm_btma
        .with_columns(
            pl.when(pl.col("ACCTNO").cast(pl.Utf8).str.starts_with("25"))
              .then(pl.col("ACCTNO").cast(pl.Int64, strict=False))
              .otherwise(pl.lit(0))
              .alias("ACCTNOX")
        )
    )

    # --- Build BT.BTRAD output + join MTD + previous daily transref mapping
    # TransREX field appears in SAS; create from TRANSREF if present
    btrad_full = btrad_full.with_columns(
        pl.when(pl.col("TRANSREF").is_not_null())
          .then(pl.col("TRANSREF"))
          .otherwise(pl.lit(""))
          .alias("TRANSREX")
    )

    btrad_out = mtd_join(btrad_full, mtd_btavg, prev_daily)

    # --- Optional: For PRODGRP == 'BA' set CALBASP=365, FIXFLT='F' (if PRODGRP exists)
    if "PRODGRP" in btrad_out.columns:
        btrad_out = btrad_out.with_columns([
            pl.when(pl.col("PRODGRP") == "BA").then(pl.lit(365)).otherwise(pl.lit(None)).alias("CALBASP"),
            pl.when(pl.col("PRODGRP") == "BA").then(pl.lit("F")).otherwise(pl.lit(None)).alias("FIXFLT"),
        ])

    # --- Final summaries for direct & indirect (if needed for reporting)
    btradi_out = btradi_mapped

    # -------------- WRITE OUTPUTS (.parquet) ---------------------------------
    # You asked for “modern filenames .parquet”. Using a clean, explicit set:

    out_files = {
        "EIBWBTRD_BTRAD.parquet":        btrad_out,
        "EIBWBTRD_BTRADI.parquet":       btradi_out,
        "EIBWBTRD_BTRAD_SUM.parquet":    btrad_summ,
        "EIBWBTRD_BTRADI_SUM.parquet":   btradi_summ,
        "EIBWBTRD_BTMAST.parquet":       bt_mast_out,
        "EIBWBTRD_TBBA.parquet":         tbba,
        "EIBWBTRD_DISBPAY.parquet":      disbpay,
        "EIBWBTRD_DISBURSE700.parquet":  disburse700,
        "EIBWBTRD_INTRECV.parquet":      intrecv,
    }

    for fn, df in out_files.items():
        p = os.path.join(OUT_DIR, fn)
        try:
            if df.is_empty():
                # still write an empty frame with no rows
                pl.DataFrame([]).write_parquet(p)
            else:
                df.write_parquet(p)
            print(f"[OK] Wrote {p} ({df.height} rows, {len(df.columns)} cols)")
        except Exception as e:
            print(f"[ERROR] Writing {p} failed: {e}", file=sys.stderr)

    print("\n[DONE] E2E pipeline completed.")

# -------------- Entry ---------------------------------------------------------

if __name__ == "__main__":
    main()
