#!/usr/bin/env python3
# EIBWBTRD.py — Full end-to-end Polars pipeline conversion of SAS job EIBWBTRD / BTRWPBBD
# Usage: python EIBWBTRD.py --reptdate 2025-04-30
#
# Requirements:
#   pip install polars pyarrow python-dateutil
#
# NOTES:
#  - Edit the INPUT_FILES dictionary: set each key's value to the path of the corresponding parquet file.
#  - Fill mapping dicts (BTPROD_MAP, BTPRODI_MAP, LOCUSTCD_MAP, SECTCD_MAP, etc.) if you have SAS formats.
#  - Script is defensive: missing columns are created with safe defaults so the pipeline runs end-to-end.

from __future__ import annotations
import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import polars as pl

pl.Config.set_tbl_formatting("UTF8_FULL")

# ---------------------------
# CONFIG — EDIT THESE PATHS
# ---------------------------
# Put the actual file paths (absolute or relative) to the Parquet files produced earlier.
# Only change the right-hand side strings to match your files.
INPUT_FILES = {
    # core BNM datasets
    "LOAN_REPTDATE":             "data/input/LOAN_REPTDATE.parquet",      # LOAN.REPTDATE
    "BNM_BTDTL":                 "data/input/BNM_BTDTL.parquet",          # BNM.BTDTL&REPTMON&NOWK
    "BNM_MAST":                  "data/input/BNM_MAST.parquet",           # BNM.MAST&REPTMON&NOWK
    "BNM_BTMAST":                "data/input/BNM_BTMAST.parquet",         # BNM.BTMAST&REPTMON&NOWK
    "BNMDAILY_BTDTL":            "data/input/BNMDAILY_BTDTL.parquet",     # BNMDAILY.BTDTL... (daily GL extract)
    # BTRSA inputs (COMM = ledger transactions; SUBA, MAST are account metadata)
    "BTRSA_COMM":                "data/input/BTRSA_COMM.parquet",         # BTRSA.COMM&REPTDAY&REPTMON
    "BTRSA_SUBA":                "data/input/BTRSA_SUBA.parquet",         # BTRSA.SUBA&REPTDAY&REPTMON
    "BTRSA_MAST":                "data/input/BTRSA_MAST.parquet",         # BTRSA.MAST&REPTDAY&REPTMON
    # PBA / MTD / previous outputs
    "PBA01":                     "data/input/PBA01.parquet",              # PBA01.PBA01&Y&M&D
    "MTD_BTAVG":                 "data/input/MTD_BTAVG.parquet",         # MTD.BTAVG&REPTMON
    "BNM_PREV_BTRAD":            "data/input/BNM_PREV_BTRAD.parquet",    # BTRAD previous period (optional)
    "BNMDAILY_D_BTRAD_PREV":     "data/input/BNMDAILY_D_BTRAD_PREV.parquet"  # prev daily for TRANSPBA_PREV
}

# Output directory
OUTPUT_DIR = "data/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------
# Helper: read parquet safely
# ---------------------------
def read_parquet_safe(path: str, expected_cols: dict | None = None) -> pl.DataFrame:
    if not os.path.exists(path):
        print(f"[WARN] Input missing: {path} — returning empty DataFrame", file=sys.stderr)
        if expected_cols:
            return pl.DataFrame(schema=expected_cols)
        return pl.DataFrame()
    try:
        df = pl.read_parquet(path)
        return df
    except Exception as e:
        print(f"[ERROR] Failed to read {path}: {e}", file=sys.stderr)
        return pl.DataFrame()

# ---------------------------
# SAS macro/date logic
# ---------------------------
@dataclass
class Macros:
    REPTDATE: date
    REPTDAY: str
    REPTMON: str
    REPTYEAR: str
    REPTMON1: str
    NOWK: str
    NOWK1: str | None
    NOWK2: str | None
    NOWK3: str | None
    RDATE: str
    SDATE_str: str
    STARTDT: date
    TDATE: date

def compute_macros(reptdate_cli: str | None = None) -> Macros:
    # determine date
    if reptdate_cli:
        reptdate = datetime.strptime(reptdate_cli, "%Y-%m-%d").date()
    else:
        # fallback to LOAN_REPTDATE parquet if present
        loan = read_parquet_safe(INPUT_FILES["LOAN_REPTDATE"])
        if not loan.is_empty() and "REPTDATE" in loan.columns:
            first = loan["REPTDATE"][0]
            if isinstance(first, (datetime, date)):
                reptdate = first if isinstance(first, date) else first.date()
            else:
                # attempt parse
                try:
                    reptdate = datetime.fromisoformat(str(first)).date()
                except:
                    reptdate = date.today()
        else:
            reptdate = date.today()

    dd = reptdate.day
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

    mm = reptdate.month
    mm1 = mm - 1 if mm > 1 else 12
    startdt = date(reptdate.year, reptdate.month, 1)
    sdate = date(reptdate.year, reptdate.month, sdd)
    macros = Macros(
        REPTDATE=reptdate,
        REPTDAY=f"{reptdate.day:02d}",
        REPTMON=f"{reptdate.month:02d}",
        REPTYEAR=f"{reptdate.year}",
        REPTMON1=f"{mm1:02d}",
        NOWK=nowk,
        NOWK1=nowk1,
        NOWK2=nowk2,
        NOWK3=nowk3,
        RDATE=reptdate.strftime("%d%m%Y"),
        SDATE_str=sdate.strftime("%d%m%Y"),
        STARTDT=startdt,
        TDATE=reptdate
    )
    print(f"[INFO] Macros -> REPTDATE={macros.REPTDATE}, REPTMON={macros.REPTMON}, NOWK={macros.NOWK}")
    return macros

# ---------------------------
# PROC FORMAT equivalents (fill in actual mappings)
# ---------------------------
# IMPORTANT: fill these mapping dicts with the correct SAS format translations if available.
BTPROD_MAP = {}     # mapping LIABCODE -> PRODCD for direct
BTPRODI_MAP = {}    # mapping LIABCODE -> PRODCD for indirect
LOCUSTCD_MAP = {}   # mapping CUSTCODE -> CUSTCD
SECTCD_MAP = {}     # $SECTCD.
STATECD_MAP = {}    # $STATECD.
COLLCD_MAP = {}     # $COLLCD.
INDSECT_MAP = {}    # mapping industrial sector 5-digit -> sectorcd
RVRSE_MAP = {}      # reverse mapping for sectors (if needed)

# GLMNEM validity list from SAS $VALID where 'N' means exclude
VALID_EXCLUDE = {
    "00620","00560","00530","00640","00660",
    "00120","00140","00160","00590","00470",
    "00490","00440","00420","00340"
}

# DISBURSE/REPAID mapping list
DISBPAY_SET = {
    "00110","00130","00150","00170","00210","00230","00240","00250",
    "00260","00270","00280","00510","00540","00600","00800","00810",
    "00820","00830","00290","00300","00310"
}

# ---------------------------
# Utility helpers
# ---------------------------
def ensure_columns(df: pl.DataFrame, cols_defaults: dict) -> pl.DataFrame:
    """Ensure columns exist in df with types (dtype, default_value). Returns new df."""
    for c, (dtype, default) in cols_defaults.items():
        if c not in df.columns:
            df = df.with_columns(pl.lit(default).cast(dtype).alias(c))
        else:
            # cast if requested to dtype
            try:
                df = df.with_columns(pl.col(c).cast(dtype).alias(c))
            except Exception:
                # ignore if cast fails
                pass
    return df

def write_outputs(df: pl.DataFrame, name: str):
    p_parquet = os.path.join(OUTPUT_DIR, f"{name}.parquet")
    p_csv = os.path.join(OUTPUT_DIR, f"{name}.csv")
    try:
        df.write_parquet(p_parquet)
        df.write_csv(p_csv)
        print(f"[WRITE] {name} → {p_parquet} ({df.height} rows, {len(df.columns)} cols)")
    except Exception as e:
        print(f"[ERROR] writing {name}: {e}", file=sys.stderr)

# ---------------------------
# Stage 1: Read inputs
# ---------------------------
def load_inputs():
    inputs = {}
    for key, path in INPUT_FILES.items():
        inputs[key] = read_parquet_safe(path)
        print(f"[LOAD] {key}: {path} → rows={inputs[key].height}, cols={len(inputs[key].columns)}")
    return inputs

# ---------------------------
# Stage 2: Build INTRECV
# ---------------------------
def build_intrecv(btdtl: pl.DataFrame) -> pl.DataFrame:
    if btdtl.is_empty():
        return pl.DataFrame()
    # Ensure columns
    btdtl = ensure_columns(btdtl, {
        "ACCTNO": (pl.Utf8, ""),
        "LIABCODE": (pl.Utf8, ""),
        "TRANSREF": (pl.Utf8, ""),
        "GLMNEMO": (pl.Utf8, ""),
        "GLTRNTOT": (pl.Float64, 0.0)
    })
    intrecv = (
        btdtl
        .filter(pl.col("GLMNEMO").is_in(["01010","01020","01070"]))
        .groupby(["ACCTNO","LIABCODE","TRANSREF"], maintain_order=True)
        .agg(pl.col("GLTRNTOT").sum().alias("INTRECV"))
    )
    print(f"[STAGE] INTRECV computed: {intrecv.height} rows")
    return intrecv

# ---------------------------
# Stage 3: BTRADE filter (GLMNEMO valid & startswith '00')
# ---------------------------
def build_btrade(btdtl: pl.DataFrame, mast: pl.DataFrame) -> pl.DataFrame:
    if btdtl.is_empty():
        return pl.DataFrame()
    btdtl = ensure_columns(btdtl, {"ACCTNO": (pl.Utf8,""), "GLMNEMO": (pl.Utf8,""), "GLTRNTOT": (pl.Float64,0.0)})
    mast = ensure_columns(mast, {"ACCTNO": (pl.Utf8,"")})
    # Join to replicate SAS MERGE by ACCTNO
    merged = btdtl.join(mast, on="ACCTNO", how="left", suffix="_MAST")
    # Filter valid and GLMNEMO starting '00'
    merged = merged.with_columns(pl.col("GLMNEMO").cast(pl.Utf8).alias("GLMNEMO"))
    merged = merged.filter((~pl.col("GLMNEMO").is_in(list(VALID_EXCLUDE))) & (pl.col("GLMNEMO").str.slice(0,2) == "00"))
    print(f"[STAGE] BTRADE rows after filter: {merged.height}")
    return merged

# ---------------------------
# Stage 4: Build BTRAD (join INTRECV and compute BALANCE/CURBAL)
# ---------------------------
def calc_btrad(btrade: pl.DataFrame, intrecv: pl.DataFrame) -> pl.DataFrame:
    if btrade.is_empty():
        return pl.DataFrame()
    btrade = ensure_columns(btrade, {"ACCTNO": (pl.Utf8,""), "LIABCODE": (pl.Utf8,""), "TRANSREF": (pl.Utf8,""), "GLTRNTOT": (pl.Float64,0.0)})
    intrecv = ensure_columns(intrecv, {"ACCTNO": (pl.Utf8,""), "LIABCODE": (pl.Utf8,""), "TRANSREF": (pl.Utf8,""), "INTRECV": (pl.Float64,0.0)})
    out = btrade.join(intrecv, on=["ACCTNO","LIABCODE","TRANSREF"], how="left")
    out = out.with_columns([
        pl.when(pl.col("INTRECV").is_not_null()).then(pl.col("GLTRNTOT") + pl.col("INTRECV")).otherwise(pl.col("GLTRNTOT")).alias("BALANCE"),
        pl.col("GLTRNTOT").alias("CURBAL")
    ])
    print(f"[STAGE] BTRAD base rows: {out.height}")
    return out

# ---------------------------
# Stage 5: Enrichment / MAST-like assembly and FAC rollups
# ---------------------------
def build_mast_from_direct(btdtl_full: pl.DataFrame, btma: pl.DataFrame, macros: Macros) -> pl.DataFrame:
    """
    Recreate MAST-style aggregation that SAS built after DIRECT/INTRECV/TRANSPOSE steps.
    We'll implement a faithful but readable approach:
      - compute DCURBAL/ICURBAL per account/subacct where GLMNEMO first two chars = '00'
      - pivot/aggregate to ACCTNO/SUBACCT/BTREL
      - join INTRECV
      - apply FAC1/FAC2 split logic approximated
    """
    if btdtl_full.is_empty():
        return pl.DataFrame()
    # Ensure columns
    btdtl_full = ensure_columns(btdtl_full, {
        "ACCTNO": (pl.Utf8, ""),
        "SUBACCT": (pl.Utf8, ""),
        "BTREL": (pl.Utf8, ""),
        "DIRCTIND": (pl.Utf8, ""),
        "GLMNEMO": (pl.Utf8, ""),
        "GLTRNTOT": (pl.Float64, 0.0)
    })
    # Filter GLMNEMO starting '00' for direct balances and aggregate
    direct = (
        btdtl_full
        .filter(pl.col("GLMNEMO").str.slice(0,2) == "00")
        .groupby(["ACCTNO","SUBACCT","BTREL","DIRCTIND"], maintain_order=True)
        .agg(pl.col("GLTRNTOT").sum().alias("GLTRNTOT"))
    )
    # Pivot DIRCTIND -> columns 'D'/'I' -> DCURBAL/ICURBAL
    # We'll map DIRCTIND 'D' => DCURBAL, 'I' => ICURBAL
    pivot = direct.with_columns(
        pl.when(pl.col("DIRCTIND") == "D").then(pl.col("GLTRNTOT")).otherwise(pl.lit(0.0)).alias("DCURBAL"),
        pl.when(pl.col("DIRCTIND") == "I").then(pl.col("GLTRNTOT")).otherwise(pl.lit(0.0)).alias("ICURBAL"),
    ).groupby(["ACCTNO","SUBACCT","BTREL"], maintain_order=True).agg([
        pl.col("DCURBAL").sum().alias("DCURBAL"),
        pl.col("ICURBAL").sum().alias("ICURBAL")
    ])
    # INTRECV per ACCTNO/SUBACCT (we recompute here from full btdtl for GLMNEMO in interest)
    intrecv = (
        btdtl_full
        .filter(pl.col("GLMNEMO").is_in(["01010","01020","01070"]))
        .groupby(["ACCTNO","SUBACCT"], maintain_order=True)
        .agg(pl.col("GLTRNTOT").sum().alias("INTRECV"))
    )
    mast_wip = pivot.join(intrecv, on=["ACCTNO","SUBACCT"], how="left")
    mast_wip = mast_wip.with_columns(
        (pl.coalesce([pl.col("DCURBAL"), pl.lit(0.0)]) + pl.coalesce([pl.col("INTRECV"), pl.lit(0.0)])).alias("DBALANCE")
    )
    # Apply FAC1/FAC2 logic: SAS reclassified some subaccounts as FAC1 (using BTREL) and aggregated
    # We'll implement a pragmatic approach:
    #  - rows where BTREL not starting with F or O and SUBACCT not SGL -> change SUBACCT := BTREL and aggregate
    cond_fac1 = (~pl.col("BTREL").str.starts_with(("F","O"))) & (~pl.col("SUBACCT").str.contains("SGL"))
    fac1 = (
        mast_wip.filter(cond_fac1)
        .with_columns(pl.col("BTREL").alias("SUBACCT_NEW"))
        .groupby(["ACCTNO","SUBACCT_NEW","BTREL"], maintain_order=True)
        .agg([
            pl.col("ICURBAL").sum().alias("XICURBAL"),
            pl.col("DCURBAL").sum().alias("XDCURBAL"),
            pl.col("DBALANCE").sum().alias("XBALANCE"),
            pl.col("INTRECV").sum().alias("XINTRECV")
        ])
    )
    # Recombine fac1 sums into mast_wip by joining on ACCTNO and BTREL -> add to original balances
    mast_ext = mast_wip.join(
        fac1.select(["ACCTNO","BTREL","XICURBAL","XDCURBAL","XBALANCE","XINTRECV"]),
        on=["ACCTNO","BTREL"], how="left"
    ).with_columns([
        (pl.coalesce([pl.col("ICURBAL"), pl.lit(0.0)]) + pl.coalesce([pl.col("XICURBAL"), pl.lit(0.0)])).alias("ICURBAL"),
        (pl.coalesce([pl.col("DCURBAL"), pl.lit(0.0)]) + pl.coalesce([pl.col("XDCURBAL"), pl.lit(0.0)])).alias("DCURBAL"),
        (pl.coalesce([pl.col("DBALANCE"), pl.lit(0.0)]) + pl.coalesce([pl.col("XBALANCE"), pl.lit(0.0)])).alias("DBALANCE"),
        (pl.coalesce([pl.col("INTRECV"), pl.lit(0.0)]) + pl.coalesce([pl.col("XINTRECV"), pl.lit(0.0)])).alias("INTRECV"),
    ]).drop(["XICURBAL","XDCURBAL","XBALANCE","XINTRECV"])
    # FAC rollup to create FACFINAL like SAS: group by ACCTNO,BTREL and set SUBACCT='OV' if aggregated
    fac_roll = (
        mast_ext.groupby(["ACCTNO","BTREL"], maintain_order=True)
        .agg([
            pl.col("ICURBAL").sum().alias("ICURBAL_F"),
            pl.col("DCURBAL").sum().alias("DCURBAL_F"),
            pl.col("DBALANCE").sum().alias("DBALANCE_F"),
            pl.col("INTRECV").sum().alias("INTRECV_F")
        ])
        .with_columns(pl.lit("OV").alias("SUBACCT"))
    )
    # Merge the FACFINAL back by (ACCTNO,SUBACCT) — if matching rows exist, choose coalesced values
    mast_full = mast_ext.join(fac_roll, left_on=["ACCTNO","SUBACCT"], right_on=["ACCTNO","SUBACCT"], how="outer", suffix="_FAC")
    # Coalesce columns: prefer existing mast_ext values, else take fac_roll values
    mast_full = mast_full.with_columns([
        pl.coalesce([pl.col("ICURBAL"), pl.col("ICURBAL_F")]).alias("ICURBAL"),
        pl.coalesce([pl.col("DCURBAL"), pl.col("DCURBAL_F")]).alias("DCURBAL"),
        pl.coalesce([pl.col("DBALANCE"), pl.col("DBALANCE_F")]).alias("DBALANCE"),
        pl.coalesce([pl.col("INTRECV"), pl.col("INTRECV_F")]).alias("INTRECV"),
    ]).select(pl.exclude(["ICURBAL_F","DCURBAL_F","DBALANCE_F","INTRECV_F"]))
    print(f"[STAGE] MAST built: {mast_full.height} rows")
    return mast_full

# ---------------------------
# Stage 6: Merge MAST with BTRSA.MAST (customer metadata)
# ---------------------------
def merge_mast_with_cust(mast_built: pl.DataFrame, btrsa_mast: pl.DataFrame) -> pl.DataFrame:
    if mast_built.is_empty():
        return pl.DataFrame()
    btrsa_mast = ensure_columns(btrsa_mast, {"ACCTNO": (pl.Utf8,"")})
    out = mast_built.join(btrsa_mast, on="ACCTNO", how="left", suffix="_CUST")
    print(f"[STAGE] MAST merged with BTRSA.MAST: {out.height} rows")
    return out

# ---------------------------
# Stage 7: Build SUBA / Hierarchy (LEVELS)
# ---------------------------
def build_hierarchy_from_suba(suba: pl.DataFrame) -> pl.DataFrame:
    """
    Convert SUBA into derived hierarchy fields used later (FACIL, PARENT, LEVEL1..LEVEL5).
    This approximates the SAS macro %LEVEL (which loops levels 2..5) — we provide simplified but consistent logic.
    """
    if suba.is_empty():
        return pl.DataFrame()
    suba = ensure_columns(suba, {"ACCTNO": (pl.Utf8,""), "SUBACCT": (pl.Utf8,""), "BTREL": (pl.Utf8,""), "SINDICAT": (pl.Utf8,""), "CREATDS": (pl.Date,None), "EXPIRDS": (pl.Date,None)})
    # Default FACIL and PARENT
    # If BTREL blank and SUBACCT == 'OV' -> FACIL='OV', PARENT='NIL', IND='A'
    suba = suba.with_columns([
        pl.when((pl.col("BTREL").is_null() | (pl.col("BTREL") == "")) & (pl.col("SUBACCT").str.strip() == "OV"))
          .then(pl.struct([pl.lit("OV").alias("FACIL"), pl.lit("NIL").alias("PARENT"), pl.lit("A").alias("IND")]))
          .otherwise(pl.struct([
              pl.when(pl.col("SUBACCT").str.lengths() >= 5).then(pl.col("SUBACCT").str.slice(0,5)).otherwise(pl.col("SUBACCT")).alias("FACIL"),
              pl.col("BTREL").alias("PARENT"),
              pl.lit("E").alias("IND")
          ])).alias("H")
    ]).with_columns([
        pl.col("H").struct.field("FACIL").alias("FACIL"),
        pl.col("H").struct.field("PARENT").alias("PARENT"),
        pl.col("H").struct.field("IND").alias("IND")
    ]).drop("H")
    # LEVEL1 default
    suba = suba.with_columns([
        pl.when((pl.col("PARENT").is_null()) | (pl.col("PARENT").str.strip() == "")).then(pl.lit("OV")).otherwise(pl.col("PARENT")).alias("LEVEL1"),
        pl.col("FACIL").alias("KEYFACI")
    ])
    # LEVEL2..5 simple placeholders: more exact logic requires recursive matching in SAS macro
    suba = suba.with_columns([
        pl.when(pl.col("SUBACCT").str.slice(1,3) == "SGL").then(pl.lit("SGL")).otherwise(pl.lit(None)).alias("LEVEL2"),
        pl.lit(None).alias("LEVEL3"),
        pl.lit(None).alias("LEVEL4"),
        pl.lit(None).alias("LEVEL5"),
        pl.lit(2).alias("LEVELS")  # baseline
    ])
    # Normalize leading digits (SAS removed leading numeric grouping for LEVEL2..5)
    def normalize_col_expr(colname):
        return pl.when(pl.col(colname).is_not_null() & (pl.col(colname).str.lengths() > 0)) \
                 .then(pl.when(pl.col(colname).str.slice(0,1).is_in([str(i) for i in range(1,10)])).then(pl.col(colname).str.slice(1,3)).otherwise(pl.col(colname))) \
                 .otherwise(pl.col(colname))
    for c in ["LEVEL2","LEVEL3","LEVEL4","LEVEL5"]:
        suba = suba.with_columns(normalize_col_expr(c).alias(c))
    print(f"[STAGE] SUBA hierarchy built: {suba.height} rows")
    return suba.select(["ACCTNO","SUBACCT","LEVELS","LEVEL1","LEVEL2","LEVEL3","LEVEL4","LEVEL5","KEYFACI","IND"])

# ---------------------------
# Stage 8: Build BTMAST (final master output)
# ---------------------------
def build_bt_mast(mast_joined: pl.DataFrame, suba_h: pl.DataFrame, direct_flags: pl.DataFrame) -> pl.DataFrame:
    """
    This mirrors the SAS block that produces BTRWH1.BTMAST&...:
      - merge mast_joined and hierarchy
      - compute DCURBALX and ICURBALX (negatives -> 0, nulls preserved)
      - compute DUNDRAWN / IUNDRAWN and apply SINDICAT rules
    """
    if mast_joined.is_empty():
        return pl.DataFrame()
    # Merge with hierarchy
    mast_joined = mast_joined.join(suba_h, on=["ACCTNO","SUBACCT"], how="left")
    # Join direct flags (D/I) computed earlier per ACCTNO,SUBACCT
    mast_joined = mast_joined.join(direct_flags, on=["ACCTNO","SUBACCT"], how="left")
    # DCURBALX/ICURBALX set negatives to zero
    mast_joined = mast_joined.with_columns([
        pl.when(pl.col("DCURBAL").is_not_null() & (pl.col("DCURBAL") < 0)).then(pl.lit(0)).otherwise(pl.col("DCURBAL")).alias("DCURBALX"),
        pl.when(pl.col("ICURBAL").is_not_null() & (pl.col("ICURBAL") < 0)).then(pl.lit(0)).otherwise(pl.col("ICURBAL")).alias("ICURBALX")
    ])
    # If D or I flags are missing, keep nulls; SAS set to '.' meaning missing; we keep None
    mast_joined = mast_joined.with_columns([
        pl.when(pl.col("I").is_null()).then(pl.col("ICURBALX")).otherwise(pl.col("ICURBALX")).alias("ICURBALX"),
        pl.when(pl.col("D").is_null()).then(pl.col("DCURBALX")).otherwise(pl.col("DCURBALX")).alias("DCURBALX")
    ])
    # DUNDRAWN/IUNDRAWN calculations: DUNDRAWN uses APPRLIMT - DCURBALX - ICURBALX (if both non-null)
    mast_joined = mast_joined.with_columns([
        pl.when(pl.col("DCURBALX").is_not_null() & pl.col("ICURBALX").is_not_null())
          .then(pl.col("APPRLIMT") - pl.col("DCURBALX") - pl.col("ICURBALX"))
          .when(pl.col("DCURBALX").is_not_null())
          .then(pl.col("APPRLIMT") - pl.col("DCURBALX"))
          .otherwise(pl.lit(None)).alias("DUNDRAWN"),
        (pl.col("APPRLIMT") - pl.col("ICURBALX")).alias("IUNDRAWN")
    ])
    # ORIGMT as SAS: EXPRDATE - ISSDTE < 366 => '10' else '20'
    mast_joined = mast_joined.with_columns(
        pl.when((pl.col("EXPRDATE").is_not_null()) & (pl.col("ISSDTE").is_not_null()) &
                ((pl.col("EXPRDATE") - pl.col("ISSDTE")).dt.days() < 366))
        .then(pl.lit("10")).otherwise(pl.lit("20")).alias("ORIGMT")
    )
    # Handle SINDICAT == 'S' zeroing rules for balances (S = syndicate)
    mast_joined = mast_joined.with_columns([
        pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("DBALANCE")).alias("DBALANCE"),
        pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("DCURBAL")).alias("DCURBAL"),
        pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("DCURBALX")).alias("DCURBALX"),
        pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("INTRECV")).alias("INTRECV"),
        pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("ICURBAL")).alias("ICURBAL"),
        pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("ICURBALX")).alias("ICURBALX"),
        pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("APPRLIMT")).alias("APPRLIMT"),
        pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("DUNDRAWN")).alias("DUNDRAWN"),
        pl.when(pl.col("SINDICAT") == "S").then(None).otherwise(pl.col("IUNDRAWN")).alias("IUNDRAWN"),
    ])
    mast_joined = mast_joined.with_columns(pl.lit("D").alias("AMTIND"))
    # Deduplicate similar to SAS PROC SORT NODUPKEYS BY ACCTNO SUBACCT
    mast_joined = mast_joined.unique(subset=["ACCTNO","SUBACCT"], keep="first")
    print(f"[STAGE] BTMAST built: {mast_joined.height} rows")
    return mast_joined

# ---------------------------
# Stage 9: DISBURSE / REPAID processing from COMM
# ---------------------------
def build_disburse_repaid(comm: pl.DataFrame, macros: Macros, pba01: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Build DISBPAY and DISBURSE700 equivalent datasets from COMM (transactions)
    """
    if comm.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    comm = ensure_columns(comm, {
        "ACCTNO": (pl.Utf8,""),
        "GLMNEMO": (pl.Utf8,""),
        "TRANSAMT": (pl.Float64,0.0),
        "TRANAMT_CCY": (pl.Float64,0.0),
        "ISSDTE": (pl.Date,None),
        "CURRENCY": (pl.Utf8,"MYR"),
        "PAYGRNO": (pl.Utf8,""),
        "TRANSREF": (pl.Utf8,""),
    })
    # Normalize GLMNEMO: if 4-char then append '0' like SAS GLMNEMO -> GLMNEMO||'0'
    comm = comm.with_columns(pl.when(pl.col("GLMNEMO").str.lengths() == 4).then(pl.col("GLMNEMO") + pl.lit("0")).otherwise(pl.col("GLMNEMO")).alias("GLMNEMO"))
    # TRANSREF: if PAYGRNO present then substr(PAYGRNO,1,7)
    comm = comm.with_columns(pl.when(pl.col("PAYGRNO").is_not_null() & (pl.col("PAYGRNO") != "")).then(pl.col("PAYGRNO").str.slice(0,7)).otherwise(pl.col("TRANSREF")).alias("TRANSREF"))
    # Date window: STARTDT..TDATE
    start_date = macros.STARTDT
    tdate = macros.TDATE
    # DISBURSE700
    disburse700 = comm.filter(
        (pl.col("GLMNEMO").is_in(["08000","09000"])) &
        (pl.col("ISSDTE").is_between(start_date, tdate))
    ).select([
        "ACCTNO","TRANSREF","ISSDTE",
        pl.col("TRANSAMT").alias("DISBURSE"),
        pl.when(pl.col("CURRENCY") != "MYR").then(pl.col("TRANAMT_CCY")).otherwise(pl.lit(None)).alias("DISBURSEFX")
    ])
    # REPAID101: GLMNEMO in ('01010','01140') & TRANSAMT < 0
    repaid101 = comm.filter(
        (pl.col("GLMNEMO").is_in(["01010","01140"])) &
        (pl.col("ISSDTE").is_between(start_date, tdate)) &
        (pl.col("TRANSAMT") < 0)
    ).groupby(["ACCTNO","TRANSREF"], maintain_order=True).agg([
        pl.col("TRANSAMT").sum().alias("REPAID"),
        pl.when(pl.col("CURRENCY") != "MYR").then(pl.col("TRANAMT_CCY")).otherwise(pl.lit(0.0)).sum().alias("REPAIDFX")
    ]).with_columns([
        pl.when(pl.col("REPAID") < 0).then(-pl.col("REPAID")).otherwise(pl.col("REPAID")).alias("REPAID"),
        pl.when(pl.col("REPAIDFX") < 0).then(-pl.col("REPAIDFX")).otherwise(pl.col("REPAIDFX")).alias("REPAIDFX")
    ])
    # DISBPAY (mapping via DISBPAY_SET)
    disbpay = comm.filter(pl.col("ISSDTE").is_between(start_date, tdate)).with_columns(
        pl.col("GLMNEMO").apply(lambda x: "Y" if x in DISBPAY_SET else "N").alias("DISBPAY_F")
    ).filter(pl.col("DISBPAY_F") == "Y").with_columns([
        pl.when(pl.col("TRANSAMT") > 0).then(pl.when(pl.col("GLMNEMO") != "01010").then(pl.col("TRANSAMT")).otherwise(pl.lit(None))).otherwise(pl.lit(None)).alias("DISBURSE"),
        pl.when(pl.col("TRANSAMT") <= 0).then(-pl.col("TRANSAMT")).otherwise(pl.lit(None)).alias("REPAID"),
        pl.when((pl.col("CURRENCY") != "MYR") & (pl.col("TRANSAMT") > 0) & (pl.col("GLMNEMO") != "01010")).then(pl.col("TRANAMT_CCY")).otherwise(pl.lit(None)).alias("DISBURSEFX"),
        pl.when((pl.col("CURRENCY") != "MYR") & (pl.col("TRANSAMT") <= 0)).then(-pl.col("TRANAMT_CCY")).otherwise(pl.lit(None)).alias("REPAIDFX")
    ]).select(["ACCTNO","TRANSREF","ISSDTE","GLMNEMO","DISBURSE","DISBURSEFX","REPAID","REPAIDFX"])
    # Attach repaid101 principal sums (REPAIDPRIN) and collapse
    disbpay = disbpay.join(repaid101.select(["ACCTNO","TRANSREF","REPAID","REPAIDFX"]).rename({"REPAID":"REPAIDPRIN","REPAIDFX":"REPAIDPRINFX"}), on=["ACCTNO","TRANSREF"], how="left")
    disbpay_final = disbpay.groupby(["ACCTNO","TRANSREF"], maintain_order=True).agg([
        pl.col("DISBURSE").sum().alias("DISBURSE"),
        pl.col("DISBURSEFX").sum().alias("DISBURSEFX"),
        pl.col("REPAID").sum().alias("REPAID"),
        pl.col("REPAIDPRIN").sum().alias("REPAIDPRIN"),
        pl.col("REPAIDPRINFX").sum().alias("REPAIDPRINFX")
    ])
    # Post-processing per SAS rules (REPAID vs DISBURSE interplay) is done later during merge
    print(f"[STAGE] DISBPAY rows: {disbpay_final.height}, DISBURSE700 rows: {disburse700.height}")
    return disbpay_final, disburse700

# ---------------------------
# Stage 10: Tie BA with PBA info (UNEARNED)
# ---------------------------
def tie_ba_with_pba(bnm_daily: pl.DataFrame, pba01: pl.DataFrame):
    """
    Recreate TB/BA/APNBT logic: join PBA (UNEARNED) into BA items to compute OUTSTAND and BALANCE.
    We'll return concatenated TBBA (TB + BA + APNBT)
    """
    if bnm_daily.is_empty():
        return pl.DataFrame()
    bnm_daily = ensure_columns(bnm_daily, {
        "OUTSTAND": (pl.Float64,0.0), "LIABCODE": (pl.Utf8,""), "DIRCTIND": (pl.Utf8,"D"),
        "TRANSREF": (pl.Utf8,""), "BRANCH": (pl.Utf8,""), "ACCTNO": (pl.Utf8,""), "FCVALUE": (pl.Float64,0.0), "CERTNO": (pl.Float64,None)
    })
    tb = bnm_daily.filter((pl.col("OUTSTAND") > 0) & (~pl.col("LIABCODE").is_in(["BAI","BAP","BAS","BAE"]))).with_columns(
        pl.when(pl.col("DIRCTIND") == "D").then(pl.col("OUTSTAND")).otherwise(pl.lit(0.0)).alias("BALANCE")
    )
    ba = bnm_daily.filter((pl.col("OUTSTAND") > 0) & (pl.col("LIABCODE").is_in(["BAI","BAP","BAS","BAE"])))
    ap = bnm_daily.filter(pl.col("OUTSTAND") <= 0)
    # PBA aggregation by TRANSREF
    pba01 = ensure_columns(pba01, {"TRANSREF": (pl.Utf8,""), "UNEARNED": (pl.Float64, 0.0), "FCVALUE": (pl.Float64, 0.0), "CERTNO": (pl.Float64,None), "UTRDF": (pl.Utf8,"")})
    pba_summ = pba01.groupby("TRANSREF", maintain_order=True).agg([
        pl.col("UNEARNED").sum().alias("UNEARNED"),
        pl.col("FCVALUE").max().alias("FCVALUE"),
        pl.col("CERTNO").max().alias("CERTNO"),
        pl.col("UTRDF").max().alias("UTRDF"),
        pl.col("YIELD").max().alias("YIELD") if "YIELD" in pba01.columns else pl.lit(None).alias("YIELD")
    ])
    # Join BA with PBA
    ba_tied = ba.join(pba_summ, on="TRANSREF", how="left").with_columns([
        pl.when(pl.col("CERTNO").is_null()).then(pl.lit(0.0)).otherwise((pl.col("FCVALUE").fill_null(0.0) - pl.col("UNEARNED").fill_null(0.0))).alias("BALANCE"),
        pl.when(pl.col("CERTNO").is_null()).then(pl.lit(0.0)).otherwise((pl.col("FCVALUE").fill_null(0.0) - pl.col("UNEARNED").fill_null(0.0))).alias("OUTSTAND")
    ])
    tbba = pl.concat([tb, ba_tied, ap], how="diagonal_relaxed")
    print(f"[STAGE] TB/BA/APNBT (tbba) rows: {tbba.height}")
    return tbba

# ---------------------------
# Stage 11: Build BTRAD & BTRADI outputs, attach DISBPAY and CLOSED handling
# ---------------------------
def finalize_btrad_outputs(btrad_direct: pl.DataFrame, disbpay: pl.DataFrame, prev_btrad: pl.DataFrame, mtd: pl.DataFrame, prev_daily: pl.DataFrame):
    """
    1) attach DISBPAY to current BTRAD on (ACCTNO, TRANSREF)
    2) create CLOSED items if prev_btrad exists in prev but missing in current and attach DISBPAY
    3) join MTD and prev_daily TRANSPBA_PREV to fill TRANSREF_PBA
    """
    if btrad_direct.is_empty():
        print("[WARN] btrad_direct empty")
        current = pl.DataFrame()
    else:
        current = ensure_columns(btrad_direct, {"ACCTNO": (pl.Utf8,""), "TRANSREF": (pl.Utf8,"")})
    disbpay = ensure_columns(disbpay, {"ACCTNO": (pl.Utf8,""), "TRANSREF": (pl.Utf8,""), "DISBURSE": (pl.Float64,0.0), "REPAID": (pl.Float64,0.0), "REPAIDPRIN": (pl.Float64,0.0)})
    # Attach DISBPAY
    merged = current.join(disbpay, on=["ACCTNO","TRANSREF"], how="left")
    # fill numeric nulls with 0 for sums
    merged = merged.with_columns([
        pl.col("DISBURSE").fill_null(0.0),
        pl.col("REPAID").fill_null(0.0),
        pl.col("REPAIDPRIN").fill_null(0.0)
    ])
    # CLOSED logic: previous BTRAD rows not present in current -> keep them but only with DISBPAY info
    prev_btrad = ensure_columns(prev_btrad, {"ACCTNO": (pl.Utf8,""), "TRANSREF": (pl.Utf8,"")})
    if not prev_btrad.is_empty():
        prev_keys = prev_btrad.select(["ACCTNO","TRANSREF"]).unique()
        cur_keys = merged.select(["ACCTNO","TRANSREF"]).unique() if not merged.is_empty() else pl.DataFrame()
        closed_keys = prev_keys.join(cur_keys, on=["ACCTNO","TRANSREF"], how="anti")
        closed = closed_keys.join(disbpay, on=["ACCTNO","TRANSREF"], how="left")
        # fill columns expected by downstream
        closed = closed.with_columns([
            pl.lit(0.0).alias("FCVALUE"),
            pl.lit(0.0).alias("INTAMT"),
            pl.lit(0.0).alias("UNEARNED"),
            pl.lit("").alias("UTRDF")
        ])
        full = pl.concat([merged, closed], how="diagonal_relaxed")
    else:
        full = merged
    # Add TRANSREX copy of TRANSREF if needed and attach MTD + prev_daily
    full = full.with_columns(pl.when(pl.col("TRANSREF").is_not_null()).then(pl.col("TRANSREF")).otherwise(pl.lit("")).alias("TRANSREX"))
    # Join MTD (MTDAVBAL_MIS) and prev_daily TRANSPBA_PREV (use keys ACCTNO, TRANSREX)
    if not mtd.is_empty():
        mtd = ensure_columns(mtd, {"ACCTNO": (pl.Utf8,""), "TRANSREX": (pl.Utf8,""), "MTDAVBAL_MIS": (pl.Float64,0.0)})
        full = full.join(mtd, on=["ACCTNO","TRANSREX"], how="left")
    if not prev_daily.is_empty():
        prev_daily = ensure_columns(prev_daily, {"ACCTNO": (pl.Utf8,""), "TRANSREX": (pl.Utf8,""), "TRANSPBA_PREV": (pl.Utf8,"")})
        full = full.join(prev_daily.select(["ACCTNO","TRANSREX","TRANSPBA_PREV"]), on=["ACCTNO","TRANSREX"], how="left")
        full = full.with_columns(pl.when((pl.col("TRANSREF_PBA").is_null()) | (pl.col("TRANSREF_PBA") == "")).then(pl.col("TRANSPBA_PREV")).otherwise(pl.col("TRANSREF_PBA")).alias("TRANSREF_PBA"))
        full = full.drop("TRANSPBA_PREV") if "TRANSPBA_PREV" in full.columns else full
    print(f"[STAGE] Final BTRAD output rows: {full.height}")
    return full

# ---------------------------
# Main orchestration
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="EIBWBTRD Polars pipeline (SAS -> Python conversion)")
    parser.add_argument("--reptdate", type=str, default=None, help="Reporting date YYYY-MM-DD (optional)")
    args = parser.parse_args()

    macros = compute_macros(args.reptdate)
    # Load inputs
    inputs = load_inputs()
    bnm_btdtl = inputs.get("BNM_BTDTL", pl.DataFrame())
    bnm_mast = inputs.get("BNM_MAST", pl.DataFrame())
    bnm_btma = inputs.get("BNM_BTMAST", pl.DataFrame())
    bnm_daily = inputs.get("BNMDAILY_BTDTL", pl.DataFrame())
    btrsa_comm = inputs.get("BTRSA_COMM", pl.DataFrame())
    btrsa_suba = inputs.get("BTRSA_SUBA", pl.DataFrame())
    btrsa_mast = inputs.get("BTRSA_MAST", pl.DataFrame())
    pba01 = inputs.get("PBA01", pl.DataFrame())
    mtd_btavg = inputs.get("MTD_BTAVG", pl.DataFrame())
    prev_btrad = inputs.get("BNM_PREV_BTRAD", pl.DataFrame())
    prev_daily = inputs.get("BNMDAILY_D_BTRAD_PREV", pl.DataFrame())

    # Step A: INTRECV from BTM BTDTL
    intrecv = build_intrecv(bnm_btdtl)
    write_outputs(intrecv, f"EIBWBTRD_INTRECV_{macros.REPTMON}_{macros.NOWK}")

    # Step B: BTRADE subset (filter GLMNEMO valid and startswith '00') and BTRAD base join
    btrade = build_btrade(bnm_btdtl, bnm_mast)
    btrad_base = calc_btrad(btrade, intrecv)
    write_outputs(btrad_base, f"EIBWBTRD_BTRAD_BASE_{macros.REPTMON}_{macros.NOWK}")

    # Step C: MAST style assembly from direct balances (pivot, fac rollups)
    mast_built = build_mast_from_direct(bnm_btdtl, bnm_btma, macros)
    write_outputs(mast_built, f"EIBWBTRD_MAST_WIP_{macros.REPTMON}_{macros.NOWK}")

    # Step D: Merge MAST built with BTRSA.MAST (customer info)
    mast_joined = merge_mast_with_cust(mast_built, btrsa_mast)
    write_outputs(mast_joined, f"EIBWBTRD_MAST_JOINED_{macros.REPTMON}_{macros.NOWK}")

    # Step E: SUBA hierarchy
    suba_h = build_hierarchy_from_suba(btrsa_suba)
    write_outputs(suba_h, f"EIBWBTRD_SUBA_HIER_{macros.REPTMON}_{macros.NOWK}")

    # Step F: Direct flags from SUBA: build D/I indicator per ACCTNO,SUBACCT
    # Build direct_flags dataset similar to SAS DIRECT step (collapse DIRCTIND)
    if not btrsa_suba.is_empty():
        direct_flags = (
            btrsa_suba.select(["ACCTNO","SUBACCT","DIRCTIND"])
            .groupby(["ACCTNO","SUBACCT"], maintain_order=True)
            .agg([
                (pl.col("DIRCTIND") == "D").any().cast(pl.Int8).alias("D"),
                (pl.col("DIRCTIND") == "I").any().cast(pl.Int8).alias("I")
            ])
        )
    else:
        direct_flags = pl.DataFrame()
    write_outputs(direct_flags, f"EIBWBTRD_DIRECT_FLAGS_{macros.REPTMON}_{macros.NOWK}")

    # Step G: Build BTMAST (final master) using mast_joined + hierarchy + direct_flags
    bt_mast = build_bt_mast(mast_joined, suba_h, direct_flags)
    write_outputs(bt_mast, f"EIBWBTRD_BTMAST_{macros.REPTMON}_{macros.NOWK}")

    # Step H: DISBURSE / REPAID
    disbpay, disburse700 = build_disburse_repaid(btrsa_comm, macros, pba01)
    write_outputs(disbpay, f"EIBWBTRD_DISBPAY_{macros.REPTMON}_{macros.NOWK}")
    write_outputs(disburse700, f"EIBWBTRD_DISBURSE700_{macros.REPTMON}_{macros.NOWK}")

    # Step I: TB/BA/APNBT tie with PBA
    tbba = tie_ba_with_pba(bnm_daily, pba01)
    write_outputs(tbba, f"EIBWBTRD_TBBA_{macros.REPTMON}_{macros.NOWK}")

    # Step J: finalize BTRAD outputs (merge disbpay into btrad base filtered for DIRCTIND='D')
    # filter btrad_base for DIRCTIND if column exists
    if "DIRCTIND" in btrad_base.columns:
        btrad_direct = btrad_base.filter(pl.col("DIRCTIND") == "D")
    else:
        btrad_direct = btrad_base
    btrad_out = finalize_btrad_outputs(btrad_direct, disbpay, prev_btrad, mtd_btavg, prev_daily)
    write_outputs(btrad_out, f"EIBWBTRD_BTRAD_FINAL_{macros.REPTMON}_{macros.NOWK}")

    # Step K: produce indirect (BTRADI) if needed
    if "DIRCTIND" in btrad_base.columns:
        btradi = btrad_base.filter(pl.col("DIRCTIND") == "I")
    else:
        btradi = pl.DataFrame()
    write_outputs(btradi, f"EIBWBTRD_BTRADI_FINAL_{macros.REPTMON}_{macros.NOWK}")

    # Final: also persist BTMAST for data warehouse (as SAS did)
    # We already saved BTMAST earlier; optionally export a trimmed set similar to SAS BT.* naming
    # Example: add ACCTNOX numeric if startswith '25'
    if not bt_mast.is_empty() and "ACCTNO" in bt_mast.columns:
        bt_for_warehouse = bt_mast.with_columns(
            pl.when(pl.col("ACCTNO").str.starts_with("25")).then(pl.col("ACCTNO").cast(pl.Int64, strict=False)).otherwise(pl.lit(0)).alias("ACCTNOX")
        )
        write_outputs(bt_for_warehouse, f"EIBWBTRD_BTMAST_FORWAREHOUSE_{macros.REPTMON}_{macros.NOWK}_{macros.REPTYEAR}")

    print("[DONE] EIBWBTRD pipeline completed.")

if __name__ == "__main__":
    main()
