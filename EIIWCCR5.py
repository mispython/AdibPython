#!/usr/bin/env python3
"""
EIIWCCR5 -> Python/Polars translation (monolithic)
Run: python eiiwccr5_polars.py

Notes:
- This file attempts to replicate the SAS EIIWCCR5 logic you provided.
- It expects input datasets as Parquet or CSV files; see INPUT_PATHS below.
- Adjust input paths to point to your extracted datasets.
- Outputs: CSV/Parquet files and fixed-width files (approximate).
"""

from datetime import datetime, timedelta, date
import polars as pl
import math
import sys
import os
from typing import Optional

# -----------------------
# CONFIG: Paths to sources
# -----------------------
# Replace these with actual paths to your exported SAS tables (parquet/csv).
# If you don't have a dataset, create a minimal test CSV or Parquet with the required columns.
INPUT_PATHS = {
    "BNM_REPTDATE": "inputs/BNM_REPTDATE.parquet",  # must contain REPTDATE (SAS date numeric or 'YYYY-MM-DD')
    "CCRISP_LOAN":  "inputs/CCRISP_LOAN.parquet",   # CCRISP.LOAN or equivalent
    "CCRISP_OVERDFS": "inputs/CCRISP_OVERDFS.parquet",
    "LIMT_OVERDFT": "inputs/LIMIT_OVERDFT.parquet",
    "BNM_LNACC4": "inputs/BNM_LNACC4.parquet",
    "DISPAY_IDISPAYMTH": "inputs/DISPAY_IDISPAYMTH.parquet",
    "WOPS_IWOPOS": "inputs/WOPS_IWOPOS.parquet",
    "WOMV_ISUMOV": "inputs/WOMV_ISUMOV.parquet",
    "ODSQ_IODSQ": "inputs/ODSQ_IODSQ.parquet",
    "LNSQ_ILNSQ": "inputs/LNSQ_ILNSQ.parquet",
    "WRIOFAC": "inputs/WRIOFAC.txt",   # fixed-width or csv listing writoff mappings used in SAS
    "BNMSUM_SUMM1": "inputs/BNMSUM_SUMM1.parquet",
    "ELDS_ELNMAX": "inputs/ELDS_ELNMAX.parquet",
    "ELDS_RVNOTE": "inputs/ELDS_RVNOTE.parquet",
    # Add more as needed...
}

OUTPUT_DIR = "outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -----------------------
# Helpers
# -----------------------
def safe_read_parquet_or_csv(path: str) -> pl.DataFrame:
    if not os.path.exists(path):
        print(f"[WARN] Input file not found: {path} -> returning empty DataFrame", file=sys.stderr)
        return pl.DataFrame()
    ext = os.path.splitext(path)[1].lower()
    if ext in (".parquet", ".pq"):
        return pl.read_parquet(path)
    elif ext in (".csv", ".txt"):
        # assume comma, allow fallback
        try:
            return pl.read_csv(path)
        except Exception:
            return pl.read_csv(path, separator="|")
    else:
        # try parquet first, then csv
        try:
            return pl.read_parquet(path)
        except Exception:
            return pl.read_csv(path)

def sas_date_to_pydate(sas_date):
    # SAS date counts days since 1960-01-01
    if sas_date is None:
        return None
    try:
        # If already a python date or ISO string
        if isinstance(sas_date, (date, datetime)):
            return sas_date
        if isinstance(sas_date, str):
            # try parse common patterns
            for fmt in ("%Y-%m-%d", "%d%b%Y", "%d%m%Y", "%d%m%y", "%Y%m%d"):
                try:
                    return datetime.strptime(sas_date, fmt).date()
                except Exception:
                    pass
            # fallback parse
            try:
                return datetime.fromisoformat(sas_date).date()
            except Exception:
                return None
        # numeric
        n = float(sas_date)
        # SAS: days since 1960-01-01
        base = date(1960, 1, 1)
        return base + timedelta(days=int(n))
    except Exception:
        return None

def format_znum(v, width, decimals=0):
    # returns string zero-padded for numeric z-format approximations
    if v is None or (isinstance(v, float) and math.isnan(v)):
        v = 0
    if decimals == 0:
        return str(int(round(v))).zfill(width)
    else:
        fmt = f"{{:0{width}.{decimals}f}}"
        return fmt.format(float(v))

def write_fixed_width(df: pl.DataFrame, layout: list, outpath: str):
    """
    layout: list of tuples (field_name, start_pos1, width, fmt_type, fmt_details)
      - fmt_type: 'int','zint','str','date','float'
    This is a simple writer: writes every row by formatting fields in given order.
    """
    with open(outpath, "w", encoding="utf-8") as fh:
        for r in df.iter_rows(named=True):
            rowbuf = []
            for field_name, width, ftype, fmt_detail in layout:
                val = r.get(field_name)
                if ftype in ("int", "zint"):
                    if val is None:
                        s = "".zfill(width)
                    else:
                        s = str(int(val)).zfill(width)
                elif ftype == "float":
                    d = fmt_detail or 2
                    if val is None:
                        s = ("0" if d==0 else f"{0:.{d}f}").zfill(width)
                    else:
                        s = f"{float(val):0{width}.{d}f}"
                elif ftype == "date":
                    if val is None:
                        s = "0"*width
                    elif isinstance(val, (date, datetime)):
                        s = val.strftime("%d%m%Y")[:width]
                    else:
                        # assume string already formatted
                        s = str(val)[:width].rjust(width,"0")
                else:
                    s = "" if val is None else str(val)
                    if len(s) > width:
                        s = s[:width]
                    else:
                        s = s.ljust(width)
                rowbuf.append(s)
            fh.write("".join(rowbuf) + "\n")

# -----------------------
# Macro / constant expansions (from your SAS job)
# -----------------------
HP = [128,130,380,381,700,705,983,993,996]
ALHP = [128,130,131,132,380,381,700,705,720,725,983,993,678,679,698,699]
ALP = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890 ")  # as characters

PRDA = [302,350,364,365,506,902,903,910,925,951]
PRDB = [983,993,996]
PRDC = [110,112,114,200,201,209,210,211,212,225,227,141,
           230,232,237,239,243,244,246]
# (other PRD lists continue as in SAS - for brevity, we reuse earlier lists if needed)
PRDD = PRDD = [
111,113,115,116,117,118,119,120,126,127,129,139,140,147,173,
170,180,181,182,193,204,205,214,215,219,220,226,228,231,233,
234,235,245,247,142,143,183,532,533,573,574,248,
236,238,240,241,242,300,301,304,305,345,359,361,362,363,
504,505,509,510,515,516,531,517,518,519,521,522,
523,524,525,526,527,528,529,530,555,556,559,560,561,564,
565,566,567,568,569,570,900,901,906,907,909,914,915,
950
]

# -----------------------
# Load input files
# -----------------------
print("Loading input datasets...")
BNM_REPTDATE = safe_read_parquet_or_csv(INPUT_PATHS.get("BNM_REPTDATE", ""))
CCRISP_LOAN  = safe_read_parquet_or_csv(INPUT_PATHS.get("CCRISP_LOAN", ""))
CCRISP_OVERDFS = safe_read_parquet_or_csv(INPUT_PATHS.get("CCRISP_OVERDFS", ""))
LIMT_OVERDFT = safe_read_parquet_or_csv(INPUT_PATHS.get("LIMT_OVERDFT", ""))
BNM_LNACC4 = safe_read_parquet_or_csv(INPUT_PATHS.get("BNM_LNACC4", ""))
DISPAY_IDISPAYMTH = safe_read_parquet_or_csv(INPUT_PATHS.get("DISPAY_IDISPAYMTH", ""))
WOPS_IWOPOS = safe_read_parquet_or_csv(INPUT_PATHS.get("WOPS_IWOPOS", ""))
WOMV_ISUMOV = safe_read_parquet_or_csv(INPUT_PATHS.get("WOMV_ISUMOV", ""))
ODSQ_IODSQ = safe_read_parquet_or_csv(INPUT_PATHS.get("ODSQ_IODSQ", ""))
LNSQ_ILNSQ = safe_read_parquet_or_csv(INPUT_PATHS.get("LNSQ_ILNSQ", ""))
WRIOFAC = safe_read_parquet_or_csv(INPUT_PATHS.get("WRIOFAC", "WRIOFAC.txt"))
BNMSUM_SUMM1 = safe_read_parquet_or_csv(INPUT_PATHS.get("BNMSUM_SUMM1", ""))

# Convert empty frames to DataFrames with no rows to avoid errors on joins
def ensure_df(d: pl.DataFrame) -> pl.DataFrame:
    return d if isinstance(d, pl.DataFrame) else pl.DataFrame()

BNM_REPTDATE = ensure_df(BNM_REPTDATE)
CCRISP_LOAN = ensure_df(CCRISP_LOAN)
CCRISP_OVERDFS = ensure_df(CCRISP_OVERDFS)
LIMT_OVERDFT = ensure_df(LIMT_OVERDFT)
BNM_LNACC4 = ensure_df(BNM_LNACC4)
DISPAY_IDISPAYMTH = ensure_df(DISPAY_IDISPAYMTH)
WOPS_IWOPOS = ensure_df(WOPS_IWOPOS)
WOMV_ISUMOV = ensure_df(WOMV_ISUMOV)
ODSQ_IODSQ = ensure_df(ODSQ_IODSQ)
LNSQ_ILNSQ = ensure_df(LNSQ_ILNSQ)
WRIOFAC = ensure_df(WRIOFAC)
BNMSUM_SUMM1 = ensure_df(BNMSUM_SUMM1)

# -----------------------
# Compute DATES dataset & SAS macro variables equivalence
# -----------------------
print("Computing date macros from BNM.REPTDATE (first row)...")
# SAS picked REPTDATE from BNM.REPTDATE (probably one-row). We do the same.
if BNM_REPTDATE.is_empty():
    # fallback: use today's date minus 1
    rptdate = date.today() - timedelta(days=1)
else:
    # Expect column REPTDATE either as numeric SAS date or ISO date string
    if "REPTDATE" in BNM_REPTDATE.columns:
        val = BNM_REPTDATE.select("REPTDATE").row(0)[0]
        dt = sas_date_to_pydate(val)
        rptdate = dt or (date.today() - timedelta(days=1))
    else:
        # take first date-like column
        for c in BNM_REPTDATE.columns:
            maybe = BNM_REPTDATE.select(c).row(0)[0]
            try:
                dt = sas_date_to_pydate(maybe)
                if dt:
                    rptdate = dt
                    break
            except Exception:
                pass
        else:
            rptdate = date.today() - timedelta(days=1)

# replicate SAS logic of week mapping
day_of_month = rptdate.day
if day_of_month == 8:
    SDD = 1; NOWK = "1"; NOWK1 = "4"
elif day_of_month == 15:
    SDD = 9; NOWK = "2"; NOWK1 = "1"
elif day_of_month == 22:
    SDD = 16; NOWK = "3"; NOWK1 = "2"
else:
    SDD = 23; NOWK = "4"; NOWK1 = "3"

MM = rptdate.month
if NOWK == '1':
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12
else:
    MM1 = MM
if NOWK == '4':
    MM2 = MM
else:
    MM2 = MM - 1
    if MM2 == 0:
        MM2 = 12

DAYS = rptdate.day
MONTHS = rptdate.month
YEARS = rptdate.year
SDATE = date(MONTHS, DAYS, YEARS) if False else rptdate  # SAS computed SDATE from MDY; keep as date
# For easy numeric use, create SDATE_INT = SAS numeric date (days since 1960-01-01)
base = date(1960, 1, 1)
REPTDATE_SASNUM = (rptdate - base).days
SDATE_SASNUM = REPTDATE_SASNUM
IDATE_py = date(YEARS, MONTHS, 1)
XDATE_py = IDATE_py - timedelta(days=IDATE_py.day - 1)  # similar to SAS XDATE
CURRDTE = date.today() - timedelta(days=1)

# store macro-style variables in a dict
MACROS = {
    "NOWK": NOWK,
    "NOWK1": NOWK1,
    "REPTMON": str(MM).zfill(2),
    "REPTMON1": str(MM1).zfill(2),
    "REPTMON2": str(MM2).zfill(2),
    "REPTYEAR": str(YEARS),
    "REPTYR2": str(YEARS % 100).zfill(2),
    "RDATE": rptdate.strftime("%d%m%Y"),
    "REPTDAY": str(DAYS).zfill(2),
    "SDATE": str(SDATE_SASNUM).zfill(5),
    "MDATE": rptdate.strftime("%Y%m%d"),
    "RDATE1": rptdate,  # python date
    "RDATE2": (date.today() - timedelta(days=1)).strftime("%y%m%d"),
    "CDATE": rptdate,
}

print("Computed MACROS:", {k: MACROS[k] for k in ("REPTMON","REPTDAY","NOWK","RDATE2")})

# -----------------------
# Begin translating key DATA steps for LOAN -> ACCTCRED etc.
# -----------------------
print("Processing LOAN / CCRISP data...")

loan = CCRISP_LOAN.clone()

# If the input is empty create empty schema to avoid failing downstream
if loan.is_empty():
    print("[WARN] CCRISP_LOAN is empty; creating empty frame with common columns used later.")
    cols = [
        "ACCTNO","NOTENO","ACCTNO","ACTY","BRANCH","TRANBRNO","SMESIZE","AADATE",
        "AADATE","ICDATE","EXODDATE","TEMPODDT","ODLMTDD","ODLMTMM","ODLMTYY",
        "PRODUCT","CENSUS","REALISAB","ODXSAMT","LMTAMT","APPRLIM2","AMOUNT",
        "APPRLIMT","BALANCE","CURBAL","FLOORDATE","PRICING","FACGPTYPE",
        "ISSUEDT","BILLCNT","IND","CAVAIAMT","PAYFREQ","SECTOR","NOTETERM"
    ]
    loan = pl.DataFrame({c: [] for c in cols})

# replicate the SAS DATA LOAN manipulations (subset of logic, focusing on major fields)
# Many SAS transformations use conditional column creation; replicate with with_columns
# Convert some numeric date-like columns to python dates if present
date_cols = ["EXODDATE","TEMPODDT","FRELEAS","APPRDATE","ISSUEDT",
             "DLVDATE","EXPRDATE","MATUREDT","DLIVRYDT","NXBILDT","CPNSTDTE","ASSMDATE","WRIOFF_CLOSE_FILE_TAG_DT"]
for c in date_cols:
    if c in loan.columns:
        # assume SAS numeric date in days since 1960 if numeric; else leave as-is
        loan = loan.with_columns([
            pl.col(c).apply(lambda v: sas_date_to_pydate(v)).alias(c + "_PY")
        ])

# Example transformations from SAS (lots more exist; I included many of them)
def transform_loan_frame(df: pl.DataFrame) -> pl.DataFrame:
    # Work on a copy
    out = df
    # Ensure commonly used columns exist to avoid KeyError
    for c in ["ACTY","BRANCH","TRANBRNO","SMESIZE","AADATE","ICDATE","EXODDATE","TEMPODDT",
              "ODLMTDD","ODLMTMM","ODLMTYY","PRODUCT","CENSUS","REALISAB","ODXSAMT",
              "LMTAMT","APPRLIM2","AMOUNT","APPRLIMT","BALANCE","CURBAL","ODSTATUS",
              "RISKCODE","ODLMTDD","ODLMTMM","ODLMTYY","PAYFREQ","NOTENO","PAIDIND",
              "BORSTAT","PRICING","FACGPTYPE","LSTTRNCD","WRITE_DOWN_BAL","NTINT",
              "INTEARN","INTAMT","REBATE","INTEARN4","BILLCNT","CAVAIAMT","FLAG1","MODELDES",
              "USER5","CAGAMAS","COL1","COL2","COL3","COL4","COL5"]:
        if c not in out.columns:
            out = out.with_columns([pl.lit(None).alias(c)])
    # Fix branch using TRANBRNO if present
    out = out.with_columns([
        pl.when(pl.col("TRANBRNO").is_not_null() & (pl.col("TRANBRNO") != 0))
          .then(pl.col("TRANBRNO"))
          .otherwise(pl.col("BRANCH"))
          .alias("BRANCH2")
    ])
    # Many SAS numeric -> multiplied by 100 (monetary scaling)
    money_cols = ["ODXSAMT","LMTAMT","APPRLIM2","AMOUNT","BALANCE","CURBAL","CAVAIAMT",
                  "BILTOT","REALISAB","REPAID","DISBURSE","REBATE","INTEARN4","INTEARN",
                  "INTAMT","UNDRAWN"]
    for mc in money_cols:
        if mc in out.columns:
            out = out.with_columns([pl.col(mc).cast(pl.Float64)])
    # Compute OUTSTAND and CURBAL logic approximations for OD and LN types
    out = out.with_columns([
        (pl.col("ACTY") == "OD").alias("IS_OD"),
    ])
    # For OD: convert sign and compute NODAYS, ARREARS etc.
    def compute_od_logic(row):
        # this function will be vectorized via apply below; keep it simple and safe
        try:
            is_od = row.get("IS_OD")
            curbal = row.get("CURBAL") or 0
            exod = row.get("EXODDATE")
            tempod = row.get("TEMPODDT")
            paydd = 0
            paymm = 0
            payyy = 0
            nodays = 0
            instalm = 0
            if is_od:
                # invert sign like SAS
                curbal = -curbal
                if (exod or tempod) and curbal <= 0:
                    # pick earliest non-zero of EXODDATE and TEMPODDT if present
                    od = None
                    if exod and tempod:
                        od = exod if exod <= tempod else tempod
                    else:
                        od = exod or tempod
                    if od:
                        # od might be python date or numeric; try sas_date_to_pydate
                        od_py = sas_date_to_pydate(od)
                        if od_py:
                            paydd = od_py.day
                            paymm = od_py.month
                            payyy = od_py.year
                            # SAS used &SDATE - ODDAYS; we use MACROS['REPTDAY']? approximate with rptdate
                            # compute number of days since od to SDATE
                            sdate_dt = MACROS["RDATE1"]
                            nodays = (sdate_dt - od_py).days + 1
                            if nodays > 0:
                                # SAS ARREARS = FLOOR(NODAYS/30.00050)
                                arrears = math.floor(nodays / 30.00050)
                                instalm = math.ceil(nodays / 30.00050)
                                return {
                                    "CURBAL": curbal,
                                    "OUTSTAND": curbal * 100,
                                    "PAYDD": paydd,
                                    "PAYMM": paymm,
                                    "PAYYR": payyy,
                                    "NODAYS": nodays,
                                    "ARREARS": arrears,
                                    "INSTALM": instalm
                                }
            # default fallthrough
            return {
                "CURBAL": curbal,
                "OUTSTAND": curbal * 100 if curbal is not None else 0,
                "PAYDD": paydd,
                "PAYMM": paymm,
                "PAYYR": payyy,
                "NODAYS": nodays,
                "ARREARS": 0,
                "INSTALM": 0
            }
        except Exception:
            return {
                "CURBAL": row.get("CURBAL", 0),
                "OUTSTAND": (row.get("CURBAL", 0) or 0) * 100,
                "PAYDD": 0,
                "PAYMM": 0,
                "PAYYR": 0,
                "NODAYS": 0,
                "ARREARS": 0,
                "INSTALM": 0
            }

    # Apply compute_od_logic row-wise only if dataset small; for large dataset vectorize logic would be better.
    # Polars apply with row-wise Python function returns dicts into columns
    if not out.is_empty():
        od_calc = out.select(pl.all()).to_pandas().apply(compute_od_logic, axis=1, result_type="expand")
        # merge od_calc columns back
        od_calc_pl = pl.from_pandas(od_calc)
        out = out.with_columns([
            od_calc_pl.get_column(k) for k in od_calc_pl.columns
        ])

    # Many more SAS rules follow; implement selected ones:
    # - RESTRUCT determination from RISKCODE / BORSTAT / MODELDES / NOTENO
    # - CLASSIFI mapping from RISKCODE
    # - SPECIALF & FCONCEPT based on LOANTYPE sets
    # - PAYFREQ -> PAYFREQC mapping
    # We'll implement the key ones used later for outputs.
    # Map CLASSIFI
    if "RISKCODE" in out.columns:
        out = out.with_columns([
            pl.when(pl.col("RISKCODE") == "1").then(pl.lit("C"))
              .when(pl.col("RISKCODE") == "2").then(pl.lit("S"))
              .when(pl.col("RISKCODE") == "3").then(pl.lit("D"))
              .when(pl.col("RISKCODE") == "4").then(pl.lit("B"))
              .otherwise(pl.lit("P"))
              .alias("CLASSIFI")
        ])
    # PAYFREQ -> PAYFREQC
    if "PAYFREQ" in out.columns:
        out = out.with_columns([
            pl.when(pl.col("PAYFREQ") == "1").then(pl.lit("14"))
              .when(pl.col("PAYFREQ") == "2").then(pl.lit("15"))
              .when(pl.col("PAYFREQ") == "3").then(pl.lit("16"))
              .when(pl.col("PAYFREQ") == "4").then(pl.lit("17"))
              .when(pl.col("PAYFREQ") == "5").then(pl.lit("18"))
              .when(pl.col("PAYFREQ") == "6").then(pl.lit("13"))
              .when(pl.col("PAYFREQ") == "9").then(pl.lit("21"))
              .otherwise(pl.col("PAYFREQ").cast(pl.Utf8))
              .alias("PAYFREQC")
        ])
    # Clean up currency and undrawn:
    if "CURCODE" in out.columns and "FORATE" in out.columns:
        out = out.with_columns([
            pl.when((pl.col("CURCODE") != "MYR") & pl.col("CURCODE").is_not_null())
              .then(pl.col("FORATE") * pl.lit(100000))
              .otherwise(pl.lit(None))
              .alias("FXRATE")
        ])
    # Default numeric fill for known columns
    numeric_defaults = ["UNDRAWN","TOTIIS","TOTIISR","TOTWOF","IISOPBAL"]
    for nd in numeric_defaults:
        if nd in out.columns:
            out = out.with_columns([pl.coalesce([pl.col(nd), pl.lit(0)]).alias(nd)])
    # final
    return out

loan_trans = transform_loan_frame(loan)

# -----------------------
# Apply merges with other datasets as in SAS (BNMSUM.SUMM1 etc.)
# -----------------------
print("Merging in BNMSUM_SUMM1 and other supporting datasets (where available)...")
if not BNMSUM_SUMM1.is_empty():
    # BNMSUM_SUMM1 had keep/rename in SAS; we mimic a simple left join on AANO
    if "AANO" in loan_trans.columns and "AANO" in BNMSUM_SUMM1.columns:
        loan_trans = loan_trans.join(BNMSUM_SUMM1, on="AANO", how="left")
else:
    print("[WARN] BNMSUM_SUMM1 not provided; skipping that merge.")

# Merge with ELDS.RVNOTE (RVNOTE) if available (user provided in earlier paste)
if INPUT_PATHS.get("ELDS_RVNOTE") and os.path.exists(INPUT_PATHS["ELDS_RVNOTE"]):
    rvnote = safe_read_parquet_or_csv(INPUT_PATHS["ELDS_RVNOTE"])
    if "AANO" in rvnote.columns and "AANO" in loan_trans.columns:
        loan_trans = loan_trans.join(rvnote.select("AANO","ACCTNO","NOTENO"), on="AANO", how="left")
        # SAS logic: merge HPRJ etc. - skipped unless provided

# -----------------------
# Writting outputs (SUBACRED, CREDITPO, PROVISIO, LEGALACT, CREDITOD, ACCTCRED)
# -----------------------
print("Preparing final outputs...")

# For the SAS job, SUBACRED and CREDITPO and PROVISIO are fixed-width files.
# We'll output CSV and attempt a fixed-width that approximates SAS PUT using layouts.

# Minimal layout for SUBACRED (the SAS code uses many columns; pick the common ones)
subac_layout = [
    ("FICODE", 9, "zint", None),
    ("APCODE", 3, "zint", None),
    ("ACCTNO", 10, "zint", None),
    ("NOTENO", 10, "zint", None),
    ("FACILITY", 5, "zint", None),
    ("SYNDICAT", 1, "str", None),
    ("SPECIALF", 2, "str", None),
    ("PURPOSES", 4, "str", None),
    ("FCONCEPT", 2, "zint", None),
    ("NOTETERM", 3, "zint", None),
    ("PAYFREQC", 2, "str", None),
    ("RESTRUCT", 1, "str", None),
    ("CAGAMAS", 8, "zint", None),
    ("RECOURSE", 1, "str", None),
    ("CUSTCODE", 2, "zint", None),
    ("SECTOR", 4, "str", None),
    ("OLDBRH", 5, "zint", None),
    ("SCORE", 3, "str", None),
    ("COSTCTR", 4, "zint", None),
    ("PAYDD", 2, "zint", None),
    ("PAYMM", 2, "zint", None),
    ("PAYYR", 4, "zint", None),
    # The SAS file includes hundreds of columns; add more if needed
]

# Prepare a frame with the needed columns (fill missing columns with defaults)
def ensure_columns_for_layout(df: pl.DataFrame, layout):
    d = df
    for name, width, ftype, fmt in layout:
        if name not in d.columns:
            if ftype in ("zint","int"):
                d = d.with_columns([pl.lit(0).alias(name)])
            else:
                d = d.with_columns([pl.lit("").alias(name)])
    return d

acctcred_df = loan_trans
acctcred_df = ensure_columns_for_layout(acctcred_df, subac_layout)

# Drop complex types (Polars may have object types from apply); coerce to strings/numbers
for c in acctcred_df.columns:
    if acctcred_df[c].dtype in (pl.Object,):
        # coerce object -> str
        acctcred_df = acctcred_df.with_columns([pl.col(c).cast(pl.Utf8).alias(c)])

# Write CSV for easy checking
acctcred_csv = os.path.join(OUTPUT_DIR, f"SUBACRED_{MACROS['REPTMON']}_{MACROS['REPTYEAR']}.csv")
acctcred_df.write_csv(acctcred_csv)
print(f"Wrote CSV: {acctcred_csv}")

# Attempt fixed-width SUBACRED
subac_path = os.path.join(OUTPUT_DIR, f"SUBACRED_{MACROS['REPTMON']}_{MACROS['REPTYEAR']}.txt")
write_fixed_width(acctcred_df, [(t[0], t[1], t[2], t[3]) for t in subac_layout], subac_path)
print(f"Wrote fixed-width SUBACRED (approx): {subac_path}")

# Prepare CREDITPO layout and file
creditpo_layout = [
    ("FICODE", 9, "zint", None),
    ("APCODE", 3, "zint", None),
    ("ACCTNO", 10, "zint", None),
    ("NOTENO", 10, "zint", None),
    ("REPTDAY", 2, "zint", None),
    ("REPTMON", 2, "zint", None),
    ("REPTYEAR", 4, "zint", None),
    ("OUTSTAND", 16, "float", 0),
    ("ARREARS", 3, "zint", None),
    ("INSTALM", 3, "zint", None),
    ("UNDRAWN", 17, "float", 0),
    ("ACCTSTAT", 1, "str", None),
    ("NODAYS", 5, "zint", None),
    ("OLDBRH", 5, "zint", None),
]
creditpo_df = acctcred_df  # reuse; in SAS, these were different outputs from same data
# Add the REPTDAY/REPTMON/REPTYEAR as macros
creditpo_df = creditpo_df.with_columns([
    pl.lit(int(MACROS["REPTDAY"])).alias("REPTDAY"),
    pl.lit(int(MACROS["REPTMON"])).alias("REPTMON"),
    pl.lit(int(MACROS["REPTYEAR"])).alias("REPTYEAR"),
])

creditpo_csv = os.path.join(OUTPUT_DIR, f"CREDITPO_{MACROS['REPTMON']}_{MACROS['REPTYEAR']}.csv")
creditpo_df.write_csv(creditpo_csv)
print(f"Wrote CSV: {creditpo_csv}")
creditpo_path = os.path.join(OUTPUT_DIR, f"CREDITPO_{MACROS['REPTMON']}_{MACROS['REPTYEAR']}.txt")
write_fixed_width(creditpo_df, [(t[0], t[1], t[2], t[3]) for t in creditpo_layout], creditpo_path)
print(f"Wrote fixed-width CREDITPO (approx): {creditpo_path}")

# PROVISIO (provision) dataset
prov_layout = [
    ("FICODE", 9, "zint", None),
    ("APCODE", 3, "zint", None),
    ("ACCTNO", 10, "zint", None),
    ("NOTENO", 10, "zint", None),
    ("REPTDAY", 2, "zint", None),
    ("REPTMON", 2, "zint", None),
    ("REPTYEAR", 4, "zint", None),
    ("CLASSIFI", 1, "str", None),
    ("ARREARS", 3, "zint", None),
    ("CURBAL", 17, "float", 0),
    ("INTERDUE", 17, "float", 0),
    ("FEEAMT", 16, "float", 0),
    ("REALISAB", 17, "float", 0),
]
prov_df = acctcred_df.filter(pl.col("IMPAIRED") == "Y") if "IMPAIRED" in acctcred_df.columns else acctcred_df
prov_df_csv = os.path.join(OUTPUT_DIR, f"PROVISIO_{MACROS['REPTMON']}_{MACROS['REPTYEAR']}.csv")
prov_df.write_csv(prov_df_csv)
print(f"Wrote CSV: {prov_df_csv}")
prov_txt = os.path.join(OUTPUT_DIR, f"PROVISIO_{MACROS['REPTMON']}_{MACROS['REPTYEAR']}.txt")
write_fixed_width(prov_df, [(t[0], t[1], t[2], t[3]) for t in prov_layout], prov_txt)
print(f"Wrote fixed-width PROVISIO (approx): {prov_txt}")

# LEGALACT (legal actions)
legal_layout = [
    ("FICODE", 9, "zint", None),
    ("APCODE", 3, "zint", None),
    ("ACCTNO", 10, "zint", None),
    ("DELQCD", 2, "str", None),
]
legal_df = acctcred_df.filter(pl.col("LEG") == "A") if "LEG" in acctcred_df.columns else pl.DataFrame()
legal_csv = os.path.join(OUTPUT_DIR, f"LEGALACT_{MACROS['REPTMON']}_{MACROS['REPTYEAR']}.csv")
if not legal_df.is_empty():
    legal_df.write_csv(legal_csv)
    write_fixed_width(legal_df, [(t[0], t[1], t[2], t[3]) for t in legal_layout], os.path.join(OUTPUT_DIR, "LEGALACT.txt"))
    print(f"Wrote CSV & fixed-width LEGALACT")
else:
    print("[INFO] No LEGALACT rows to write.")

# CREDITOD: Obsolete in many cases but try to extract OD rows with certain account range
od_df = acctcred_df.filter((pl.col("ACCTNO") >= 3000000000) & (pl.col("ACCTNO") <= 3999999999)) if "ACCTNO" in acctcred_df.columns else pl.DataFrame()
if not od_df.is_empty():
    creditod_csv = os.path.join(OUTPUT_DIR, f"CREDITOD_{MACROS['REPTMON']}_{MACROS['REPTYEAR']}.csv")
    od_df.write_csv(creditod_csv)
    print(f"Wrote CSV CREDITOD: {creditod_csv}")
else:
    print("[INFO] No CREDITOD rows to write.")

# ACCTCRED full export (the big SUBACRED-like full dataset)
acctcred_full_csv = os.path.join(OUTPUT_DIR, f"ACCTCRED_{MACROS['REPTMON']}_{MACROS['REPTYEAR']}.csv")
acctcred_df.write_csv(acctcred_full_csv)
acctcred_full_parquet = os.path.join(OUTPUT_DIR, f"ACCTCRED_{MACROS['REPTMON']}_{MACROS['REPTYEAR']}.parquet")
acctcred_df.write_parquet(acctcred_full_parquet)
print(f"Wrote ACCTCRED CSV and Parquet: {acctcred_full_csv}, {acctcred_full_parquet}")

# -----------------------
# Additional outputs e.g. REPAID7B computed from OD accounts (SAS created ODR7B)
# -----------------------
print("Computing REPAID7B (repay detail for OD accounts)...")
if not acctcred_df.is_empty():
    # In SAS, ODR7B was created by selecting OD accounts with REPAID > 0 and splitting by MTD_REPAY_TYPE.
    if "REPAID" in acctcred_df.columns:
        od_repaid = acctcred_df.filter((pl.col("ACCTNO") >= 3000000000) & (pl.col("ACCTNO") <= 3999999999) & (pl.col("REPAID") > 0))
        # Construct rows for MTD_REPAY_TYPE10_AMT etc.
        records = []
        for row in od_repaid.iter_rows(named=True):
            m10 = row.get("MTD_REPAY_TYPE10_AMT") or 0
            m20 = row.get("MTD_REPAY_TYPE20_AMT") or 0
            m30 = row.get("MTD_REPAY_TYPE30_AMT") or 0
            if m10 > 0:
                records.append({
                    "BRANCH": row.get("BRANCH"),
                    "ACCTNO": row.get("ACCTNO"),
                    "REPAY_SRCE": "1200",
                    "REPAY_TYPE": "10",
                    "REPAID_AMT": m10,
                    "MTD_REPAID_AMT": row.get("MTD_REPAID_AMT") or 0
                })
            if m20 > 0:
                records.append({
                    "BRANCH": row.get("BRANCH"),
                    "ACCTNO": row.get("ACCTNO"),
                    "REPAY_SRCE": "1200",
                    "REPAY_TYPE": "20",
                    "REPAID_AMT": m20,
                    "MTD_REPAID_AMT": row.get("MTD_REPAID_AMT") or 0
                })
            if m30 > 0:
                records.append({
                    "BRANCH": row.get("BRANCH"),
                    "ACCTNO": row.get("ACCTNO"),
                    "REPAY_SRCE": "1200",
                    "REPAY_TYPE": "30",
                    "REPAID_AMT": m30,
                    "MTD_REPAID_AMT": row.get("MTD_REPAID_AMT") or 0
                })
        repaid7b_df = pl.from_records(records) if records else pl.DataFrame()
        if not repaid7b_df.is_empty():
            outpath = os.path.join(OUTPUT_DIR, f"REPAID7B_{MACROS['REPTMON']}_{MACROS['REPTYEAR']}.csv")
            repaid7b_df.write_csv(outpath)
            print(f"Wrote REPAID7B CSV: {outpath}")
        else:
            print("[INFO] No REPAID7B rows.")
    else:
        print("[INFO] REPAID column missing; skipping REPAID7B creation.")
else:
    print("[INFO] ACCTCRED is empty; skipping repay computation.")

# -----------------------
# LNACC4 merge and final CCRIS outputs if requested
# -----------------------
print("Merging LNACC4 (if present) and finalizing ACCT-level outputs...")

if not BNM_LNACC4.is_empty() and "ACCTNO" in acctcred_df.columns:
    # rename columns to match SAS mapping
    lnacc4 = BNM_LNACC4
    # create derived ACCT_... fields as in SAS
    lnacc4 = lnacc4.with_columns([
        (pl.col("TOTACBAL") * 100).alias("ACCT_OUTBAL"),
        (pl.col("MNIAPLMT") * 100).alias("ACCT_APLMT"),
    ])
    acctcred_df = acctcred_df.join(lnacc4.select(["ACCTNO","ACCT_OUTBAL","ACCT_APLMT"]), on="ACCTNO", how="left")

# Write final ACCTCRED again including LNACC fields
acctcred_final_csv = os.path.join(OUTPUT_DIR, "ACCTCRED_FINAL.csv")
acctcred_df.write_csv(acctcred_final_csv)
print("Wrote final ACCTCRED CSV:", acctcred_final_csv)

# -----------------------
# End
# -----------------------
print("EIIWCCR5 -> Polars translation script completed.")
print(f"Check {OUTPUT_DIR} for output files.")
print("Note: This translation attempts to preserve the SAS logic at a high level.")
print("If you want byte-for-byte identical fixed-width layouts or additional SAS-format mappings,")
print("tell me which exact target file(s) to refine and provide sample inputs for format mapping.")
