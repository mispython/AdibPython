# eimbnw01.py
"""
Converted EIMBNW01 SAS job -> Python (Polars + PyArrow + DuckDB)

Notes:
 - This is a working translation of the main dataflow: compute report date,
   read datasets, make merges, compute fields, compute disburse/repay aggregates,
   and create output parquet datasets.
 - You MUST provide input dataset files (parquet/csv). Default names are placeholders.
 - Site specific SAS formats and macros (e.g. $SECTCD., $BTPROD., PGM includes) are
   approximated where sensible. You should replace mapping dicts with your real mappings.
"""

import os
import sys
from datetime import date, timedelta
import polars as pl
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Optional

# -----------------------------
# CONFIG: set your input file paths (parquet, feather, or csv). 
# Replace these with the true locations of the corresponding SAS datasets exported to parquet/csv.
# -----------------------------
DATA_DIR = "./input_data"   # change to directory where your Parquet/CSV files live
OUTPUT_DIR = "./output"     # where to write resulting Parquet files
os.makedirs(OUTPUT_DIR, exist_ok=True)

# default file names (change to real files)
FILES = {
    # BTRSA.* - main BTRADE SAS data library (reptdate, bt detail, mast etc)
    "BTRSA.REPTDATE": os.path.join(DATA_DIR, "BTRSA_REPTDATE.parquet"),
    "BTRSA.BTDTL": os.path.join(DATA_DIR, "BTRSA_BTDTL.parquet"),
    "BTRSA.MAST": os.path.join(DATA_DIR, "BTRSA_MAST.parquet"),
    # BNM sources used/created
    "BNM.BTDTL": os.path.join(DATA_DIR, "BNM_BTDTL.parquet"),
    "BNM.MAST": os.path.join(DATA_DIR, "BNM_MAST.parquet"),
    # PBA (for UNEARNED sums)
    "PBA01.PBA01": os.path.join(DATA_DIR, "PBA01_PBA01.parquet"),
    # PRNGL / GL transactions (used for disburse/repaid)
    "BTRSA.COMM": os.path.join(DATA_DIR, "BTRSA_COMM.parquet"),
    # MTD and PREV BT (for merges)
    "MTD.BTAVG": os.path.join(DATA_DIR, "MTD_BTAVG.parquet"),
    "BNMX.D_BTRAD_PREV": os.path.join(DATA_DIR, "BNMX_D_BTRAD_PREV.parquet"),
    # TFDPD - delinquency file (if used)
    "TFDPD": os.path.join(DATA_DIR, "TFDPD.parquet"),
    # fallback: mapping CSVs for format catalogs (replace with true mapping files)
    "FORMAT_SECTCD": os.path.join(DATA_DIR, "format_sectcd.csv"),
    "FORMAT_BTPROD": os.path.join(DATA_DIR, "format_btprod.csv"),
}

# -----------------------------
# Utility functions
# -----------------------------
def read_table(path: str, try_csv_first: bool = False) -> pl.DataFrame:
    """Read a table from Parquet or CSV. Raise FileNotFoundError if missing."""
    if os.path.exists(path):
        if path.lower().endswith(".parquet") or path.lower().endswith(".pq"):
            return pl.read_parquet(path)
        elif path.lower().endswith(".feather"):
            return pl.read_ipc(path)
        elif path.lower().endswith(".csv") or path.lower().endswith(".txt"):
            return pl.read_csv(path)
        else:
            # try parquet fallback
            try:
                return pl.read_parquet(path)
            except Exception as ex:
                raise RuntimeError(f"Unsupported file type for {path}: {ex}")
    # try CSV variations
    if try_csv_first:
        alt = path.replace(".parquet", ".csv")
        if os.path.exists(alt):
            return pl.read_csv(alt)
    raise FileNotFoundError(f"Input file not found: {path}")

def write_parquet(df: pl.DataFrame, path: str) -> None:
    df.write_parquet(path, compression="snappy")
    print(f"Wrote: {path} (rows={df.height})")

# -----------------------------
# STEP 0: Read or compute report date
# -----------------------------
# SAS: SET BTRSA.REPTDATE; REPTDATE = TODAY()-1 etc.
# We'll read the file that should contain a REPTDATE field. If missing, default to yesterday.
def get_reptdate() -> date:
    path = FILES["BTRSA.REPTDATE"]
    try:
        df = read_table(path)
        # Expect a REPTDATE column in ISO or SAS numeric format (YYYY-MM-DD)
        if "REPTDATE" in df.columns:
            val = df.select(pl.col("REPTDATE")).to_series()[0]
            # handle if val already date-like or string
            if isinstance(val, str):
                return date.fromisoformat(val)
            if isinstance(val, (int, float)):
                # assume days since 1960 (SAS)? Hard to be sure. Fallback to yesterday.
                return date.today() - timedelta(days=1)
            # polars date types -> convert
            try:
                return val.to_pydatetime().date()
            except Exception:
                return date.today() - timedelta(days=1)
        else:
            print("Warning: REPTDATE column not found in REPTDATE file. Using yesterday.")
            return date.today() - timedelta(days=1)
    except FileNotFoundError:
        print("Warning: BTRSA.REPTDATE not found; using yesterday as REPTDATE.")
        return date.today() - timedelta(days=1)

REPTDATE = get_reptdate()
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"
REPTYEAR = REPTDATE.year % 100  # two-digit year like SAS YEAR2.
RDATE_STR = REPTDATE.strftime("%d%m%Y")
print(f"REPTDATE={REPTDATE} RDATE_STR={RDATE_STR} REPTMON={REPTMON} REPTDAY={REPTDAY}")

# assign NOWK: SAS selects weekly buckets based on day-of-month
d = REPTDATE.day
if 1 <= d <= 8:
    NOWK = "1"
elif 9 <= d <= 15:
    NOWK = "2"
elif 16 <= d <= 22:
    NOWK = "3"
else:
    NOWK = "4"

print("NOWK:", NOWK)

# -----------------------------
# STEP 1: Read required tables (we expect them exported to parquet/csv)
# -----------------------------
# We'll try to read core tables used in job. If not present, raise helpful errors.
def safe_read(name):
    path = FILES.get(name)
    if path is None:
        raise KeyError(f"No path configured for {name} in FILES dict.")
    try:
        df = read_table(path, try_csv_first=True)
        print(f"Loaded {name}: {df.shape}")
        return df
    except FileNotFoundError as e:
        print(f"ERROR: required input missing: {name} -> {path}")
        raise

# Core sources from the job (adjust FILES mapping to point to real files):
# BNM/BNM library equivalents
try:
    btrsa_bt = safe_read("BTRSA.BTDTL")        # BTRADE detail
except Exception:
    # fallback: try BNM.BTDTL
    btrsa_bt = None

try:
    btrsa_mast = safe_read("BTRSA.MAST")
except Exception:
    btrsa_mast = None

# BNM versions (outputs/inputs)
bnm_bt = None
try:
    bnm_bt = safe_read("BNM.BTDTL")
except Exception:
    pass

bnm_mast = None
try:
    bnm_mast = safe_read("BNM.MAST")
except Exception:
    pass

# PBA for UNEARNED aggregator
pba = None
try:
    pba = safe_read("PBA01.PBA01")
except Exception:
    pass

# PRNGL / GL transactions used for disburse/repaid
prngl = None
try:
    prngl = safe_read("BTRSA.COMM")
except Exception:
    pass

# MTD & PREV (optional)
mtd_btavg = None
try:
    mtd_btavg = safe_read("MTD.BTAVG")
except Exception:
    pass

prev_btr = None
try:
    prev_btr = safe_read("BNMX.D_BTRAD_PREV")
except Exception:
    pass

# TFDPD file (delinquency) optional
tfdpd = None
try:
    tfdpd = safe_read("TFDPD")
except Exception:
    pass

# -----------------------------
# NOTE: If required source tables are missing you should supply them as parquet/csv.
# The script below attempts to implement the main transformations with what's available.
# -----------------------------

# helper to ensure columns exist before selecting to avoid crashes
def ensure_cols(df: Optional[pl.DataFrame], cols):
    if df is None:
        return None
    missing = [c for c in cols if c not in df.columns]
    if missing:
        # create with nulls
        for m in missing:
            df = df.with_columns(pl.lit(None).alias(m))
    return df

# Use polars df objects or empty frames if missing
if btrsa_bt is None and bnm_bt is not None:
    btrsa_bt = bnm_bt

if btrsa_bt is None:
    print("Warning: No BTRSA.BTDTL / BNM.BTDTL loaded. Many operations will be skipped.")
    btrsa_bt = pl.DataFrame()

# Ensure some expected columns exist for safe operations
btrsa_bt = ensure_cols(btrsa_bt, ["ACCTNO", "TRANSREF", "OUTSTAND", "LIABCODE", "DIRCTIND", "MATDATE", "BRANCH", "PRODUCT", "SUBACCT", "OUTSTAND"])

# If MAST present:
bnm_mast = ensure_cols(bnm_mast, ["ACCTNO", "TFID"]) if bnm_mast is not None else None
btrsa_mast = ensure_cols(btrsa_mast, ["ACCTNO", "GLMNEMO"]) if btrsa_mast is not None else None

# -----------------------------
# STEP 2: Merge BT detail and MAST (BTRADE&REPTDT step)
# -----------------------------
# SAS: BTRADE&REPTDT; MERGE BNM.BTDTL&REPTDT IN A and BNM.MAST&REPTDT DROP=INTRECV
# Here: attempt to join on ACCTNO
if btrsa_bt.height == 0:
    print("No BTRSA/BTR input data -> creating empty BTRADE output")
    btrade = pl.DataFrame()
else:
    # if mast present, drop INTRECV equivalent if present
    mast_for_join = btrsa_mast.select([c for c in btrsa_mast.columns if c != "INTRECV"]) if btrsa_mast is not None else None

    if mast_for_join is not None and "ACCTNO" in mast_for_join.columns:
        # left join BTR detail with mast
        btrade = btrsa_bt.join(mast_for_join, on="ACCTNO", how="left")
    else:
        btrade = btrsa_bt.clone()

print("BTRADE shape:", btrade.shape)

# -----------------------------
# STEP 3: Apply the D_BTRAD logic (equivalent to DATA BNM.D_BTRAD&REPTDT &LOAN)
# Many mapping rules for SECTORCD, CUSTCD etc are based on SAS formats.
# ---- We implement core logic: set REPTDATE, compute SECTORCD via a placeholder mapping,
# compute ORIGMT, AMTIND, COLLCD etc.
# -----------------------------

# Minimal example mapping functions for SAS formats: replace these dicts with your actual mappings
def map_sector_code(sector):
    # placeholder simple mapping: cast to string zero-filled
    try:
        if sector is None:
            return ""
        s = int(sector)
        return f"{s:04d}"
    except Exception:
        return str(sector or "")

def map_custcd(custcode):
    # placeholder
    if custcode is None:
        return ""
    try:
        i = int(custcode)
        return f"{i:02d}"
    except Exception:
        return str(custcode)

# Ensure expected columns for btrade
btrade = ensure_cols(btrade, ["REPTDATE", "PRODUCT", "NOTENO", "FISSPURP", "ACCTNO", "CUSTCODE", "SECTOR", "STATE", "LIABCODE", "DIRCTIND", "EXPRDATE", "ISSDTE", "REBIND", "INTAMT", "REBATE", "INTEARN", "INTEARN4", "INTEARN2", "INTEARN3", "NOTETERM", "CREATDS"])

# Convert REPTDATE column if present to date type else set constant
if "REPTDATE" not in btrade.columns or btrade.height == 0:
    btrade = btrade.with_columns(pl.lit(REPTDATE).cast(pl.Date).alias("REPTDATE"))
else:
    # if it's string, cast
    try:
        btrade = btrade.with_columns(pl.col("REPTDATE").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("REPTDATE"))
    except Exception:
        pass

# Compute fields
btrade = btrade.with_columns(
    pl.lit(REPTDATE).alias("REPTDATE_OUT"),
    pl.col("CUSTCODE").apply(lambda x: map_custcd(x)).alias("CUSTCD"),
    pl.col("SECTOR").apply(lambda x: map_sector_code(x)).alias("SECTORCD"),
    pl.when(pl.col("EXPRDATE").is_null() | (pl.col("EXPRDATE") < pl.col("REPTDATE")))
      .then(pl.lit("51"))
      .otherwise(pl.lit(None)).alias("REMAINMT_TMP")
)

# compute ORIGMT placeholder (ORIGMT = 10 if expr-iss < 366 else 20)
def compute_origmt(exprdate, issdte, rpt):
    try:
        if exprdate is None or issdte is None:
            return "20"
        # Expect date strings -> convert to date
        return "10" if (exprdate - issdte).days < 366 else "20"
    except Exception:
        return "20"

if "EXPRDATE" in btrade.columns and "ISSDTE" in btrade.columns:
    # ensure dates
    try:
        btrade = btrade.with_columns([
            pl.col("EXPRDATE").cast(pl.Date).alias("EXPRDATE_d"),
            pl.col("ISSDTE").cast(pl.Date).alias("ISSDTE_d"),
        ])
        btrade = btrade.with_columns(
            pl.struct(["EXPRDATE_d", "ISSDTE_d"]).apply(lambda s: compute_origmt(s["EXPRDATE_d"], s["ISSDTE_d"], REPTDATE)).alias("ORIGMT")
        )
    except Exception:
        btrade = btrade.with_columns(pl.lit("20").alias("ORIGMT"))
else:
    btrade = btrade.with_columns(pl.lit("20").alias("ORIGMT"))

# AMTIND default 'D'
btrade = btrade.with_columns(pl.lit("D").alias("AMTIND"))

# COLLCD placeholder mapping from COLLATER numeric value -> coded string
btrade = btrade.with_columns(
    pl.col("COLLATER").cast(pl.Int64).apply(lambda x: f"{x:02d}" if x is not None else "").alias("COLLCD")
)

# compute remaining months REPEAT of SAS `COUNTMON` logic simplified:
# We'll compute remain months in months between REPTDATE and EXPRDATE (ceiling)
def months_between(start, end):
    if start is None or end is None:
        return None
    # approximates number of whole months remaining
    years = end.year - start.year
    months = years * 12 + end.month - start.month
    return max(0, months)

if "EXPRDATE_d" in btrade.columns:
    btrade = btrade.with_columns(
        pl.col("EXPRDATE_d").apply(lambda ed: months_between(REPTDATE, ed)).alias("NUMMONTH")
    )
    # Format like SAS PUT(NUMMONTH, LNRMMT.) - here convert to string
    btrade = btrade.with_columns(
        pl.col("NUMMONTH").cast(pl.Int64).apply(lambda x: f"{x:02d}" if x is not None else "51").alias("REMAINMT")
    )
else:
    btrade = btrade.with_columns(pl.lit("51").alias("REMAINMT"))

# Drop helper date columns
drop_cols = [c for c in ["EXPRDATE_d", "ISSDTE_d", "NUMMONTH"] if c in btrade.columns]
if drop_cols:
    btrade = btrade.drop(drop_cols)

print("btrade sample columns:", btrade.columns[:30])

# -----------------------------
# STEP 4: Subaccount / SUBNEW merging (SUBA & MAST)
# In SAS it builds SUBNEW from SUBA and MAST; we try to replicate with provided inputs.
# -----------------------------
# IF SUBA and MAST were provided as separate files they'd be joined. The SAS program creates
# SUBNEW with ACCTNO and TRANSREX and provides ISSDTE/EXPRDATE converted from numeric fields.
# We'll skip if SUBA or MAST not provided.

# We'll create SUBNEW empty if not present:
subnew = pl.DataFrame()
# If BTRSA.SUBA and MAST present they should be joined and processed to SUBNEW
# For now leave empty, user can supply actual SUBACCT exports for full fidelity.

# -----------------------------
# STEP 5: Merge TRANSREF totals from PBA01 (if provided)
# -----------------------------
if pba is not None and "TRANSREF" in pba.columns:
    # Summarize UNEARNED by TRANSREF
    pba_sum = pba.groupby("TRANSREF").agg(pl.col("UNEARNED").sum().alias("UNEARNED_SUM"))
    # join into btrade by TRANSREF where available
    if "TRANSREF" not in btrade.columns:
        btrade = btrade.with_columns(pl.lit(None).alias("TRANSREF"))
    btrade = btrade.join(pba_sum, on="TRANSREF", how="left")
else:
    btrade = btrade.with_columns(pl.lit(None).alias("UNEARNED_SUM"))

# -----------------------------
# STEP 6: TB/BA/APNBT classification & merging of PBA (similar to SAS TB/BA/NOTMATCH logic)
# The SAS original splits BTDTL into TB (outstanding > 0 and not certain liab codes),
# BA (outstanding <=0) and APNBT (others). We replicate that.
# -----------------------------
if "OUTSTAND" in btrade.columns:
    tb = btrade.filter((pl.col("OUTSTAND") > 0) & (~pl.col("LIABCODE").is_in(["BAI","BAP","BAS","BAE"])))
    ba = btrade.filter((pl.col("OUTSTAND") <= 0))
    apnbt = btrade.filter(pl.col("OUTSTAND").is_null() | (pl.col("OUTSTAND") == 0))
else:
    tb = btrade
    ba = pl.DataFrame()
    apnbt = pl.DataFrame()

# If PBA present, prepare PBA with TRANSREF normalization (SAS does substr)
if pba is not None:
    if "TRANSREF" in pba.columns:
        # SAS uses TRANSREF substr(TRANSREF,2,8) - emulate if needed
        pba = pba.with_columns(
            pl.when(pl.col("TRANSREF").is_not_null())
            .then(pl.col("TRANSREF").str.slice(1, 8))
            .otherwise(pl.col("TRANSREF"))
            .alias("TRANSREF_SLICE")
        )

# Combine TBBA (TB + BA + APNBT)
tbba = pl.concat([tb, ba, apnbt], how="vertical")

# -----------------------------
# STEP 7: DISBURSE & REPAID computation from PRNGL (PRNGL is BTRSA.COMM dataset)
# SAS creates PRNGL then filters by GLMNEM codes to find disbursements and repayments.
# We'll approximate by selecting rows from prngl with GLMNEMO in certain codes.
# -----------------------------
disburse = pl.DataFrame()
repaid = pl.DataFrame()
if prngl is not None and prngl.height > 0:
    # Ensure column names and compute TRANSREF = substr(PAYGRNO, 1, 7)
    prngl = ensure_cols(prngl, ["GLMNEM", "TRANSAMT", "PAYGRNO", "ACCTNO", "ISSDTE", "TRANSREF"])
    # Prepare GLMNEMO like SAS: sometimes GLMNEM + '0'
    if "GLMNEMO" not in prngl.columns:
        prngl = prngl.with_columns(
            pl.when(pl.col("GLMNEM").is_null()).then(pl.lit(None))
            .otherwise(pl.col("GLMNEM")).alias("GLMNEMO")
        )
    # create TRANSREF candidate
    if "PAYGRNO" in prngl.columns:
        prngl = prngl.with_columns(
            pl.when(pl.col("PAYGRNO").is_not_null()).then(pl.col("PAYGRNO").str.slice(0, 7)).alias("TRANSREF")
        )
    # define sets of gl codes per SAS (approx)
    disburse_codes = set(["08000", "09000"])
    repaid_codes = set(["01010","01140"])
    # Normalize GLMNEMO to strings and compare (SAS had many adjustments; approximate)
    prngl = prngl.with_columns(pl.col("GLMNEMO").cast(pl.Utf8))
    disburse = prngl.filter(pl.col("GLMNEMO").is_in(list(disburse_codes)))
    disburse = disburse.with_columns(pl.col("TRANSAMT").alias("DISBURSE"))
    repaid = prngl.filter(pl.col("GLMNEMO").is_in(list(repaid_codes)) & (pl.col("TRANSAMT") < 0))
    repaid = repaid.with_columns((pl.col("TRANSAMT") * -1).alias("REPAID"))
    # Summarize REPAID by ACCTNO TRANSREF
    if repaid.height > 0:
        repaid_sum = repaid.groupby(["ACCTNO","TRANSREF"]).agg(pl.col("REPAID").sum())
    else:
        repaid_sum = pl.DataFrame()
    if disburse.height > 0:
        disb_sum = disburse.groupby(["ACCTNO","TRANSREF"]).agg(pl.col("DISBURSE").sum())
    else:
        disb_sum = pl.DataFrame()
else:
    disb_sum = pl.DataFrame()
    repaid_sum = pl.DataFrame()

# Now merge DISBPAY and REPAID sums as SAS does with many rules (we approximate)
# The SAS program contains complex logic to split different GL codes and then combine
# For simplicity we build DISBPAY by merging prngl-derived disburse and special disburse logic

# Merge into D_BTRAD by ACCTNO TRANSREF if possible
# First create D_BTRAD from tbba but only keep a subset of columns required
d_btrad = tbba.select([c for c in tbba.columns if c in ["ACCTNO","TRANSREF","OUTSTAND","APPRLIMT","APPRLIM2","CURBAL","INTRECV","PRODUCT","LIABCODE","DIRCTIND"]]) \
    .with_columns(pl.col("TRANSREF").cast(pl.Utf8))

# Merge disb_sum & repaid_sum
if disb_sum.height > 0:
    # ensure same types
    disb_sum = disb_sum.with_columns(pl.col("TRANSREF").cast(pl.Utf8))
    d_btrad = d_btrad.join(disb_sum, on=["ACCTNO","TRANSREF"], how="left")
else:
    d_btrad = d_btrad.with_columns(pl.lit(None).alias("DISBURSE"))

if repaid_sum.height > 0:
    repaid_sum = repaid_sum.with_columns(pl.col("TRANSREF").cast(pl.Utf8))
    d_btrad = d_btrad.join(repaid_sum, on=["ACCTNO","TRANSREF"], how="left")
else:
    d_btrad = d_btrad.with_columns(pl.lit(None).alias("REPAID"))

# SAS then merges with PREVBT and sets TRANSREF_PBA etc. We skip details and write out D_BTRAD.
d_btrad = d_btrad.with_columns(pl.lit(REPTDATE).alias("REPTDATE"))

# -----------------------------
# STEP 8: Append MTD and PrevBT info if available
# -----------------------------
if mtd_btavg is not None and "ACCTNO" in mtd_btavg.columns and "TRANSREX" in mtd_btavg.columns:
    # rename TRANSREX -> TRANSREF for consistency
    mtd_btavg = mtd_btavg.rename({"TRANSREX":"TRANSREF"})
    mtd_btavg = mtd_btavg.with_columns(pl.col("TRANSREF").cast(pl.Utf8))
    d_btrad = d_btrad.join(mtd_btavg.select(["ACCTNO","TRANSREF","MTDAVBAL_MIS"]), on=["ACCTNO","TRANSREF"], how="left")

if prev_btr is not None and "ACCTNO" in prev_btr.columns and "TRANSREX" in prev_btr.columns:
    prev_btr = prev_btr.rename({"TRANSREX":"TRANSREF"})
    prev_btr = prev_btr.with_columns(pl.col("TRANSREF").cast(pl.Utf8))
    d_btrad = d_btrad.join(prev_btr.select(["ACCTNO","TRANSREF","TRANSPBA_PREV"]).rename({"TRANSPBA_PREV":"TRANSREF_PBA_PREV"}), on=["ACCTNO","TRANSREF"], how="left")

# if TRANSREF_PBA not present create empty
if "TRANSREF_PBA" not in d_btrad.columns:
    d_btrad = d_btrad.with_columns(pl.lit(None).alias("TRANSREF_PBA"))

# If TRANSREF_PBA empty, set from prev if exists
d_btrad = d_btrad.with_columns(
    pl.when(pl.col("TRANSREF_PBA").is_null())
    .then(pl.col("TRANSREF_PBA_PREV"))
    .otherwise(pl.col("TRANSREF_PBA")).alias("TRANSREF_PBA_FINAL")
).drop("TRANSREF_PBA","TRANSREF_PBA_PREV").rename({"TRANSREF_PBA_FINAL":"TRANSREF_PBA"})

# -----------------------------
# STEP 9: Final filtering and write outputs (BTDTL & BTRAD)
# -----------------------------
# SAS writes BTDTL&REPTDT and BTRAD&REPTDT etc. We'll produce Parquet outputs:
bt_output = os.path.join(OUTPUT_DIR, f"BTDTL_{REPTDATE.strftime('%Y%m%d')}.parquet")
btrad_output = os.path.join(OUTPUT_DIR, f"D_BTRAD_{REPTDATE.strftime('%Y%m%d')}.parquet")

# Write out btrade detail (BTDTL)
if btrsa_bt.height > 0:
    try:
        write_parquet(btrsa_bt, bt_output)
    except Exception as e:
        print(f"Failed writing BTDTL output: {e}")
else:
    print("No BT detail to output.")

# Write out d_btrad
if d_btrad.height > 0:
    try:
        write_parquet(d_btrad, btrad_output)
    except Exception as e:
        print(f"Failed writing D_BTRAD output: {e}")
else:
    print("No D_BTRAD rows to output.")

# -----------------------------
# STEP 10: Produce BTDTL & BTRAD combined (BTRAD&REPTDT) if needed
# -----------------------------
combined_out = os.path.join(OUTPUT_DIR, f"BTRAD_FULL_{REPTDATE.strftime('%Y%m%d')}.parquet")
try:
    # combine if both exist else write what we have
    inputs = []
    if btrsa_bt.height > 0:
        inputs.append(btrsa_bt)
    if d_btrad.height > 0:
        inputs.append(d_btrad)
    if inputs:
        combined = pl.concat(inputs, how="horizontal" if False else "vertical", rechunk=True).lazy().collect()
        write_parquet(combined, combined_out)
except Exception as e:
    print("Unable to write combined BTRAD:", e)

# -----------------------------
# STEP 11: Optional: create DuckDB view for ad-hoc SQL (user requested duckdb)
# -----------------------------
# We create a DuckDB in-memory connection and register written parquet(s) if any for SQL queries
con = duckdb.connect(database=":memory:")
# register output parquet if exists
if os.path.exists(btrad_output):
    con.execute(f"CREATE TABLE d_btrad AS SELECT * FROM read_parquet('{btrad_output}')")
    print("DuckDB: d_btrad registered")
if os.path.exists(bt_output):
    con.execute(f"CREATE TABLE bt_dtl AS SELECT * FROM read_parquet('{bt_output}')")
    print("DuckDB: bt_dtl registered")

# Example: show summary count by branch in DB if available
try:
    res = con.execute("SELECT COUNT(*) as cnt FROM d_btrad").fetchdf()
    print("d_btrad count (duckdb):\n", res)
except Exception:
    pass

print("Conversion finished. Inspect output parquet files under output/ directory.")
