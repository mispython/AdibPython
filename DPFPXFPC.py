from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.ipc as ipc
import polars as pl
import pandas as pd
import pyreadstat
import importlib.util
import sys

# ============================================
# LIBRARY MAPPINGS (adjust to your environment)
# ============================================
ROOT = Path(".")
SACA   = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTRUT/conv/" 
ISACA  = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTRUT/islamic" 
FDLIB  = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTRUT/fd"  
IFDLIB = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTRUT/ifd"
PGM    = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/"  # PBBDPFMT.py location
HOST   = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMTRUT"
CLIENT_RPT = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTRUT/" / "CLIENT.txt"

HOST.mkdir(parents=True, exist_ok=True)


# ==================================================
# Helper to read SAS7BDAT files using pyreadstat
# ==================================================
def read_sas7bdat(filepath: Path) -> pl.DataFrame:
    """Read SAS7BDAT file and convert to Polars DataFrame using pyreadstat"""
    sas_path = filepath if filepath.suffix == '.sas7bdat' else filepath.with_suffix('.sas7bdat')
    if not sas_path.exists():
        raise FileNotFoundError(f"SAS7BDAT file not found: {sas_path}")
    
    # Read SAS file with metadata
    df, meta = pyreadstat.read_sas7bdat(str(sas_path))
    
    # Convert to Polars DataFrame
    return pl.from_pandas(df)


# ==================================================
# Helper to write SAS7BDAT files using pyreadstat
# ==================================================
def write_sas7bdat(df: pl.DataFrame, filepath: Path):
    """Write DataFrame to SAS7BDAT format using pyreadstat"""
    sas_path = filepath if filepath.suffix == '.sas7bdat' else filepath.with_suffix('.sas7bdat')
    # Convert to pandas for pyreadstat writing
    pd_df = df.to_pandas()
    pyreadstat.write_sas7bdat(pd_df, str(sas_path))


# ==================================================
# Import PBBDPFMT.py and use its format functions
# ==================================================
def load_pbbdpfmt():
    """Load format functions from PBBDPFMT.py"""
    pgm_path = PGM / "PBBDPFMT.py"
    if not pgm_path.exists():
        raise FileNotFoundError(f"PBBDPFMT.py not found: {pgm_path}")
    
    # Import the module
    spec = importlib.util.spec_from_file_location("PBBDPFMT", pgm_path)
    pbbdpfmt = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pbbdpfmt)
    
    return pbbdpfmt

# Load formats once
pbbdpfmt = load_pbbdpfmt()


def apply_sas_format(df: pl.DataFrame, source_col: str, format_func, out_col: str) -> pl.DataFrame:
    """
    Apply SAS format using a format function from PBBDPFMT.py
    """
    return df.with_columns(
        pl.col(source_col).map_elements(
            lambda x: format_func(x) if x is not None else '',
            return_dtype=pl.Utf8
        ).alias(out_col)
    )


def safe_select(df: pl.DataFrame, cols: list) -> pl.DataFrame:
    """Select only columns that exist in the DataFrame"""
    available_cols = [c for c in cols if c in df.columns]
    missing_cols = [c for c in cols if c not in df.columns]
    if missing_cols:
        print(f"Warning: Missing columns: {missing_cols}")
    return df.select(available_cols)


# =========================
# 1) REPTDATE - Use current date minus 1 day
# =========================
REPTDATE = datetime.now() - timedelta(days=1)
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON  = f"{REPTDATE.month:02d}"

print(f"Processing for month: {REPTMON} (Report Date: {REPTDATE.strftime('%Y-%m-%d')})")
print(f"Report Date based on: current date minus 1 day")

# =========================
# 2) SA - Saving accounts with SAPROD format
# =========================
print("\nLoading SA data...")
saving_cols = ["ACCTNO","OPENIND","PURPOSE","PRODUCT"]

# Read and check columns
sa_saca = read_sas7bdat(SACA / "saving.sas7bdat")
sa_isaca = read_sas7bdat(ISACA / "saving.sas7bdat")
print(f"SACA.SAVING columns: {sa_saca.columns}")
print(f"ISACA.SAVING columns: {sa_isaca.columns}")

SA = (
    pl.concat([
        safe_select(sa_saca, saving_cols),
        safe_select(sa_isaca, saving_cols),
    ], how="vertical_relaxed")
    .filter(~pl.col("OPENIND").is_in(["B","C","P"]))
)

# Apply SAPROD format
SA = apply_sas_format(SA, source_col="PRODUCT", format_func=pbbdpfmt.saprod_format, out_col="PRODCD")
SA = SA.select(["ACCTNO","PRODCD","PURPOSE","PRODUCT"])

# =========================
# 3) CA - Current accounts with CAPROD format
# =========================
print("\nLoading CA data...")
current_cols = ["ACCTNO","OPENIND","PURPOSE","PRODUCT"]

ca_saca = read_sas7bdat(SACA / "current.sas7bdat")
ca_isaca = read_sas7bdat(ISACA / "current.sas7bdat")
print(f"SACA.CURRENT columns: {ca_saca.columns}")
print(f"ISACA.CURRENT columns: {ca_isaca.columns}")

CA = (
    pl.concat([
        safe_select(ca_saca, current_cols),
        safe_select(ca_isaca, current_cols),
    ], how="vertical_relaxed")
    .filter(~pl.col("OPENIND").is_in(["B","C","P"]))
)

# Apply CAPROD format
CA = apply_sas_format(CA, source_col="PRODUCT", format_func=pbbdpfmt.caprod_format, out_col="PRODCD")
CA = CA.select(["ACCTNO","PRODCD","PURPOSE","PRODUCT"])

# =========================
# 4) FD - Fixed Deposit base
# =========================
print("\nLoading FD base data...")
fd_base_cols = ["ACCTNO","PURPOSE","PRODUCT"]

fd_saca = read_sas7bdat(SACA / "fd.sas7bdat")
fd_isaca = read_sas7bdat(ISACA / "fd.sas7bdat")
print(f"SACA.FD columns: {fd_saca.columns}")
print(f"ISACA.FD columns: {fd_isaca.columns}")

# Check if PRODUCT column exists, if not, look for alternative
if "PRODUCT" not in fd_saca.columns:
    # Use ACCTTYPE or INTPLAN as product code for FD
    if "ACCTTYPE" in fd_saca.columns:
        print("Using ACCTTYPE as PRODUCT for FD base")
        fd_saca = fd_saca.rename({"ACCTTYPE": "PRODUCT"})
    elif "INTPLAN" in fd_saca.columns:
        print("Using INTPLAN as PRODUCT for FD base")
        fd_saca = fd_saca.rename({"INTPLAN": "PRODUCT"})
    else:
        print("Creating PRODUCT column from first available code column")
        fd_saca = fd_saca.with_columns(pl.lit(0).alias("PRODUCT"))

if "PRODUCT" not in fd_isaca.columns:
    if "ACCTTYPE" in fd_isaca.columns:
        print("Using ACCTTYPE as PRODUCT for FD Islamic base")
        fd_isaca = fd_isaca.rename({"ACCTTYPE": "PRODUCT"})
    elif "INTPLAN" in fd_isaca.columns:
        print("Using INTPLAN as PRODUCT for FD Islamic base")
        fd_isaca = fd_isaca.rename({"INTPLAN": "PRODUCT"})
    else:
        print("Creating PRODUCT column for FD Islamic base")
        fd_isaca = fd_isaca.with_columns(pl.lit(0).alias("PRODUCT"))

FD_base = (
    pl.concat([
        safe_select(fd_saca, fd_base_cols),
        safe_select(fd_isaca, fd_base_cols),
    ], how="vertical_relaxed")
    .sort("ACCTNO")
)

# =========================
# 5) FDCD - Fixed Deposit product codes
# =========================
print("\nLoading FDCD data...")
fdcd_cols = ["ACCTNO","ACCTTYPE","OPENIND","INTPLAN"]

fdcd_fdlib = read_sas7bdat(FDLIB / "fd.sas7bdat")
fdcd_ifdlib = read_sas7bdat(IFDLIB / "fd.sas7bdat")
print(f"FDLIB.FD columns: {fdcd_fdlib.columns}")
print(f"IFDLIB.FD columns: {fdcd_ifdlib.columns}")

FDCD_union = pl.concat([
    safe_select(fdcd_fdlib, fdcd_cols),
    safe_select(fdcd_ifdlib, fdcd_cols),
], how="vertical_relaxed")

FDCD = (
    FDCD_union
    .filter(~pl.col("ACCTTYPE").is_in([397,398]) & pl.col("OPENIND").is_in(["D","O"]))
)

# Apply FDPROD format
FDCD = apply_sas_format(FDCD, source_col="INTPLAN", format_func=pbbdpfmt.fdprod_format, out_col="PRODCD")

# Apply overrides
FDCD = FDCD.with_columns(
    pl.when(pl.col("ACCTTYPE").is_in([315,394]))
    .then(pl.lit("42132"))
    .when(pl.col("ACCTTYPE").is_in([397,398]))
    .then(pl.lit("42199"))
    .otherwise(pl.col("PRODCD"))
    .alias("PRODCD")
)

# PROC SORT DATA=FDCD NODUPKEYS; BY ACCTNO;RUN;
FDCD = (
    FDCD.sort(["ACCTNO"])
    .unique(subset=["ACCTNO"], keep="first")
    .select(["ACCTNO","PRODCD"])
)

# =========================
# 6) FD - Merge base with product codes
# =========================
FD = FD_base.join(FDCD, on="ACCTNO", how="inner")

# =========================
# 7) DEP - Combined deposits with filters
# =========================
print("\nCombining deposits...")
DEP = pl.concat([SA, CA, FD], how="vertical_relaxed")

valid_prodcd = ['42110','42310','42120','42320','42130',
                '42133','42132','42180','42610','42630','34180',
                '42199','42699']
DEP = DEP.filter(pl.col("PRODCD").is_in(valid_prodcd))

# Only apply PRODUCT filter if PRODUCT column exists
if "PRODUCT" in DEP.columns:
    DEP = DEP.filter(
        ~(
            pl.col("PRODCD").is_in(["42199","42699"])
            & ~pl.col("PRODUCT").is_in([72,413])
        )
    )

# PROC SORT DATA=DEP; BY ACCTNO;RUN;
DEP = DEP.sort("ACCTNO")

# =========================
# 8) MERGEX - Deposits with PURPOSE in ('5','6')
# =========================
MERGEX = DEP.filter(pl.col("PURPOSE").is_in(["5","6"]))

# =========================
# 9) CLIENT - Parse fixed-width text file
# =========================
print("\nParsing CLIENT file...")
def parse_client_fixed_width(path: Path) -> pl.DataFrame:
    rows = []
    if not path.exists():
        print(f"Warning: CLIENT file not found: {path}")
        return pl.DataFrame({
            "ACCTNO": pl.Series([], dtype=pl.Int64),
            "NAME": pl.Series([], dtype=pl.Utf8),
            "KEY": pl.Series([], dtype=pl.Utf8)
        })
    
    with path.open("r", encoding="latin1", errors="ignore") as f:
        for line in f:
            acct_str = line[1:11] if len(line) >= 11 else ""
            acct_str = acct_str.strip()
            
            if acct_str and all(c in "0123456789" for c in acct_str):
                name_str = line[20:60] if len(line) >= 60 else ""
                name_str = name_str.rstrip()
                key = name_str[:10] if name_str else ""
                rows.append({
                    "ACCTNO": int(acct_str),
                    "NAME": name_str,
                    "KEY": key
                })
    
    if not rows:
        return pl.DataFrame({
            "ACCTNO": pl.Series([], dtype=pl.Int64),
            "NAME": pl.Series([], dtype=pl.Utf8),
            "KEY": pl.Series([], dtype=pl.Utf8)
        })
    return pl.DataFrame(rows)

CLIENT = parse_client_fixed_width(CLIENT_RPT)

if len(CLIENT) > 0:
    CLIENT = CLIENT.sort("ACCTNO").unique(subset=["ACCTNO"], keep="first")
    CLIENT = CLIENT.join(DEP.select("ACCTNO").unique(), on="ACCTNO", how="inner")

# =========================
# 10) HOST.TRUST&REPTMON - Trust accounts
# =========================
TRUST = pl.concat([
    MERGEX.select(["ACCTNO"]),
    CLIENT.select(["ACCTNO"])
], how="vertical_relaxed")

# =========================
# 11) HOST.FDCD&REPTMON - FD entity mapping
# =========================
print("\nCreating FDCD_MONTH...")
FD_PBB  = read_sas7bdat(FDLIB / "fd.sas7bdat").select(["ACCTNO"]).with_columns(ENTITY=pl.lit("PBB "))
FD_PIBB = read_sas7bdat(IFDLIB / "fd.sas7bdat").select(["ACCTNO"]).with_columns(ENTITY=pl.lit("PIBB "))
FDCD_MONTH = pl.concat([FD_PBB, FD_PIBB], how="vertical_relaxed").select(["ACCTNO","ENTITY"])

# =========================
# 12) Write outputs in both SAS7BDAT and Parquet formats
# =========================

# Write TRUST dataset
trust_path = HOST / f"TRUST{REPTMON}"
TRUST.write_parquet(trust_path.with_suffix('.parquet'))
write_sas7bdat(TRUST, trust_path.with_suffix('.sas7bdat'))

# Write FDCD_MONTH dataset
fdcd_path = HOST / f"FDCD{REPTMON}"
FDCD_MONTH.write_parquet(fdcd_path.with_suffix('.parquet'))
write_sas7bdat(FDCD_MONTH, fdcd_path.with_suffix('.sas7bdat'))

# Build Arrow IPC transport file
tables = {
    f"TRUST{REPTMON}": TRUST.to_arrow(),
    f"FDCD{REPTMON}": FDCD_MONTH.to_arrow(),
}

ipc_path = HOST / f"TRUST_FDCD_{REPTMON}.arrow"
with pa.ipc.new_file(ipc_path, tables[f"TRUST{REPTMON}"].schema) as writer:
    for name, table in tables.items():
        writer.write_table(table, name)

print(f"\nOutput files:")
print(f"  {trust_path}.parquet")
print(f"  {trust_path}.sas7bdat")
print(f"  {fdcd_path}.parquet")
print(f"  {fdcd_path}.sas7bdat")
print(f"  {ipc_path}")
print(f"\nReport Date (current date - 1 day): {REPTDATE.strftime('%Y-%m-%d')}")
print(f"Report Month: {REPTMON}")
print(f"Report Year: {REPTYEAR}")
