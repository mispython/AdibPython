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
import os

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
def read_sas7bdat(filepath: Path, columns: list = None) -> pl.DataFrame:
    """Read SAS7BDAT file and convert to Polars DataFrame using pyreadstat"""
    sas_path = filepath if filepath.suffix == '.sas7bdat' else filepath.with_suffix('.sas7bdat')
    if not sas_path.exists():
        raise FileNotFoundError(f"SAS7BDAT file not found: {sas_path}")
    
    # Read only specified columns if provided (much faster)
    if columns:
        df, meta = pyreadstat.read_sas7bdat(str(sas_path), usecols=columns)
    else:
        df, meta = pyreadstat.read_sas7bdat(str(sas_path))
    
    # Convert to Polars DataFrame and ensure ACCTNO is Int64
    result = pl.from_pandas(df)
    
    # Cast ACCTNO to Int64 if it exists and is float
    if 'ACCTNO' in result.columns and result.schema['ACCTNO'] in [pl.Float64, pl.Float32]:
        result = result.with_columns(pl.col('ACCTNO').cast(pl.Int64))
    
    return result


# ==================================================
# SAS session manager (singleton)
# ==================================================
_sas_session = None

def get_sas_session():
    """Get or create SAS session"""
    global _sas_session
    if _sas_session is None:
        import saspy
        _sas_session = saspy.SASsession(results='TEXT')
        print("SAS session established")
    return _sas_session

def close_sas_session():
    """Close SAS session if open"""
    global _sas_session
    if _sas_session is not None:
        try:
            _sas_session.endsas()
        except:
            pass
        _sas_session = None

def write_sas7bdat(df: pl.DataFrame, filepath: Path, table_name: str):
    """Write DataFrame to SAS7BDAT format using saspy.
    Uses DATA step to create permanent SAS dataset instead of PROC EXPORT.
    """
    sas = get_sas_session()
    sas_path = filepath.with_suffix('.sas7bdat')
    pd_df = df.to_pandas()
    
    # Get the directory and dataset name from path
    sas_dir = str(sas_path.parent)
    sas_name = sas_path.stem
    
    # Upload DataFrame to SAS as a temporary dataset
    sas.df2sd(pd_df, table=f'_temp_{table_name}', libref='WORK')
    
    # Use SAS libname and DATA step to create the permanent dataset
    sas_code = f"""
        LIBNAME OUTDIR "{sas_dir}";
        
        DATA OUTDIR.{sas_name};
            SET WORK._temp_{table_name};
        RUN;
        
        LIBNAME OUTDIR CLEAR;
        
        PROC DATASETS LIBRARY=WORK NOLIST;
            DELETE _temp_{table_name};
        RUN;
    """
    
    result = sas.submit(sas_code)
    
    # Check for errors in log
    log = result['LOG']
    if 'ERROR' in log:
        print(f"  Warning: SAS log contains errors for {table_name}")
        for line in log.split('\n'):
            if 'ERROR' in line:
                print(f"    {line.strip()}")
    
    print(f"  Written: {sas_path}")


# ==================================================
# Import PBBDPFMT.py and use its format functions
# ==================================================
def load_pbbdpfmt():
    """Load format functions from PBBDPFMT.py"""
    pgm_path = PGM / "PBBDPFMT.py"
    if not pgm_path.exists():
        raise FileNotFoundError(f"PBBDPFMT.py not found: {pgm_path}")
    
    spec = importlib.util.spec_from_file_location("PBBDPFMT", pgm_path)
    pbbdpfmt = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pbbdpfmt)
    
    return pbbdpfmt

# Load formats once
pbbdpfmt = load_pbbdpfmt()


def apply_sas_format_fast(df: pl.DataFrame, source_col: str, format_func, out_col: str) -> pl.DataFrame:
    """
    Apply SAS format efficiently using vectorized operations.
    First creates a mapping from unique values, then joins.
    """
    # Get unique values from source column
    unique_vals = df.select(pl.col(source_col).unique()).to_series().to_list()
    
    # Create mapping dictionary from unique values
    mapping = {}
    for val in unique_vals:
        if val is not None:
            mapping[val] = format_func(val)
        else:
            mapping[None] = ''
    
    # Create a mapping DataFrame
    map_df = pl.DataFrame({
        source_col: list(mapping.keys()),
        out_col: list(mapping.values())
    })
    
    # Ensure matching types for join
    src_dtype = df.schema[source_col]
    map_df = map_df.with_columns(pl.col(source_col).cast(src_dtype))
    
    # Left join to apply format
    result = df.join(map_df, on=source_col, how="left")
    
    return result


# =========================
# 1) REPTDATE - Use current date minus 1 day
# =========================
REPTDATE = datetime.now() - timedelta(days=1)
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON  = f"{REPTDATE.month:02d}"

print(f"Processing for month: {REPTMON} (Report Date: {REPTDATE.strftime('%Y-%m-%d')})")

# Define the consistent column order for DEP
DEP_COLS = ["ACCTNO", "PRODCD", "PURPOSE", "PRODUCT"]

# =========================
# 2) SA - Saving accounts with SAPROD format
# =========================
print("Loading SA data...")
saving_cols = ["ACCTNO","OPENIND","PURPOSE","PRODUCT"]

SA = pl.concat([
    read_sas7bdat(SACA / "saving.sas7bdat", columns=saving_cols),
    read_sas7bdat(ISACA / "saving.sas7bdat", columns=saving_cols),
], how="vertical_relaxed")

# Filter
SA = SA.filter(~pl.col("OPENIND").is_in(["B","C","P"]))

# Apply SAPROD format
print("  Applying SAPROD format...")
SA = apply_sas_format_fast(SA, source_col="PRODUCT", format_func=pbbdpfmt.saprod_format, out_col="PRODCD")
SA = SA.select(DEP_COLS)

# =========================
# 3) CA - Current accounts with CAPROD format
# =========================
print("Loading CA data...")
current_cols = ["ACCTNO","OPENIND","PURPOSE","PRODUCT"]

CA = pl.concat([
    read_sas7bdat(SACA / "current.sas7bdat", columns=current_cols),
    read_sas7bdat(ISACA / "current.sas7bdat", columns=current_cols),
], how="vertical_relaxed")

CA = CA.filter(~pl.col("OPENIND").is_in(["B","C","P"]))

# Apply CAPROD format
print("  Applying CAPROD format...")
CA = apply_sas_format_fast(CA, source_col="PRODUCT", format_func=pbbdpfmt.caprod_format, out_col="PRODCD")
CA = CA.select(DEP_COLS)

# =========================
# 4) FD - Fixed Deposit base
# =========================
print("Loading FD base data...")

# For SACA.FD, PRODUCT doesn't exist, use ACCTTYPE
fd_saca_cols = ["ACCTNO", "PURPOSE", "ACCTTYPE"]
fd_saca = read_sas7bdat(SACA / "fd.sas7bdat", columns=fd_saca_cols)
fd_saca = fd_saca.select(["ACCTNO", "PURPOSE", "ACCTTYPE"]).rename({"ACCTTYPE": "PRODUCT"})

# For ISACA.FD, PRODUCT exists
fd_isaca_cols = ["ACCTNO", "PURPOSE", "PRODUCT"]
fd_isaca = read_sas7bdat(ISACA / "fd.sas7bdat", columns=fd_isaca_cols)

FD_base = (
    pl.concat([fd_saca, fd_isaca], how="vertical_relaxed")
    .sort("ACCTNO")
)

# =========================
# 5) FDCD - Fixed Deposit product codes
# =========================
print("Loading FDCD data...")
fdcd_cols = ["ACCTNO","ACCTTYPE","OPENIND","INTPLAN"]

FDCD_union = pl.concat([
    read_sas7bdat(FDLIB / "fd.sas7bdat", columns=fdcd_cols),
    read_sas7bdat(IFDLIB / "fd.sas7bdat", columns=fdcd_cols),
], how="vertical_relaxed")

FDCD = FDCD_union.filter(
    ~pl.col("ACCTTYPE").is_in([397,398]) & pl.col("OPENIND").is_in(["D","O"])
)

# Apply FDPROD format
print("  Applying FDPROD format...")
FDCD = apply_sas_format_fast(FDCD, source_col="INTPLAN", format_func=pbbdpfmt.fdprod_format, out_col="PRODCD")

# Apply overrides
FDCD = FDCD.with_columns(
    pl.when(pl.col("ACCTTYPE").is_in([315,394]))
    .then(pl.lit("42132"))
    .when(pl.col("ACCTTYPE").is_in([397,398]))
    .then(pl.lit("42199"))
    .otherwise(pl.col("PRODCD"))
    .alias("PRODCD")
)

# PROC SORT DATA=FDCD NODUPKEYS; BY ACCTNO;
FDCD = (
    FDCD.sort(["ACCTNO"])
    .unique(subset=["ACCTNO"], keep="first")
    .select(["ACCTNO","PRODCD"])
)

# =========================
# 6) FD - Merge base with product codes
# =========================
print("Merging FD with FDCD...")
FD = FD_base.join(FDCD, on="ACCTNO", how="inner")
FD = FD.select(DEP_COLS)

# =========================
# 7) DEP - Combined deposits with filters
# =========================
print("Combining deposits...")
DEP = pl.concat([SA, CA, FD], how="vertical_relaxed")

# Ensure ACCTNO is Int64 for consistency
DEP = DEP.with_columns(pl.col("ACCTNO").cast(pl.Int64))

valid_prodcd = ['42110','42310','42120','42320','42130',
                '42133','42132','42180','42610','42630','34180',
                '42199','42699']
DEP = DEP.filter(pl.col("PRODCD").is_in(valid_prodcd))

DEP = DEP.filter(
    ~(
        pl.col("PRODCD").is_in(["42199","42699"])
        & ~pl.col("PRODUCT").cast(pl.Int64).is_in([72,413])
    )
)

DEP = DEP.sort("ACCTNO")

# =========================
# 8) MERGEX - Deposits with PURPOSE in ('5','6')
# =========================
MERGEX = DEP.filter(pl.col("PURPOSE").is_in(["5","6"]))

# =========================
# 9) CLIENT - Parse fixed-width text file
# =========================
print("Parsing CLIENT file...")
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
    # Ensure ACCTNO types match for join
    CLIENT = CLIENT.with_columns(pl.col("ACCTNO").cast(pl.Int64))
    DEP_ACCTNO = DEP.select("ACCTNO").unique().with_columns(pl.col("ACCTNO").cast(pl.Int64))
    CLIENT = CLIENT.join(DEP_ACCTNO, on="ACCTNO", how="inner")

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
print("Creating FDCD_MONTH...")
FD_PBB  = read_sas7bdat(FDLIB / "fd.sas7bdat", columns=["ACCTNO"]).with_columns(ENTITY=pl.lit("PBB "))
FD_PIBB = read_sas7bdat(IFDLIB / "fd.sas7bdat", columns=["ACCTNO"]).with_columns(ENTITY=pl.lit("PIBB "))
FDCD_MONTH = pl.concat([FD_PBB, FD_PIBB], how="vertical_relaxed").select(["ACCTNO","ENTITY"])

# =========================
# 12) Write outputs
# =========================
print("Writing output files...")

# Write TRUST dataset
trust_name = f"TRUST{REPTMON}"
trust_path = HOST / trust_name

# Write Parquet
TRUST.write_parquet(trust_path.with_suffix('.parquet'))
print(f"  Written: {trust_path}.parquet")

# Write SAS7BDAT using saspy
write_sas7bdat(TRUST, trust_path, trust_name)

# Write FDCD_MONTH dataset
fdcd_name = f"FDCD{REPTMON}"
fdcd_path = HOST / fdcd_name

# Write Parquet
FDCD_MONTH.write_parquet(fdcd_path.with_suffix('.parquet'))
print(f"  Written: {fdcd_path}.parquet")

# Write SAS7BDAT using saspy
write_sas7bdat(FDCD_MONTH, fdcd_path, fdcd_name)

# Build Arrow IPC transport file (mirror of PROC CPORT)
# Since the two tables have different schemas, we'll write them as separate Arrow files
# or use a dictionary-based approach
ipc_path = HOST / f"TRUST_FDCD_{REPTMON}.ipc"

# Write a combined IPC file using record batches with custom metadata
# Simplest approach: write two separate Arrow IPC files
trust_ipc = HOST / f"TRUST{REPTMON}.arrow"
fdcd_ipc = HOST / f"FDCD{REPTMON}.arrow"

with pa.ipc.new_file(pa.OSFile(str(trust_ipc), 'wb'), TRUST.to_arrow().schema) as writer:
    writer.write(TRUST.to_arrow())

with pa.ipc.new_file(pa.OSFile(str(fdcd_ipc), 'wb'), FDCD_MONTH.to_arrow().schema) as writer:
    writer.write(FDCD_MONTH.to_arrow())

print(f"  Written: {trust_ipc}")
print(f"  Written: {fdcd_ipc}")

# Close SAS session
close_sas_session()

print(f"\nReport Date: {REPTDATE.strftime('%Y-%m-%d')} (current date - 1 day)")
print(f"Report Month: {REPTMON}")
print(f"TRUST records: {len(TRUST):,}")
print(f"FDCD_MONTH records: {len(FDCD_MONTH):,}")
