from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.ipc as ipc
import duckdb
import polars as pl
import pandas as pd
import pyreadstat
import subprocess
import sys

# ============================================
# LIBRARY MAPPINGS (adjust to your environment)
# ============================================
# SAS LIBNAME SACA  -> folder with SAS7BDAT tables for PBB MNITB
# SAS LIBNAME ISACA -> folder with SAS7BDAT tables for PIBB MNITB
# SAS LIBNAME FD    -> folder with SAS7BDAT tables for PBB MNIFD
# SAS LIBNAME IFD   -> folder with SAS7BDAT tables for PIBB MNIFD
# SAS LIBNAME HOST  -> output folder representing SAP.PBB.QRF.DP.LIST
# SAS DD CLIENT     -> fixed-width text file SAP.B033.DP.SOLCA.RPT
# SAS PGM(PBBDPFMT) -> Python program for format processing

ROOT = Path(".")  # repo root
SACA   = ROOT / "sas" / "python" / "virt_edw" / "Data_Warehouse" / "MIS" / "XMIS" / "input" / "prod" / "EIBMTRUT" / "conv" 
ISACA  = ROOT / "sas" / "python" / "virt_edw" / "Data_Warehouse" / "MIS" / "XMIS" / "input" / "prod" / "EIBMTRUT" / "islamic" 
FDLIB  = ROOT / "sas" / "python" / "virt_edw" / "Data_Warehouse" / "MIS" / "XMIS" / "input" / "prod" / "EIBMTRUT" / "fd"  
IFDLIB = ROOT / "sas" / "python" / "virt_edw" / "Data_Warehouse" / "MIS" / "XMIS" / "input" / "prod" / "EIBMTRUT" / "ifd"
PGM    = ROOT / "parquet_input" / "PGM"  / "PBBDPFMT"  # PBBDPFMT.py location
HOST   = ROOT / "sas" / "python" / "virt_edw" / "Data_Warehouse" / "MIS" / "XMIS" / "output" / "EIBMTRUT"
CLIENT_RPT = ROOT / "sas" / "python" / "virt_edw" / "Data_Warehouse" / "MIS" / "XMIS" / "input" / "prod" / "EIBMTRUT" / "CLIENT.txt"

HOST.mkdir(parents=True, exist_ok=True)


# ==================================================
# Helper to read SAS7BDAT files using pyreadstat
# ==================================================
def read_sas7bdat(filepath: Path) -> pl.DataFrame:
    """Read SAS7BDAT file and convert to Polars DataFrame using pyreadstat"""
    sas_path = filepath.with_suffix('.sas7bdat')
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
    sas_path = filepath.with_suffix('.sas7bdat')
    # Convert to pandas for pyreadstat writing
    pd_df = df.to_pandas()
    pyreadstat.write_sas7bdat(pd_df, str(sas_path))


# ==================================================
# Call PBBDPFMT.py program for format processing
# ==================================================
def apply_format_pgm(df: pl.DataFrame, source_col: str, format_name: str, out_col: str, temp_dir: Path) -> pl.DataFrame:
    """
    Apply SAS format by calling PBBDPFMT.py program
    The program takes input data, format name, and returns formatted data
    """
    # Create temporary files for data exchange
    temp_input = temp_dir / f"temp_input_{format_name}.parquet"
    temp_output = temp_dir / f"temp_output_{format_name}.parquet"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # Save the input dataframe to parquet
    df.write_parquet(temp_input)
    
    # Call PBBDPFMT.py program
    pgm_path = PGM / "PBBDPFMT.py"
    if not pgm_path.exists():
        raise FileNotFoundError(f"PBBDPFMT.py not found: {pgm_path}")
    
    # Execute the format program
    cmd = [
        sys.executable,  # Python interpreter
        str(pgm_path),
        "--input", str(temp_input),
        "--output", str(temp_output),
        "--format", format_name,
        "--source-col", source_col,
        "--target-col", out_col
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"PBBDPFMT output for {format_name}: {result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error calling PBBDPFMT.py: {e.stderr}")
        raise
    
    # Read the formatted output
    formatted_df = pl.read_parquet(temp_output)
    
    # Clean up temp files
    temp_input.unlink(missing_ok=True)
    temp_output.unlink(missing_ok=True)
    
    return formatted_df


# Alternative: If PBBDPFMT.py is importable and provides functions
def apply_format_import(df: pl.DataFrame, source_col: str, format_name: str, out_col: str) -> pl.DataFrame:
    """
    Apply SAS format by importing and calling PBBDPFMT.py functions directly
    """
    import importlib.util
    
    pgm_path = PGM / "PBBDPFMT.py"
    if not pgm_path.exists():
        raise FileNotFoundError(f"PBBDPFMT.py not found: {pgm_path}")
    
    # Import the module
    spec = importlib.util.spec_from_file_location("PBBDPFMT", pgm_path)
    pbbdpfmt = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pbbdpfmt)
    
    # Call the format function if it exists
    # Assuming PBBDPFMT.py has a function like: apply_format(df, source_col, format_name, out_col)
    if hasattr(pbbdpfmt, 'apply_format'):
        return pbbdpfmt.apply_format(df, source_col, format_name, out_col)
    elif hasattr(pbbdpfmt, format_name):
        # If it provides format dictionaries
        format_dict = getattr(pbbdpfmt, format_name)
        if isinstance(format_dict, dict):
            return apply_format_dict(df, source_col, format_dict, out_col)
    
    raise AttributeError(f"PBBDPFMT.py doesn't have required format function or dictionary for {format_name}")


# Simple dictionary-based format application (fallback)
def apply_format_dict(df: pl.DataFrame, source_col: str, format_dict: dict, out_col: str) -> pl.DataFrame:
    """
    Apply SAS format using dictionary mapping
    """
    # Convert format dict to DataFrame for joining
    fmt_df = pl.DataFrame({
        "key": list(format_dict.keys()),
        "value": list(format_dict.values())
    })
    
    # Ensure matching types
    src_dtype = df.schema[source_col]
    if src_dtype == pl.Utf8:
        fmt_df = fmt_df.with_columns(pl.col("key").cast(pl.Utf8))
    else:
        fmt_df = fmt_df.with_columns(pl.col("key").cast(pl.Float64))
        df = df.with_columns(pl.col(source_col).cast(pl.Float64))
    
    # Perform left join
    result = df.join(
        fmt_df.rename({"value": out_col}),
        left_on=source_col,
        right_on="key",
        how="left"
    ).drop("key")
    
    return result


# =========================
# 1) REPTDATE, &REPTMON
# =========================
# SACA.REPTDATE is assumed to be a single row with column REPTDATE
reptdate_df = read_sas7bdat(SACA / "REPTDATE")

# Coerce to date; support various formats
def to_date_expr(col: pl.Expr) -> pl.Expr:
    return (
        pl.when(col.is_dtype(pl.Date))
        .then(col)
        .when(col.is_dtype(pl.Int64) | col.is_dtype(pl.Int32))
        .then(pl.datetime(
            (col // 10000).cast(pl.Int32),
            ((col % 10000) // 100).cast(pl.Int32),
            (col % 100).cast(pl.Int32)
        ).cast(pl.Date))
        .otherwise(pl.col("REPTDATE").str.strptime(pl.Date, fmt="%Y-%m-%d", strict=False))
    )

reptdate_df = reptdate_df.with_columns(REPTDATE=to_date_expr(pl.col("REPTDATE")))
REPTDATE = reptdate_df.select(pl.col("REPTDATE")).item(0, 0)
# Use timedelta(days=1) as requested
REPTDATE_ADJUSTED = REPTDATE - timedelta(days=1)
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON  = f"{REPTDATE.month:02d}"

print(f"Processing for month: {REPTMON} (Date: {REPTDATE}, Adjusted: {REPTDATE_ADJUSTED})")

# =========================
# 2) SA and CA (PUT with formats) - calling PBBDPFMT.py
# =========================
# SA: from SACA.SAVING and ISACA.SAVING; filter OPENIND NOT IN ('B','C','P'); PRODCD=PUT(PRODUCT,SAPROD.)
saving_cols = ["ACCTNO","OPENIND","PURPOSE","PRODUCT"]
SA = (
    pl.concat([
        read_sas7bdat(SACA / "SAVING").select(saving_cols),
        read_sas7bdat(ISACA / "SAVING").select(saving_cols),
    ], how="vertical_relaxed")
    .filter(~pl.col("OPENIND").is_in(["B","C","P"]))
)

# Apply format using PBBDPFMT.py
try:
    SA = apply_format_import(SA, source_col="PRODUCT", format_name="SAPROD", out_col="PRODCD")
except:
    # Fallback to subprocess call if import doesn't work
    SA = apply_format_pgm(SA, source_col="PRODUCT", format_name="SAPROD", out_col="PRODCD", temp_dir=HOST / "temp")

SA = SA.select(["ACCTNO","PRODCD","PURPOSE","PRODUCT"])

# CA: from SACA.CURRENT and ISACA.CURRENT; same filter; PRODCD=PUT(PRODUCT,CAPROD.)
current_cols = ["ACCTNO","OPENIND","PURPOSE","PRODUCT"]
CA = (
    pl.concat([
        read_sas7bdat(SACA / "CURRENT").select(current_cols),
        read_sas7bdat(ISACA / "CURRENT").select(current_cols),
    ], how="vertical_relaxed")
    .filter(~pl.col("OPENIND").is_in(["B","C","P"]))
)

# Apply format using PBBDPFMT.py
try:
    CA = apply_format_import(CA, source_col="PRODUCT", format_name="CAPROD", out_col="PRODCD")
except:
    CA = apply_format_pgm(CA, source_col="PRODUCT", format_name="CAPROD", out_col="PRODCD", temp_dir=HOST / "temp")

CA = CA.select(["ACCTNO","PRODCD","PURPOSE","PRODUCT"])

# =========================
# 3) FD (base) and FDCD (product coding from FD libs)
# =========================
# Base FD (keep ACCTNO PURPOSE PRODUCT) from SACA.FD and ISACA.FD
fd_base_cols = ["ACCTNO","PURPOSE","PRODUCT"]
FD_base = pl.concat([
    read_sas7bdat(SACA / "FD").select(fd_base_cols),
    read_sas7bdat(ISACA / "FD").select(fd_base_cols),
], how="vertical_relaxed").sort("ACCTNO")

# FDCD: from FD.FD and IFD.FD; filters & mappings
fdcd_cols = ["ACCTNO","ACCTTYPE","OPENIND","INTPLAN"]
FDCD_union = pl.concat([
    read_sas7bdat(FDLIB / "FD").select(fdcd_cols).with_columns(ENTITY_SRC=pl.lit("PBB")),
    read_sas7bdat(IFDLIB / "FD").select(fdcd_cols).with_columns(ENTITY_SRC=pl.lit("PIBB")),
], how="vertical_relaxed")

FDCD = (
    FDCD_union
    .filter(~pl.col("ACCTTYPE").is_in([397,398]) & pl.col("OPENIND").is_in(["D","O"]))
)

# PRODCD = PUT(INTPLAN, FDPROD.) - call PBBDPFMT.py
try:
    FDCD = apply_format_import(FDCD, source_col="INTPLAN", format_name="FDPROD", out_col="PRODCD")
except:
    FDCD = apply_format_pgm(FDCD, source_col="INTPLAN", format_name="FDPROD", out_col="PRODCD", temp_dir=HOST / "temp")

# Overrides:
FDCD = FDCD.with_columns(
    pl.when(pl.col("ACCTTYPE").is_in([315,394])).then(pl.lit("42132"))
     .when(pl.col("ACCTTYPE").is_in([397,398])).then(pl.lit("42199"))
     .otherwise(pl.col("PRODCD"))
     .alias("PRODCD")
)

# NODUPKEYS by ACCTNO — keep first occurrence
FDCD = (
    FDCD.sort(["ACCTNO"])
         .unique(subset=["ACCTNO"], keep="first")
         .select(["ACCTNO","PRODCD"])
)

# Merge FD = FD_base inner join FDCD by ACCTNO; keep only matches (IF A AND B)
FD = (
    FD_base.join(FDCD, on="ACCTNO", how="inner")
)

# =========================
# 4) DEP = SA ∪ CA ∪ FD with filters on PRODCD, PRODUCT
# =========================
DEP = pl.concat([SA, CA, FD], how="vertical_relaxed")

valid_prodcd = ['42110','42310','42120','42320','42130',
                '42133','42132','42180','42610','42630','34180',
                '42199','42699']
DEP = DEP.filter(pl.col("PRODCD").is_in(valid_prodcd))

DEP = DEP.filter(
    ~(
        pl.col("PRODCD").is_in(["42199","42699"])
        & ~pl.col("PRODUCT").is_in([72,413])
    )
)

DEP = DEP.sort("ACCTNO")

# =========================
# 5) MERGEX = DEP where PURPOSE in ('5','6')
# =========================
MERGEX = DEP.filter(pl.col("PURPOSE").is_in(["5","6"]))

# =========================
# 6) CLIENT fixed-width parse + join with DEP
# =========================
# SAS:
#   @002 ACCTNO 10.   (positions 2-11, 1-based)
#   @021 NAME $40.    (positions 21-60, 1-based)
#   Keep record only if ACCTNO contains digits only.
def parse_client_fixed_width(path: Path) -> pl.DataFrame:
    rows = []
    with path.open("r", encoding="latin1", errors="ignore") as f:
        for line in f:
            # Convert to 0-based slices; end exclusive
            acct_str = line[1:11] if len(line) >= 11 else ""
            acct_str = acct_str.strip()
            if acct_str and acct_str.isdigit():
                name_str = line[20:60] if len(line) >= 60 else ""
                name_str = name_str.rstrip()
                rows.append({"ACCTNO": int(acct_str), "NAME": name_str, "KEY": name_str[:10]})
    if not rows:
        return pl.DataFrame({"ACCTNO": pl.Series([], dtype=pl.Int64),
                             "NAME": pl.Series([], dtype=pl.Utf8),
                             "KEY":  pl.Series([], dtype=pl.Utf8)})
    return pl.DataFrame(rows)

CLIENT = parse_client_fixed_width(CLIENT_RPT)

# PROC SORT NODUPKEYS BY ACCTNO
CLIENT = CLIENT.sort("ACCTNO").unique(subset=["ACCTNO"], keep="first")

# MERGE CLIENT(IN=A) with DEP(KEEP=ACCTNO) (IN=B); IF A & B
CLIENT = CLIENT.join(DEP.select("ACCTNO").unique(), on="ACCTNO", how="inner")

# =========================
# 7) HOST.TRUST&REPTMON (KEEP=ACCTNO) = MERGEX stacked on CLIENT
# =========================
TRUST = pl.concat([
    MERGEX.select(["ACCTNO"]),
    CLIENT.select(["ACCTNO"])
], how="vertical_relaxed")

# =========================
# 8) HOST.FDCD&REPTMON (KEEP=ACCTNO, ENTITY) from FD.FD and IFD.FD (entity tagging)
# =========================
FD_PBB  = read_sas7bdat(FDLIB / "FD").select(["ACCTNO"]).with_columns(ENTITY=pl.lit("PBB "))
FD_PIBB = read_sas7bdat(IFDLIB / "FD").select(["ACCTNO"]).with_columns(ENTITY=pl.lit("PIBB "))
FDCD_MONTH = pl.concat([FD_PBB, FD_PIBB], how="vertical_relaxed").select(["ACCTNO","ENTITY"])

# =========================
# 9) Write outputs in both SAS7BDAT and Parquet formats
# =========================

# Write TRUST
trust_path = HOST / f"TRUST{REPTMON}"
TRUST.write_parquet(trust_path.with_suffix('.parquet'))
write_sas7bdat(TRUST, trust_path)

# Write FDCD_MONTH
fdcd_path = HOST / f"FDCD{REPTMON}"
FDCD_MONTH.write_parquet(fdcd_path.with_suffix('.parquet'))
write_sas7bdat(FDCD_MONTH, fdcd_path)

# Also build a single Arrow IPC transport (mirror of PROC CPORT)
# — pack both tables into one file (for shipping)
tables = {
    f"TRUST{REPTMON}": TRUST.to_arrow(),
    f"FDCD{REPTMON}": FDCD_MONTH.to_arrow(),
}

# Write Arrow IPC file
ipc_path = HOST / f"TRUST_FDCD_{REPTMON}.arrow"
with pa.ipc.new_file(ipc_path, tables[f"TRUST{REPTMON}"].schema) as writer:
    for name, table in tables.items():
        writer.write_table(table, name)

# Clean up temp directory if exists
temp_dir = HOST / "temp"
if temp_dir.exists():
    import shutil
    shutil.rmtree(temp_dir)

print(f"Written: {trust_path}.parquet")
print(f"Written: {trust_path}.sas7bdat")
print(f"Written: {fdcd_path}.parquet")
print(f"Written: {fdcd_path}.sas7bdat")
print(f"Written: {ipc_path}")
print(f"Report Date: {REPTDATE}")
print(f"Adjusted Report Date (minus 1 day): {REPTDATE_ADJUSTED}")
