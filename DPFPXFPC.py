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
    
    Args:
        df: Input DataFrame
        source_col: Column name to format
        format_func: Format function that takes a value and returns formatted string
        out_col: Output column name
    """
    # Use map_elements to apply the format function to each value
    return df.with_columns(
        pl.col(source_col).map_elements(
            lambda x: format_func(x) if x is not None else '',
            return_dtype=pl.Utf8
        ).alias(out_col)
    )


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
# DATA SA(KEEP=ACCTNO PRODCD PURPOSE PRODUCT);
#    SET SACA.SAVING ISACA.SAVING;
#    WHERE OPENIND NOT IN ('B','C','P');
#    PRODCD=PUT(PRODUCT, SAPROD.);
# RUN;
saving_cols = ["ACCTNO","OPENIND","PURPOSE","PRODUCT"]
SA = (
    pl.concat([
        read_sas7bdat(SACA / "saving.sas7bdat").select(saving_cols),
        read_sas7bdat(ISACA / "saving.sas7bdat").select(saving_cols),
    ], how="vertical_relaxed")
    .filter(~pl.col("OPENIND").is_in(["B","C","P"]))
)

# Apply SAPROD format using pbbdpfmt.saprod_format function
SA = apply_sas_format(SA, source_col="PRODUCT", format_func=pbbdpfmt.saprod_format, out_col="PRODCD")
SA = SA.select(["ACCTNO","PRODCD","PURPOSE","PRODUCT"])

# =========================
# 3) CA - Current accounts with CAPROD format
# =========================
# DATA CA(KEEP=ACCTNO PRODCD PURPOSE PRODUCT);
#    SET SACA.CURRENT ISACA.CURRENT;
#    WHERE OPENIND NOT IN ('B','C','P');
#    PRODCD=PUT(PRODUCT, CAPROD.);
# RUN;
current_cols = ["ACCTNO","OPENIND","PURPOSE","PRODUCT"]
CA = (
    pl.concat([
        read_sas7bdat(SACA / "current.sas7bdat").select(current_cols),
        read_sas7bdat(ISACA / "current.sas7bdat").select(current_cols),
    ], how="vertical_relaxed")
    .filter(~pl.col("OPENIND").is_in(["B","C","P"]))
)

# Apply CAPROD format using pbbdpfmt.caprod_format function
CA = apply_sas_format(CA, source_col="PRODUCT", format_func=pbbdpfmt.caprod_format, out_col="PRODCD")
CA = CA.select(["ACCTNO","PRODCD","PURPOSE","PRODUCT"])

# =========================
# 4) FD - Fixed Deposit base
# =========================
# DATA FD(KEEP=ACCTNO PURPOSE PRODUCT);
#    SET SACA.FD ISACA.FD;
# RUN;
# PROC SORT DATA=FD; BY ACCTNO;RUN;
fd_base_cols = ["ACCTNO","PURPOSE","PRODUCT"]
FD_base = (
    pl.concat([
        read_sas7bdat(SACA / "fd.sas7bdat").select(fd_base_cols),
        read_sas7bdat(ISACA / "fd.sas7bdat").select(fd_base_cols),
    ], how="vertical_relaxed")
    .sort("ACCTNO")
)

# =========================
# 5) FDCD - Fixed Deposit product codes
# =========================
# DATA FDCD(KEEP=ACCTNO PRODCD);
#    SET FD.FD IFD.FD;
#    WHERE ACCTTYPE NOT IN (397,398) AND OPENIND IN ('D','O');
#    PRODCD = PUT(INTPLAN, FDPROD.);
#    IF ACCTTYPE IN (315,394) THEN PRODCD='42132'; ELSE
#    IF ACCTTYPE IN (397,398) THEN PRODCD='42199';
# RUN;
fdcd_cols = ["ACCTNO","ACCTTYPE","OPENIND","INTPLAN"]
FDCD_union = pl.concat([
    read_sas7bdat(FDLIB / "fd.sas7bdat").select(fdcd_cols),
    read_sas7bdat(IFDLIB / "fd.sas7bdat").select(fdcd_cols),
], how="vertical_relaxed")

FDCD = (
    FDCD_union
    .filter(~pl.col("ACCTTYPE").is_in([397,398]) & pl.col("OPENIND").is_in(["D","O"]))
)

# Apply FDPROD format using pbbdpfmt.fdprod_format function
FDCD = apply_sas_format(FDCD, source_col="INTPLAN", format_func=pbbdpfmt.fdprod_format, out_col="PRODCD")

# Apply overrides (matching SAS IF/ELSE logic)
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
# DATA FD;
#    MERGE FD(IN=A) FDCD(IN=B);
#    BY ACCTNO;
#    IF A AND B;
# RUN;
FD = FD_base.join(FDCD, on="ACCTNO", how="inner")

# =========================
# 7) DEP - Combined deposits with filters
# =========================
# DATA DEP;
#    SET SA CA FD;
#    IF PRODCD IN ('42110','42310','42120','42320','42130',
#                  '42133','42132','42180','42610','42630','34180',
#                  '42199','42699');
#    IF PRODCD IN ('42199','42699') AND PRODUCT NOT IN (72,413)
#       THEN DELETE;
# RUN;
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

# PROC SORT DATA=DEP; BY ACCTNO;RUN;
DEP = DEP.sort("ACCTNO")

# =========================
# 8) MERGEX - Deposits with PURPOSE in ('5','6')
# =========================
# DATA MERGEX;
#    SET DEP;
#    WHERE PURPOSE IN ('5','6');
# RUN;
MERGEX = DEP.filter(pl.col("PURPOSE").is_in(["5","6"]))

# =========================
# 9) CLIENT - Parse fixed-width text file
# =========================
def parse_client_fixed_width(path: Path) -> pl.DataFrame:
    rows = []
    with path.open("r", encoding="latin1", errors="ignore") as f:
        for line in f:
            # @002 ACCTNO 10. (positions 2-11, 1-based)
            acct_str = line[1:11] if len(line) >= 11 else ""
            acct_str = acct_str.strip()
            
            # IF COMPRESS(ACCTNO, "1234567890") = ' ' - check if all digits
            if acct_str and all(c in "0123456789" for c in acct_str):
                # @021 NAME $40. (positions 21-60, 1-based)
                name_str = line[20:60] if len(line) >= 60 else ""
                name_str = name_str.rstrip()
                # KEY = SUBSTR(NAME,1,10)
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

# PROC SORT DATA=CLIENT NODUPKEYS; BY ACCTNO;
CLIENT = CLIENT.sort("ACCTNO").unique(subset=["ACCTNO"], keep="first")

# DATA CLIENT;
#    MERGE CLIENT(IN=A) DEP (IN=B KEEP=ACCTNO);
#    BY ACCTNO;
#    IF A & B;
# RUN;
CLIENT = CLIENT.join(DEP.select("ACCTNO").unique(), on="ACCTNO", how="inner")

# =========================
# 10) HOST.TRUST&REPTMON - Trust accounts
# =========================
# DATA HOST.TRUST&REPTMON(KEEP=ACCTNO);
#    SET MERGEX CLIENT;
# RUN;
TRUST = pl.concat([
    MERGEX.select(["ACCTNO"]),
    CLIENT.select(["ACCTNO"])
], how="vertical_relaxed")

# =========================
# 11) HOST.FDCD&REPTMON - FD entity mapping
# =========================
# DATA HOST.FDCD&REPTMON(KEEP=ACCTNO ENTITY);
#    SET FD.FD(IN=A) IFD.FD(IN=B);
#    IF B THEN ENTITY = 'PIBB ';
#    ELSE      ENTITY = 'PBB ';
# RUN;
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

# Build Arrow IPC transport file (mirror of PROC CPORT)
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
