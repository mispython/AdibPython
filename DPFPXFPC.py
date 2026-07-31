from __future__ import annotations

from pathlib import Path
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.ipc as ipc
import duckdb
import polars as pl

# ============================================
# LIBRARY MAPPINGS (adjust to your environment)
# ============================================
# SAS LIBNAME SACA  -> folder with Parquet tables for PBB MNITB
# SAS LIBNAME ISACA -> folder with Parquet tables for PIBB MNITB
# SAS LIBNAME FD    -> folder with Parquet tables for PBB MNIFD
# SAS LIBNAME IFD   -> folder with Parquet tables for PIBB MNIFD
# SAS LIBNAME HOST  -> output folder representing SAP.PBB.QRF.DP.LIST
# SAS DD CLIENT     -> fixed-width text file SAP.B033.DP.SOLCA.RPT
# SAS PGM(PBBDPFMT) -> CNTLOUT-like parquet(s) for formats (SAPROD, CAPROD, FDPROD)

ROOT = Path(".")  # repo root
SACA   = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTRUT" / "conv" 
ISACA  = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTRUT" / "islamic" 
FDLIB  = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTRUT" / "fd"  
IFDLIB = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTRUT" / "ifd"
PGM    = ROOT / "parquet_input" / "PGM"  / "PBBDPFMT"  # PROC FORMAT cntlout parquet(s)
HOST   = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMTRUT"
CLIENT_RPT = ROOT / "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTRUT" / "CLIENT.txt"  # TEXT FILE

HOST.mkdir(parents=True, exist_ok=True)


# ==================================================
# Helpers to apply PROC FORMAT (PGM concern-resolved)
# ==================================================
# Expect CNTLOUT-like parquet per format with columns at least:
# FMTNAME, START, END, LABEL (numeric/char START/END; inclusive range)
# LOW/HIGH are pre-resolved to numeric/char bounds in these files.

def load_format(fmtname: str) -> pl.DataFrame:
    # load corresponding parquet; you can also consolidate all formats in one file and filter by FMTNAME
    path = PGM / f"{fmtname}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Missing CNTLOUT parquet for format {fmtname}: {path}")
    df = pl.read_parquet(path)
    # Normalize columns
    expected = {"FMTNAME","START","END","LABEL"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"{fmtname} parquet missing columns: {missing}")
    return df

def apply_format_range(df: pl.DataFrame, source_col: str, fmtname: str, out_col: str) -> pl.DataFrame:
    """
    Faithfully reproduces SAS PUT(x, fmt.) with range handling via non-equi join:
    - JOIN on START <= x <= END
    - If overlapping rules exist, Polars' asof-style approach isn’t enough; we do explicit filter join.
    - Assumes START/END types match the source column's logical type.
    """
    fmt = load_format(fmtname)

    # Ensure numeric-vs-string alignment. In practice your CNTLOUT exports should already match.
    # We'll coerce based on dtype of source column.
    src_dtype = df.schema[source_col]
    if src_dtype == pl.Utf8:
        fmt = fmt.with_columns(
            START=pl.col("START").cast(pl.Utf8),
            END  = pl.col("END").cast(pl.Utf8),
        )
    else:
        # numeric
        fmt = fmt.with_columns(
            START=pl.col("START").cast(pl.Float64),
            END  = pl.col("END").cast(pl.Float64),
        )
        df = df.with_columns(pl.col(source_col).cast(pl.Float64))

    # Perform non-equi join by expanding then filtering; to stay scalable, we use a cross-filter trick in DuckDB.
    # (DuckDB helps for correctness/clarity; still Parquet in/out; logic preserved.)
    con = duckdb.connect()
    con.register("df_src", df.to_arrow())
    con.register("df_fmt", fmt.to_arrow())
    res = con.execute(f"""
        SELECT s.*, f.LABEL AS {out_col}
        FROM df_src s
        JOIN df_fmt f
          ON s.{source_col} >= f.START
         AND s.{source_col} <= f.END
    """).arrow()
    con.close()
    return pl.from_arrow(res)

# =========================
# 1) REPTDATE, &REPTMON
# =========================
# SACA.REPTDATE is assumed to be a single row with column REPTDATE (date or yyyymmdd int/str)
reptdate_tbl = SACA / "REPTDATE.parquet"
reptdate_df = pl.read_parquet(reptdate_tbl)

# Coerce to date; support int YYYYMMDD or string
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
REPTYEAR = f"{REPTDATE.year:04d}"
REPTMON  = f"{REPTDATE.month:02d}"

# =========================
# 2) SA and CA (PUT with formats)
# =========================
# SA: from SACA.SAVING and ISACA.SAVING; filter OPENIND NOT IN ('B','C','P'); PRODCD=PUT(PRODUCT,SAPROD.)
saving_cols = ["ACCTNO","OPENIND","PURPOSE","PRODUCT"]
SA = (
    pl.concat([
        pl.read_parquet(SACA / "SAVING.parquet").select(saving_cols),
        pl.read_parquet(ISACA / "SAVING.parquet").select(saving_cols),
    ], how="vertical_relaxed")
    .filter(~pl.col("OPENIND").is_in(["B","C","P"]))
)
SA = apply_format_range(SA, source_col="PRODUCT", fmtname="SAPROD", out_col="PRODCD") \
        .select(["ACCTNO","PRODCD","PURPOSE","PRODUCT"])

# CA: from SACA.CURRENT and ISACA.CURRENT; same filter; PRODCD=PUT(PRODUCT,CAPROD.)
current_cols = ["ACCTNO","OPENIND","PURPOSE","PRODUCT"]
CA = (
    pl.concat([
        pl.read_parquet(SACA / "CURRENT.parquet").select(current_cols),
        pl.read_parquet(ISACA / "CURRENT.parquet").select(current_cols),
    ], how="vertical_relaxed")
    .filter(~pl.col("OPENIND").is_in(["B","C","P"]))
)
CA = apply_format_range(CA, source_col="PRODUCT", fmtname="CAPROD", out_col="PRODCD") \
        .select(["ACCTNO","PRODCD","PURPOSE","PRODUCT"])

# =========================
# 3) FD (base) and FDCD (product coding from FD libs)
# =========================
# Base FD (keep ACCTNO PURPOSE PRODUCT) from SACA.FD and ISACA.FD
fd_base_cols = ["ACCTNO","PURPOSE","PRODUCT"]
FD_base = pl.concat([
    pl.read_parquet(SACA / "FD.parquet").select(fd_base_cols),
    pl.read_parquet(ISACA / "FD.parquet").select(fd_base_cols),
], how="vertical_relaxed").sort("ACCTNO")

# FDCD: from FD.FD and IFD.FD; filters & mappings
fdcd_cols = ["ACCTNO","ACCTTYPE","OPENIND","INTPLAN"]
FDCD_union = pl.concat([
    pl.read_parquet(FDLIB  / "FD.parquet").select(fdcd_cols).with_columns(ENTITY_SRC=pl.lit("PBB")),
    pl.read_parquet(IFDLIB / "FD.parquet").select(fdcd_cols).with_columns(ENTITY_SRC=pl.lit("PIBB")),
], how="vertical_relaxed")

FDCD = (
    FDCD_union
    .filter(~pl.col("ACCTTYPE").is_in([397,398]) & pl.col("OPENIND").is_in(["D","O"]))
)
# PRODCD = PUT(INTPLAN, FDPROD.)
FDCD = apply_format_range(FDCD, source_col="INTPLAN", fmtname="FDPROD", out_col="PRODCD")

# Overrides:
FDCD = FDCD.with_columns(
    pl.when(pl.col("ACCTTYPE").is_in([315,394])).then(pl.lit("42132"))
     .when(pl.col("ACCTTYPE").is_in([397,398])).then(pl.lit("42199"))
     .otherwise(pl.col("PRODCD"))
     .alias("PRODCD")
)

# NODUPKEYS by ACCTNO — keep first occurrence
FDCD = (
    FDCD.sort(["ACCTNO"])  # SAS PROC SORT before NODUPKEYS keeps first by BY key
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
# 7) HOST.TRUST&REPTMON (KEEP=ACCTNO)  = MERGEX stacked on CLIENT
# =========================
TRUST = pl.concat([
    MERGEX.select(["ACCTNO"]),
    CLIENT.select(["ACCTNO"])
], how="vertical_relaxed")
# Note: SAS does not de-duplicate here; we keep as-is.

# =========================
# 8) HOST.FDCD&REPTMON (KEEP=ACCTNO, ENTITY) from FD.FD and IFD.FD (entity tagging)
# =========================
FD_PBB  = pl.read_parquet(FDLIB  / "FD.parquet").select(["ACCTNO"]).with_columns(ENTITY=pl.lit("PBB "))
FD_PIBB = pl.read_parquet(IFDLIB / "FD.parquet").select(["ACCTNO"]).with_columns(ENTITY=pl.lit("PIBB "))
FDCD_MONTH = pl.concat([FD_PBB, FD_PIBB], how="vertical_relaxed").select(["ACCTNO","ENTITY"])

# =========================
# 9) Write outputs (mirror of HOST library members)
# =========================
trust_path = HOST / f"TRUST{REPTMON}.parquet"
fdcd_path  = HOST / f"FDCD{REPTMON}.parquet"

TRUST.write_parquet(trust_path)
FDCD_MONTH.write_parquet(fdcd_path)

# Also build a single Arrow IPC transport (mirror of PROC CPORT)
# — pack both tables into one file (for shipping)
tables = {
    f"TRUST{REPTMON}": TRUST.to_arrow(),
    f"FDCD{REPTMON}":  FDCD_MONTH.to_arrow(),
}


print(f"Written: {trust_path}")
print(f"Written: {fdcd_path}")



all inputs are in sas7bdat sas dataset, the pgm is PBBDPFMT.py which already existed. output in sas7bdat and parquet. use datetime timedelta - 1 instead.
