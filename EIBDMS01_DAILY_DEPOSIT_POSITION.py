import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl
from datetime import datetime
from typing import Dict

# ---------- Configuration: update the parquet file paths ----------
INPUT_PATHS = {
    "MNITB_REPTDATE": "MNITB/REPTDATE.parquet",
    "MNITB_FD":       "MNITB/FD.parquet",
    "MNIIB_FD":       "MNIIB/FD.parquet",
    "MNITB_SAVING":   "MNITB/SAVING.parquet",
    "MNIIB_SAVING":   "MNIIB/SAVING.parquet",
    "MNITB_CURRENT":  "MNITB/CURRENT.parquet",
    "MNIIB_CURRENT":  "MNIIB/CURRENT.parquet",
    # optional: product mapping tables if you have them as parquet
    # "FDPROD_MAP": "LOOKUPS/FDPROD.parquet",
    # "SAPROD_MAP": "LOOKUPS/SAPROD.parquet",
    # "CAPROD_MAP": "LOOKUPS/CAPROD.parquet",
}

# ---------- Placeholder mappings (SAS formats) ----------
# You MUST replace these with your authoritative mappings from SAS formats.
# For example: FDPROD maps INTPLAN -> PRODCD (string like '42630' etc.)
# Example values here are illustrative only.
FDPROD_MAP: Dict[int, str] = {
    # intplan : product_code
    # e.g. 340: '42610', ...
}
SAPROD_MAP: Dict[int, str] = {
    # product -> SAP product code mapping
}
CAPROD_MAP: Dict[int, str] = {
    # product -> CAPROC mapping
}

# ---------- Connect to DuckDB ----------
con = duckdb.connect(database=":memory:")

# ---------- Helper functions ----------
def read_parquet_table(path: str) -> pa.Table:
    """Read parquet using DuckDB (fast) returning a PyArrow table."""
    # using duckdb's read_parquet for consistent schema handling
    q = f"SELECT * FROM read_parquet('{path}')"
    return con.execute(q).arrow()

def pa_to_pl(pa_table: pa.Table) -> pl.DataFrame:
    """Convert PyArrow Table to Polars DataFrame."""
    return pl.from_arrow(pa_table)

def pl_to_pa(df: pl.DataFrame) -> pa.Table:
    """Convert Polars DataFrame to PyArrow Table."""
    return df.to_arrow()

def map_intplan_to_prodcd(intplan_series: pl.Series, mapping: Dict[int, str], default: str = "") -> pl.Series:
    """Map integer intplan codes to product code strings via mapping dict."""
    if not mapping:
        # If mapping not provided, return empty string column
        return pl.Series("", dtype=pl.Utf8, length=len(intplan_series))
    # polars map:
    mapping_items = mapping.items()
    # build expression (faster than apply)
    # Create a mapping DataFrame and join would be more robust
    mp_df = pl.DataFrame({"INTPLAN_KEY": list(mapping.keys()), "PRODCD_VAL": list(mapping.values())})
    tmp = pl.DataFrame({"INTPLAN": intplan_series})
    tmp = tmp.join(mp_df.with_columns(pl.col("INTPLAN_KEY").cast(intplan_series.dtype)), left_on="INTPLAN", right_on="INTPLAN_KEY", how="left")
    return tmp["PRODCD_VAL"].fill_null(default)

# ---------- 1. Read REPTDATE and derive macro variables ----------
reptdate_pa = read_parquet_table(INPUT_PATHS["MNITB_REPTDATE"])
# assume there's at least one row; take first REPTDATE
reptdate_pl = pa_to_pl(reptdate_pa)
if "REPTDATE" not in reptdate_pl.columns:
    raise RuntimeError("MNITB.REPTDATE parquet does not contain REPTDATE column")
reptdate_val = reptdate_pl.select(pl.col("REPTDATE")).to_series()[0]
# ensure it's a python date
if isinstance(reptdate_val, (str, bytes)):
    # try parse
    reptdate_dt = datetime.fromisoformat(reptdate_val).date()
else:
    try:
        reptdate_dt = reptdate_val
        if isinstance(reptdate_dt, pl.Date) or isinstance(reptdate_dt, pl.Datetime):
            # polars type; convert
            reptdate_dt = pl.Series([reptdate_dt]).to_list()[0]
    except Exception:
        reptdate_dt = reptdate_val

if isinstance(reptdate_dt, datetime):
    reptdate_dt = reptdate_dt.date()

REPTYEAR = f"{reptdate_dt.year}"
REPTMON  = f"{reptdate_dt.month:02d}"
REPTDAY  = f"{reptdate_dt.day:02d}"
# RDATE numeric form in SAS N5: replicate as integer YYYYMMDD? SAS Z5 used earlier — keep string date for reuse
RDATE = reptdate_dt.strftime("%d%m%Y")  # matches DDMMYYYY used in some SAS calls

# ---------- 2. Load datasets (as Polars DataFrames for transformations) ----------
fd_mnitb_pa  = read_parquet_table(INPUT_PATHS["MNITB_FD"])
fd_mniib_pa  = read_parquet_table(INPUT_PATHS["MNIIB_FD"])
sav_mnitb_pa = read_parquet_table(INPUT_PATHS["MNITB_SAVING"])
sav_mniib_pa = read_parquet_table(INPUT_PATHS["MNIIB_SAVING"])
cur_mnitb_pa = read_parquet_table(INPUT_PATHS["MNITB_CURRENT"])
cur_mniib_pa = read_parquet_table(INPUT_PATHS["MNIIB_CURRENT"])

fd_mnitb  = pa_to_pl(fd_mnitb_pa)
fd_mniib  = pa_to_pl(fd_mniib_pa)
sav_mnitb = pa_to_pl(sav_mnitb_pa)
sav_mniib = pa_to_pl(sav_mniib_pa)
cur_mnitb = pa_to_pl(cur_mnitb_pa)
cur_mniib = pa_to_pl(cur_mniib_pa)

# ---------- 3. Apply FD (Fixed deposit) logic ----------
# Create FD (MNITB) filtered dataset (like SAS FD dataset)
# From SAS: SET MNITB.FD; REPTDATE=&RDATE; IF OPENIND NOT IN ('B','C','P'); PRODCD=PUT(INTPLAN,FDPROD.); IF PRODCD EQ '42630' THEN DELETE; IF CURBAL GE 0; TOTFD=CURBAL;
fd = fd_mnitb.lazy()
fd = fd.filter(~pl.col("OPENIND").is_in(["B", "C", "P"]))
fd = fd.filter(pl.col("CURBAL") >= 0)
# apply PRODCD mapping via FDPROD_MAP if exists; otherwise produce blank
if FDPROD_MAP:
    fd = fd.with_columns(map_intplan_to_prodcd(pl.col("INTPLAN"), FDPROD_MAP).alias("PRODCD"))
else:
    fd = fd.with_columns(pl.lit("").alias("PRODCD"))
# drop PRODCD '42630' if that maps
fd = fd.filter(pl.col("PRODCD") != "42630")
# add REPTDATE and TOTFD
fd = fd.with_columns([
    pl.lit(reptdate_dt).alias("REPTDATE"),
    pl.col("CURBAL").alias("TOTFD")
])
fd = fd.collect()

# FD interbranch (MNIIB) -> FDI
fdi = fd_mniib.lazy()
fdi = fdi.filter(~pl.col("OPENIND").is_in(["B", "C", "P"]))
fdi = fdi.filter(pl.col("CURBAL") >= 0)
if FDPROD_MAP:
    fdi = fdi.with_columns(map_intplan_to_prodcd(pl.col("INTPLAN"), FDPROD_MAP).alias("PRODCD"))
else:
    fdi = fdi.with_columns(pl.lit("").alias("PRODCD"))
fdi = fdi.filter(pl.col("PRODCD") != "42630")
fdi = fdi.with_columns([
    pl.lit(reptdate_dt).alias("REPTDATE"),
    pl.col("CURBAL").alias("TOTFDI")
])
fdi = fdi.collect()

# ---------- 4. Savings datasets ----------
savg = sav_mnitb.lazy()
savg = savg.filter(~pl.col("OPENIND").is_in(["B", "C", "P"]))
# MAP PRODUCT -> SAPROD (placeholder)
if SAPROD_MAP:
    savg = savg.with_columns(map_intplan_to_prodcd(pl.col("PRODUCT"), SAPROD_MAP).alias("PRODCD"))
else:
    savg = savg.with_columns(pl.lit("").alias("PRODCD"))
savg = savg.filter(pl.col("CURBAL") >= 0)
savg = savg.with_columns([
    pl.lit(reptdate_dt).alias("REPTDATE"),
    pl.col("CURBAL").alias("TOTSAVG")
])
savg = savg.collect()

savgi = sav_mniib.lazy()
savgi = savgi.filter(~pl.col("OPENIND").is_in(["B", "C", "P"]))
if SAPROD_MAP:
    savgi = savgi.with_columns(map_intplan_to_prodcd(pl.col("PRODUCT"), SAPROD_MAP).alias("PRODCD"))
else:
    savgi = savgi.with_columns(pl.lit("").alias("PRODCD"))
savgi = savgi.filter(pl.col("CURBAL") >= 0)
# additional TOTMBSA for product 214
savgi = savgi.with_columns([
    pl.lit(reptdate_dt).alias("REPTDATE"),
    pl.col("CURBAL").alias("TOTSAVGI"),
    pl.when(pl.col("PRODUCT") == 214).then(pl.col("CURBAL")).otherwise(0).alias("TOTMBSA")
])
savgi = savgi.collect()

# ---------- 5. Current accounts (MNITB) ----------
# This is the most involved part (ACE logic)
cur = cur_mnitb.lazy()
cur = cur.filter(~pl.col("OPENIND").is_in(["B", "C", "P"]))
# CAPROD mapping from PRODUCT -> PRODCD string
if CAPROD_MAP:
    cur = cur.with_columns(map_intplan_to_prodcd(pl.col("PRODUCT"), CAPROD_MAP).alias("PRODCD"))
else:
    cur = cur.with_columns(pl.lit("").alias("PRODCD"))

cur = cur.filter(pl.col("CURBAL") >= 0)

# Create necessary aggregated measure columns
# default zero columns
cur = cur.with_columns([
    pl.lit(reptdate_dt).alias("REPTDATE"),
    pl.lit(0.0).alias("TOTVOSC"),
    pl.lit(0.0).alias("TOTVOSF"),
    pl.lit(0.0).alias("TOTDMND"),
    pl.lit(0.0).alias("TOTSAV1"),
    pl.lit(0.0).alias("ACESA"),
    pl.lit(0.0).alias("ACECA"),
])

# Apply SAS ACE logic:
# IF PRODUCT=104 THEN TOTVOSC=CURBAL;
# IF PRODUCT=105 THEN TOTVOSF=CURBAL;
# IF PRODUCT NOT IN &ACE THEN TOTDMND=CURBAL; (ACE list not provided; we'll implement generic logic for products needing ACE)
# IF PRODUCT IN (150,151,152,181) THEN ...
# IF PRODUCT=177 THEN ...
ACE_PRODUCTS = {150, 151, 152, 181}  # as in SAS
def cur_logic(df: pl.DataFrame) -> pl.DataFrame:
    # column by column implement logic
    df = df.with_columns([
        pl.when(pl.col("PRODUCT") == 104).then(pl.col("CURBAL")).otherwise(pl.col("TOTVOSC")).alias("TOTVOSC"),
        pl.when(pl.col("PRODUCT") == 105).then(pl.col("CURBAL")).otherwise(pl.col("TOTVOSF")).alias("TOTVOSF"),
    ])
    # default TOTDMND: if product not in ACE (ACE not defined explicitly in SAS extract, they referenced &ACE macro),
    # We'll treat &ACE as empty set here — in SAS they used a macro &ACE; if you have that list, set ACE_SET variable.
    ACE_SET = set()  # TODO: populate with actual ACE product list if exists
    df = df.with_columns([
        pl.when(~pl.col("PRODUCT").is_in(ACE_SET)).then(pl.col("CURBAL")).otherwise(pl.col("TOTDMND")).alias("TOTDMND")
    ])
    # Handle special ACE capping logic for products in ACE_PRODUCTS
    df = df.with_columns([
        pl.when(pl.col("PRODUCT").is_in(ACE_PRODUCTS) & (pl.col("CURBAL") > 5000))
          .then(pl.col("CURBAL") - 5000)
          .otherwise(pl.col("TOTSAV1"))
          .alias("TOTSAV1"),
        pl.when(pl.col("PRODUCT").is_in(ACE_PRODUCTS) & (pl.col("CURBAL") > 5000))
          .then(5000)
          .otherwise(pl.col("ACECA"))
          .alias("ACECA"),
        pl.when(pl.col("PRODUCT").is_in(ACE_PRODUCTS) & (pl.col("CURBAL") > 5000))
          .then(5000)
          .otherwise(pl.col("TOTDMND"))
          .alias("TOTDMND"),
    ])
    # Product 177 special
    df = df.with_columns([
        pl.when(pl.col("PRODUCT") == 177 & (pl.col("CURBAL") > 5000))
          .then(pl.col("CURBAL") - 5000)
          .otherwise(pl.col("TOTSAV1"))
          .alias("TOTSAV1"),
        pl.when(pl.col("PRODUCT") == 177 & (pl.col("CURBAL") > 5000))
          .then(5000)
          .otherwise(pl.col("TOTDMND"))
          .alias("TOTDMND"),
    ])
    return df

cur = cur.collect()
cur = cur_logic(cur)

# ---------- 6. Current (MNIIB) secondary aggregated TOTDMNDI ----------
cur_i = cur_mniib.lazy()
cur_i = cur_i.filter(~pl.col("OPENIND").is_in(["B", "C", "P"]))
if CAPROD_MAP:
    cur_i = cur_i.with_columns(map_intplan_to_prodcd(pl.col("PRODUCT"), CAPROD_MAP).alias("PRODCD"))
else:
    cur_i = cur_i.with_columns(pl.lit("").alias("PRODCD"))
cur_i = cur_i.filter(pl.col("CURBAL") >= 0)
cur_i = cur_i.with_columns([
    pl.lit(reptdate_dt).alias("REPTDATE"),
    pl.col("CURBAL").alias("TOTDMNDI")
])
cur_i = cur_i.collect()

# ---------- 7. Aggregations: group by REPTDATE and sum required columns ----------
# FDC: sum TOTFD by REPTDATE
fdf = fd.groupby("REPTDATE").agg(pl.col("TOTFD").sum().alias("TOTFD"))
# FDI
fdif = fdi.groupby("REPTDATE").agg(pl.col("TOTFDI").sum().alias("TOTFDI"))

# SAVG (MNITB SAVG)
sac = savg.groupby("REPTDATE").agg(pl.col("TOTSAVG").sum().alias("TOTSAVG"))

# SAVGI
sai = savgi.groupby("REPTDATE").agg([
    pl.col("TOTSAVGI").sum().alias("TOTSAVGI"),
    pl.col("TOTMBSA").sum().alias("TOTMBSA")
])

# CURRENT aggregations CAC: sum TOTVOSC TOTVOSF TOTDMND TOTSAV1 ACESA ACECA
cac = cur.groupby("REPTDATE").agg([
    pl.col("TOTVOSC").sum().alias("TOTVOSC"),
    pl.col("TOTVOSF").sum().alias("TOTVOSF"),
    pl.col("TOTDMND").sum().alias("TOTDMND"),
    pl.col("TOTSAV1").sum().alias("TOTSAV1"),
    pl.col("ACESA").sum().alias("ACESA"),
    pl.col("ACECA").sum().alias("ACECA"),
])

# CAI: TOTDMNDI
cai = cur_i.groupby("REPTDATE").agg(pl.col("TOTDMNDI").sum().alias("TOTDMNDI"))

# ---------- 8. Merge all summary tables into DYPOSN-like dataframe ----------
# Start from REPTDATE value as single-row key
base = pl.DataFrame({
    "REPTDATE": [reptdate_dt]
})

# left join sequentially, fill missing with 0
def left_join_fill_zero(df_left: pl.DataFrame, df_right: pl.DataFrame) -> pl.DataFrame:
    if df_right.height == 0:
        return df_left
    merged = df_left.join(df_right, on="REPTDATE", how="left")
    # fill nulls with 0 for numeric columns
    for c in merged.columns:
        if c == "REPTDATE":
            continue
        if merged[c].dtype in (pl.Float64, pl.Int64, pl.Float32, pl.Int32):
            merged = merged.with_columns(pl.col(c).fill_null(0))
    return merged

dyp = base
for tbl in [fdf, fdif, sac, sai, cai, cac]:
    dyp = left_join_fill_zero(dyp, tbl)

# dyp now contains combined aggregated columns
# Ensure columns names consistent with SAS output names if needed
# e.g. TOTFD, TOTFDI, TOTSAVG, TOTSAVGI, TOTMBSA, TOTDMNDI, TOTVOSC, TOTVOSF, TOTDMND, TOTSAV1, ACESA, ACECA

# ---------- 9. Append to MIS monthly dataset logic ----------
# In SAS they `PROC APPEND` to MIS.DYPOSN&REPTMON
# Here we write the result to local files and optionally to a MIS folder by month

out_parquet = f"MIS_DYPOSN_{REPTMON}.parquet"
out_csv     = f"MIS_DYPOSN_{REPTMON}.csv"

# Convert polars df to pyarrow and write parquet with pyarrow (as requested)
dyp_pa = pl_to_pa(dyp)
pq.write_table(dyp_pa, out_parquet)

# Write CSV using Polars (fast)
dyp.write_csv(out_csv)

print(f"Output written to: {out_parquet} and {out_csv}")

# ---------- 10. (Optional) append to existing MIS.DYPOSN<REPTMON> parquet if exists ----------
# If you want to append into an existing monthly parquet file, you can:
# - read existing parquet into DataFrame, concatenate, deduplicate and overwrite the file.
# This is left commented; enable if you require append semantics.

# import os
# if os.path.exists(out_parquet):
#     existing = pl.read_parquet(out_parquet)
#     combined = pl.concat([existing, dyp], how="vertical").unique(subset=["REPTDATE"])
#     pq.write_table(pl_to_pa(combined), out_parquet)
# else:
#     pq.write_table(dyp_pa, out_parquet)

# End of script
