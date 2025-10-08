import duckdb              
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
from datetime import date, timedelta

# ----------------------------
# INPUT / OUTPUT configuration
# ----------------------------
# Update filenames/paths here if different
INPUT_TEMPLATE = "TEMPL.parquet"     # template with DESC column (first row excluded in SAS)
INPUT_MANUAL   = "MNL1.parquet"      # manual NSFR data (CSV/parquet with LINE, UTNMA1..3)
INPUT_GL       = "GLPIVB.parquet"    # walker GL dataset (SET_ID, AMOUNT, SIGN)
INPUT_EQUA     = "EQUA.parquet"      # equation file (UTNREF, UTNMA1..UTNTTL etc.)

REPTDATE = date.today().replace(day=1) - timedelta(days=1)
REPTMON = f"{REPTDATE.month:02d}"
REPTDAY = f"{REPTDATE.day:02d}"
RDATE = REPTDATE.strftime("%d/%m/%Y")

OUTPUT_PARQUET = f"NSFR_OUTPUT_{REPTMON}.parquet"
OUTPUT_CSV     = f"NSFR_OUTPUT_{REPTMON}.csv"

# ----------------------------
# NSFR mapping (from SAS PROC FORMAT $NSFRCD)
# format: 'SET_ID' -> 'NNNN_I' where NNNN is ITEM code and I is bucket index (1..3)
# ----------------------------
NSFR_MAPPING = {
    # entries taken/translated from SAS $NSFRCD mapping in the original job
    "S-SHARE":            "0006_3",
    "S-OS SALES":         "0074_1",
    "S-SSTPAY":           "0076_1", "S-PBS DLRS": "0076_1", "S-REMI CA": "0076_1",
    # Several mapped to '0076_3'
    "S-LEASE ROUA":       "0076_3", "S-PROVTAX(C)": "0076_3", "S-PROVOTH": "0076_3",
    "S-ALLW COMM":        "0076_3", "S-PROVCLGFEE": "0076_3", "S-AFRECADV": "0076_3",
    "S-ACCEXP":           "0076_3", "S-SUNCRE KAP": "0076_3", "S-SUNCRE": "0076_3",
    "S-LOANCONTRO":       "0076_3", "S-PBS PAYB": "0076_3",
    "S-PETTY CASH":       "0084_1",
    "S-STADEPBNM":        "0085_3",
    "S-BNMFL 1MTH":       "0116_1", "S-PBB CUR": "0116_1", "S-PIBB CUR": "0116_1",
    "S-PB CA OTH":        "0116_1", "S-CB": "0116_1", "S-REMI FD": "0116_1",
    "S-LBFD":             "0116_1", "S-MBFL 1MTH": "0116_1", "S-LBFL 1MTH": "0116_1",
    "S-DNBFI 1MTH":       "0116_1", "S-LBFL": "0116_1",
    "LCR-FIOPSDEP":       "0140_1",
    "S-BNM FIX":          "0152_1", "S-BNM": "0152_1",
    "S-MARGIN COL":       "0245_3",
    "S-DTAX":             "0247_3", "S-IA": "0247_3",
    "S-O/S PUR C":        "0248_1", "S-CLIENT CTL": "0248_1",
    "S-RCF":              "0256_1", "S-SM F": "0256_1", "S-TLF": "0256_1",
    "OBS00100100":        "0260_1"
    # Add other mappings here if needed...
}

# Precompute reverse mapping with normalized keys (strip whitespace)
NSFR_MAP_NORMALIZED = {k.strip(): v for k, v in NSFR_MAPPING.items()}


# ----------------------------
# Helper functions
# ----------------------------
def map_setid_to_item_and_bucket(set_id: str):
    """
    Given a SET_ID string, map to (item:int, bucket_index:int) using NSFR_MAP_NORMALIZED.
    Returns (None, None) if not mapped.
    """
    if set_id is None:
        return None, None
    s = set_id.strip()
    # try direct match
    mapped = NSFR_MAP_NORMALIZED.get(s)
    if mapped is None:
        return None, None
    try:
        item_str, idx_str = mapped.split("_")
        return int(item_str), int(idx_str)
    except Exception:
        return None, None


# ----------------------------
# 1) Load template and prepare (exclude first record as in SAS IF ITEM>1)
# ----------------------------
tpl = pl.read_parquet(INPUT_TEMPLATE)

# Ensure there's a DESC column; if not, try first column rename
if "DESC" not in tpl.columns:
    # If template has a single column of description, name it DESC
    if len(tpl.columns) == 1:
        tpl = tpl.rename({tpl.columns[0]: "DESC"})
    else:
        # try common name fallbacks
        for c in tpl.columns:
            if c.lower().startswith("desc"):
                tpl = tpl.rename({c: "DESC"})
                break

tpl = tpl.with_row_count("ITEM", offset=1)  # SAS used _N_ (1-based)
# SAS code: ITEM = _N_; IF ITEM > 1; *Exclude;
tpl = tpl.filter(pl.col("ITEM") > 1)

# Ensure ITEM is integer for joins
tpl = tpl.with_columns(pl.col("ITEM").cast(pl.Int64))

# ----------------------------
# 2) Load manual NSFR (MNL1) dataset
# Expect columns: LINE (string), UTNMA1, UTNMA2, UTNMA3  (may be numeric)
# ----------------------------
mnl = pl.read_parquet(INPUT_MANUAL)

# If the file uses 'LINE' naming similar to SAS, extract ITEM from LINE (substr(6,3) in SAS)
if "LINE" in mnl.columns:
    # SAS: ITEM = SUBSTR(LINE,6,3)*1;
    mnl = mnl.with_columns(
        pl.col("LINE").str.slice(5, 3).cast(pl.Int64).alias("ITEM")  # 0-based slice; SAS 1-based
    ).drop("LINE")
elif "ITEM" not in mnl.columns:
    # fallback: assume there's an 'UTNREF' or 'ITEM' already in file; otherwise fail gracefully
    pass

# Ensure numeric UTNMA1..3 exist
for c in ("UTNMA1", "UTNMA2", "UTNMA3"):
    if c not in mnl.columns:
        mnl = mnl.with_columns(pl.lit(0).alias(c))

# Keep only ITEM and UTNMA columns
mnl = mnl.select([c for c in ["ITEM", "UTNMA1", "UTNMA2", "UTNMA3"] if c in mnl.columns])

# ----------------------------
# 3) Load Walker GL (GLPIVB)
# Expect columns: SET_ID (string), AMOUNT (numeric), SIGN (string) - adapt if different
# ----------------------------
gl = pl.read_parquet(INPUT_GL)

# Normalize column names for SET_ID and AMOUNT if different
if "SET_ID" not in gl.columns:
    # try common variants
    for c in gl.columns:
        if c.lower().startswith("set") or c.lower().startswith("set_id"):
            gl = gl.rename({c: "SET_ID"})
            break
if "AMOUNT" not in gl.columns:
    for c in gl.columns:
        if "amount" in c.lower() or "amt" in c.lower():
            gl = gl.rename({c: "AMOUNT"})
            break

# Convert amounts and place into UTNMA1/2/3 according to mapping
# We'll build a record per GL row with ITEM and UTNMA1..3
def gl_row_mapper(set_id, amount):
    item, idx = map_setid_to_item_and_bucket(set_id)
    if item is None:
        return None
    # amount in SAS was divided by 1000 (they used AMOUNT/1000). Keep same behavior:
    val = amount / 1000.0 if amount is not None else 0.0
    u1 = val if idx == 1 else 0.0
    u2 = val if idx == 2 else 0.0
    u3 = val if idx == 3 else 0.0
    return (item, u1, u2, u3)

# Map GL rows
gl_rows = []
for row in gl.iter_rows(named=True):
    mapped = gl_row_mapper(row.get("SET_ID"), row.get("AMOUNT"))
    if mapped is not None:
        gl_rows.append(mapped)

if len(gl_rows) > 0:
    gl_df = pl.DataFrame(gl_rows, schema=["ITEM", "UTNMA1", "UTNMA2", "UTNMA3"])
else:
    # empty fallback
    gl_df = pl.DataFrame({"ITEM": pl.Series([], dtype=pl.Int64),
                          "UTNMA1": pl.Series([], dtype=pl.Float64),
                          "UTNMA2": pl.Series([], dtype=pl.Float64),
                          "UTNMA3": pl.Series([], dtype=pl.Float64)})

# ----------------------------
# 4) Load Equation (EQUA) file
# Expect columns: UTNREF, UTNMA1..UTNMA5, UTNTTL
# SAS: ITEM = SUBSTR(UTNREF,3,5)*1;
# ----------------------------
eq = pl.read_parquet(INPUT_EQUA)

# Normalize column names if needed
if "UTNREF" not in eq.columns:
    for c in eq.columns:
        if "ref" in c.lower():
            eq = eq.rename({c: "UTNREF"})
            break

# Create ITEM from UTNREF
if "UTNREF" in eq.columns:
    # SAS used SUBSTR(UTNREF,3,5) (1-based) -> Python slice starting at index 2 length 5
    eq = eq.with_columns(
        pl.col("UTNREF").str.slice(2, 5).cast(pl.Int64).alias("ITEM")
    )

# Ensure UTNMA1..3 columns exist, else 0
for c in ("UTNMA1", "UTNMA2", "UTNMA3"):
    if c not in eq.columns:
        eq = eq.with_columns(pl.lit(0.0).alias(c))

eq_subset = eq.select(["ITEM", "UTNMA1", "UTNMA2", "UTNMA3"])

# ----------------------------
# 5) Consolidate (GL + EQU + MANUAL)
# ----------------------------
# Standardize types
for df in (gl_df, eq_subset, mnl):
    if "ITEM" in df.columns:
        df = df.with_columns(pl.col("ITEM").cast(pl.Int64))

# Append datasets vertically
consolidated = pl.concat([gl_df, eq_subset, mnl], how="vertical", rechunk=True)

# Keep only valid ITEM (>0)
consolidated = consolidated.filter(pl.col("ITEM") > 0)

# Fill nulls with zeros
consolidated = consolidated.fill_null(0.0)

# ----------------------------
# 6) Summarize by ITEM
# ----------------------------
agg = consolidated.groupby("ITEM").agg([
    pl.col("UTNMA1").sum().alias("UTNMA1"),
    pl.col("UTNMA2").sum().alias("UTNMA2"),
    pl.col("UTNMA3").sum().alias("UTNMA3")
])

# ----------------------------
# 7) Join to template and post-process
# ----------------------------
final = tpl.join(agg, on="ITEM", how="left")

# Fill NAs with zero
final = final.fill_null(0.0)

# Make absolute values (SAS used ABS)
final = final.with_columns([
    pl.col("UTNMA1").abs().alias("UTNMA1"),
    pl.col("UTNMA2").abs().alias("UTNMA2"),
    pl.col("UTNMA3").abs().alias("UTNMA3")
])

# Reorder columns for readability: ITEM, DESC, UTNMA1..3 and any others
cols = final.columns
desired_order = ["ITEM", "DESC", "UTNMA1", "UTNMA2", "UTNMA3"]
# keep remaining columns after the requested ones
other_cols = [c for c in cols if c not in desired_order]
final = final.select([c for c in desired_order if c in final.columns] + other_cols)

# ----------------------------
# 8) Export using PyArrow (Parquet + CSV)
# ----------------------------
# Convert to Arrow Table (without pandas)
arrow_table = final.to_arrow()

# Write parquet
pq.write_table(arrow_table, OUTPUT_PARQUET)

# Write CSV via pyarrow.csv
# pyarrow.csv.write_csv expects a Table
pacsv.write_csv(arrow_table, OUTPUT_CSV)

print(f"NSFR consolidation finished for reporting month {REPTMON}.")
print(f"Outputs: {OUTPUT_PARQUET}, {OUTPUT_CSV}")
