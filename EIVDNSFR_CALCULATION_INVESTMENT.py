# EIVDNSFR
# Purpose: NSFR Regulatory Report Consolidation

import duckdb
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import polars as pl

# 1️⃣ Load data
template = pl.read_csv("TEMPL.csv")
manual = pl.read_csv("MNL1.csv")
gl = pl.read_csv("GLPIVB.txt", separator="|")
eq = pl.read_csv("EQUA.txt", separator="|")

# 2️⃣ Define mapping (from SAS $NSFRCD.)
nsfr_mapping = {
    "S-SHARE": "0006_3",
    "S-OS SALES": "0074_1",
    "S-SSTPAY": "0076_1",
    "S-PETTY CASH": "0084_1",
    "S-STADEPBNM": "0085_3",
    "S-BNM FIX": "0152_1",
    "S-MARGIN COL": "0245_3",
    "S-DTAX": "0247_3",
    "S-O/S PUR C": "0248_1",
    "S-RCF": "0256_1",
    "OBS00100100": "0260_1",
}

# 3️⃣ Map GL to item
gl = gl.with_columns([
    pl.col("SET_ID").map_dict(nsfr_mapping).alias("GLFMT")
]).filter(pl.col("GLFMT").is_not_null())

# 4️⃣ Extract item number and bucket
gl = gl.with_columns([
    pl.col("GLFMT").str.slice(0, 4).cast(pl.Int64).alias("ITEM"),
    pl.col("GLFMT").str.slice(5, 1).cast(pl.Int64).alias("I"),
    (pl.col("AMOUNT") / 1000).alias("AMOUNT_K")
])

# 5️⃣ Pivot bucket columns dynamically
gl_pivot = gl.pivot(index="ITEM", columns="I", values="AMOUNT_K", aggregate_function="sum")

# 6️⃣ Combine all sources
lcrall = pl.concat([manual, eq, gl_pivot], how="diagonal")

# 7️⃣ Summarize totals
totlcr = lcrall.group_by("ITEM").agg([
    pl.sum("UTNMA1"),
    pl.sum("UTNMA2"),
    pl.sum("UTNMA3"),
])

# 8️⃣ Merge with template
final = template.join(totlcr, on="ITEM", how="left")

# 9️⃣ Output to CSV and Parquet
pq.write_table(final.to_arrow(), "NSFR_OUTPUT.parquet")
csv.write_csv(final.to_arrow(), "NSFR_OUTPUT.csv")

print(" EIVDNSFR_CONVERT completed successfully.")
