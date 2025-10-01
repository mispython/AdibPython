import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import polars as pl
import numpy as np

# =======================================
# JOB NAME: EIBDDPSA_SAVING_ACCOUNT_LOAD
# =======================================

# --- Connect to DuckDB (in-memory or file) ---
con = duckdb.connect(database=":memory:")

# --- Step 1: Load REPTDATE (from parquet) ---
reptdate_tbl = ds.dataset("DPTRBL_REPTDATE.parquet").to_table()
con.register("DPTRBL_REPTDATE", reptdate_tbl)
reptdate = pl.from_arrow(reptdate_tbl)

# Determine week (WK) bucket
def get_week_bucket(day: int) -> str:
    if 1 <= day <= 8:
        return "1"
    elif 9 <= day <= 15:
        return "2"
    elif 16 <= day <= 22:
        return "3"
    else:
        return "4"

reptdate = reptdate.with_columns([
    reptdate["REPTDATE"].dt.day().apply(get_week_bucket).alias("WK"),
    pl.when(reptdate["WK"] != "4")
      .then(reptdate["REPTDATE"].dt.month() - 1)
      .otherwise(reptdate["REPTDATE"].dt.month())
      .alias("MM")
])
reptdate = reptdate.with_columns([
    pl.when(reptdate["MM"] == 0).then(12).otherwise(reptdate["MM"]).alias("MM")
])

# Macro equivalents
REPTMON1 = f"{int(reptdate['MM'][0]):02d}"
REPTYEAR = reptdate["REPTDATE"][0].year
REPTMON  = f"{reptdate['REPTDATE'][0].month:02d}"
REPTDAY  = f"{reptdate['REPTDATE'][0].day:02d}"
NOWK     = reptdate["WK"][0]

# --- Step 2: CIS data ---
cis_tbl = ds.dataset("CIS_DEPOSIT.parquet").to_table()
con.register("CIS_DEPOSIT", cis_tbl)
cis = pl.from_arrow(cis_tbl)

cis = cis.filter(cis["SECCUST"] == "901").with_columns([
    cis["CITIZEN"].alias("COUNTRY"),
    cis["BIRTHDAT"].str.slice(0, 2).alias("BDD"),
    cis["BIRTHDAT"].str.slice(2, 2).alias("BMM"),
    cis["BIRTHDAT"].str.slice(4, 4).alias("BYY"),
    cis["CUSTNAM1"].alias("NAME")
])

cis = cis.with_columns([
    ((cis["BMM"] + "/" + cis["BDD"] + "/" + cis["BYY"])
        .str.strptime(pl.Date, "%m/%d/%Y", strict=False)
        .alias("DOBCIS"))
])
cis = cis.unique(subset=["ACCTNO"])

# --- Step 3: MONTHLY macro equivalent ---
if NOWK != "4" and REPTMON == "01":
    sa_tbl  = ds.dataset("DPTRBL_SAVING.parquet").to_table()
    uma_tbl = ds.dataset("DPTRBL_UMA.parquet").to_table()
else:
    crmsa_tbl = ds.dataset(f"MNICRM_SA{REPTMON1}.parquet").to_table()
    crmsa_tbl = pl.from_arrow(crmsa_tbl).unique(subset=["ACCTNO"]).drop("COSTCTR", True).to_arrow()

    sa_tbl = ds.dataset("DPTRBL_SAVING.parquet").to_table()
    con.register("sa_src", sa_tbl)
    con.register("crmsa_src", crmsa_tbl)
    sa_tbl = con.execute("""
        SELECT s.* 
        FROM sa_src s 
        INNER JOIN crmsa_src c ON s.ACCTNO = c.ACCTNO
    """).fetch_arrow_table()

    crmuma_tbl = ds.dataset(f"MNICRM_UMA{REPTMON1}.parquet").to_table()
    crmuma_tbl = pl.from_arrow(crmuma_tbl).unique(subset=["ACCTNO"]).drop("COSTCTR", True).to_arrow()

    uma_tbl = ds.dataset("DPTRBL_UMA.parquet").to_table()
    con.register("uma_src", uma_tbl)
    con.register("crmuma_src", crmuma_tbl)
    uma_tbl = con.execute("""
        SELECT u.* 
        FROM uma_src u 
        INNER JOIN crmuma_src c ON u.ACCTNO = c.ACCTNO
    """).fetch_arrow_table()

# --- Step 4: Combine SA and UMA ---
savg = pa.concat_tables([sa_tbl, uma_tbl])

savg_pl = pl.from_arrow(savg)

# Split STATCD into chars
savg_pl = savg_pl.with_columns([
    savg_pl["STATCD"].cast(pl.Utf8).str.slice(i-1, 1).alias(f"ACSTATUS{i}") for i in range(4, 11)
])

# Parse dates
date_cols = [
    ("BDATE", "%m%d%Y"),
    ("OPENDT", "%m%d%Y"),
    ("REOPENDT", "%m%d%Y"),
    ("CLOSEDT", "%m%d%Y"),
    ("LASTTRAN", "%m%d%y"),
    ("DTLSTCUST", "%m%d%y")
]
for col, fmt in date_cols:
    if col in savg_pl.columns:
        savg_pl = savg_pl.with_columns([
            savg_pl[col].cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, fmt, strict=False).alias(col)
        ])

savg_pl = savg_pl.with_columns([
    savg_pl["PURPOSE"].replace("4", "1")
])
savg = savg_pl.to_arrow()

# --- Step 5: Merge CIS + SAVG + SAACC + MISMTD ---
saacc_tbl  = ds.dataset("SIGNA_SMSACC.parquet").to_table()
mismtd_tbl = ds.dataset(f"MISMTD_SAVG{REPTMON}.parquet").to_table()

con.register("cis_tbl", cis.to_arrow())
con.register("savg_tbl", savg)
con.register("saacc_tbl", saacc_tbl)
con.register("mismtd_tbl", mismtd_tbl)

final_df = con.execute("""
    SELECT c.*, s.*, a.*, m.*
    FROM cis_tbl c
    INNER JOIN savg_tbl s ON c.ACCTNO = s.ACCTNO
    LEFT JOIN saacc_tbl a ON c.ACCTNO = a.ACCTNO
    LEFT JOIN mismtd_tbl m ON c.ACCTNO = m.ACCTNO
""").fetch_arrow_table()

# ESIGNATURE cleanup
final_pl = pl.from_arrow(final_df).with_columns([
    pl.when(pl.col("ESIGNATURE") == "").then("N").otherwise(pl.col("ESIGNATURE")).alias("ESIGNATURE")
])

# --- Save final dataset ---
final_table = final_pl.to_arrow()
out_parquet = f"SAVING_SA{REPTYEAR}{REPTMON}{REPTDAY}.parquet"
out_csv     = f"SAVING_SA{REPTYEAR}{REPTMON}{REPTDAY}.csv"

# Save Parquet
pq_writer = pa.parquet.ParquetWriter(out_parquet, final_table.schema)
pq_writer.write_table(final_table)
pq_writer.close()

# Save CSV via DuckDB
con.register("final_result", final_table)
con.execute(f"COPY final_result TO '{out_csv}' (HEADER, DELIMITER ',')")

print("✅ Job completed successfully.")
print(f"   - Parquet: {out_parquet}")
print(f"   - CSV    : {out_csv}")
