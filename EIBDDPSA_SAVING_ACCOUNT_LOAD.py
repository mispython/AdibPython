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

# --- Step 1: Load REPTDATE ---
reptdate_tbl = con.execute("SELECT * FROM DPTRBL_REPTDATE").fetch_arrow_table()
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
cis_tbl = con.execute("SELECT * FROM CIS_DEPOSIT").fetch_arrow_table()
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
    sa_tbl  = con.execute("SELECT *, 0 AS FEEYTD FROM DPTRBL_SAVING").fetch_arrow_table()
    uma_tbl = con.execute("SELECT *, 0 AS FEEYTD FROM DPTRBL_UMA").fetch_arrow_table()
else:
    crmsa_tbl = con.execute(f"SELECT * FROM MNICRM_SA{REPTMON1}").fetch_arrow_table()
    crmsa_tbl = pl.from_arrow(crmsa_tbl).unique(subset=["ACCTNO"]).drop("COSTCTR", True).to_arrow()

    sa_tbl = con.execute("SELECT * FROM DPTRBL_SAVING").fetch_arrow_table()
    sa_tbl = con.execute("SELECT s.* FROM sa_tbl s INNER JOIN crmsa_tbl c ON s.ACCTNO = c.ACCTNO").fetch_arrow_table()

    crmuma_tbl = con.execute(f"SELECT * FROM MNICRM_UMA{REPTMON1}").fetch_arrow_table()
    crmuma_tbl = pl.from_arrow(crmuma_tbl).unique(subset=["ACCTNO"]).drop("COSTCTR", True).to_arrow()

    uma_tbl = con.execute("SELECT * FROM DPTRBL_UMA").fetch_arrow_table()
    uma_tbl = con.execute("SELECT u.* FROM uma_tbl u INNER JOIN crmuma_tbl c ON u.ACCTNO = c.ACCTNO").fetch_arrow_table()

# --- Step 4: Combine SA and UMA ---
savg = pa.concat_tables([sa_tbl, uma_tbl])

# Use Polars only for STATCD split + date parsing
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
saacc_tbl  = con.execute("SELECT * FROM SIGNA_SMSACC").fetch_arrow_table()
mismtd_tbl = con.execute(f"SELECT * FROM MISMTD_SAVG{REPTMON}").fetch_arrow_table()

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
con.register("final_result", final_pl.to_arrow())
con.execute(f"""
    CREATE TABLE SAVING_SA{REPTYEAR}{REPTMON}{REPTDAY} AS 
    SELECT * FROM final_result
""")
