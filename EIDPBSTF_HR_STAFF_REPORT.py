# ===============================================================
#  EIDPBSTF_CONVERTED.py
#  Converted from SAS EIDPBSTF
#  Tools: DuckDB + Polars + PyArrow
#  Purpose: Merge HR, CIS, and account/product datasets for reporting
# ===============================================================

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
from datetime import datetime

# ---------------------------------------------------------------
# STEP 1: Load REPTDATE to derive reporting month
# ---------------------------------------------------------------
con = duckdb.connect()

# Assume DP_REPTDATE is a Parquet or CSV source
reptdate_df = pl.read_parquet("DP_REPTDATE.parquet")
reptmon = reptdate_df["REPTDATE"][0].month
reptmon_str = f"{reptmon:02d}"

# ---------------------------------------------------------------
# STEP 2: Load and prepare STAFF alias list
# ---------------------------------------------------------------
staff = pl.read_csv("HCMS.csv", has_header=False, new_columns=["ALIAS"])

# Sort
staff = staff.sort("ALIAS")

# ---------------------------------------------------------------
# STEP 3: Load CIS.CUSTDLY and filter PRISEC=901
# ---------------------------------------------------------------
cis = (
    pl.read_parquet("CIS_CUSTDLY.parquet")
    .filter(pl.col("PRISEC") == 901)
    .select(["CUSTNAME", "CUSTNO", "ACCTNO", "PRISEC", "ALIAS", "ACCTCODE"])
    .sort("ALIAS")
)

# ---------------------------------------------------------------
# STEP 4: Merge STAFF + CIS
# ---------------------------------------------------------------
cishr = (
    staff.join(cis, on="ALIAS", how="left")
    .with_columns([
        pl.when(pl.col("CUSTNO").is_not_null()).then(1).otherwise(0).alias("TAGC")
    ])
)

# ---------------------------------------------------------------
# STEP 5: CARD & ACCT
# ---------------------------------------------------------------
card = pl.read_csv(
    "CARD.csv",
    new_columns=["ACCTNO", "CANCODE", "CARDNO"],
    skip_rows=0,
)
acct = pl.read_csv(
    "ACCT.csv",
    new_columns=["ACCTNO", "CLOSECD", "INTCODE"],
)

card_merged = (
    card.join(acct, on="ACCTNO", how="left")
    .with_columns([
        pl.lit("CC").alias("APPLCODE")
    ])
    .rename({"CARDNO": "ACCTNO"})
    .drop("ACCTNO_right")
)

# ---------------------------------------------------------------
# STEP 6: Fixed Deposit (FD)
# ---------------------------------------------------------------
fd = (
    pl.concat([pl.read_parquet("FD_FD.parquet"), pl.read_parquet("IFD_FD.parquet")])
    .filter(
        (pl.col("OPENIND").is_in(["B", "C", "P"]).not_())
        & (pl.col("INTPLAN").is_between(400, 411)
           | pl.col("INTPLAN").is_between(448, 459)
           | pl.col("INTPLAN").is_between(461, 469)
           | pl.col("INTPLAN").is_between(640, 658))
    )
    .with_columns(pl.lit("DP").alias("APPLCODE"))
    .select(["ACCTNO", "ACCTTYPE", "APPLCODE"])
    .rename({"ACCTTYPE": "PRODUCT"})
    .unique(subset=["ACCTNO"])
)

# ---------------------------------------------------------------
# STEP 7: Deposit (DP)
# ---------------------------------------------------------------
dp = (
    pl.concat([
        pl.read_parquet("DP_SAVING.parquet"),
        pl.read_parquet("IDP_SAVING.parquet"),
        pl.read_parquet("DP_CURRENT.parquet"),
        pl.read_parquet("IDP_CURRENT.parquet")
    ])
    .filter(
        (pl.col("OPENIND").is_in(["B", "C", "P"]).not_())
        & (pl.col("PRODUCT").is_in([201, 215, 106, 97, 164, 481, 151, 158]))
    )
    .with_columns(pl.lit("DP").alias("APPLCODE"))
    .select(["ACCTNO", "PRODUCT", "APPLCODE"])
)

# ---------------------------------------------------------------
# STEP 8: Loan (LN)
# ---------------------------------------------------------------
ln = (
    pl.concat([pl.read_parquet("LN_LNNOTE.parquet"), pl.read_parquet("ILN_LNNOTE.parquet")])
    .filter(
        (pl.col("PAIDIND").is_in(["P", "C"]).not_())
        & (pl.col("LOANTYPE").is_in([
            4,5,6,7,15,20,25,26,27,28,29,30,31,32,33,34,60,61,
            100,101,102,103,104,105
        ]))
    )
    .with_columns(pl.lit("LN").alias("APPLCODE"))
    .select(["ACCTNO", "NOTENO", "LOANTYPE", "APPLCODE"])
    .rename({"LOANTYPE": "PRODUCT"})
)

# ---------------------------------------------------------------
# STEP 9: Combine all products
# ---------------------------------------------------------------
allprod = pl.concat([card_merged, dp, fd, ln]).sort("ACCTNO")

# ---------------------------------------------------------------
# STEP 10: Merge HRD STAFF & Product data
# ---------------------------------------------------------------
hrd_staff = (
    cishr.join(allprod, on="ACCTNO", how="left")
    .with_columns([
        pl.when(pl.col("APPLCODE").is_not_null()).then(1).otherwise(0).alias("TAGA")
    ])
)

# ---------------------------------------------------------------
# STEP 11: Final filter and output
# ---------------------------------------------------------------
final = (
    hrd_staff.filter(
        (pl.col("TAGC") == 1)
        & (
            (pl.col("TAGA") == 1)
            | ((pl.col("APPLCODE") == "CC") & (pl.col("CANCODE") == ""))
        )
    )
    .with_columns([
        pl.when(pl.col("APPLCODE").is_in(["DP", "LN"]))
          .then(pl.col("PRODUCT").cast(pl.Utf8).str.zfill(3))
          .otherwise(pl.col("ACCTCODE"))
          .alias("PRODCODE")
    ])
    .select(["ALIAS", "APPLCODE", "ACCTNO", "NOTENO", "PRODCODE"])
)

# ---------------------------------------------------------------
# STEP 12: Save Output (PyArrow)
# ---------------------------------------------------------------
arrow_table = final.to_arrow()

# Output to both CSV and Parquet
pq.write_table(arrow_table, f"OUTPUT_STAFF_{reptmon_str}.parquet")
csv.write_csv(arrow_table, f"OUTPUT_STAFF_{reptmon_str}.csv")

print(f"✅ Successfully generated OUTPUT_STAFF_{reptmon_str}.parquet and .csv")
