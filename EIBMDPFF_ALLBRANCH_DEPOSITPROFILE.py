import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import polars as pl

# --------------------------------------------------
# Step 1: Load DEP.REPTDATE
# --------------------------------------------------
reptdate = duckdb.sql("SELECT REPTDATE FROM dep_reptdate").to_df()
RDATE = reptdate["REPTDATE"].max().strftime("%d-%m-%Y")  # DDMMYY format

# --------------------------------------------------
# Step 2: Prepare CISDP (Customer Deposit Accounts)
# --------------------------------------------------
cisdp = duckdb.sql("""
    SELECT ACCTNO, CUSTNO
    FROM (
        SELECT ACCTNO, CUSTNO FROM cisdp_deposit
        UNION
        SELECT ACCTNO, CUSTNO FROM cissa_deposit
    )
    GROUP BY ACCTNO, CUSTNO
""").to_arrow()

# --------------------------------------------------
# Step 3: Load Savings (SA), Current (CA), FD and filter products
# --------------------------------------------------
# Product exclusion lists
STFSA = (151,181,200,201,215)
STFCA = (50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,
         101,103,106,151,158,164,180,181,182)

sa = duckdb.sql(f"""
    SELECT * FROM dep_saving
    WHERE PRODUCT NOT IN {STFSA}
""").to_arrow()

ca = duckdb.sql(f"""
    SELECT * FROM dep_current
    WHERE PRODUCT NOT IN {STFCA}
""").to_arrow()

fd = duckdb.sql("""
    SELECT * FROM dep_fd
    WHERE NOT (
        (400 <= INTPLAN AND INTPLAN <= 428) OR
        (448 <= INTPLAN AND INTPLAN <= 469) OR
        (600 <= INTPLAN AND INTPLAN <= 639) OR
        (720 <= INTPLAN AND INTPLAN <= 740) OR
        (470 <= INTPLAN AND INTPLAN <= 499) OR
        (548 <= INTPLAN AND INTPLAN <= 573)
    )
""").to_arrow()

# --------------------------------------------------
# Step 4: Combine SA, CA, FD into DPBR
# --------------------------------------------------
dpbr = duckdb.sql("""
    SELECT ACCTNO, CUSTNO, CURBAL
    FROM (
        SELECT * FROM sa
        UNION ALL
        SELECT * FROM ca
        UNION ALL
        SELECT * FROM fd
    )
    WHERE (1 < BRANCH AND BRANCH < 271)
      AND OPENIND NOT IN ('B','C','P')
""").to_arrow()

# --------------------------------------------------
# Step 5: Merge with CISDP on ACCTNO
# --------------------------------------------------
dpbr = duckdb.sql("""
    SELECT d.ACCTNO, d.CUSTNO, d.CURBAL
    FROM dpbr d
    JOIN cisdp c
      ON d.ACCTNO = c.ACCTNO
""").to_arrow()

# --------------------------------------------------
# Step 6: Aggregate balance per customer (BRCHCD = 'XX')
# --------------------------------------------------
dpbr_df = pl.from_arrow(dpbr)

dpbr_df = (
    dpbr_df
    .groupby("CUSTNO")
    .agg([
        pl.col("CURBAL").sum().alias("BALANCE")
    ])
    .with_columns([
        pl.lit("XX").alias("BRCHCD"),
        pl.lit(1).alias("ACCT")
    ])
)

# --------------------------------------------------
# Step 7: Apply DEPRAN bucket (Deposit Range)
# --------------------------------------------------
def depran_bucket(balance: float) -> str:
    if balance < 50000: return " 0. BELOW RM 50,000 "
    elif balance < 100000: return " 1. RM 50,000 - BELOW RM100,000 "
    elif balance < 200000: return " 2. RM100,000 - BELOW RM200,000 "
    elif balance < 300000: return " 3. RM200,000 - BELOW RM300,000 "
    elif balance < 400000: return " 4. RM300,000 - BELOW RM400,000 "
    elif balance < 500000: return " 5. RM400,000 - BELOW RM500,000 "
    elif balance < 600000: return " 6. RM500,000 - BELOW RM600,000 "
    elif balance < 700000: return " 7. RM600,000 - BELOW RM700,000 "
    elif balance < 800000: return " 8. RM700,000 - BELOW RM800,000 "
    elif balance < 900000: return " 9. RM800,000 - BELOW RM900,000 "
    elif balance < 1000000: return "10. RM900,000 - BELOW RM 1 MILL "
    elif balance < 2000000: return "11. RM1 MILL - BELOW RM2 MILL "
    elif balance < 3000000: return "12. RM2 MILL - BELOW RM3 MILL "
    elif balance < 4000000: return "13. RM3 MILL - BELOW RM4 MILL "
    elif balance < 5000000: return "14. RM4 MILL - BELOW RM5 MILL "
    elif balance < 6000000: return "15. RM5 MILL - BELOW RM6 MILL "
    elif balance < 7000000: return "16. RM6 MILL - BELOW RM7 MILL "
    elif balance < 8000000: return "17. RM7 MILL - BELOW RM8 MILL "
    elif balance < 9000000: return "18. RM8 MILL - BELOW RM9 MILL "
    elif balance < 10000000: return "19. RM9 MILL - BELOW RM10 MILL "
    else: return "20. RM10 MILL & ABOVE "

dpbr_df = dpbr_df.with_columns(
    dpbr_df["BALANCE"].map_elements(depran_bucket, return_dtype=pl.Utf8).alias("DRANGE")
)

# --------------------------------------------------
# Step 8: Summary report
# --------------------------------------------------
dpbr_summary = (
    dpbr_df
    .groupby(["BRCHCD", "DRANGE"])
    .agg([
        pl.col("BALANCE").sum().alias("BALANCE"),
        pl.col("ACCT").sum().alias("ACCT")
    ])
    .sort(["BRCHCD", "DRANGE"])
)

# --------------------------------------------------
# Final Output
# --------------------------------------------------
print("EIBMDPFF_ALLBRANCH_DEPOSITPROFILE - as of", RDATE)
print(dpbr_summary)
