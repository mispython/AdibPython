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
