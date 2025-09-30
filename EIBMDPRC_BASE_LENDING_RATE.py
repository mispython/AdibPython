# ============================================================
# EIBMDPRC_DPBR
# Translation of SAS PROC FORMAT + DATA steps + PROC REPORT
# ============================================================

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# --- Step 1: Define DEPRAN categories
def depran_range(balance: float) -> str:
    ranges = [
        (0, 50000,  " 0. BELOW RM 50,000"),
        (50000, 100000, " 1. RM 50,000 - BELOW RM100,000"),
        (100000, 200000, " 2. RM100,000 - BELOW RM200,000"),
        (200000, 300000, " 3. RM200,000 - BELOW RM300,000"),
        (300000, 400000, " 4. RM300,000 - BELOW RM400,000"),
        (400000, 500000, " 5. RM400,000 - BELOW RM500,000"),
        (500000, 600000, " 6. RM500,000 - BELOW RM600,000"),
        (600000, 700000, " 7. RM600,000 - BELOW RM700,000"),
        (700000, 800000, " 8. RM700,000 - BELOW RM800,000"),
        (800000, 900000, " 9. RM800,000 - BELOW RM900,000"),
        (900000, 1000000, "10. RM900,000 - BELOW RM1 MILL"),
        (1000000, 2000000, "11. RM1 MILL - BELOW RM2 MILL"),
        (2000000, 3000000, "12. RM2 MILL - BELOW RM3 MILL"),
        (3000000, 4000000, "13. RM3 MILL - BELOW RM4 MILL"),
        (4000000, 5000000, "14. RM4 MILL - BELOW RM5 MILL"),
        (5000000, 6000000, "15. RM5 MILL - BELOW RM6 MILL"),
        (6000000, 7000000, "16. RM6 MILL - BELOW RM7 MILL"),
        (7000000, 8000000, "17. RM7 MILL - BELOW RM8 MILL"),
        (8000000, 9000000, "18. RM8 MILL - BELOW RM9 MILL"),
        (9000000, 10000000, "19. RM9 MILL - BELOW RM10 MILL"),
    ]
    for low, high, label in ranges:
        if low <= balance < high:
            return label
    return "20. RM10 MILL & ABOVE"

# --- Step 2: Read source deposit datasets (replace with actual Arrow tables)
# Here placeholders: dep_saving, dep_current, dep_fd, cisdp_deposit, cissa_deposit
# Assume already loaded into Arrow or Parquet.

con = duckdb.connect()

# Register base tables (from Parquet or existing Arrow tables)
# con.register("dep_saving", dep_saving)
# con.register("dep_current", dep_current)
# con.register("dep_fd", dep_fd)
# con.register("cisdp_deposit", cisdp_deposit)
# con.register("cissa_deposit", cissa_deposit)

# --- Step 3: CISDP union + dedupe
con.execute("""
    CREATE OR REPLACE TABLE cisdp AS
    SELECT CUSTNO, ACCTNO FROM (
        SELECT * FROM cisdp_deposit
        UNION ALL
        SELECT * FROM cissa_deposit
    )
    GROUP BY CUSTNO, ACCTNO
""")

# --- Step 4: Filter SA / CA / FD
STFSA = (151,181,200,201,215)
STFCA = (50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,
         101,103,106,151,158,164,180,181,182)

con.execute("""
    CREATE OR REPLACE TABLE sa AS
    SELECT * FROM dep_saving WHERE PRODUCT NOT IN {stfsa}
""".format(stfsa=STFSA))

con.execute("""
    CREATE OR REPLACE TABLE ca AS
    SELECT * FROM dep_current WHERE PRODUCT NOT IN {stfca}
""".format(stfca=STFCA))

con.execute("""
    CREATE OR REPLACE TABLE fd AS
    SELECT * FROM dep_fd
    WHERE NOT (
        (INTPLAN BETWEEN 400 AND 428) OR
        (INTPLAN BETWEEN 448 AND 469) OR
        (INTPLAN BETWEEN 600 AND 639) OR
        (INTPLAN BETWEEN 720 AND 740) OR
        (INTPLAN BETWEEN 470 AND 499) OR
        (INTPLAN BETWEEN 548 AND 573)
    )
""")

# --- Step 5: Combine + filter branches, indicators
BRANCH_FILTER = (66,168)

con.execute("""
    CREATE OR REPLACE TABLE dpbr_base AS
    SELECT ACCTNO, CUSTNO, CURBAL, BRANCH AS BRCHCD
    FROM (
        SELECT * FROM sa
        UNION ALL
        SELECT * FROM ca
        UNION ALL
        SELECT * FROM fd
    )
    WHERE BRANCH IN {branches}
      AND OPENIND NOT IN ('B','C','P')
      AND USER3 IN ('3','4','6','7','8')
""".format(branches=BRANCH_FILTER))

# --- Step 6: Merge with CISDP (account link)
con.execute("""
    CREATE OR REPLACE TABLE dpbr AS
    SELECT b.*, c.CUSTNO
    FROM dpbr_base b
    LEFT JOIN cisdp c ON b.ACCTNO = c.ACCTNO
    WHERE c.ACCTNO IS NOT NULL
""")

# --- Step 7: Aggregate per branch-customer, assign DEPRAN
df = con.execute("""
    SELECT BRCHCD, CUSTNO, SUM(CURBAL) AS BALANCE, COUNT(*) AS ACCT
    FROM dpbr
    GROUP BY BRCHCD, CUSTNO
""").df()

df["DRANGE"] = df["BALANCE"].apply(depran_range)

# --- Step 8: Final summary (like PROC SUMMARY)
df_summary = (
    df.groupby(["BRCHCD","DRANGE"], as_index=False)
      .agg({"BALANCE":"sum", "ACCT":"sum"})
)

# --- Step 9: Export to Parquet
table_out = pa.Table.from_pandas(df_summary)
pq.write_table(table_out, "EIBMDPRC_DPBR.parquet")
