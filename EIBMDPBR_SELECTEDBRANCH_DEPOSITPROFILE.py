import duckdb
import pyarrow as pa
import pyarrow.compute as pc

# ==============================
# EIBMDPBR_SELECTEDBRANCH_DEPOSITPROFILE
# ==============================

# --- Branch, staff, and filtering sets ---
BRH = {13,15,28,33,54,60,66,68,90,94,110,123,129,130,135,136,
       145,153,157,168,78,79,179,183,185,187,198,200,207,216}

STFSA = {151,181,200,201,215}
STFCA = {50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,
         101,103,106,151,158,164,180,181,182}

# --- Load source tables (assuming Arrow tables) ---
# Replace with actual readers (CSV/Parquet/etc.)
cisdp_deposit = pa.table({})
cissa_deposit = pa.table({})
dep_saving    = pa.table({})
dep_current   = pa.table({})
dep_fd        = pa.table({})

# --- Combine CISDP sources, dedupe ---
cisdp = pa.concat_tables([cisdp_deposit, cissa_deposit])
cisdp = duckdb.query("SELECT DISTINCT CUSTNO, ACCTNO FROM cisdp").to_arrow_table()

# --- SA: saving accounts excluding staff ---
sa = duckdb.query("""
    SELECT * FROM dep_saving
    WHERE PRODUCT NOT IN (SELECT UNNEST(list_value) FROM (VALUES (list_value)) 
                          CROSS JOIN UNNEST(?::INT[]) AS list_value)
""", [list(STFSA)]).to_arrow_table()

# --- CA: current accounts excluding staff ---
ca = duckdb.query("""
    SELECT * FROM dep_current
    WHERE PRODUCT NOT IN (SELECT UNNEST(list_value) FROM (VALUES (list_value)) 
                          CROSS JOIN UNNEST(?::INT[]) AS list_value)
""", [list(STFCA)]).to_arrow_table()

# --- FD: filter out certain INTPLAN ranges ---
fd = duckdb.query("""
    SELECT * FROM dep_fd
    WHERE NOT (
        (400 <= INTPLAN AND INTPLAN <= 428) OR
        (448 <= INTPLAN AND INTPLAN <= 469) OR
        (600 <= INTPLAN AND INTPLAN <= 639) OR
        (720 <= INTPLAN AND INTPLAN <= 740) OR
        (470 <= INTPLAN AND INTPLAN <= 499) OR
        (548 <= INTPLAN AND INTPLAN <= 573)
    )
""").to_arrow_table()

# --- Combine DPBR base set ---
dpbr = pa.concat_tables([sa, ca, fd])
dpbr = duckdb.query("""
    SELECT ACCTNO, CUSTNO, CURBAL, BRANCH
    FROM dpbr
    WHERE BRANCH IN (SELECT UNNEST(?::INT[]))
      AND OPENIND NOT IN ('B','C','P')
""", [list(BRH)]).to_arrow_table()

# --- Map BRANCH -> BRCHCD (using existing format mapping table if available)
# For now assume BRCHCD is just string version of BRANCH
dpbr = dpbr.append_column("BRCHCD", pc.cast(dpbr["BRANCH"], pa.string()))

# --- Merge CISDP ---
dpbr = duckdb.query("""
    SELECT d.ACCTNO, d.CUSTNO, d.CURBAL, d.BRCHCD
    FROM dpbr d
    INNER JOIN cisdp c ON d.ACCTNO = c.ACCTNO
""").to_arrow_table()

# --- Aggregate BALANCE per BRCHCD + CUSTNO ---
dpbr_bal = duckdb.query("""
    SELECT BRCHCD, CUSTNO, SUM(CURBAL) AS BALANCE
    FROM dpbr
    GROUP BY BRCHCD, CUSTNO
""").to_arrow_table()

# --- Define deposit ranges (DEPRAN) ---
def map_depran(balance: float) -> str:
    if balance < 50000: return " 0. BELOW RM 50,000"
    elif balance < 100000: return " 1. RM 50,000 - BELOW RM100,000"
    elif balance < 200000: return " 2. RM100,000 - BELOW RM200,000"
    elif balance < 300000: return " 3. RM200,000 - BELOW RM300,000"
    elif balance < 400000: return " 4. RM300,000 - BELOW RM400,000"
    elif balance < 500000: return " 5. RM400,000 - BELOW RM500,000"
    elif balance < 600000: return " 6. RM500,000 - BELOW RM600,000"
    elif balance < 700000: return " 7. RM600,000 - BELOW RM700,000"
    elif balance < 800000: return " 8. RM700,000 - BELOW RM800,000"
    elif balance < 900000: return " 9. RM800,000 - BELOW RM900,000"
    elif balance < 1000000: return "10. RM900,000 - BELOW RM1 MILL"
    elif balance < 2000000: return "11. RM1 MILL - BELOW RM2 MILL"
    elif balance < 3000000: return "12. RM2 MILL - BELOW RM3 MILL"
    elif balance < 4000000: return "13. RM3 MILL - BELOW RM4 MILL"
    elif balance < 5000000: return "14. RM4 MILL - BELOW RM5 MILL"
    elif balance < 6000000: return "15. RM5 MILL - BELOW RM6 MILL"
    elif balance < 7000000: return "16. RM6 MILL - BELOW RM7 MILL"
    elif balance < 8000000: return "17. RM7 MILL - BELOW RM8 MILL"
    elif balance < 9000000: return "18. RM8 MILL - BELOW RM9 MILL"
    elif balance < 10000000: return "19. RM9 MILL - BELOW RM10 MILL"
    else: return "20. RM10 MILL & ABOVE"

dpbr_bal = dpbr_bal.append_column(
    "DRANGE",
    pa.array([map_depran(b.as_py()) for b in dpbr_bal["BALANCE"]])
)

# --- Add ACCT count = 1 per CUSTNO ---
dpbr_bal = dpbr_bal.append_column("ACCT", pa.array([1]*len(dpbr_bal)))

# --- Final summary: group by BRCHCD, DRANGE ---
dpbr_final = duckdb.query("""
    SELECT BRCHCD, DRANGE,
           SUM(ACCT) AS NO_OF_CUSTOMER,
           SUM(BALANCE) AS OUTSTANDING_AMOUNT
    FROM dpbr_bal
    GROUP BY BRCHCD, DRANGE
    ORDER BY BRCHCD, DRANGE
""").to_arrow_table()

# Result: dpbr_final
print(dpbr_final.schema)
print(dpbr_final.to_pandas().head())
