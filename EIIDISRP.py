import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime

# ============================================================================
# PATH CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
DAILY_DIR = OUTPUT_DIR / 'daily'
REPORT_DIR = OUTPUT_DIR / 'reports'

# Input paths
DEPO_REPTDATE = INPUT_DIR / 'depo' / 'reptdate.parquet'
DEPO_CURRENT = INPUT_DIR / 'depo' / 'current.parquet'
DEPX_CURRENT = INPUT_DIR / 'depx' / 'current.parquet'
DAILY_EXCLUDE_PREV = DAILY_DIR / 'exclude_{prevday}.parquet'
DEP_LOAN_PREV = INPUT_DIR / 'dep' / 'loan{prevmon}{nowk2}.parquet'
DEP_LOAN_CURR = INPUT_DIR / 'dep' / 'loan{reptmon}{nowk2}.parquet'
TRX_DPBTRAN = INPUT_DIR / 'trx' / 'dpbtran{reptyear}{reptmon}{nowk}.parquet'
SASD_LOAN = INPUT_DIR / 'sasd' / 'loan{reptmon}.parquet'

# Output paths
DAILY_TEST = DAILY_DIR / 'test{reptday}.parquet'
DAILY_TRX_DISB = DAILY_DIR / 'trx_disb.parquet'
DAILY_TRANX = DAILY_DIR / 'tranx.parquet'
DAILY_EXCLUDE = DAILY_DIR / 'exclude_{reptday}.parquet'
DAILY_EXCLUDEX = DAILY_DIR / 'excludex_{reptday}.parquet'
DAILY_ODX_BASE = DAILY_DIR / 'odx_base{reptmon}.parquet'
DAILY_OD_BASE = DAILY_DIR / 'od_base{reptmon}.parquet'
DAILY_EXCLM = DAILY_DIR / 'exclm_{reptmon}.parquet'
REPORT_EXCLUDE = REPORT_DIR / 'exclude_report_{reptday}.txt'
REPORT_DISBREP = REPORT_DIR / 'disbrep_report_{reptmon}.txt'

# Create output directories
DAILY_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Initialize DuckDB
# ============================================================================

con = duckdb.connect()

# Product format mapping (ODPROD)
con.execute("""
    CREATE TEMP TABLE odprod_fmt AS
    SELECT * FROM (VALUES
        (500,'34110'),(501,'34110'),(502,'34110'),(503,'34110'),(504,'34110'),
        (505,'34110'),(506,'34110'),(507,'34110'),(508,'34110'),(509,'34110'),
        (510,'34110'),(511,'34110'),(512,'34110'),(513,'34110'),(514,'34110'),
        (515,'34110'),(516,'34110'),(517,'34110'),(518,'34110'),(519,'34110'),
        (520,'34180'),(521,'34180'),(522,'34180'),(523,'34180'),(524,'34180'),
        (525,'34240'),(526,'34240'),(527,'34240'),(528,'34240'),(529,'34240')
    ) AS t(product, prodcd)
""")

# ============================================================================
# Get REPTDATE and calculate variables
# ============================================================================

reptdate_df = pl.read_parquet(DEPO_REPTDATE)
reptdate = reptdate_df['reptdate'][0]

day = reptdate.day
mm = reptdate.month
year = reptdate.year

# Determine week (NOWK)
if 1 <= day <= 8:
    nowk = '01'
elif 9 <= day <= 15:
    nowk = '02'
elif 16 <= day <= 22:
    nowk = '03'
else:
    nowk = '04'

# Calculate MM2 (previous month)
mm2 = mm - 1 if mm > 1 else 12

# Calculate previous day
dd = day
prev = dd - 1

# Calculate PREVX (last day of previous month)
if mm in [1, 3, 5, 7, 8, 10, 12]:
    prevx = 31
elif mm in [4, 6, 9, 11]:
    prevx = 30
else:  # February
    leap = year % 4
    prevx = 29 if leap == 0 else 28

# Format variables
nowk2 = '4'
reptyear = f"{year % 100:02d}"
reptmon = f"{mm:02d}"
reptmon2 = f"{mm2:02d}"
reptday = f"{day:02d}"
prevday = f"{prev:02d}"
prevmon = f"{mm2:02d}"
prevx_str = f"{prevx:02d}"

print(f"Processing date: {reptdate}")
print(f"Week: {nowk}, Month: {reptmon}, Year: {reptyear}")
print(f"Previous day: {prevday}, Previous month: {prevmon}, PREVX: {prevx_str}")

# Build file paths with variables
depo_current_path = str(DEPO_CURRENT)
depx_current_path = str(DEPX_CURRENT)
daily_exclude_prev_path = str(DAILY_EXCLUDE_PREV).format(prevday=prevday)
dep_loan_prev_path = str(DEP_LOAN_PREV).format(prevmon=prevmon, nowk2=nowk2)
dep_loan_curr_path = str(DEP_LOAN_CURR).format(reptmon=reptmon, nowk2=nowk2)
trx_dpbtran_path = str(TRX_DPBTRAN).format(reptyear=reptyear, reptmon=reptmon, nowk=nowk)
sasd_loan_path = str(SASD_LOAN).format(reptmon=reptmon)

daily_test_path = str(DAILY_TEST).format(reptday=reptday)
daily_exclude_path = str(DAILY_EXCLUDE).format(reptday=reptday)
daily_excludex_path = str(DAILY_EXCLUDEX).format(reptday=reptday)
daily_odx_base_path = str(DAILY_ODX_BASE).format(reptmon=reptmon)
daily_od_base_path = str(DAILY_OD_BASE).format(reptmon=reptmon)
daily_exclm_path = str(DAILY_EXCLM).format(reptmon=reptmon)
report_exclude_path = str(REPORT_EXCLUDE).format(reptday=reptday)
report_disbrep_path = str(REPORT_DISBREP).format(reptmon=reptmon)

# Print REPTDATE info (equivalent to PROC PRINT)
print(f"\nREPTDATE Information:")
print(f"  REPTDATE: {reptdate}")
print(f"  REPTYEAR: {reptyear}")
print(f"  REPTMON: {reptmon}")
print(f"  REPTDAY: {reptday}")
print(f"  NOWK: {nowk}")

# ============================================================================
# Process ODX from DEPO.CURRENT
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE odx AS
    SELECT c.*, f.prodcd
    FROM read_parquet('{depo_current_path}') c
    LEFT JOIN odprod_fmt f ON c.product = f.product
    WHERE f.prodcd IN ('34180', '34240')
      AND c.curbal <= 0
""")

# ============================================================================
# AVG Macro - Process OD based on REPTDAY
# ============================================================================

if reptday == '01':
    # First day of month - use DEPX.CURRENT, rename CURBAL to ODAMT
    con.execute(f"""
        CREATE TEMP TABLE od AS
        SELECT c.acctno, c.product, c.curbal as odamt, f.prodcd
        FROM read_parquet('{depx_current_path}') c
        LEFT JOIN odprod_fmt f ON c.product = f.product
        WHERE c.curbal <= 0
        ORDER BY acctno
    """)
else:
    # Use previous day's EXCLUDE file, rename columns
    con.execute(f"""
        CREATE TEMP TABLE od AS
        SELECT acctno, product, odamtx as odamt, repaidx as repaid, 
               disbursex as disburse, prodcd
        FROM read_parquet('{daily_exclude_prev_path}')
    """)

# Filter OD for OD products only
con.execute("""
    CREATE TEMP TABLE od_filtered AS
    SELECT o.*, f.prodcd as prodcd_new
    FROM od o
    LEFT JOIN odprod_fmt f ON o.product = f.product
    WHERE COALESCE(f.prodcd, o.prodcd) IN ('34180', '34240')
      AND odamt <= 0
""")

# Update prodcd if needed
con.execute("""
    CREATE TEMP TABLE od_final AS
    SELECT acctno, product, odamt, repaid, disburse,
           COALESCE(prodcd_new, prodcd) as prodcd
    FROM od_filtered
""")

# ============================================================================
# Merge ODX and OD to create TEST
# ============================================================================

con.execute("""
    CREATE TEMP TABLE test_merged AS
    SELECT 
        COALESCE(odx.acctno, od.acctno) as acctno,
        COALESCE(odx.product, od.product) as product,
        COALESCE(odx.prodcd, od.prodcd) as prodcd,
        COALESCE(odx.curbal, 0) as curbal,
        od.odamt,
        od.repaid,
        od.disburse
    FROM odx
    FULL OUTER JOIN od_final od ON odx.acctno = od.acctno
    ORDER BY acctno
""")

con.execute(f"COPY test_merged TO '{daily_test_path}'")

# ============================================================================
# FIRST Macro - Handle first day logic
# ============================================================================

if reptday == '01':
    con.execute(f"""
        CREATE TEMP TABLE od_old AS
        SELECT DISTINCT acctno
        FROM read_parquet('{dep_loan_prev_path}')
        WHERE curbal <= 0
        ORDER BY acctno
    """)
    
    con.execute(f"""
        CREATE TEMP TABLE test_with_old AS
        SELECT t.*
        FROM read_parquet('{daily_test_path}') t
        LEFT JOIN od_old o ON t.acctno = o.acctno
        ORDER BY t.acctno
    """)
    
    con.execute(f"COPY test_with_old TO '{daily_test_path}'")

# ============================================================================
# Process transaction data - DISBURSEMENTS
# ============================================================================

disb_trancodes = [293, 946, 945, 954, 208, 816, 807, 810, 
                  829, 835, 858, 830, 985, 831, 833, 980, 832]

con.execute(f"""
    CREATE TEMP TABLE trx_disb AS
    SELECT acctno, trancode, tranamt, reptdate
    FROM read_parquet('{trx_dpbtran_path}')
    WHERE reptdate = DATE '{reptdate}'
      AND trancode IN {tuple(disb_trancodes)}
""")

con.execute(f"COPY trx_disb TO '{str(DAILY_TRX_DISB)}'")

# Aggregate disbursements
con.execute("""
    CREATE TEMP TABLE disbx AS
    SELECT acctno, SUM(tranamt) as tranxdisb
    FROM trx_disb
    GROUP BY acctno
""")

# ============================================================================
# Process transaction data - REPAYMENTS
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE trx_repaid AS
    SELECT acctno, trancode, tranamt
    FROM read_parquet('{trx_dpbtran_path}')
    WHERE reptdate = DATE '{reptdate}'
      AND trancode IN (761, 762)
""")

con.execute(f"""
    CREATE TEMP TABLE trxrep_617 AS
    SELECT acctno, trancode, tranamt
    FROM read_parquet('{trx_dpbtran_path}')
    WHERE reptdate = DATE '{reptdate}'
      AND trancode = 617
""")

con.execute(f"""
    CREATE TEMP TABLE trxrep_821 AS
    SELECT acctno, trancode, tranamt
    FROM read_parquet('{trx_dpbtran_path}')
    WHERE reptdate = DATE '{reptdate}'
      AND trancode = 821
""")

# Aggregate repayments - TRX_REPAID uses BY (sorted), others use CLASS
con.execute("""
    CREATE TEMP TABLE repx AS
    SELECT acctno, SUM(tranamt) as tranxrepx
    FROM trx_repaid
    GROUP BY acctno
""")

con.execute("""
    CREATE TEMP TABLE repx617 AS
    SELECT acctno, SUM(tranamt) as tranxrep617
    FROM trxrep_617
    GROUP BY acctno
""")

con.execute("""
    CREATE TEMP TABLE repx821 AS
    SELECT acctno, SUM(tranamt) as tranxrep821
    FROM trxrep_821
    GROUP BY acctno
""")

# Calculate write-offs
con.execute("""
    CREATE TEMP TABLE woff AS
    SELECT 
        COALESCE(r617.acctno, r821.acctno) as acctno,
        COALESCE(r617.tranxrep617, 0) as tranxrep617,
        COALESCE(r821.tranxrep821, 0) as tranxrep821,
        COALESCE(r617.tranxrep617, 0) - COALESCE(r821.tranxrep821, 0) as tranxwoff
    FROM repx617 r617
    FULL OUTER JOIN repx821 r821 ON r617.acctno = r821.acctno
    ORDER BY acctno
""")

# Merge repayments with write-offs
con.execute("""
    CREATE TEMP TABLE woff_fin AS
    SELECT 
        COALESCE(r.acctno, w.acctno) as acctno,
        COALESCE(r.tranxrepx, 0) as tranxrepx,
        COALESCE(w.tranxrep617, 0) as tranxrep617,
        COALESCE(w.tranxrep821, 0) as tranxrep821,
        COALESCE(w.tranxwoff, 0) as tranxwoff,
        COALESCE(r.tranxrepx, 0) + COALESCE(w.tranxwoff, 0) as tranxrep
    FROM repx r
    FULL OUTER JOIN woff w ON r.acctno = w.acctno
    ORDER BY acctno
""")

# Merge disbursements and repayments
con.execute("""
    CREATE TEMP TABLE tranx AS
    SELECT 
        COALESCE(w.acctno, d.acctno) as acctno,
        COALESCE(w.tranxrepx, 0) as tranxrepx,
        COALESCE(w.tranxrep617, 0) as tranxrep617,
        COALESCE(w.tranxrep821, 0) as tranxrep821,
        COALESCE(w.tranxwoff, 0) as tranxwoff,
        COALESCE(w.tranxrep, 0) as tranxrep,
        COALESCE(d.tranxdisb, 0) as tranxdisb
    FROM woff_fin w
    FULL OUTER JOIN disbx d ON w.acctno = d.acctno
    ORDER BY acctno
""")

con.execute(f"COPY tranx TO '{str(DAILY_TRANX)}'")

# ============================================================================
# Create EXCLUDE file with calculations
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE exclude_base AS
    SELECT 
        t.acctno,
        t.product,
        t.prodcd,
        t.curbal,
        t.odamt,
        t.repaid as repaid_prev,
        t.disburse as disburse_prev,
        COALESCE(tx.tranxdisb, 0) as tranxdisb,
        COALESCE(tx.tranxrep, 0) as tranxrep,
        COALESCE(tx.tranxrep617, 0) as tranxrep617,
        COALESCE(tx.tranxrep821, 0) as tranxrep821,
        COALESCE(tx.tranxwoff, 0) as tranxwoff,
        COALESCE(tx.tranxrepx, 0) as tranxrepx
    FROM read_parquet('{daily_test_path}') t
    LEFT JOIN tranx tx ON t.acctno = tx.acctno
""")

# Apply business logic as per SAS
con.execute("""
    CREATE TEMP TABLE exclude_calc AS
    SELECT *,
        CASE WHEN curbal IS NULL THEN 0 ELSE curbal END as curbal_adj,
        CASE WHEN curbal > 0 THEN 0 
             WHEN curbal IS NULL THEN 0 
             ELSE curbal END as odamtx,
        COALESCE(odamt, 0) as odamt_adj
    FROM exclude_base
""")

con.execute("""
    CREATE TEMP TABLE exclude_with_flags AS
    SELECT *,
        odamtx as curod,
        odamt_adj as preod,
        CASE WHEN (odamtx >= 0 AND odamt_adj >= 0) THEN 0 ELSE tranxdisb END as tranxdisb_adj,
        CASE WHEN (odamtx >= 0 AND odamt_adj >= 0) THEN 0 ELSE tranxrep END as tranxrep_adj
    FROM exclude_calc
""")

con.execute("""
    CREATE TEMP TABLE exclude_amts AS
    SELECT *,
        odamtx - odamt_adj as amt1
    FROM exclude_with_flags
""")

con.execute("""
    CREATE TEMP TABLE exclude_amtx AS
    SELECT *,
        (amt1 + tranxdisb_adj) - tranxrep_adj as amtx
    FROM exclude_amts
""")

con.execute("""
    CREATE TEMP TABLE exclude_disbrep AS
    SELECT *,
        CASE WHEN amtx < 0 THEN amtx * (-1) ELSE 0 END as disbursex,
        CASE WHEN amtx >= 0 THEN amtx ELSE 0 END as repaidx
    FROM exclude_amtx
""")

# Calculate running sums using window functions
con.execute("""
    CREATE TABLE exclude_final AS
    SELECT *,
        SUM(repaidx) OVER (ORDER BY acctno ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as repasum,
        SUM(disbursex) OVER (ORDER BY acctno ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as disbsum
    FROM exclude_disbrep
    ORDER BY acctno
""")

con.execute(f"COPY exclude_final TO '{daily_exclude_path}'")

# ============================================================================
# BASE Macro - Process on last day of month
# ============================================================================

if reptday == prevx_str:
    print(f"\nLast day of month ({reptday}) - processing BASE files...")
    
    # Create ODX_BASE - merge previous and current month loan files
    con.execute(f"""
        CREATE TABLE odx_base_calc AS
        SELECT 
            COALESCE(curr.acctno, prev.acctno) as acctno,
            prev.curbal as ocurbal,
            prev.product as oproduct,
            curr.curbal,
            curr.product
        FROM read_parquet('{dep_loan_curr_path}') curr
        FULL OUTER JOIN read_parquet('{dep_loan_prev_path}') prev 
            ON curr.acctno = prev.acctno
    """)
    
    # Add product codes
    con.execute("""
        CREATE TABLE odx_base_prodcd AS
        SELECT o.*,
               f1.prodcd as prodcd,
               f2.prodcd as oprodcd
        FROM odx_base_calc o
        LEFT JOIN odprod_fmt f1 ON o.product = f1.product
        LEFT JOIN odprod_fmt f2 ON o.oproduct = f2.product
        WHERE (f1.prodcd IN ('34180', '34240') OR f2.prodcd IN ('34180', '34240'))
          AND (curbal <= 0 OR ocurbal <= 0)
          AND NOT (curbal IS NULL AND ocurbal > 0)
    """)
    
    # Keep only ACCTNO
    con.execute("""
        CREATE TABLE odx_base AS
        SELECT DISTINCT acctno
        FROM odx_base_prodcd
        ORDER BY acctno
    """)
    
    con.execute(f"COPY odx_base TO '{daily_odx_base_path}'")
    
    # Create EXCLUDEX - merge ODX_BASE with EXCLUDE, rename columns
    con.execute(f"""
        CREATE TABLE excludex_base AS
        SELECT e.*,
               e.disbsum as disburse,
               e.repasum as repaid
        FROM read_parquet('{daily_odx_base_path}') b
        INNER JOIN read_parquet('{daily_exclude_path}') e ON b.acctno = e.acctno
        WHERE e.acctno BETWEEN 3000000000 AND 3999999999
    """)
    
    con.execute(f"COPY excludex_base TO '{daily_excludex_path}'")
    
    # Generate EXCLUDE report (PROC PRINT with SUM)
    exclude_rpt = con.execute(f"""
        SELECT acctno, repaid, disburse
        FROM read_parquet('{daily_exclude_path}')
        ORDER BY acctno
    """).fetchall()
    
    total_repaid = sum(row[1] if row[1] else 0 for row in exclude_rpt)
    total_disburse = sum(row[2] if row[2] else 0 for row in exclude_rpt)
    
    with open(report_exclude_path, 'w') as f:
        f.write(f"1EXCLUDE REPORT - DAY {reptday}, MONTH {reptmon}, YEAR 20{reptyear}\n")
        f.write(f" \n")
        f.write(f"{'ACCTNO':>15} {'REPAID':>20} {'DISBURSE':>20}\n")
        f.write(f"{'-'*60}\n")
        
        for row in exclude_rpt:
            acctno = row[0] if row[0] else ''
            repaid = row[1] if row[1] else 0
            disburse = row[2] if row[2] else 0
            f.write(f"{acctno:>15} {repaid:>20.2f} {disburse:>20.2f}\n")
        
        f.write(f"{'='*60}\n")
        f.write(f"{'TOTAL':>15} {total_repaid:>20.2f} {total_disburse:>20.2f}\n")
    
    print(f"  Generated report: {report_exclude_path}")
    
    # Create OD_BASE - merge previous month loan with current month SASD loan
    con.execute(f"""
        CREATE TABLE od_base AS
        SELECT 
            COALESCE(prev.acctno, curr.acctno) as acctno,
            COALESCE(prev.noteno, curr.noteno) as noteno,
            prev.*, curr.*
        FROM read_parquet('{dep_loan_prev_path}') prev
        FULL OUTER JOIN read_parquet('{sasd_loan_path}') curr 
            ON prev.acctno = curr.acctno AND prev.noteno = curr.noteno
        WHERE COALESCE(prev.prodcd, curr.prodcd) != 'N'
        ORDER BY acctno, noteno
    """)
    
    con.execute(f"COPY od_base TO '{daily_od_base_path}'")
    
    # Filter DEPO.CURRENT for MTD amounts
    con.execute(f"""
        CREATE TEMP TABLE ca AS
        SELECT acctno, mtd_disbursed_amt, mtd_repaid_amt, mtd_repay_type10_amt
        FROM read_parquet('{depo_current_path}')
        WHERE mtd_disbursed_amt > 0 OR mtd_repaid_amt > 0
        ORDER BY acctno
    """)
    
    # Merge OD_BASE with CA and create final EXCLUDE and EXCLM
    con.execute(f"""
        CREATE TABLE exclude_ca AS
        SELECT 
            o.*,
            CASE WHEN c.mtd_disbursed_amt < 0 THEN 0 ELSE c.mtd_disbursed_amt END as disburse,
            CASE WHEN c.mtd_repaid_amt < 0 THEN 0 ELSE c.mtd_repaid_amt END as repaid,
            c.mtd_repay_type10_amt as repaid10
        FROM read_parquet('{daily_od_base_path}') o
        INNER JOIN ca c ON o.acctno = c.acctno
        ORDER BY acctno
    """)
    
    # Save as both EXCLUDE and EXCLM
    con.execute(f"COPY exclude_ca TO '{daily_exclude_path}'")
    con.execute(f"COPY exclude_ca TO '{daily_exclm_path}'")
    
    # Create summary by PRODCD (PROC SUMMARY)
    con.execute(f"""
        CREATE TEMP TABLE disbreptot AS
        SELECT prodcd,
               SUM(disburse) as disburse,
               SUM(repaid) as repaid
        FROM read_parquet('{daily_exclude_path}')
        GROUP BY prodcd
        ORDER BY prodcd
    """)
    
    # Generate DISBREP summary report (PROC PRINT with SUM)
    disbrep_rpt = con.execute("SELECT * FROM disbreptot").fetchall()
    
    sum_disburse = sum(row[1] if row[1] else 0 for row in disbrep_rpt)
    sum_repaid = sum(row[2] if row[2] else 0 for row in disbrep_rpt)
    
    with open(report_disbrep_path, 'w') as f:
        f.write(f"1DISBURSEMENT & REPAYMENT SUMMARY - MONTH {reptmon}, YEAR 20{reptyear}\n")
        f.write(f" \n")
        f.write(f"{'PRODCD':<10} {'DISBURSE':>25} {'REPAID':>25}\n")
        f.write(f"{'-'*65}\n")
        
        for row in disbrep_rpt:
            prodcd = row[0] if row[0] else ''
            disburse = row[1] if row[1] else 0
            repaid = row[2] if row[2] else 0
            f.write(f"{prodcd:<10} {disburse:>25.2f} {repaid:>25.2f}\n")
        
        f.write(f"{'='*65}\n")
        f.write(f"{'TOTAL':<10} {sum_disburse:>25.2f} {sum_repaid:>25.2f}\n")
    
    print(f"  Generated report: {report_disbrep_path}")
    print(f"\nBASE processing complete:")
    print(f"  - {daily_odx_base_path}")
    print(f"  - {daily_od_base_path}")
    print(f"  - {daily_exclm_path}")

con.close()

print(f"\nProcessing complete. Main output files:")
print(f"  - {daily_test_path}")
print(f"  - {str(DAILY_TRX_DISB)}")
print(f"  - {str(DAILY_TRANX)}")
print(f"  - {daily_exclude_path}")
if reptday == prevx_str:
    print(f"  - {daily_excludex_path}")
    print(f"  - {report_exclude_path}")
    print(f"  - {report_disbrep_path}")
