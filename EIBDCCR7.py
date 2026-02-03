import duckdb
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LNPROD = {}  # Loan product format mapping

con = duckdb.connect()

# ============================================================================
# GET REPORTING DATE
# ============================================================================

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/loan/reptdate.parquet')").fetchone()[0]
year, month, day = reptdate.year, f"{reptdate.month:02d}", f"{reptdate.day:02d}"

print(f"Report Date: {year}-{month}-{day}")

# Write date headers to all 3 output files
for fname in ['output1.txt', 'output2.txt', 'output3.txt']:
    with open(OUTPUT_DIR/fname, 'w') as f:
        f.write(f"{year}{month}{day}\n")

# ============================================================================
# SECTION 1: REVOLVING CREDIT (RC) - OUTPUT1
# ============================================================================

# Load current and previous loan notes
con.execute(f"""
    CREATE TEMP TABLE lnnote_curr AS
    SELECT acctno, noteno, ntbrch, curbal, acctrevdt, loantype
    FROM read_parquet('{INPUT_DIR}/loan/lnnote.parquet')
    WHERE loantype = 34190
""")

con.execute(f"""
    CREATE TEMP TABLE lnnote_prev AS
    SELECT acctno, noteno, acctrevdt preacctrevdt
    FROM read_parquet('{INPUT_DIR}/oloan/lnnote.parquet')
""")

# Merge and filter for updated reviews
con.execute("""
    CREATE TEMP TABLE lnnote AS
    SELECT c.*, 
           CASE WHEN c.acctrevdt NOT IN (0,.) 
                THEN TRY_STRPTIME(LPAD(CAST(c.acctrevdt AS VARCHAR),8,'0'),'%d%m%Y') END acctrevdt_d,
           CASE WHEN p.preacctrevdt NOT IN (0,.) 
                THEN TRY_STRPTIME(LPAD(CAST(p.preacctrevdt AS VARCHAR),8,'0'),'%d%m%Y') END preacctrevdt_d
    FROM lnnote_curr c
    LEFT JOIN lnnote_prev p ON c.acctno = p.acctno AND c.noteno = p.noteno
    WHERE c.acctrevdt_d > COALESCE(p.preacctrevdt_d, DATE'1900-01-01')
""")

# Get CIS individual/organization indicator
con.execute(f"""
    CREATE TEMP TABLE cisln AS
    SELECT acctno, indorg
    FROM read_parquet('{INPUT_DIR}/cisln/loan.parquet')
    WHERE seccust = '901'
""")

# Get EREV (Electronic Review) data
con.execute(f"""
    CREATE TEMP TABLE ereva AS
    SELECT CAST(acctnod AS BIGINT) acctno, CAST(noteno AS BIGINT) noteno, reviewno
    FROM read_parquet('{INPUT_DIR}/erev2/eln12.parquet')
""")

con.execute(f"""
    CREATE TEMP TABLE erevb AS
    SELECT reviewno, apprdt, oriexpdt,
           SUBSTRING(oriexpdt,7,4)||SUBSTRING(oriexpdt,4,2)||SUBSTRING(oriexpdt,1,2) orixdate
    FROM (SELECT reviewno, apprdt, oriexpdt, 
                 ROW_NUMBER() OVER (PARTITION BY reviewno ORDER BY apprdt DESC) rn
          FROM read_parquet('{INPUT_DIR}/erev1/eln1.parquet'))
    WHERE rn = 1
""")

con.execute("""
    CREATE TEMP TABLE erev AS
    SELECT a.acctno, a.noteno, a.reviewno, b.apprdt, b.orixdate
    FROM ereva a
    LEFT JOIN erevb b ON a.reviewno = b.reviewno
""")

# Merge all and create output
con.execute("""
    CREATE TEMP TABLE lnnote_final AS
    SELECT l.*, c.indorg, e.reviewno, e.apprdt, e.orixdate,
           l.curbal * 100 curbal_adj,
           CASE WHEN l.acctrevdt_d IS NOT NULL 
                THEN STRFTIME(l.acctrevdt_d, '%Y%m%d') ELSE '00000000' END revdate,
           CASE WHEN l.preacctrevdt_d IS NOT NULL 
                THEN STRFTIME(l.preacctrevdt_d, '%Y%m%d') ELSE '00000000' END prevdate,
           CASE WHEN e.apprdt IS NOT NULL 
                THEN STRFTIME(e.apprdt, '%Y%m%d') ELSE '00000000' END revapprdt
    FROM lnnote l
    LEFT JOIN cisln c ON l.acctno = c.acctno
    LEFT JOIN (SELECT acctno, noteno, reviewno, apprdt, orixdate,
                      ROW_NUMBER() OVER (PARTITION BY acctno, noteno ORDER BY apprdt DESC) rn
               FROM erev) e ON l.acctno = e.acctno AND l.noteno = e.noteno AND e.rn = 1
""")

with open(OUTPUT_DIR/'output1.txt', 'a') as f:
    for r in con.execute("""
        SELECT ntbrch, acctno, noteno, COALESCE(indorg,''), revdate, curbal_adj,
               prevdate, COALESCE(reviewno,''), COALESCE(revapprdt,''), COALESCE(orixdate,'')
        FROM lnnote_final
    """).fetchall():
        f.write(f"{r[0]:03d}{r[1]:010d}{r[2]:010d} {r[3]:1s} {r[4]:8s}{r[5]:016.0f}"
                f"{r[6]:8s}{r[7]:17s}{r[8]:8s}{r[9]:8s}\n")

print(f"OUTPUT1 (RC): {con.execute('SELECT COUNT(*) FROM lnnote_final').fetchone()[0]} records")

# ============================================================================
# SECTION 2: OVERDRAFT (OD) - OUTPUT2
# ============================================================================

# Load current and previous OD limits
con.execute(f"""
    CREATE TEMP TABLE odrev AS
    SELECT branch, acctno, lmtdesc, lmtid, reviewdt, lmtamt
    FROM read_parquet('{INPUT_DIR}/limit/overdft.parquet')
    WHERE apprlimt > 1 AND lmtamt > 1
""")

con.execute(f"""
    CREATE TEMP TABLE prevodrev AS
    SELECT acctno, lmtid, reviewdt previewdt
    FROM read_parquet('{INPUT_DIR}/olimit/overdft.parquet')
""")

con.execute("""
    CREATE TEMP TABLE odreview AS
    SELECT o.*, 
           CASE WHEN o.reviewdt NOT IN (0,.) 
                THEN TRY_STRPTIME(SUBSTRING(LPAD(CAST(o.reviewdt AS VARCHAR),11,'0'),1,8),'%m%d%Y') END reviewdt_d,
           CASE WHEN p.previewdt NOT IN (0,.) 
                THEN TRY_STRPTIME(SUBSTRING(LPAD(CAST(p.previewdt AS VARCHAR),11,'0'),1,8),'%m%d%Y') END previewdt_d
    FROM odrev o
    LEFT JOIN prevodrev p ON o.acctno = p.acctno AND o.lmtid = p.lmtid
    WHERE o.reviewdt_d > COALESCE(p.previewdt_d, DATE'1900-01-01')
""")

# Aggregate limits by account and description
con.execute("""
    CREATE TEMP TABLE odlimit AS
    SELECT acctno, lmtdesc, SUM(lmtamt) lmtamt
    FROM odreview
    GROUP BY acctno, lmtdesc
""")

con.execute("""
    CREATE TEMP TABLE odreview_agg AS
    SELECT o.*, l.lmtamt lmtamt_tot
    FROM (SELECT acctno, lmtdesc, branch, MAX(reviewdt_d) reviewdt_d, MAX(previewdt_d) previewdt_d,
                 ANY_VALUE(lmtid) lmtid
          FROM odreview GROUP BY acctno, lmtdesc, branch) o
    JOIN odlimit l ON o.acctno = l.acctno AND o.lmtdesc = l.lmtdesc
""")

# Get CIS deposit indicator
con.execute(f"""
    CREATE TEMP TABLE cisdp AS
    SELECT acctno, indorg
    FROM read_parquet('{INPUT_DIR}/cisdp/deposit.parquet')
    WHERE seccust = '901'
""")

# Merge with EREV (deduplicated by account)
con.execute("""
    CREATE TEMP TABLE erevcom_od AS
    SELECT acctno, reviewno, apprdt, orixdate
    FROM (SELECT acctno, reviewno, apprdt, orixdate,
                 ROW_NUMBER() OVER (PARTITION BY acctno ORDER BY apprdt DESC) rn
          FROM erev) 
    WHERE rn = 1
""")

con.execute(f"""
    CREATE TEMP TABLE odreview_final AS
    SELECT o.*, c.indorg, e.reviewno, e.apprdt, e.orixdate,
           o.lmtamt_tot * 100 lmtamt_adj,
           'R' appltype,
           '{year}{month}{day}' appldate,
           CASE WHEN o.reviewdt_d IS NOT NULL 
                THEN STRFTIME(o.reviewdt_d, '%Y%m%d') ELSE '00000000' END revdate,
           CASE WHEN o.previewdt_d IS NOT NULL 
                THEN STRFTIME(o.previewdt_d, '%Y%m%d') ELSE '00000000' END prevdate,
           CASE WHEN e.apprdt IS NOT NULL 
                THEN STRFTIME(e.apprdt, '%Y%m%d') ELSE '00000000' END revapprdt
    FROM odreview_agg o
    LEFT JOIN cisdp c ON o.acctno = c.acctno
    LEFT JOIN erevcom_od e ON o.acctno = e.acctno
""")

with open(OUTPUT_DIR/'output2.txt', 'a') as f:
    for r in con.execute("""
        SELECT branch, acctno, lmtdesc, COALESCE(indorg,''), prevdate, revdate,
               lmtamt_adj, appldate, appltype, COALESCE(reviewno,''), 
               COALESCE(revapprdt,''), COALESCE(orixdate,'')
        FROM odreview_final
    """).fetchall():
        f.write(f"{r[0]:03d}{r[1]:010d} {r[2]:20s} {r[3]:1s} {r[4]:8s}{r[5]:8s}"
                f"{r[6]:017.0f}{r[7]:8s}{r[8]:1s} {r[9]:17s}{r[10]:8s}{r[11]:8s}\n")

print(f"OUTPUT2 (OD): {con.execute('SELECT COUNT(*) FROM odreview_final').fetchone()[0]} records")

# ============================================================================
# SECTION 3: BANKERS' TRADE (BT) - OUTPUT3
# ============================================================================

# Load current and previous BT reviews
con.execute(f"""
    CREATE TEMP TABLE bt_curr AS
    SELECT acctno, reviewdt, ovlimit
    FROM read_parquet('{INPUT_DIR}/bt/btreview.parquet')
""")

con.execute(f"""
    CREATE TEMP TABLE bt_prev AS
    SELECT acctno, reviewdt previewdt
    FROM read_parquet('{INPUT_DIR}/obt/btreview.parquet')
""")

con.execute("""
    CREATE TEMP TABLE bt AS
    SELECT c.*, 
           c.ovlimit * 100 ovlimit_adj,
           CASE WHEN c.reviewdt NOT IN (0,.) 
                THEN TRY_STRPTIME(LPAD(CAST(c.reviewdt AS VARCHAR),8,'0'),'%d%m%Y') END reviewdt_d,
           CASE WHEN p.previewdt NOT IN (0,.) 
                THEN TRY_STRPTIME(LPAD(CAST(p.previewdt AS VARCHAR),8,'0'),'%d%m%Y') END previewdt_d
    FROM bt_curr c
    LEFT JOIN bt_prev p ON c.acctno = p.acctno
    WHERE c.reviewdt_d > COALESCE(p.previewdt_d, DATE'1900-01-01')
""")

con.execute("""
    CREATE TEMP TABLE bt_final AS
    SELECT b.*, e.reviewno, e.apprdt, e.orixdate,
           CASE WHEN b.reviewdt_d IS NOT NULL 
                THEN STRFTIME(b.reviewdt_d, '%Y%m%d') ELSE '00000000' END revdate,
           CASE WHEN b.previewdt_d IS NOT NULL 
                THEN STRFTIME(b.previewdt_d, '%Y%m%d') ELSE '00000000' END prevdate,
           CASE WHEN e.apprdt IS NOT NULL 
                THEN STRFTIME(e.apprdt, '%Y%m%d') ELSE '00000000' END revapprdt
    FROM bt b
    LEFT JOIN erevcom_od e ON b.acctno = e.acctno
""")

with open(OUTPUT_DIR/'output3.txt', 'a') as f:
    for r in con.execute("""
        SELECT acctno, prevdate, revdate, ovlimit_adj, 
               COALESCE(reviewno,''), COALESCE(revapprdt,''), COALESCE(orixdate,'')
        FROM bt_final
    """).fetchall():
        f.write(f" {r[0]:010d}{r[1]:8s}{r[2]:8s}{r[3]:017.0f}{r[4]:17s}{r[5]:8s}{r[6]:8s}\n")

print(f"OUTPUT3 (BT): {con.execute('SELECT COUNT(*) FROM bt_final').fetchone()[0]} records")

con.close()
print(f"\nCompleted: OUTPUT1 (RC), OUTPUT2 (OD), OUTPUT3 (BT) in {OUTPUT_DIR}")
