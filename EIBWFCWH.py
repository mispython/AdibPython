import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime
import calendar

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
FCYS_DIR = OUTPUT_DIR / 'fcys'
MFCY_DIR = OUTPUT_DIR / 'mfcy'

# Input paths
DPTRBL_REPTDATE = INPUT_DIR / 'dptrbl' / 'reptdate.parquet'
DPTRBL_CURRENT = INPUT_DIR / 'dptrbl' / 'current.parquet'
DPTRBL_VOSTRO = INPUT_DIR / 'dptrbl' / 'vostro.parquet'
DPTRBL_FD = INPUT_DIR / 'dptrbl' / 'fd.parquet'
MIS_FCY = INPUT_DIR / 'mis' / 'fcy{reptmon}.parquet'
MIS_VOST = INPUT_DIR / 'mis' / 'vost{reptmon}.parquet'
MISFD_FCYFD = INPUT_DIR / 'misfd' / 'fcyfd{reptmon}.parquet'
MNIFD_FD = INPUT_DIR / 'mnifd' / 'fd.parquet'
CIS_CISR1CA = INPUT_DIR / 'cis' / 'cisr1ca{reptyear}{reptmon}{reptday}.parquet'
CIS_CISR1FD = INPUT_DIR / 'cis' / 'cisr1fd{reptyear}{reptmon}{reptday}.parquet'
SIGNA_SMSACC = INPUT_DIR / 'signa' / 'smsacc.parquet'
MISMTD_CAVG = INPUT_DIR / 'mismtd' / 'cavg{reptmon}.parquet'
MISMTD_FAVG = INPUT_DIR / 'mismtd' / 'favg{reptmon}.parquet'
RAW_LIMIT = INPUT_DIR / 'raw' / 'limit.txt'
RAWFD_LIMIT = INPUT_DIR / 'rawfd' / 'limit.txt'

# Output paths
FCYS_REPTDATE = FCYS_DIR / 'reptdate.parquet'
FCYS_FCY = FCYS_DIR / 'fcy{reptyear}{reptmon}{reptday}.parquet'
FCYS_VOSTRO = FCYS_DIR / 'vostro{reptyear}{reptmon}{reptday}.parquet'
FCYS_FCY_MONTHLY = FCYS_DIR / 'fcy{reptmon}{nowk}{reptyear}.parquet'
MFCY_FCY_MONTHLY = MFCY_DIR / 'fcy{reptmon}{nowk}{reptyear}.parquet'

FCYS_DIR.mkdir(parents=True, exist_ok=True)
MFCY_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Initialize and get REPTDATE
# ============================================================================
con = duckdb.connect()

reptdate_df = pl.read_parquet(DPTRBL_REPTDATE)
reptdate = reptdate_df['reptdate'][0]

day = reptdate.day
mm = reptdate.month
year = reptdate.year

# Determine week
if 1 <= day <= 8: nowk = '1'
elif 9 <= day <= 15: nowk = '2'
elif 16 <= day <= 22: nowk = '3'
else: nowk = '4'

# Calculate last day of month
lastday = calendar.monthrange(year, mm)[1]

# Format variables
reptyear = f"{year % 100:02d}"
reptmon = f"{mm:02d}"
reptday = f"{day:02d}"
rdate = reptdate.strftime('%d%m%Y')
dayone = datetime(year, mm, 1)

print(f"Date: {reptdate}, Week: {nowk}, Last day: {lastday}")

# Save REPTDATE
reptdate_df.write_parquet(FCYS_REPTDATE)

# ============================================================================
# Process FCY Current Accounts
# ============================================================================
# Calculate average balance
con.execute(f"""
    CREATE TEMP TABLE averbal_fcy AS
    SELECT acctno, SUM(curbalus) as totmtbal
    FROM read_parquet('{str(MIS_FCY).format(reptmon=reptmon)}')
    WHERE reptdate BETWEEN DATE '{dayone}' AND DATE '{reptdate}'
    GROUP BY acctno
""")

# Process CURRENT
con.execute(f"""
    CREATE TEMP TABLE fcysfcy AS
    SELECT *,
        purpose as classifi,
        intrate as rate,
        int1 as intpay,
        psreason as reason,
        SUBSTRING(statcd, 4, 1) as acstatus4,
        SUBSTRING(statcd, 5, 1) as acstatus5,
        SUBSTRING(statcd, 6, 1) as acstatus6,
        SUBSTRING(statcd, 7, 1) as acstatus7,
        SUBSTRING(statcd, 8, 1) as acstatus8,
        SUBSTRING(statcd, 9, 1) as acstatus9,
        SUBSTRING(statcd, 10, 1) as acstatus10,
        sector as purposem,
        TRY_STRPTIME(SUBSTRING(LPAD(CAST(lasttran AS VARCHAR), 9, '0'), 1, 6), '%m%d%y') as lasttr,
        CASE WHEN bdate > 0 THEN TRY_STRPTIME(SUBSTRING(LPAD(CAST(bdate AS VARCHAR), 11, '0'), 1, 8), '%m%d%Y') END as births,
        CASE WHEN dtlstcust NOT IN (0, NULL) THEN TRY_STRPTIME(SUBSTRING(LPAD(CAST(dtlstcust AS VARCHAR), 9, '0'), 1, 6), '%m%d%y') END as lstcust,
        TRY_STRPTIME(SUBSTRING(LPAD(CAST(opendt AS VARCHAR), 11, '0'), 1, 8), '%m%d%Y') as odates,
        TRY_STRPTIME(SUBSTRING(LPAD(CAST(reopendt AS VARCHAR), 11, '0'), 1, 8), '%m%d%Y') as rodates
    FROM read_parquet('{DPTRBL_CURRENT}')
    WHERE product BETWEEN 400 AND 434 AND product != 412
      AND NOT (acctno BETWEEN 3000000000 AND 3999999999 AND nxt_stmt_cycle_dt IS NOT NULL)
      AND openind NOT IN ('B', 'C', 'P')
""")

# Load CIS and LIMIT
con.execute(f"""
    CREATE TEMP TABLE cisdp AS
    SELECT acctno, custno, citizen, race, seccust as primary
    FROM read_parquet('{str(CIS_CISR1CA).format(reptyear=reptyear, reptmon=reptmon, reptday=reptday)}')
    WHERE seccust = '901'
""")

con.execute(f"""
    CREATE TEMP TABLE limit_ca AS
    SELECT CAST(SUBSTRING(line, 1, 11) AS BIGINT) as custno,
           CAST(SUBSTRING(line, 12, 10) AS DECIMAL(10,2)) as perlimt,
           SUBSTRING(line, 22, 8) as keyword,
           CAST(SUBSTRING(line, 39, 2) AS INTEGER) as purpose,
           CAST(SUBSTRING(line, 39, 2) AS INTEGER) as purposec
    FROM read_csv('{RAW_LIMIT}', columns={{'line': 'VARCHAR'}}, header=false)
""")

# Merge and calculate
con.execute(f"""
    CREATE TEMP TABLE fcyca AS
    SELECT f.*, c.custno, c.citizen, c.race, c.primary, l.perlimt, l.purposec,
           a.totmtbal, s.esignature,
           m.mtdavbal,
           DATE '{reptdate}' - DATE '{dayone}' + 1 as days,
           COALESCE(a.totmtbal, 0) / (DATE '{reptdate}' - DATE '{dayone}' + 1) as averbal,
           f.curbal as curbalrm,
           COALESCE(s.esignature, 'N') as esignature_final
    FROM fcysfcy f
    LEFT JOIN cisdp c ON f.acctno = c.acctno
    LEFT JOIN limit_ca l ON c.custno = l.custno AND f.purposem = l.purpose
    LEFT JOIN averbal_fcy a ON f.acctno = a.acctno
    LEFT JOIN read_parquet('{str(SIGNA_SMSACC)}') s ON f.acctno = s.acctno
    LEFT JOIN read_parquet('{str(MISMTD_CAVG).format(reptmon=reptmon)}') m ON f.acctno = m.acctno
""")

# ============================================================================
# Process VOSTRO
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE averbal_vs AS
    SELECT acctno, SUM(curbalus) as totmtbal
    FROM read_parquet('{str(MIS_VOST).format(reptmon=reptmon)}')
    WHERE reptdate BETWEEN DATE '{dayone}' AND DATE '{reptdate}'
    GROUP BY acctno
""")

con.execute(f"""
    CREATE TEMP TABLE fcyvs AS
    SELECT v.*, c.custno, c.race, l.perlimt,
           a.totmtbal,
           DATE '{reptdate}' - DATE '{dayone}' + 1 as days,
           COALESCE(a.totmtbal, 0) / (DATE '{reptdate}' - DATE '{dayone}' + 1) as averbal,
           v.curbal as curbalrm
    FROM (
        SELECT *,
            purpose as classifi,
            sector as purposem,
            TRY_STRPTIME(SUBSTRING(LPAD(CAST(lasttran AS VARCHAR), 9, '0'), 1, 6), '%m%d%y') as lasttr,
            CASE WHEN bdate > 0 THEN TRY_STRPTIME(SUBSTRING(LPAD(CAST(bdate AS VARCHAR), 11, '0'), 1, 8), '%m%d%Y') END as births,
            TRY_STRPTIME(SUBSTRING(LPAD(CAST(opendt AS VARCHAR), 11, '0'), 1, 8), '%m%d%Y') as odates
        FROM read_parquet('{DPTRBL_VOSTRO}')
    ) v
    LEFT JOIN cisdp c ON v.acctno = c.acctno
    LEFT JOIN limit_ca l ON c.custno = l.custno AND v.purposem = l.purpose
    LEFT JOIN averbal_vs a ON v.acctno = a.acctno
""")

# ============================================================================
# Process FD
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE averbal_fd AS
    SELECT acctno, cdno, SUM(curbalus) as totmtbal
    FROM read_parquet('{str(MISFD_FCYFD).format(reptmon=reptmon)}')
    WHERE reptdate BETWEEN DATE '{dayone}' AND DATE '{reptdate}'
    GROUP BY acctno, cdno
""")

con.execute(f"""
    CREATE TEMP TABLE limit_fd AS
    SELECT CAST(SUBSTRING(line, 1, 11) AS BIGINT) as custno,
           CAST(SUBSTRING(line, 12, 10) AS DECIMAL(10,2)) as perlimt,
           SUBSTRING(line, 22, 8) as keyword,
           CAST(SUBSTRING(line, 39, 2) AS INTEGER) as purpose,
           CAST(SUBSTRING(line, 39, 2) AS INTEGER) as purposec
    FROM read_csv('{RAWFD_LIMIT}', columns={{'line': 'VARCHAR'}}, header=false)
""")

con.execute(f"""
    CREATE TEMP TABLE fdcd AS
    SELECT f.*, c.custno, c.race, l.perlimt, l.purposec,
           a.totmtbal,
           DATE '{reptdate}' - DATE '{dayone}' + 1 as days,
           COALESCE(a.totmtbal, 0) / (DATE '{reptdate}' - DATE '{dayone}' + 1) as averbal,
           f.curbal as curbalrm,
           f.statec as state,
           CASE WHEN f.matdate > 0 THEN 
               MAKE_DATE(
                   CAST(SUBSTRING(LPAD(CAST(matdate AS VARCHAR), 8, '0'), 3, 2) AS INTEGER) + 2000,
                   CAST(SUBSTRING(LPAD(CAST(matdate AS VARCHAR), 8, '0'), 5, 2) AS INTEGER),
                   CAST(SUBSTRING(LPAD(CAST(matdate AS VARCHAR), 8, '0'), 7, 2) AS INTEGER)
               ) END as matdate_new,
           CASE WHEN f.orgdate > 0 THEN
               MAKE_DATE(
                   CAST(SUBSTRING(LPAD(CAST(orgdate AS VARCHAR), 9, '0'), 5, 2) AS INTEGER) + 2000,
                   CAST(SUBSTRING(LPAD(CAST(orgdate AS VARCHAR), 9, '0'), 1, 2) AS INTEGER),
                   CAST(SUBSTRING(LPAD(CAST(orgdate AS VARCHAR), 9, '0'), 3, 2) AS INTEGER)
               ) END as orgdate_new,
           m.mtdavbal
    FROM read_parquet('{str(MNIFD_FD)}') f
    LEFT JOIN read_parquet('{DPTRBL_FD}') d ON f.acctno = d.acctno
    LEFT JOIN read_parquet('{str(CIS_CISR1FD).format(reptyear=reptyear, reptmon=reptmon, reptday=reptday)}') c ON f.acctno = c.acctno AND c.seccust = '901'
    LEFT JOIN limit_fd l ON c.custno = l.custno AND d.sector = l.purpose
    LEFT JOIN averbal_fd a ON f.acctno = a.acctno AND f.cdno = a.cdno
    LEFT JOIN read_parquet('{str(MISMTD_FAVG).format(reptmon=reptmon)}') m ON f.acctno = m.acctno
""")

# ============================================================================
# Combine and finalize
# ============================================================================
con.execute("""
    CREATE TEMP TABLE fcys_all AS
    SELECT * FROM fcyca
    UNION ALL
    SELECT * FROM fdcd
""")

con.execute("""
    CREATE TEMP TABLE fcy_final AS
    SELECT *,
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY custno, purposem ORDER BY acctno) = 1 
             THEN perlimt ELSE 0 END as perlimt_adj
    FROM fcys_all
    WHERE openind NOT IN ('B', 'C', 'P')
    ORDER BY custno, purposem
""")

con.execute(f"COPY fcy_final TO '{str(FCYS_FCY).format(reptyear=reptyear, reptmon=reptmon, reptday=reptday)}'")

con.execute("""
    CREATE TEMP TABLE fcyvs_final AS
    SELECT *,
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY custno, purposem ORDER BY acctno) = 1 
             THEN perlimt ELSE 0 END as perlimt_adj
    FROM fcyvs
    ORDER BY custno, purposem
""")

con.execute(f"COPY fcyvs_final TO '{str(FCYS_VOSTRO).format(reptyear=reptyear, reptmon=reptmon, reptday=reptday)}'")

# ============================================================================
# Monthly processing (last day only)
# ============================================================================
if reptday == f"{lastday:02d}":
    print(f"Last day of month - creating monthly files...")
    
    con.execute(f"""
        COPY (SELECT * FROM fcy_final) 
        TO '{str(FCYS_FCY_MONTHLY).format(reptmon=reptmon, nowk=nowk, reptyear=reptyear)}'
    """)
    
    con.execute(f"""
        COPY (SELECT * FROM fcy_final) 
        TO '{str(MFCY_FCY_MONTHLY).format(reptmon=reptmon, nowk=nowk, reptyear=reptyear)}'
    """)

con.close()
print(f"\nProcessing complete!")
