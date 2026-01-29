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

# Input paths
DEPOSIT_REPTDATE = INPUT_DIR / 'deposit' / 'reptdate.parquet'
DEPOSIT_CURRENT = INPUT_DIR / 'deposit' / 'current.parquet'
DEPOSIT_FD = INPUT_DIR / 'deposit' / 'fd.parquet'
CISDP_DEPOSIT = INPUT_DIR / 'cisdp' / 'deposit.parquet'
CISFD_DEPOSIT = INPUT_DIR / 'cisfd' / 'deposit.parquet'
FD_FD = INPUT_DIR / 'fd' / 'fd.parquet'
DPCUST_TXT = INPUT_DIR / 'dpcust.txt'
PMFUND_TXT = INPUT_DIR / 'pmfund.txt'

# Output paths
BNM_CAFD = OUTPUT_DIR / 'bnm' / 'cafd.parquet'
SASLIST_REPORT = OUTPUT_DIR / 'artb_report.txt'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
(OUTPUT_DIR / 'bnm').mkdir(parents=True, exist_ok=True)

# ============================================================================
# Initialize DuckDB and setup formats (PBBDPFMT, ARTBFMT)
# ============================================================================
con = duckdb.connect()

# CAPROD format (from PBBDPFMT)
con.execute("""
    CREATE TEMP TABLE caprod_fmt AS SELECT * FROM (VALUES
        (300,'34130'),(301,'34130'),(302,'34130'),(303,'34130'),(304,'34130'),
        (305,'34130'),(306,'34130'),(307,'34130'),(308,'34130'),(309,'34130'),
        (310,'34130'),(311,'34130'),(312,'34130'),(313,'34130'),(314,'34130'),
        (315,'34130'),(316,'34130'),(317,'34130'),(318,'34130'),(319,'34130'),
        (320,'N'),(321,'N'),(322,'N')
    ) AS t(product, prodcd)
""")

# FUNDTYPE format (from ARTBFMT)
con.execute("""
    CREATE TEMP TABLE fundtype_fmt AS SELECT * FROM (VALUES
        (1, 'PUBLIC'), (2, 'NON-PUBLIC')
    ) AS t(pmtyp, pmtype)
""")

# ============================================================================
# Get REPTDATE and calculate variables
# ============================================================================
reptdate_df = pl.read_parquet(DEPOSIT_REPTDATE)
reptdate = reptdate_df['reptdate'][0]

day = reptdate.day
if day == 8: nowk = '1'
elif day == 15: nowk = '2'
elif day == 22: nowk = '3'
else: nowk = '4'

reptyear = str(reptdate.year)
reptmon = f"{reptdate.month:02d}"
reptday = f"{reptdate.day:02d}"
tdate = reptdate.strftime('%d/%m/%Y')
rdate = reptdate

print(f"ARTB Report Processing - {tdate}")

# ============================================================================
# Load PM Fund list
# ============================================================================
pmfund_list = []
if Path(PMFUND_TXT).exists():
    with open(PMFUND_TXT) as f:
        pmfund_list = [line.strip() for line in f if line.strip()]

# Target customers
target_custnos = [3523050, 11335374, 11880426, 3728510, 13158067, 14368177,
                  14368641, 14369065, 14387105, 14932947, 14960789, 15254645,
                  15241330, 15310964, 15352797]

# ============================================================================
# Process Current Accounts (CA)
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE cisca AS
    SELECT custno, acctno, custname, 
           CASE WHEN newic != '' THEN newic ELSE oldic END as icno,
           'CA' as cistype
    FROM read_parquet('{CISDP_DEPOSIT}')
    WHERE seccust = '901'
      AND acctno BETWEEN 3000000000 AND 3999999999
""")

con.execute(f"""
    CREATE TEMP TABLE ca AS
    SELECT c.*, f.prodcd, 
           c.curbal + COALESCE(c.intpaybl, 0) as balance,
           COALESCE(ca.custname, c.name) as custname_final,
           ca.custno, ca.icno, ca.cistype
    FROM read_parquet('{DEPOSIT_CURRENT}') c
    LEFT JOIN caprod_fmt f ON c.product = f.product
    LEFT JOIN cisca ca ON c.acctno = ca.acctno
    WHERE c.curbal > 0 AND f.prodcd != 'N'
""")

# ============================================================================
# Process Fixed Deposits (FD)
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE cisfd AS
    SELECT custno, acctno, custname,
           CASE WHEN newic != '' THEN newic ELSE oldic END as icno,
           'FD' as cistype
    FROM read_parquet('{CISFD_DEPOSIT}')
    WHERE seccust = '901'
      AND (acctno BETWEEN 1000000000 AND 1999999999
        OR acctno BETWEEN 7000000000 AND 7999999999
        OR acctno BETWEEN 4000000000 AND 6999999999)
""")

con.execute(f"""
    CREATE TEMP TABLE fd AS
    SELECT f.*, 
           f.curbal + COALESCE(f.intpaybl, 0) as balance,
           COALESCE(cf.custname, f.name) as custname_final,
           cf.custno, cf.icno, cf.cistype
    FROM read_parquet('{DEPOSIT_FD}') f
    LEFT JOIN cisfd cf ON f.acctno = cf.acctno
    WHERE f.curbal > 0
""")

# ============================================================================
# Combine CA and FD, filter by target customers
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE cafd AS
    SELECT 
        acctno, custno, cistype, curbal, balance,
        custname_final as custname,
        CASE WHEN cistype = 'CA' THEN curbal ELSE NULL END as cabal,
        CASE WHEN cistype = 'FD' THEN curbal ELSE NULL END as fdbal
    FROM (
        SELECT * FROM ca
        UNION ALL
        SELECT * FROM fd
    )
    WHERE custno IN ({','.join(map(str, target_custnos))})
      AND curbal > 0
""")

# ============================================================================
# Load fund names and classify PM vs Non-PM
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE depo AS
    SELECT 
        CAST(SUBSTRING(line, 1, 11) AS BIGINT) as acctno,
        SUBSTRING(line, 12, 40) as fundname,
        REPLACE(SUBSTRING(line, 12, 40), ' ', '') as fnamex
    FROM read_csv('{DPCUST_TXT}', columns={{'line': 'VARCHAR'}}, header=false)
""")

pmfund_sql = ','.join([f"'{f}'" for f in pmfund_list]) if pmfund_list else "''"

con.execute(f"""
    CREATE TEMP TABLE cafd_funds AS
    SELECT c.*, 
           COALESCE(d.fundname, c.custname) as fundname,
           d.fnamex,
           CASE WHEN d.fnamex IN ({pmfund_sql}) THEN 1 ELSE 2 END as pmtyp
    FROM cafd c
    LEFT JOIN depo d ON c.acctno = d.acctno
""")

con.execute("""
    CREATE TABLE bnm_cafd AS
    SELECT c.*, 
           f.pmtype,
           SUBSTRING(c.fnamex, 1, 15) as fundmne
    FROM cafd_funds c
    LEFT JOIN fundtype_fmt f ON c.pmtyp = f.pmtyp
""")

con.execute(f"COPY bnm_cafd TO '{BNM_CAFD}'")

# ============================================================================
# Generate Summary Report
# ============================================================================
summary = con.execute("""
    SELECT pmtype, 
           SUM(COALESCE(cabal, 0)) as cabal,
           SUM(COALESCE(fdbal, 0)) as fdbal,
           SUM(curbal) as curbal
    FROM bnm_cafd
    GROUP BY pmtype
    ORDER BY pmtype
""").fetchall()

total = con.execute("""
    SELECT SUM(COALESCE(cabal, 0)) as cabal,
           SUM(COALESCE(fdbal, 0)) as fdbal,
           SUM(curbal) as curbal
    FROM bnm_cafd
""").fetchone()

with open(SASLIST_REPORT, 'w') as f:
    f.write('P U B L I C   B A N K   B E R H A D\n')
    f.write(f'REPORT ID : EIBDARTB @ {tdate}\n\n')
    f.write('TABLE 1: SUMMARY TABLE FOR AMANAH RAYA GROUP\n\n')
    f.write('PUBLIC BANK (PBB)              ;;;CURRENT ACCOUNTS       ;FIXED DEPOSITS          ;TOTAL (RM)\n')
    
    for i, row in enumerate(summary, 1):
        f.write(f'{i}) {row[0]:12s}               ;;;{row[1]:24,.2f};{row[2]:24,.2f};{row[3]:24,.2f}\n')
    
    f.write(f'TOTAL                          ;;;{total[0]:24,.2f};{total[1]:24,.2f};{total[2]:24,.2f}\n\n')

# ============================================================================
# Detailed CA Breakdown
# ============================================================================
    f.write('TABLE 2: DETAILED BREAKDOWN LISTING FOR AMANAH RAYA GROUP\n')
    f.write('I) CURRENT ACCOUNT\n')
    f.write('CIS NO.        ;CUST. MNEMONIC       ;CUSTOMER FULL NAME                                                                                              ;CURRENT BALANCE        ;INTEREST (%)   ;BALANCE (CURRENT BALANCE + ACCRUED INTEREST)\n')

# CA by PM type
for pmtyp, pmtype in [(1, 'A'), (2, 'B')]:
    ca_recs = con.execute(f"""
        SELECT custno, fundmne, fundname, curbal, balance
        FROM bnm_cafd
        WHERE cistype = 'CA' AND pmtyp = {pmtyp}
        ORDER BY acctno
    """).fetchall()
    
    ca_sum = con.execute(f"""
        SELECT SUM(curbal), SUM(balance)
        FROM bnm_cafd
        WHERE cistype = 'CA' AND pmtyp = {pmtyp}
    """).fetchone()
    
    with open(SASLIST_REPORT, 'a') as f:
        ftype = 'PUBLIC' if pmtyp == 1 else 'NON-PUBLIC'
        f.write(f'({pmtype}) {ftype} MUTUAL FUND PORTFOLIO\n')
        
        for rec in ca_recs:
            f.write(f'{rec[0]:14d};{rec[1]:22s};{rec[2]:118s};{rec[3]:23,.2f};              ;{rec[4]:23,.2f}\n')
        
        f.write('\n')
        f.write(f'SUBTOTAL ({pmtype})                                                                                                      ;{ca_sum[0]:23,.2f}; -            ;{ca_sum[1]:23,.2f}\n\n')

ca_total = con.execute("""
    SELECT SUM(curbal), SUM(balance)
    FROM bnm_cafd WHERE cistype = 'CA'
""").fetchone()

with open(SASLIST_REPORT, 'a') as f:
    f.write(f'TOTAL (A)+(B)                                                                                                ;{ca_total[0]:23,.2f}; -            ;{ca_total[1]:23,.2f}\n\n')

# ============================================================================
# Detailed FD Breakdown
# ============================================================================
with open(SASLIST_REPORT, 'a') as f:
    f.write('II) FIXED DEPOSITS\n')
    f.write('CIS NO.        ;CUST. MNEMONIC       ;CUSTOMER FULL NAME                                ;TRANSACTION DATE  ;MATURITY DATE     ;ORIGINAL TENOR        ;REMAINING DAYS TO MATURITY;CURRENT BALANCE        ;INTEREST (%)   ;BALANCE (CURRENT BALANCE + ACCRUED INTEREST)\n')

# Process FD with MNI data
con.execute(f"""
    CREATE TEMP TABLE arfd AS
    SELECT a.*, m.depodte, m.matdate, m.matid, m.term, m.rate, m.intpay,
           TRY_STRPTIME(SUBSTRING(LPAD(CAST(m.depodte AS VARCHAR), 11, '0'), 1, 8), '%m%d%Y') as depo_dt,
           TRY_STRPTIME(LPAD(CAST(m.matdate AS VARCHAR), 8, '0'), '%y%m%d') as mat_dt,
           DATE '{rdate}' - TRY_STRPTIME(LPAD(CAST(m.matdate AS VARCHAR), 8, '0'), '%y%m%d') as rmaindt,
           a.curbal + COALESCE(m.intpay, 0) as balance_calc
    FROM bnm_cafd a
    INNER JOIN read_parquet('{FD_FD}') m ON a.acctno = m.acctno
    WHERE a.cistype = 'FD'
""")

for pmtyp, pmtype in [(1, 'C'), (2, 'D')]:
    fd_recs = con.execute(f"""
        SELECT custno, fundmne, fundname, depo_dt, mat_dt, term, 
               COALESCE(matid, 'M') as matid, rmaindt, curbal, rate, balance_calc
        FROM arfd
        WHERE pmtyp = {pmtyp}
        ORDER BY fundname, acctno
    """).fetchall()
    
    fd_sum = con.execute(f"""
        SELECT SUM(term), SUM(rmaindt), SUM(curbal), SUM(balance_calc)
        FROM arfd WHERE pmtyp = {pmtyp}
    """).fetchone()
    
    with open(SASLIST_REPORT, 'a') as f:
        ftype = 'PUBLIC' if pmtyp == 1 else 'NON-PUBLIC'
        f.write(f'({pmtype}) {ftype} MUTUAL FUND PORTFOLIO\n')
        
        current_fund = None
        fund_term, fund_rmain, fund_curbal, fund_bal = 0, 0, 0, 0
        
        for rec in fd_recs:
            if current_fund != rec[2]:
                if current_fund:
                    f.write(f'SUBTOTAL                                                                                      ;{fund_term:6d} M        ;{fund_rmain:6d}                   ;{fund_curbal:23,.2f}; -            ;{fund_bal:23,.2f}\n\n')
                current_fund = rec[2]
                fund_term, fund_rmain, fund_curbal, fund_bal = 0, 0, 0, 0
            
            depdte = rec[3].strftime('%d/%m/%Y') if rec[3] else ''
            matdte = rec[4].strftime('%d/%m/%Y') if rec[4] else ''
            
            f.write(f'{rec[0]:14d};{rec[1]:22s};{rec[2]:50s};{depdte:18s};{matdte:18s};{rec[5]:6d} {rec[6]:1s}       ;{rec[7]:6d}                   ;{rec[8]:23,.2f};{rec[9]:15.2f};{rec[10]:23,.2f}\n')
            
            fund_term += rec[5] or 0
            fund_rmain += rec[7] or 0
            fund_curbal += rec[8] or 0
            fund_bal += rec[10] or 0
        
        if current_fund:
            f.write(f'SUBTOTAL                                                                                      ;{fund_term:6d} M        ;{fund_rmain:6d}                   ;{fund_curbal:23,.2f}; -            ;{fund_bal:23,.2f}\n\n')
        
        f.write(f'SUBTOTAL ({pmtype})                                                                               ;{fd_sum[0]:6d} M        ;{fd_sum[1]:6d}                   ;{fd_sum[2]:23,.2f}; -            ;{fd_sum[3]:23,.2f}\n\n')

fd_total = con.execute("""
    SELECT SUM(term), SUM(rmaindt), SUM(curbal), SUM(balance_calc)
    FROM arfd
""").fetchone()

grand_total = con.execute("""
    SELECT SUM(CASE WHEN cistype='FD' THEN term ELSE 0 END),
           SUM(CASE WHEN cistype='FD' THEN rmaindt ELSE 0 END),
           SUM(curbal), SUM(balance)
    FROM bnm_cafd a
    LEFT JOIN arfd f ON a.acctno = f.acctno
""").fetchone()

with open(SASLIST_REPORT, 'a') as f:
    f.write(f'TOTAL (C)+(D)                                                                                 ;{fd_total[0]:6d} M        ;{fd_total[1]:6d}                   ;{fd_total[2]:23,.2f}; -            ;{fd_total[3]:23,.2f}\n')
    f.write(f'GRAND TOTAL (A+B+C+D)                                        ;;                  ;{grand_total[0]:6d} M        ;{grand_total[1]:6d}                   ;{grand_total[2]:23,.2f}; -            ;{grand_total[3]:23,.2f}\n')

con.close()

print(f"\nProcessing complete!")
print(f"Output files:")
print(f"  - {BNM_CAFD}")
print(f"  - {SASLIST_REPORT}")
