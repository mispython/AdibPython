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
LOAN_REPTDATE = INPUT_DIR / 'loan' / 'reptdate.parquet'
DPLP = INPUT_DIR / 'dplp' / 'dplp{reptmon}.parquet'
LNLP = INPUT_DIR / 'lnlp1' / 'lnbtran{reptyear}{reptmon}{nowk}.parquet'
DPCC = INPUT_DIR / 'dpcc' / 'dpcc{reptmon}.parquet'
CTCS = INPUT_DIR / 'ctcs' / 'ctcs{reptmon}.parquet'
LOAN_LNNOTE = INPUT_DIR / 'loan' / 'lnnote.parquet'
ILOAN_LNNOTE = INPUT_DIR / 'iloan' / 'lnnote.parquet'
RENCC_TXT = INPUT_DIR / 'rencc.txt'
RENLN_TXT = INPUT_DIR / 'renln.txt'

# Output paths
BNM_TRANX2 = OUTPUT_DIR / 'bnm' / 'tranx2.parquet'
IBNM_TRANX2 = OUTPUT_DIR / 'ibnm' / 'tranx2.parquet'
CONV_REPORT = OUTPUT_DIR / 'loan_repayment_weekly_conv.txt'
ISLAMIC_REPORT = OUTPUT_DIR / 'loan_repayment_weekly_islamic.txt'

(OUTPUT_DIR / 'bnm').mkdir(parents=True, exist_ok=True)
(OUTPUT_DIR / 'ibnm').mkdir(parents=True, exist_ok=True)

# ============================================================================
# Initialize DuckDB and setup formats
# ============================================================================
con = duckdb.connect()

# DESC format (Conventional)
con.execute("""CREATE TEMP TABLE desc_fmt AS SELECT * FROM (VALUES
    (4,'HL'),(5,'HL'),(6,'HL'),(7,'HL'),(60,'HL'),(61,'HL'),(62,'HL'),(70,'HL'),
    (200,'HL'),(201,'HL'),(202,'HL'),(203,'HL'),(204,'HL'),(205,'HL'),
    (15,'HL'),(20,'HL'),(71,'HL'),(72,'HL'),(380,'HL'),(381,'HL'),(700,'HL'),(705,'HL'),(720,'HL'),(725,'HL'),
    (25,'PL'),(26,'PL'),(27,'PL'),(28,'PL'),(29,'PL'),(30,'PL'),(31,'PL'),(32,'PL'),(33,'PL'),(34,'PL'),
    (73,'PL'),(74,'PL'),(75,'PL'),(76,'PL'),(77,'PL'),(78,'PL'),(79,'PL'),
    (320,'PL'),(321,'PL'),(322,'PL'),(323,'PL'),(324,'PL'),(355,'PL'),(356,'PL'),(357,'PL'),(358,'PL'),
    (364,'PL'),(365,'PL'),(391,'PL')
) AS t(loantype, prod)""")

# IDESC format (Islamic)
con.execute("""CREATE TEMP TABLE idesc_fmt AS SELECT * FROM (VALUES
    (110,'HL'),(111,'HL'),(112,'HL'),(113,'HL'),(114,'HL'),(115,'HL'),(116,'HL'),(117,'HL'),(118,'HL'),(119,'HL'),
    (124,'HL'),(139,'HL'),(140,'HL'),(141,'HL'),(142,'HL'),(145,'HL'),(150,'HL'),(151,'HL'),
    (128,'HL'),(130,'HL'),(131,'HL'),(132,'HL'),(196,'HL'),
    (122,'PL'),(135,'PL'),(136,'PL'),(137,'PL'),(138,'PL'),(194,'PL'),(195,'PL')
) AS t(loantype, prod)""")

# ============================================================================
# Get REPTDATE and calculate week dates
# ============================================================================
reptdate_df = pl.read_parquet(LOAN_REPTDATE)
reptdate = reptdate_df['reptdate'][0]

day = reptdate.day
mm = reptdate.month
year = reptdate.year

# Calculate PREVDATE based on week
if day == 8:
    prevdate = datetime(year, mm, 1).date()
    wk = 1
elif day == 15:
    prevdate = datetime(year, mm, 9).date()
    wk = 2
elif day == 22:
    prevdate = datetime(year, mm, 16).date()
    wk = 3
else:
    prevdate = datetime(year, mm, 23).date()
    wk = 4

nowk = f"{wk:02d}"
reptmon = f"{mm:02d}"
reptyear = f"{year % 100:02d}"
rdate = reptdate.strftime('%d%m%y')

print(f"Weekly Loan Repayment Report - {rdate}")
print(f"Week: {nowk}, Period: {prevdate} to {reptdate}")

# Build paths
dplp_path = str(DPLP).format(reptmon=reptmon)
lnlp_path = str(LNLP).format(reptyear=reptyear, reptmon=reptmon, nowk=nowk)
dpcc_path = str(DPCC).format(reptmon=reptmon)
ctcs_path = str(CTCS).format(reptmon=reptmon)

# ============================================================================
# Load DPLP (filtered by week)
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE dplp AS
    SELECT *
    FROM read_parquet('{dplp_path}')
    WHERE reptdate BETWEEN DATE '{prevdate}' AND DATE '{reptdate}'
    ORDER BY acctno, reptdate, tranamt
""")

# ============================================================================
# Load LNLP (weekly data)
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE lnlp AS
    SELECT *, reptdate as trandt
    FROM read_parquet('{lnlp_path}')
    ORDER BY acctno, reptdate, tranamt
""")

# ============================================================================
# Match LNLP and DPLP (Cash)
# ============================================================================
con.execute("""
    CREATE TEMP TABLE otcln_base AS
    SELECT l.*
    FROM lnlp l
    INNER JOIN dplp d ON l.acctno = d.acctno 
                      AND l.trandt = d.reptdate 
                      AND l.tranamt = d.tranamt
""")

# ============================================================================
# Load LNNOTE
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE lnnote AS
    SELECT acctno, noteno, loantype, costctr
    FROM read_parquet('{LOAN_LNNOTE}')
    UNION ALL
    SELECT acctno, noteno, loantype, costctr
    FROM read_parquet('{ILOAN_LNNOTE}')
    ORDER BY acctno, noteno
""")

# Classify cash transactions
con.execute("""
    CREATE TEMP TABLE otcln_classified AS
    SELECT o.*, l.loantype, l.costctr,
           'CASH' as item, 1 as seq,
           CASE WHEN (l.costctr BETWEEN 3001 AND 3998) OR l.costctr IN (4043, 4048) THEN 'I' ELSE 'C' END as bank_type
    FROM otcln_base o
    LEFT JOIN lnnote l ON o.acctno = l.acctno AND o.noteno = l.noteno
""")

con.execute("""
    CREATE TEMP TABLE otcln_conv AS
    SELECT o.*, COALESCE(d.prod, 'OT') as prod
    FROM otcln_classified o
    LEFT JOIN desc_fmt d ON o.loantype = d.loantype
    WHERE bank_type = 'C'
""")

con.execute("""
    CREATE TEMP TABLE otcln_islamic AS
    SELECT o.*, COALESCE(i.prod, 'OT') as prod
    FROM otcln_classified o
    LEFT JOIN idesc_fmt i ON o.loantype = i.loantype
    WHERE bank_type = 'I'
""")

# ============================================================================
# Load DPCC (Credit Card Cash)
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE otccc AS
    SELECT *, 'CC' as prod, 'CASH' as item, 1 as seq
    FROM read_parquet('{dpcc_path}')
    WHERE reptdate BETWEEN DATE '{prevdate}' AND DATE '{reptdate}'
""")

# ============================================================================
# Load CTCS (Cheque Transactions)
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE ctcs_base AS
    SELECT *
    FROM read_parquet('{ctcs_path}')
    WHERE reptdate BETWEEN DATE '{prevdate}' AND DATE '{reptdate}'
      AND ind != 'OC'
    ORDER BY acctno, noteno
""")

con.execute("""
    CREATE TEMP TABLE ctcs_items AS
    SELECT c.*, l.loantype, l.costctr,
           CASE 
               WHEN c.prodind = 'O' AND c.ind = 'HC' THEN 'HOUSE CHEQUE'
               WHEN c.prodind = 'O' AND c.ind = 'LC' THEN 'LOCAL CHEQUE'
               WHEN c.prodind = 'C' THEN 'CDM'
           END as item,
           CASE 
               WHEN c.prodind = 'O' AND c.ind = 'HC' THEN 2
               WHEN c.prodind = 'O' AND c.ind = 'LC' THEN 3
               WHEN c.prodind = 'C' THEN 7
           END as seq,
           CASE WHEN (l.costctr BETWEEN 3001 AND 3998) OR l.costctr IN (4043, 4048) THEN 'I' ELSE 'C' END as bank_type
    FROM ctcs_base c
    LEFT JOIN lnnote l ON c.acctno = l.acctno AND c.noteno = l.noteno
""")

con.execute("""
    CREATE TEMP TABLE ctcs_conv AS
    SELECT c.*, COALESCE(d.prod, 'OT') as prod
    FROM ctcs_items c
    LEFT JOIN desc_fmt d ON c.loantype = d.loantype
    WHERE bank_type = 'C'
""")

con.execute("""
    CREATE TEMP TABLE ctcs_islamic AS
    SELECT c.*, COALESCE(i.prod, 'OT') as prod
    FROM ctcs_items c
    LEFT JOIN idesc_fmt i ON c.loantype = i.loantype
    WHERE bank_type = 'I'
""")

# ============================================================================
# EFT (Electronic Fund Transfer)
# ============================================================================
con.execute("""
    CREATE TEMP TABLE eft_base AS
    SELECT *,
           CASE 
               WHEN channel IN (501, 502) AND trancode = 614 THEN 'ATM'
               WHEN channel IN (521, 522) AND trancode = 614 THEN 'INTERBANK'
               WHEN channel IN (513, 514) AND trancode = 614 THEN 'CDT'
           END as item,
           CASE 
               WHEN channel IN (501, 502) AND trancode = 614 THEN 4
               WHEN channel IN (521, 522) AND trancode = 614 THEN 5
               WHEN channel IN (513, 514) AND trancode = 614 THEN 8
           END as seq
    FROM lnlp
    WHERE channel IN (501, 502, 521, 522, 513, 514) AND trancode = 614
""")

con.execute("""
    CREATE TEMP TABLE eft_classified AS
    SELECT e.*, l.loantype, l.costctr,
           CASE WHEN (l.costctr BETWEEN 3001 AND 3998) OR l.costctr IN (4043, 4048) THEN 'I' ELSE 'C' END as bank_type
    FROM eft_base e
    LEFT JOIN lnnote l ON e.acctno = l.acctno AND e.noteno = l.noteno
""")

con.execute("""
    CREATE TEMP TABLE eft_conv AS
    SELECT e.*, COALESCE(d.prod, 'OT') as prod
    FROM eft_classified e
    LEFT JOIN desc_fmt d ON e.loantype = d.loantype
    WHERE bank_type = 'C'
""")

con.execute("""
    CREATE TEMP TABLE eft_islamic AS
    SELECT e.*, COALESCE(i.prod, 'OT') as prod
    FROM eft_classified e
    LEFT JOIN idesc_fmt i ON e.loantype = i.loantype
    WHERE bank_type = 'I'
""")

# ============================================================================
# RENTAS (if files exist)
# ============================================================================
if Path(RENCC_TXT).exists():
    con.execute(f"""
        CREATE TEMP TABLE rencc AS
        SELECT 
            TRY_STRPTIME(SUBSTRING(line, 21, 8), '%y%m%d') as trandt,
            CAST(SUBSTRING(line, 29, 16) AS BIGINT) as acctno,
            CAST(SUBSTRING(line, 45, 16) AS DECIMAL(18,2)) as tranamt,
            'CC' as prod, 'RENTAS' as item, 6 as seq
        FROM read_csv('{RENCC_TXT}', columns={{'line': 'VARCHAR'}}, header=false)
    """)

if Path(RENLN_TXT).exists():
    con.execute(f"""
        CREATE TEMP TABLE renln_base AS
        SELECT 
            CAST(SUBSTRING(line, 13, 5) AS INTEGER) as noteno,
            CAST(SUBSTRING(line, 18, 3) AS INTEGER) as loantype,
            TRY_STRPTIME(SUBSTRING(line, 21, 8), '%y%m%d') as trandt,
            CAST(SUBSTRING(line, 29, 11) AS BIGINT) as acctno,
            CAST(SUBSTRING(line, 40, 16) AS DECIMAL(18,2)) as tranamt,
            'RENTAS' as item, 6 as seq
        FROM read_csv('{RENLN_TXT}', columns={{'line': 'VARCHAR'}}, header=false)
    """)
    
    con.execute("""
        CREATE TEMP TABLE renln_classified AS
        SELECT r.*, l.costctr,
               CASE WHEN (l.costctr BETWEEN 3001 AND 3998) OR l.costctr IN (4043, 4048) THEN 'I' ELSE 'C' END as bank_type
        FROM renln_base r
        LEFT JOIN lnnote l ON r.acctno = l.acctno AND r.noteno = l.noteno
    """)
    
    con.execute("""
        CREATE TEMP TABLE renln_conv AS
        SELECT r.*, COALESCE(d.prod, 'OT') as prod
        FROM renln_classified r
        LEFT JOIN desc_fmt d ON r.loantype = d.loantype
        WHERE bank_type = 'C'
    """)
    
    con.execute("""
        CREATE TEMP TABLE renln_islamic AS
        SELECT r.*, COALESCE(i.prod, 'OT') as prod
        FROM renln_classified r
        LEFT JOIN idesc_fmt i ON r.loantype = i.loantype
        WHERE bank_type = 'I'
    """)

# ============================================================================
# Consolidate
# ============================================================================
con.execute("""
    CREATE TABLE tranx2_conv AS
    SELECT *, tranamt / 1000.0 as tranamt1 FROM (
        SELECT acctno, trandt, tranamt, prod, item, seq FROM otcln_conv
        UNION ALL SELECT acctno, reptdate as trandt, tranamt, prod, item, seq FROM otccc
        UNION ALL SELECT acctno, reptdate as trandt, tranamt, prod, item, seq FROM ctcs_conv
        UNION ALL SELECT acctno, trandt, tranamt, prod, item, seq FROM eft_conv
        UNION ALL SELECT acctno, trandt, tranamt, prod, item, seq FROM renln_conv
        UNION ALL SELECT acctno, trandt, tranamt, prod, item, seq FROM rencc
    ) ORDER BY seq
""")

con.execute("""
    CREATE TABLE tranx2_islamic AS
    SELECT *, tranamt / 1000.0 as tranamt1 FROM (
        SELECT acctno, trandt, tranamt, prod, item, seq FROM otcln_islamic
        UNION ALL SELECT acctno, reptdate as trandt, tranamt, prod, item, seq FROM ctcs_islamic
        UNION ALL SELECT acctno, trandt, tranamt, prod, item, seq FROM eft_islamic
        UNION ALL SELECT acctno, trandt, tranamt, prod, item, seq FROM renln_islamic
    ) ORDER BY seq
""")

con.execute(f"COPY tranx2_conv TO '{BNM_TRANX2}'")
con.execute(f"COPY tranx2_islamic TO '{IBNM_TRANX2}'")

# ============================================================================
# Generate Reports
# ============================================================================
def generate_report(table_name, title, output_file):
    results = con.execute(f"""
        SELECT seq, item, prod,
               COUNT(*) as n,
               SUM(tranamt1) as sum_tranamt
        FROM {table_name}
        WHERE prod NOT IN ('MS', 'OT')
        GROUP BY CUBE(seq, item, prod)
        ORDER BY seq NULLS LAST, prod NULLS LAST
    """).fetchall()
    
    with open(output_file, 'w') as f:
        f.write('REPORT ID : EIBQEPC2\n')
        f.write(f'{title}\n')
        f.write(f'LOAN REPAYMENT BY CUSTOMER AS AT {rdate}\n\n')
        f.write(f'{"ITEM":<20} {"PROD":<30} {"NUMBER OF TRANSACTION":>30} {"VALUE (RM\'000)":>25}\n')
        f.write('=' * 110 + '\n')
        
        current_seq = None
        for row in results:
            seq, item, prod, n, sum_val = row
            
            if seq is None and item is None and prod is None:
                f.write('-' * 110 + '\n')
                f.write(f'{"TOTAL":<50} {n:>30,} {sum_val:>25,.2f}\n')
            elif seq is not None and prod is None:
                if current_seq != seq:
                    if current_seq is not None:
                        f.write('\n')
                    current_seq = seq
                f.write(f'{item:<50} {n:>30,} {sum_val:>25,.2f}\n')
            elif prod is not None:
                f.write(f'{"  " + prod:<50} {n:>30,} {sum_val:>25,.2f}\n')

generate_report('tranx2_conv', 'PUBLIC BANK BERHAD', CONV_REPORT)
generate_report('tranx2_islamic', 'PUBLIC ISLAMIC BANK BERHAD', ISLAMIC_REPORT)

con.close()

print(f"\nProcessing complete!")
print(f"Output files:")
print(f"  - {BNM_TRANX2}")
print(f"  - {IBNM_TRANX2}")
print(f"  - {CONV_REPORT}")
print(f"  - {ISLAMIC_REPORT}")
