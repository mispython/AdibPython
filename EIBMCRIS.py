import duckdb
from pathlib import Path
from datetime import datetime

# ============================================================================
# PATH CONFIGURATION - Edit these paths to match your environment
# ============================================================================

# Base directories
BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'

# Input paths
LOAN_LNNOTE = INPUT_DIR / 'loan' / 'lnnote.parquet'
LNADDR_LNNAME = INPUT_DIR / 'lnaddr' / 'lnname.parquet'
BNM_LOAN = INPUT_DIR / 'bnm' / 'loan{reptmon}{nowk}.parquet'
BNM_ULOAN = INPUT_DIR / 'bnm' / 'uloan{reptmon}{nowk}.parquet'
DEPOSIT_TXT = INPUT_DIR / 'deposit.txt'
MNICS_CIS = INPUT_DIR / 'mnics' / 'cis.parquet'
ODADDR_SAVINGS = INPUT_DIR / 'odaddr' / 'savings.parquet'
OD_OVERDFT = INPUT_DIR / 'od' / 'overdft.parquet'

# Output paths
SAVE_LOAN = OUTPUT_DIR / 'save_loan.parquet'
PAGE02_PARQUET = OUTPUT_DIR / 'page02.parquet'
PAGE04_PARQUET = OUTPUT_DIR / 'page04.parquet'
PAGE01_TXT = OUTPUT_DIR / 'page01.txt'
PAGE02_TXT = OUTPUT_DIR / 'page02.txt'
PAGE03_TXT = OUTPUT_DIR / 'page03.txt'
PAGE04_TXT = OUTPUT_DIR / 'page04.txt'

# Create output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================================

# Initialize DuckDB connection
con = duckdb.connect()

# Get reporting date and derive variables (mimics SAS REPTDATE logic)
# In production, you would read from LOAN.REPTDATE dataset
# For now, using current date or you can hardcode a specific date
reptdate_value = con.execute("SELECT CURRENT_DATE").fetchone()[0]
# reptdate_value = datetime(2026, 1, 23).date()  # Uncomment to use specific date

day = reptdate_value.day
nowk = '1' if day == 8 else '2' if day == 15 else '3' if day == 22 else '4'
reptyear = str(reptdate_value.year)
reptmon = f"{reptdate_value.month:02d}"
reptday = f"{reptdate_value.day:02d}"
reptdte = reptday + reptmon + reptyear
reptdate_str = f"'{reptdate_value}'"  # For SQL queries

# Format mappings
payfq_map = {'1': 'M', '2': 'Q', '3': 'S', '4': 'A', '5': 'B', '9': 'I'}
risk_map = {1: 'C', 2: 'S', 3: 'D', 4: 'B'}

# Read and process loan data
con.execute(f"""
    CREATE TEMP TABLE lnnote AS 
    SELECT * FROM read_parquet('{LOAN_LNNOTE}')
    ORDER BY acctno
""")

con.execute(f"""
    CREATE TEMP TABLE lnname AS 
    SELECT * FROM read_parquet('{LNADDR_LNNAME}')
    ORDER BY acctno
""")

# Merge loan name and address
con.execute("""
    CREATE TEMP TABLE lnaddr AS
    SELECT n.acctno, n.taxno AS oldic, n.guarend AS newic,
           n.nameln1, n.nameln2, n.nameln3, n.nameln4, n.nameln5
    FROM lnnote ln
    INNER JOIN lnname n ON ln.acctno = n.acctno
""")

# Process main loan data
bnm_loan_path = str(BNM_LOAN).format(reptmon=reptmon, nowk=nowk)
bnm_uloan_path = str(BNM_ULOAN).format(reptmon=reptmon, nowk=nowk)

con.execute(f"""
    CREATE TEMP TABLE loan_base AS
    SELECT *, 
           CASE WHEN source = 'uloan' THEN commno ELSE noteno END AS noteno
    FROM (
        SELECT *, 'loan' AS source FROM read_parquet('{bnm_loan_path}')
        UNION ALL
        SELECT *, 'uloan' AS source FROM read_parquet('{bnm_uloan_path}')
    )
    WHERE custcode NOT IN (1,2,3,10,11,12,81,91)
""")

# Handle duplicate product codes
con.execute("""
    CREATE TEMP TABLE loan_dedup AS
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY acctno ORDER BY acctno) AS rn
        FROM loan_base
        WHERE prodcd IN ('34190','34690')
    ) WHERE rn = 1
    UNION ALL
    SELECT *, 1 AS rn FROM loan_base WHERE prodcd NOT IN ('34190','34690')
""")

# Calculate total limits
con.execute("""
    CREATE TEMP TABLE temp AS
    SELECT acctno, 
           SUM(apprlimt) AS totlimt,
           COUNT(*) AS loancnt
    FROM loan_dedup
    GROUP BY acctno
""")

# Read GP3 data
con.execute(f"""
    CREATE TEMP TABLE gp3klu AS
    SELECT 
        CAST(SUBSTRING(line, 1, 3) AS INTEGER) AS branch,
        CAST(SUBSTRING(line, 4, 10) AS BIGINT) AS acctno,
        CAST(SUBSTRING(line, 14, 5) AS INTEGER) AS zero5,
        CAST(SUBSTRING(line, 19, 2) AS INTEGER) AS rptday,
        CAST(SUBSTRING(line, 21, 2) AS INTEGER) AS rptmon,
        CAST(SUBSTRING(line, 23, 4) AS INTEGER) AS rptyear,
        CAST(SUBSTRING(line, 27, 12) AS DECIMAL(12,2)) AS scv,
        CAST(SUBSTRING(line, 39, 12) AS DECIMAL(14,2)) AS iis,
        CAST(SUBSTRING(line, 51, 12) AS DECIMAL(14,2)) AS iisr,
        CAST(SUBSTRING(line, 63, 12) AS DECIMAL(14,2)) AS iisw,
        CAST(SUBSTRING(line, 75, 12) AS DECIMAL(14,2)) AS prinwoff,
        SUBSTRING(line, 87, 1) AS gp3ind,
        NULL AS noteno
    FROM read_csv('{DEPOSIT_TXT}', columns={{'line': 'VARCHAR'}}, header=false)
    ORDER BY acctno, noteno
""")

# Filter loans >= 1M or NPL
con.execute("""
    CREATE TEMP TABLE loan_filtered AS
    SELECT l.*, t.totlimt, t.loancnt
    FROM loan_dedup l
    INNER JOIN temp t ON l.acctno = t.acctno
    WHERE t.totlimt >= 1000000 OR l.loanstat = 3
""")

# Merge with GP3
con.execute(f"""
    CREATE TEMP TABLE loan AS
    SELECT l.*,
           g.scv, g.iis, g.iisr, g.iisw, g.prinwoff, g.gp3ind,
           CASE WHEN l.acctype != 'OD' 
                THEN l.balance + (-1)*l.curbal + (-1)*l.feeamt 
                ELSE NULL END AS intout,
           '{reptdte}' AS reptdte
    FROM loan_filtered l
    LEFT JOIN gp3klu g ON l.acctno = g.acctno AND l.noteno = g.noteno
""")

# Director info (PAGE02)
con.execute(f"""
    CREATE TEMP TABLE dirinfo AS
    SELECT DISTINCT
           l.branch, l.acctno, l.reptdte,
           CASE WHEN REGEXP_REPLACE(c.dirnic, '[0 ]', '') = '' THEN '' ELSE c.dirnic END AS dirnic,
           CASE WHEN REGEXP_REPLACE(c.diroic, '[0 ]', '') = '' THEN '' ELSE c.diroic END AS diroic,
           c.dirname
    FROM (SELECT DISTINCT branch, acctno, reptdte FROM loan) l
    LEFT JOIN read_parquet('{MNICS_CIS}') c ON l.acctno = c.acctno
""")

con.execute(f"COPY dirinfo TO '{PAGE02_PARQUET}'")

# Write PAGE02 text file
with open(PAGE02_TXT, 'w') as f:
    for row in con.execute("SELECT * FROM dirinfo ORDER BY branch, acctno").fetchall():
        line = f"0233{row[0]:05d}{row[1]:010d}{row[2]}{row[3]:20s}{row[4]:20s}  {row[5]:60s}MYN"
        f.write(line + '\n')

# Process address data
con.execute(f"""
    CREATE TEMP TABLE addr AS
    SELECT acctno, nameln2, nameln3, nameln4, nameln5, oldic, newic
    FROM lnaddr
    UNION ALL
    SELECT acctno, nameln2, nameln3, nameln4, nameln5, oldic, newic
    FROM read_parquet('{ODADDR_SAVINGS}')
""")

# Get CIS data
con.execute(f"""
    CREATE TEMP TABLE cis_temp AS
    SELECT DISTINCT acctno, name, sic
    FROM read_parquet('{MNICS_CIS}')
""")

# Process OD data
con.execute(f"""
    CREATE TEMP TABLE loan2 AS
    SELECT acctno, branch, balance, product
    FROM read_parquet('{bnm_loan_path}')
    WHERE acctype = 'OD' 
      AND (apprlimt >= 0 OR balance < 0)
      AND (acctno <= 3900000000 OR acctno > 3999999999)
""")

con.execute(f"""
    CREATE TEMP TABLE od AS
    SELECT DISTINCT acctno, excessdt, toddate, riskcode
    FROM read_parquet('{OD_OVERDFT}')
    WHERE excessdt > 0 OR toddate > 0
""")

# Calculate OD days overdue
con.execute(f"""
    CREATE TEMP TABLE loanod AS
    SELECT l.acctno,
           CASE WHEN bldate IS NOT NULL 
                THEN DATE {reptdate_str} - bldate + 1 
                ELSE NULL END AS days
    FROM (
        SELECT l.acctno,
               CASE WHEN o.excessdt > 0 AND o.toddate > 0 THEN
                    CASE WHEN excdate <= toddt THEN excdate ELSE toddt END
               WHEN o.excessdt > 0 THEN excdate
               WHEN o.toddate > 0 THEN toddt
               ELSE NULL END AS bldate
        FROM loan2 l
        LEFT JOIN (
            SELECT acctno, excessdt, toddate,
                   TRY_STRPTIME(SUBSTRING(LPAD(CAST(excessdt AS VARCHAR), 11, '0'), 1, 8), '%m%d%Y') AS excdate,
                   TRY_STRPTIME(SUBSTRING(LPAD(CAST(toddate AS VARCHAR), 11, '0'), 1, 8), '%m%d%Y') AS toddt
            FROM od
        ) o ON l.acctno = o.acctno
        WHERE l.branch IS NOT NULL AND l.product NOT IN (517, 500)
    ) l
""")

# Final loan table
con.execute(f"""
    CREATE TABLE save_loan AS
    SELECT 
        l.*,
        a.nameln2, a.nameln3, a.nameln4, a.nameln5,
        CASE WHEN REGEXP_REPLACE(a.newic, '[0 ]', '') = '' THEN '' ELSE a.newic END AS newic,
        CASE WHEN REGEXP_REPLACE(a.oldic, '[0 ]', '') = '' THEN '' ELSE a.oldic END AS oldic,
        a.nameln2 AS addr1,
        TRIM(a.nameln3) || ' ' || TRIM(a.nameln4) || ' ' || a.nameln5 AS addr2,
        c.name, c.sic,
        od.days,
        CASE WHEN l.acctno BETWEEN 2990000000 AND 2999999999 THEN 'I' ELSE 'C' END AS fitype,
        CASE WHEN l.loancnt > 1 THEN 'M' ELSE 'S' END AS loanopt,
        LPAD(CAST(EXTRACT(DAY FROM l.apprdate) AS VARCHAR), 2, '0') || 
        LPAD(CAST(EXTRACT(MONTH FROM l.apprdate) AS VARCHAR), 2, '0') || 
        CAST(EXTRACT(YEAR FROM l.apprdate) AS VARCHAR) AS apprdte,
        CASE WHEN l.acctype = 'OD' AND l.closedte > 0 THEN
            LPAD(CAST(EXTRACT(DAY FROM l.closedte) AS VARCHAR), 2, '0') || 
            LPAD(CAST(EXTRACT(MONTH FROM l.closedte) AS VARCHAR), 2, '0') || 
            CAST(EXTRACT(YEAR FROM l.closedte) AS VARCHAR)
        WHEN l.acctype = 'LN' AND l.paidind = 'P' AND l.balance <= 0 AND l.closedte > 0 THEN
            LPAD(CAST(EXTRACT(DAY FROM l.closedte) AS VARCHAR), 2, '0') || 
            LPAD(CAST(EXTRACT(MONTH FROM l.closedte) AS VARCHAR), 2, '0') || 
            CAST(EXTRACT(YEAR FROM l.closedte) AS VARCHAR)
        ELSE '' END AS closdte,
        CASE WHEN l.acctype = 'LN' THEN CAST(l.product AS VARCHAR) ELSE '34110' END AS prodcd,
        CASE WHEN l.acctype = 'LN' THEN COALESCE(payfreq_map.val, 'O') ELSE 'A' END AS payterm,
        CASE WHEN l.secure != 'S' THEN 'C' ELSE l.secure END AS secure,
        CASE WHEN l.product IN (225, 226) THEN 'C' ELSE 'N' END AS cagamas,
        COALESCE(risk_map.val, 'P') AS riskc,
        CASE 
            WHEN l.borstat = 'F' THEN 999
            WHEN l.borstat = 'W' THEN 0
            ELSE FLOOR(COALESCE(od.days, 
                CASE WHEN l.acctype = 'LN' AND l.bldate IS NOT NULL AND l.balance >= 1 
                     THEN DATE {reptdate_str} - l.bldate 
                     ELSE 0 END) / 30.00050)
        END AS mtharr
    FROM loan l
    LEFT JOIN addr a ON l.acctno = a.acctno
    LEFT JOIN cis_temp c ON l.acctno = c.acctno
    LEFT JOIN loanod od ON l.acctno = od.acctno
    LEFT JOIN (VALUES ('1','M'),('2','Q'),('3','S'),('4','A'),('5','B'),('9','I')) 
        AS payfreq_map(key, val) ON CAST(l.payfreq AS VARCHAR) = payfreq_map.key
    LEFT JOIN (VALUES (1,'C'),(2,'S'),(3,'D'),(4,'B')) 
        AS risk_map(key, val) ON l.riskrte = risk_map.key
""")

con.execute(f"COPY save_loan TO '{SAVE_LOAN}'")

# Generate PAGE01
with open(PAGE01_TXT, 'w') as f:
    for row in con.execute("""
        SELECT DISTINCT ON (branch, acctno)
            branch, acctno, reptdte, newic, oldic, name, addr1, addr2, 
            custcd, sic, fitype, loanopt, totlimt, loancnt
        FROM save_loan
        ORDER BY branch, acctno
    """).fetchall():
        line = f"0233{row[0]:05d}{row[1]:010d}{row[2]}{row[3]:20s}{row[4]:20s}  {row[5]:60s}"
        line += f"{row[6]:45s}{row[7]:90s}{row[8]:2s}MY{row[9]:04d}NN"
        line += f"{row[10]}{row[11]}{row[12]:12.0f}{row[13]:02d}N"
        f.write(line + '\n')

# Generate PAGE03
with open(PAGE03_TXT, 'w') as f:
    for row in con.execute("""
        SELECT branch, acctno, noteno, reptdte, prodcd, apprdte, closdte,
               sectorcd, noteterm, payterm, mtharr, riskc, secure, cagamas,
               curbal, intout, feeamt, balance
        FROM save_loan
        ORDER BY branch, acctno, noteno
    """).fetchall():
        line = f"0233{row[0]:05d}{row[1]:010d}{row[2]:05d}{row[3]}{row[4]:5s}"
        line += f"{row[5]:8s}{row[6]:8s}        {row[7]:4s}{row[8]:3d}"
        line += f"{row[9]}{row[10]:3d}{row[10]:3d}{row[11]}{row[12]}NN{row[13]}SNNMYR"
        line += f"{row[14]:12.0f}{row[15]:12.0f}{row[16]:12.0f}{row[17]:12.0f}N"
        f.write(line + '\n')

# Generate PAGE04
con.execute(f"""
    CREATE TEMP TABLE page04 AS
    SELECT branch, acctno, zero5, 
           '{reptdte}' AS reptdate,
           COALESCE(scv, 0) AS scv,
           COALESCE(iis, 0) AS iis,
           COALESCE(iisr, 0) AS iisr,
           COALESCE(iisw, 0) AS iisw,
           COALESCE(prinwoff, 0) AS prinwoff,
           gp3ind
    FROM gp3klu
    ORDER BY branch, acctno
""")

con.execute(f"COPY page04 TO '{PAGE04_PARQUET}'")

with open(PAGE04_TXT, 'w') as f:
    for row in con.execute("SELECT * FROM page04").fetchall():
        line = f"{row[0]:03d}{row[1]:010d}{row[2]:05d}{row[3]}"
        line += f"{row[4]:012.0f}{row[5]:014.2f}{row[6]:014.2f}{row[7]:014.2f}"
        line += f"{row[8]:014.2f}{row[9]}"
        f.write(line + '\n')

# Exclusion report
con.execute("""
    CREATE TEMP TABLE exclude AS
    SELECT p.*, o.excessdt, o.toddate
    FROM page04 p
    LEFT JOIN od o ON p.acctno = o.acctno
    WHERE o.acctno IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY p.acctno ORDER BY p.acctno) > 1
""")

print("Exceptional Report on OD:")
print(con.execute("SELECT * FROM exclude").df())

con.close()
print("\nProcessing complete. Output files generated:")
print(f"- {SAVE_LOAN}")
print(f"- {PAGE02_PARQUET}")
print(f"- {PAGE04_PARQUET}")
print(f"- {PAGE01_TXT}")
print(f"- {PAGE02_TXT}")
print(f"- {PAGE03_TXT}")
print(f"- {PAGE04_TXT}")
