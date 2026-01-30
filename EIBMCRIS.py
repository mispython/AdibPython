import duckdb
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# PBBCRFMT - Format Definitions
PAYFQ = {'1':'M', '2':'Q', '3':'S', '4':'A', '5':'B', '9':'I'}
RISK = {1:'C', 2:'S', 3:'D', 4:'B'}
LNPRODC = {}  # Add your product code mappings: {100:'34120', 200:'34130', ...}

con = duckdb.connect()

# ============================================================================
# GET REPORTING DATE
# ============================================================================

try:
    reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/loan/reptdate.parquet')").fetchone()[0]
except:
    reptdate = con.execute("SELECT CURRENT_DATE").fetchone()[0]
    print(f"Warning: Using current date {reptdate}")

day = reptdate.day
nowk = {8:'1', 15:'2', 22:'3'}.get(day, '4')
reptyear, reptmon, reptday = str(reptdate.year), f"{reptdate.month:02d}", f"{reptdate.day:02d}"
reptdte = reptday + reptmon + reptyear

print(f"Report: {reptday}/{reptmon}/{reptyear}, Week: {nowk}")

# Create format lookup tables
con.executemany("CREATE TEMP TABLE payfq_fmt(k VARCHAR, v VARCHAR); INSERT INTO payfq_fmt VALUES(?,?)", 
                [(k,v) for k,v in PAYFQ.items()])
con.executemany("CREATE TEMP TABLE risk_fmt(k INT, v VARCHAR); INSERT INTO risk_fmt VALUES(?,?)", 
                [(k,v) for k,v in RISK.items()])
con.executemany("CREATE TEMP TABLE lnprodc_fmt(k INT, v VARCHAR); INSERT INTO lnprodc_fmt VALUES(?,?)", 
                [(k,v) for k,v in LNPRODC.items()])

# ============================================================================
# BUILD BASE DATASETS
# ============================================================================

# Loan address
con.execute(f"""
    CREATE TEMP TABLE lnaddr AS
    SELECT n.acctno, n.taxno AS oldic, n.guarend AS newic, 
           n.nameln1, n.nameln2, n.nameln3, n.nameln4, n.nameln5
    FROM read_parquet('{INPUT_DIR}/loan/lnnote.parquet') ln
    JOIN read_parquet('{INPUT_DIR}/lnaddr/lnname.parquet') n ON ln.acctno = n.acctno
""")

# Load loans (regular + unsecured)
con.execute(f"""
    CREATE TEMP TABLE loan_base AS
    SELECT *, CASE WHEN src='u' THEN commno ELSE noteno END AS noteno_final
    FROM (
        SELECT *, 'r' src FROM read_parquet('{INPUT_DIR}/bnm/loan{reptmon}{nowk}.parquet')
        UNION ALL
        SELECT *, 'u' src FROM read_parquet('{INPUT_DIR}/bnm/uloan{reptmon}{nowk}.parquet')
    ) WHERE custcode NOT IN (1,2,3,10,11,12,81,91)
""")

# Deduplicate dual accounts
con.execute("""
    CREATE TEMP TABLE loan_dedup AS
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY acctno ORDER BY acctno) AS rn
        FROM loan_base WHERE prodcd IN ('34190','34690')
    ) WHERE rn = 1
    UNION ALL
    SELECT *, 1 AS rn FROM loan_base WHERE prodcd NOT IN ('34190','34690')
""")

# Aggregate limits
con.execute("CREATE TEMP TABLE temp AS SELECT acctno, SUM(apprlimt) totlimt, COUNT(*) loancnt FROM loan_dedup GROUP BY 1")

# Load GP3 fixed-width
con.execute(f"""
    CREATE TEMP TABLE gp3klu AS
    SELECT CAST(SUBSTRING(line,1,3) AS INT) branch, CAST(SUBSTRING(line,4,10) AS BIGINT) acctno,
           CAST(SUBSTRING(line,14,5) AS INT) zero5, CAST(SUBSTRING(line,27,12) AS DECIMAL(12,2)) scv,
           CAST(SUBSTRING(line,39,12) AS DECIMAL(14,2)) iis, CAST(SUBSTRING(line,51,12) AS DECIMAL(14,2)) iisr,
           CAST(SUBSTRING(line,63,12) AS DECIMAL(14,2)) iisw, CAST(SUBSTRING(line,75,12) AS DECIMAL(14,2)) prinwoff,
           SUBSTRING(line,87,1) gp3ind, NULL::INT noteno
    FROM read_csv('{INPUT_DIR}/deposit.txt', columns={{'line':'VARCHAR'}}, header=false)
""")

# Filter loans >= 1M or NPL, merge with GP3
con.execute(f"""
    CREATE TEMP TABLE loan AS
    SELECT l.*, t.totlimt, t.loancnt, g.scv, g.iis, g.iisr, g.iisw, g.prinwoff, g.gp3ind,
           CASE WHEN l.acctype!='OD' THEN l.balance-l.curbal-l.feeamt END intout, '{reptdte}' reptdte
    FROM loan_dedup l
    JOIN temp t ON l.acctno=t.acctno
    LEFT JOIN gp3klu g ON l.acctno=g.acctno AND l.noteno_final=g.noteno
    WHERE t.totlimt>=1000000 OR l.loanstat=3
""")

# ============================================================================
# O/D DAYS OVERDUE
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE loanod AS
    SELECT acctno, CASE WHEN bldate IS NOT NULL THEN DATE'{reptdate}'-bldate+1 END days
    FROM (
        SELECT l.acctno,
               CASE WHEN o.excessdt>0 AND o.toddate>0 THEN LEAST(excdate,toddt)
                    WHEN o.excessdt>0 THEN excdate WHEN o.toddate>0 THEN toddt END bldate
        FROM (SELECT DISTINCT acctno, branch, product FROM read_parquet('{INPUT_DIR}/bnm/loan{reptmon}{nowk}.parquet')
              WHERE acctype='OD' AND (apprlimt>=0 OR balance<0) 
              AND (acctno<=3900000000 OR acctno>3999999999)) l
        LEFT JOIN (SELECT acctno, excessdt, toddate,
                   TRY_STRPTIME(SUBSTRING(LPAD(CAST(excessdt AS VARCHAR),11,'0'),1,8),'%m%d%Y') excdate,
                   TRY_STRPTIME(SUBSTRING(LPAD(CAST(toddate AS VARCHAR),11,'0'),1,8),'%m%d%Y') toddt
                   FROM read_parquet('{INPUT_DIR}/od/overdft.parquet')
                   WHERE excessdt>0 OR toddate>0) o ON l.acctno=o.acctno
        WHERE l.branch IS NOT NULL AND l.product NOT IN (517,500)
    )
""")

# ============================================================================
# FINAL LOAN DATASET WITH FORMATS
# ============================================================================

con.execute(f"""
    CREATE TABLE save_loan AS
    SELECT l.*, 
           CASE WHEN REGEXP_REPLACE(a.newic,'[0 ]','')='' THEN ' ' ELSE a.newic END newic,
           CASE WHEN REGEXP_REPLACE(a.oldic,'[0 ]','')='' THEN ' ' ELSE a.oldic END oldic,
           COALESCE(a.nameln2,'') addr1,
           COALESCE(TRIM(a.nameln3)||' '||TRIM(a.nameln4)||' '||a.nameln5,'') addr2,
           COALESCE(c.name,'') name, COALESCE(c.sic,0) sic, od.days,
           CASE WHEN l.acctno BETWEEN 2990000000 AND 2999999999 THEN 'I' ELSE 'C' END fitype,
           CASE WHEN l.loancnt>1 THEN 'M' ELSE 'S' END loanopt,
           LPAD(CAST(EXTRACT(DAY FROM l.apprdate) AS VARCHAR),2,'0')||
           LPAD(CAST(EXTRACT(MONTH FROM l.apprdate) AS VARCHAR),2,'0')||
           CAST(EXTRACT(YEAR FROM l.apprdate) AS VARCHAR) apprdte,
           CASE WHEN l.acctype='OD' AND l.closedte>0 THEN
                LPAD(CAST(EXTRACT(DAY FROM l.closedte) AS VARCHAR),2,'0')||
                LPAD(CAST(EXTRACT(MONTH FROM l.closedte) AS VARCHAR),2,'0')||
                CAST(EXTRACT(YEAR FROM l.closedte) AS VARCHAR)
                WHEN l.acctype='LN' AND l.paidind='P' AND l.balance<=0 AND l.closedte>0 THEN
                LPAD(CAST(EXTRACT(DAY FROM l.closedte) AS VARCHAR),2,'0')||
                LPAD(CAST(EXTRACT(MONTH FROM l.closedte) AS VARCHAR),2,'0')||
                CAST(EXTRACT(YEAR FROM l.closedte) AS VARCHAR)
                ELSE '' END closdte,
           CASE WHEN l.acctype='LN' THEN COALESCE(pc.v,LPAD(CAST(l.product AS VARCHAR),5,'0')) ELSE '34110' END prodcd,
           CASE WHEN l.acctype='LN' THEN COALESCE(pf.v,'O') ELSE 'A' END payterm,
           CASE WHEN l.secure!='S' THEN 'C' ELSE l.secure END secure,
           CASE WHEN l.product IN (225,226) THEN 'C' ELSE 'N' END cagamas,
           COALESCE(rf.v,'P') riskc,
           CASE WHEN l.borstat='F' THEN 999 WHEN l.borstat='W' THEN 0
                ELSE FLOOR(COALESCE(od.days,CASE WHEN l.acctype='LN' AND l.bldate IS NOT NULL AND l.balance>=1 
                     THEN DATE'{reptdate}'-l.bldate ELSE 0 END)/30.00050) END mtharr
    FROM loan l
    LEFT JOIN (SELECT * FROM lnaddr UNION ALL SELECT acctno,oldic,newic,nameln1,nameln2,nameln3,nameln4,nameln5 
               FROM read_parquet('{INPUT_DIR}/odaddr/savings.parquet')) a ON l.acctno=a.acctno
    LEFT JOIN (SELECT DISTINCT acctno,name,sic FROM read_parquet('{INPUT_DIR}/mnics/cis.parquet')) c ON l.acctno=c.acctno
    LEFT JOIN loanod od ON l.acctno=od.acctno
    LEFT JOIN payfq_fmt pf ON CAST(l.payfreq AS VARCHAR)=pf.k
    LEFT JOIN risk_fmt rf ON l.riskrte=rf.k
    LEFT JOIN lnprodc_fmt pc ON l.product=pc.k
""")

con.execute(f"COPY save_loan TO '{OUTPUT_DIR}/save_loan.parquet'")

# ============================================================================
# PAGE 02: DIRECTOR INFO
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE dirinfo AS
    SELECT DISTINCT l.branch, l.acctno, l.reptdte,
           CASE WHEN REGEXP_REPLACE(c.dirnic,'[0 ]','')='' THEN ' ' ELSE c.dirnic END dirnic,
           CASE WHEN REGEXP_REPLACE(c.diroic,'[0 ]','')='' THEN ' ' ELSE c.diroic END diroic,
           COALESCE(c.dirname,'') dirname
    FROM (SELECT DISTINCT branch,acctno,reptdte FROM loan) l
    LEFT JOIN read_parquet('{INPUT_DIR}/mnics/cis.parquet') c ON l.acctno=c.acctno
    ORDER BY 1,2
""")

con.execute(f"COPY dirinfo TO '{OUTPUT_DIR}/page02.parquet'")

with open(OUTPUT_DIR/'page02.txt','w') as f:
    for r in con.execute("SELECT * FROM dirinfo").fetchall():
        f.write(f"0233{r[0]:05d}{r[1]:010d}{r[2]}{r[3]:20s}{r[4]:20s}  {r[5]:60s}MYN\n")

print(f"PAGE02: {con.execute('SELECT COUNT(*) FROM dirinfo').fetchone()[0]} records")

# ============================================================================
# PAGE 01: BORROWER PROFILE
# ============================================================================

with open(OUTPUT_DIR/'page01.txt','w') as f:
    for r in con.execute("""
        SELECT DISTINCT ON (branch,acctno) branch,acctno,reptdte,newic,oldic,name,addr1,addr2,
               custcd,sic,fitype,loanopt,totlimt,loancnt
        FROM save_loan ORDER BY branch,acctno
    """).fetchall():
        f.write(f"0233{r[0]:05d}{r[1]:010d}{r[2]}{r[3]:20s}{r[4]:20s}  {r[5]:60s}"
                f"{r[6]:45s}{r[7]:90s}{r[8]:2s}MY{r[9]:04d}NN{r[10]}{r[11]}{r[12]:12.0f}{r[13]:02d}N\n")

print(f"PAGE01: {con.execute('SELECT COUNT(DISTINCT acctno) FROM save_loan').fetchone()[0]} records")

# ============================================================================
# PAGE 03: LOAN DETAILS
# ============================================================================

with open(OUTPUT_DIR/'page03.txt','w') as f:
    for r in con.execute("""
        SELECT branch,acctno,noteno_final,reptdte,prodcd,apprdte,closdte,sectorcd,noteterm,
               payterm,mtharr,riskc,secure,cagamas,curbal,intout,feeamt,balance
        FROM save_loan ORDER BY 1,2,3
    """).fetchall():
        f.write(f"0233{r[0]:05d}{r[1]:010d}{r[2]:05d}{r[3]}{r[4]:5s}{r[5]:8s}{r[6]:8s}          "
                f"{r[7]:4s}{r[8]:3d}{r[9]}{r[10]:3d}{r[10]:3d}{r[11]}{r[12]}NN{r[13]}SNNMYR"
                f"{r[14]:12.0f}{r[15]:12.0f}{r[16]:12.0f}{r[17]:12.0f}N\n")

print(f"PAGE03: {con.execute('SELECT COUNT(*) FROM save_loan').fetchone()[0]} records")

# ============================================================================
# PAGE 04: GP3 DATA
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE page04 AS
    SELECT branch,acctno,zero5,'{reptdte}' reptdate,COALESCE(scv,0) scv,COALESCE(iis,0) iis,
           COALESCE(iisr,0) iisr,COALESCE(iisw,0) iisw,COALESCE(prinwoff,0) prinwoff,gp3ind
    FROM gp3klu ORDER BY 1,2
""")

con.execute(f"COPY page04 TO '{OUTPUT_DIR}/page04.parquet'")

with open(OUTPUT_DIR/'page04.txt','w') as f:
    for r in con.execute("SELECT * FROM page04").fetchall():
        f.write(f"{r[0]:03d}{r[1]:010d}{r[2]:05d}{r[3]}{r[4]:012.0f}{r[5]:014.2f}"
                f"{r[6]:014.2f}{r[7]:014.2f}{r[8]:014.2f}{r[9]}\n")

print(f"PAGE04: {con.execute('SELECT COUNT(*) FROM page04').fetchone()[0]} records")

# ============================================================================
# EXCEPTIONAL REPORT
# ============================================================================

exc = con.execute("""
    SELECT p.*,o.excessdt,o.toddate FROM page04 p
    LEFT JOIN (SELECT DISTINCT acctno,excessdt,toddate FROM read_parquet('{INPUT_DIR}/od/overdft.parquet')
               WHERE excessdt>0 OR toddate>0) o ON p.acctno=o.acctno
    WHERE o.acctno IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY p.acctno ORDER BY p.acctno)>1
""").fetchall()

if exc:
    print(f"\nEXCEPTIONAL O/D: {len(exc)} duplicate records")
else:
    print("\nNo exceptional O/D duplicates")

con.close()
print(f"\nCompleted: PAGE01-04 generated in {OUTPUT_DIR}")
