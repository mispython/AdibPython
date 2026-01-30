import duckdb
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Format mappings (PBBBTFMT, PBBLNFMT)
LIAB_MAP = {}      # Liability code to facility mapping
NSRSLIAB_MAP = {}  # NSRS facility code mapping
BTFCEPT_MAP = {}   # BT finance concept mapping
DAYR_MAP = {}      # Days to arrears range mapping

con = duckdb.connect()

# ============================================================================
# GET REPORTING DATE
# ============================================================================

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/loan/reptdate.parquet')").fetchone()[0]
day = reptdate.day

if 1 <= day <= 8:
    nowk, nowk1 = '1', '4'
elif 9 <= day <= 15:
    nowk, nowk1 = '2', '1'
elif 16 <= day <= 22:
    nowk, nowk1 = '3', '2'
else:
    nowk, nowk1 = '4', '3'

mm = reptdate.month
mm1 = mm - 1 if nowk != '1' else (12 if mm == 1 else mm - 1)

reptmon, reptmon1 = f"{mm:02d}", f"{mm1:02d}"
reptyear, reptyea2 = f"{reptdate.year % 100:02d}", f"{reptdate.year % 100:02d}"
reptday = f"{day:02d}"
rdate = int((reptdate - reptdate.replace(month=1, day=1)).days) + 1

print(f"Report: {reptday}/{reptmon}/{reptyear}, Week: {nowk}, PrevWeek: {nowk1}")

# Create format tables
con.executemany("CREATE TEMP TABLE liab_fmt(k VARCHAR, v VARCHAR); INSERT INTO liab_fmt VALUES(?,?)",
                [(k,v) for k,v in LIAB_MAP.items()])
con.executemany("CREATE TEMP TABLE nsrsliab_fmt(k VARCHAR, v VARCHAR); INSERT INTO nsrsliab_fmt VALUES(?,?)",
                [(k,v) for k,v in NSRSLIAB_MAP.items()])
con.executemany("CREATE TEMP TABLE btfcept_fmt(k VARCHAR, v VARCHAR); INSERT INTO btfcept_fmt VALUES(?,?)",
                [(k,v) for k,v in BTFCEPT_MAP.items()])

# ============================================================================
# LOAD MASTER DATA (MAST)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE mast AS
    SELECT CAST(acctnox AS BIGINT) acctno, ficode branch, 
           CASE WHEN retailid='C' THEN 999 ELSE 0 END apcode,
           COALESCE(custcode, 99) custcode,
           CASE WHEN sector IN ('  ','') THEN '9999' ELSE sector END sector,
           '     ' ficody, 0 oldbrh
    FROM read_parquet('{INPUT_DIR}/btrsa/mast{reptyear}{reptmon}{reptday}.parquet')
    WHERE CAST(acctnox AS BIGINT) > 2500000000
""")

# ============================================================================
# LOAD CREDIT DATA (CRED) - CALCULATE ARREARS
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE cred AS
    SELECT CAST(acctnox AS BIGINT) acctno, transref, outstand, 
           CASE WHEN maturedx IN ('000000','      ','') THEN 99999
                ELSE TRY_STRPTIME(maturedx, '%y%m%d') END matureds,
           CASE WHEN matureds > 0 AND DATE'{reptdate}' >= matureds 
                THEN DATE'{reptdate}' - matureds + 1 ELSE 0 END nodays,
           CASE WHEN nodays > 0 THEN 1 ELSE 0 END instalm,
           SUBSTRING(transref, 1, 7) transrex, outstand outstandx
    FROM read_parquet('{INPUT_DIR}/btrsa/cred{reptyear}{reptmon}{reptday}.parquet')
    WHERE CAST(acctnox AS BIGINT) > 2500000000 
      AND CAST(acctnox AS BIGINT) != 2501900811
      AND transref != '   ' AND outstand >= 0
""")

# Calculate arrears from days
con.execute("""
    CREATE TEMP TABLE cred_arr AS
    SELECT *, 
           CASE WHEN nodays > 0 THEN 
                CASE WHEN nodays <= 30 THEN 1
                     WHEN nodays <= 60 THEN 2
                     WHEN nodays <= 90 THEN 3
                     WHEN nodays <= 120 THEN 4
                     WHEN nodays <= 150 THEN 5
                     WHEN nodays <= 180 THEN 6
                     ELSE ROUND(nodays / 30.0) END
                ELSE 0 END arrears
    FROM cred
""")

# ============================================================================
# MERGE WITH INTEREST RATES AND PREVIOUS BALANCE
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE intrt AS
    SELECT CAST(acctnox AS BIGINT) acctno, SUBSTRING(transref,1,7) transrex,
           intrate, outstand, liabcode
    FROM read_parquet('{INPUT_DIR}/bnmd/btdtl{reptyea2}{reptmon}{reptday}.parquet')
""")

con.execute("""
    CREATE TEMP TABLE cred_full AS
    SELECT c.*, i.intrate, i.liabcode,
           COALESCE(c.outstand, 0) balance, 0.0 unearned, 0.0 repaid, 0.0 disburse
    FROM cred_arr c
    LEFT JOIN intrt i ON c.acctno = i.acctno AND c.transrex = i.transrex
""")

# ============================================================================
# LOAD SUBACCOUNT DATA (SUBA)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE suba AS
    SELECT CAST(acctnox AS BIGINT) acctno, transref
    FROM read_parquet('{INPUT_DIR}/btrsa/suba{reptyear}{reptmon}{reptday}.parquet')
    WHERE CAST(acctnox AS BIGINT) > 2500000000 
      AND CAST(acctnox AS BIGINT) != 2501900811
      AND transref != '   '
""")

con.execute(f"""
    CREATE TEMP TABLE suba9 AS
    SELECT CAST(acctnox AS BIGINT) acctno, limtcurm, limtcurf, creatds
    FROM read_parquet('{INPUT_DIR}/btrsa/suba{reptyear}{reptmon}{reptday}.parquet')
    WHERE CAST(acctnox AS BIGINT) > 2500000000 
      AND CAST(acctnox AS BIGINT) != 2501900811
      AND subacct = 'OV' AND transref = '   '
""")

# ============================================================================
# CREATE ACCOUNT LEVEL DATA
# ============================================================================

con.execute("""
    CREATE TEMP TABLE acct AS
    SELECT m.*, s.limtcurm, s.limtcurf, s.creatds,
           'MYR' currency,
           s.limtcurm * 100 apprlimt,
           s.limtcurf * 100 apprlim2,
           CASE WHEN s.creatds > 0 THEN
                SUBSTRING(LPAD(CAST(s.creatds AS VARCHAR),6,'0'),5,2) issuedd,
                SUBSTRING(LPAD(CAST(s.creatds AS VARCHAR),6,'0'),3,2) issuemm,
                '20' issueya,
                SUBSTRING(LPAD(CAST(s.creatds AS VARCHAR),6,'0'),1,2) issueyy
                ELSE '00' issuedd, '00' issuemm, '00' issueya, '00' issueyy END,
           0.0 lmtamt
    FROM mast m
    JOIN suba9 s ON m.acctno = s.acctno
""")

# Handle USD account
con.execute("""
    UPDATE acct SET currency = 'USD', apprlim2 = limtcurm * 100, apprlimt = limtcurf * 100
    WHERE acctno = 2500000002
""")

# Write ACCOUNT file
with open(OUTPUT_DIR/'account.txt','w') as f:
    for r in con.execute("SELECT * FROM acct").fetchall():
        f.write(f"{r[0]:5s}{r[1]:04d}{r[2]:03d}{r[3]:010d}"
                f"                              {r[7]:3s}{r[8]:024.0f}{r[9]:016.0f}"
                f"{r[10]:02s}{r[11]:02s}{r[12]:02s}{r[13]:02s}{r[6]:05d}{r[14]:016.0f}\n")

# ============================================================================
# MERGE CRED WITH SUBA AND APPLY FACILITY CODES
# ============================================================================

con.execute("""
    CREATE TEMP TABLE btr2 AS
    SELECT c.*, s.transref, 
           COALESCE(l.v, '') facility,
           COALESCE(n.v, '') faccode,
           CASE WHEN c.nodays > 0 AND c.outstand < 1 THEN 0 ELSE c.arrears END arrears_adj,
           CASE WHEN c.nodays > 0 AND c.outstand < 1 THEN 0 ELSE c.instalm END instalm_adj,
           CASE WHEN c.nodays > 0 AND c.outstand < 1 THEN 0 ELSE c.nodays END nodays_adj,
           c.nodays nodayx,
           CASE WHEN facility = '34470' THEN balance + unearned ELSE balance END balance_adj,
           CASE WHEN c.liabcode IN ('DAU','DDU','DDT','POS') THEN '59'
                WHEN c.liabcode IN ('MCF','MDL','MFL','MLL','MOD','MOF','MOL','IEF',
                     'BAE','BAI','BAP','BAS') THEN '53'
                WHEN c.liabcode IN ('VAL','DIL','FIL','TML','TMC','TMO','TNL','TNC','TND') THEN '79'
                ELSE '00' END typeprc
    FROM cred_full c
    JOIN suba s ON c.acctno = s.acctno AND c.transref = s.transref
    LEFT JOIN liab_fmt l ON c.liabcode = l.k
    LEFT JOIN nsrsliab_fmt n ON c.liabcode = n.k
    WHERE SUBSTRING(COALESCE(l.v, ''), 1, 2) = '34'
""")

# Handle UTRDF refinancing codes
con.execute("""
    UPDATE btr2 SET faccode = 
        CASE WHEN liabcode IN ('BAE','BEI') THEN '34471'
             WHEN liabcode IN ('BAI','BII') THEN '34472'
             WHEN liabcode IN ('BAP','BAS','BPI','BSI') THEN '34475'
             ELSE faccode END
    WHERE utrdf = 'R'
""")

# ============================================================================
# AGGREGATE BY ACCOUNT AND FACILITY
# ============================================================================

con.execute("""
    CREATE TEMP TABLE btr3a AS
    SELECT acctno, facility, 
           SUM(balance_adj) outstand, SUM(instalm_adj) instalm,
           SUM(unearned) unearned, SUM(repaid) repaid, SUM(disburse) disburse
    FROM btr2
    GROUP BY acctno, facility
""")

con.execute("""
    CREATE TEMP TABLE btr3b AS
    SELECT acctno, SUM(intrate) intrate, COUNT(*) freq
    FROM cred_full
    GROUP BY acctno
""")

con.execute("""
    CREATE TEMP TABLE btr3 AS
    SELECT a.*, COALESCE(b.intrate / b.freq, 0) intratex
    FROM btr3a a
    LEFT JOIN btr3b b ON a.acctno = b.acctno
""")

# Get max days per account-facility
con.execute("""
    CREATE TEMP TABLE btr2x AS
    SELECT acctno, facility, MAX(nodays_adj) max_nodays, 
           MAX(arrears_adj) arrears, ANY_VALUE(nodayx) nodayx, 
           ANY_VALUE(faccode) faccode, ANY_VALUE(typeprc) typeprc
    FROM btr2
    GROUP BY acctno, facility
""")

# ============================================================================
# CREATE FINAL SUBACCOUNT CREDIT DATA
# ============================================================================

con.execute("""
    CREATE TEMP TABLE subcr AS
    SELECT x.*, a.outstand, a.instalm, 
           a.unearned * 100 unearned, a.repaid * 100 repaid, 
           a.disburse * 100 disburse, a.intratex * 100 intratex,
           '    ' noteno,
           CASE WHEN x.facility IN ('34810','34831','34832','34840','34850','34860')
                THEN 0 ELSE x.arrears END arrears_final
    FROM btr2x x
    JOIN btr3 a ON x.acctno = a.acctno AND x.facility = a.facility
""")

# ============================================================================
# MERGE WITH MASTER AND CREATE SUBA OUTPUT
# ============================================================================

con.execute("""
    CREATE TEMP TABLE suba_out AS
    SELECT m.*, s.*, a.apprlim2,
           ' 00000000 00000000' dataxx, 0.0 odxsamt, 0.0 biltot,
           'O' acctstat, 12 noteterm, '00' fconcept,
           COALESCE(m.syndicat, 'N') syndicat,
           CASE WHEN m.specialf IN ('N',' ') THEN '00' ELSE m.specialf END specialf,
           CASE WHEN m.purposes IN ('   ','0000') THEN '5300' ELSE m.purposes END purposes,
           CASE WHEN m.payfreqc = '  ' THEN '19' ELSE m.payfreqc END payfreqc
    FROM mast m
    JOIN subcr s ON m.acctno = s.acctno
    LEFT JOIN acct a ON m.acctno = a.acctno
""")

# Apply facility-specific rules
con.execute("""
    UPDATE suba_out SET faccode = '34480', fconcept = '99'
    WHERE liabcode IN ('FTI','FTL')
""")

# Load shared security status
con.execute(f"""
    CREATE TEMP TABLE btrsm AS
    SELECT acctno, 
           CASE WHEN sm_date > 0 THEN
                LPAD(CAST(EXTRACT(DAY FROM sm_date) AS VARCHAR),2,'0')||
                LPAD(CAST(EXTRACT(MONTH FROM sm_date) AS VARCHAR),2,'0')||
                CAST(EXTRACT(YEAR FROM sm_date) AS VARCHAR)
                ELSE '00000000' END sm_dat1,
           COALESCE(sm_status, 'N') sm_status
    FROM read_parquet('{INPUT_DIR}/loan/lnacct.parquet')
    WHERE acctno BETWEEN 2500000000 AND 2599999999
""")

# Calculate undrawn
con.execute("""
    CREATE TEMP TABLE subq AS
    SELECT acctno, SUM(outstand) outx
    FROM suba_out
    GROUP BY acctno
""")

con.execute("""
    CREATE TEMP TABLE suba_final AS
    SELECT s.*, q.outx, s.apprlim2 - COALESCE(q.outx, 0) undrawn,
           COALESCE(b.sm_dat1, '00000000') sm_dat1,
           COALESCE(b.sm_status, 'N') sm_status,
           '000000000000000' rmsbba
    FROM suba_out s
    LEFT JOIN subq q ON s.acctno = q.acctno
    LEFT JOIN btrsm b ON s.acctno = b.acctno
""")

# Write CREDIT file
con.execute(f"CREATE TABLE credit_out AS SELECT * FROM suba_final")

with open(OUTPUT_DIR/'credit.txt','w') as f:
    for r in con.execute("""
        SELECT ficody, branch, apcode, acctno, noteno, facility, outstand,
               arrears_final, instalm, undrawn, acctstat, nodays_adj, oldbrh,
               biltot, odxsamt, balance_adj curbal, 0 intamt, 0 feeamt, 
               repaid, disburse, faccode
        FROM suba_final
    """).fetchall():
        f.write(f"{r[0]:5s}{r[1]:04d}{r[2]:03d}{r[3]:010d}"
                f"                              {r[4]:5s}{r[5]:5s}"
                f"                    {reptday}{reptmon}{reptyear}"
                f"{r[6]:016.0f}{r[7]:03d}{r[8]:03d}{r[9]:017.0f}{r[10]:1s}{r[11]:05d}"
                f"{r[12]:05d}{r[13]:017.0f}{r[14]:017.0f}"
                f"                                                     "
                f"{r[15]:017.0f}{r[16]:017.0f}{r[17]:017.0f}{r[18]:015.0f}{r[19]:015.0f}"
                f"                {r[20]:5s}\n")

# Write SUBACCOUNT file
with open(OUTPUT_DIR/'subaccount.txt','w') as f:
    for r in con.execute("""
        SELECT ficody, branch, apcode, acctno, noteno, facility, facility,
               syndicat, specialf, purposes, fconcept, noteterm, payfreqc, dataxx,
               custcode, sector, oldbrh, unearned, sm_status, sm_dat1, rmsbba,
               intratex, typeprc, faccode, sector sectfiss, custcode custfiss
        FROM suba_final
    """).fetchall():
        f.write(f"{r[0]:5s}{r[1]:04d}{r[2]:03d}{r[3]:010d}"
                f"                              {r[4]:5s}{r[5]:5s}"
                f"                         {r[6]:5s}{r[7]:1s}{r[8]:2s}{r[9]:4s}"
                f"{r[10]:02d}{r[11]:03d}{r[12]:2s}{r[13]:18s}{r[14]:02d}{r[15]:4s}"
                f"{r[16]:05d}{r[17]:017.0f}{r[18]:1s}{r[19]:8s}"
                f"                                            {r[20]:15s}"
                f"{r[21]:5.2f}{r[22]:2s}{r[23]:5s}"
                f"      {r[24]:4s}{r[25]:02d}\n")

print(f"CREDIT: {con.execute('SELECT COUNT(*) FROM suba_final').fetchone()[0]} records")

# ============================================================================
# PROVISIONS (NPL ACCOUNTS)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE provi AS
    SELECT CAST(p.acctnox AS BIGINT) acctno, SUBSTRING(p.transref,1,7) transrex,
           p.prinamt, p.intamt, p.iisamt, p.totiisr, p.writoff, p.nplind
    FROM read_parquet('{INPUT_DIR}/btrsa/prov{reptyear}{reptmon}{reptday}.parquet') p
    WHERE CAST(p.acctnox AS BIGINT) > 2500000000
""")

con.execute("""
    CREATE TEMP TABLE provi_merged AS
    SELECT p.*, b.facility, b.nodayx nodays, b.arrears, b.outstand, b.faccode, b.liabcode
    FROM provi p
    JOIN btr2 b ON p.acctno = b.acctno AND p.transrex = b.transrex
    WHERE b.nodays > 89 OR p.nplind IN ('P','F')
""")

# Clean up days for paid/fraud
con.execute("""
    UPDATE provi_merged SET nodays = 0 
    WHERE nplind IN ('P','F') OR outstand < 1
""")

# Aggregate provisions
con.execute("""
    CREATE TEMP TABLE provis AS
    SELECT acctno, facility,
           SUM(prinamt) prinamt, SUM(intamt) intamt, 
           SUM(iisamt) iisamt, SUM(totiisr) totiisr, SUM(writoff) writoff
    FROM provi_merged
    GROUP BY acctno, facility
""")

# Get max days for classification
con.execute("""
    CREATE TEMP TABLE provi_final AS
    SELECT p.*, ps.prinamt, ps.intamt, ps.iisamt, ps.totiisr, ps.writoff,
           CASE WHEN (p.nodays BETWEEN 90 AND 182) OR p.nplind = 'F' THEN 'D'
                WHEN p.nodays > 182 THEN 'B'
                WHEN p.nplind = 'P' THEN 'P'
                WHEN p.outstand < 1 THEN 'P'
                END classify
    FROM (SELECT acctno, facility, MAX(nodays) nodays, ANY_VALUE(nplind) nplind,
                 ANY_VALUE(outstand) outstand, ANY_VALUE(faccode) faccode,
                 ANY_VALUE(arrears) arrears, ANY_VALUE(liabcode) liabcode
          FROM provi_merged GROUP BY acctno, facility) p
    JOIN provis ps ON p.acctno = ps.acctno AND p.facility = ps.facility
""")

# Merge with master
con.execute("""
    CREATE TEMP TABLE provi_out AS
    SELECT p.*, m.ficody, m.branch, m.apcode, m.retailid,
           p.writoff * 100 totwof, p.totiisr * (-100) totiisr_adj,
           p.prinamt * 100 curbal, p.intamt * 100 intamt_adj, p.iisamt * 100 totiis,
           {reptday} reptday, {reptmon} reptmon, {reptyear} reptyear,
           ' ' gp3ind, 0 oldbrh, 0 realisvl, 0 iisdanah, 0 feeamt,
           0 iistrans, 0 spopbal, 0 spcharge, 0 spwoamt, 0 spwbamt, 0 sptrans, 0 spdanah, 0 iisopbal
    FROM provi_final p
    JOIN mast m ON p.acctno = m.acctno
""")

# Adjust for special facilities
con.execute("""
    UPDATE provi_out SET arrears = 0
    WHERE facility IN ('34810','34831','34832','34840','34850','34860')
""")

# Write PROVISION file
with open(OUTPUT_DIR/'provision.txt','w') as f:
    for r in con.execute("""
        SELECT ficody, branch, apcode, acctno, facility, reptday, reptmon, reptyear,
               classify, arrears, curbal, intamt_adj, feeamt, realisvl, iisopbal,
               totiis, totiisr_adj, totwof, iisdanah, iistrans, spopbal, spcharge,
               spwbamt, spwoamt, spdanah, sptrans, gp3ind, oldbrh, faccode
        FROM provi_out
    """).fetchall():
        f.write(f"{r[0]:5s}{r[1]:04d}{r[2]:03d}{r[3]:010d}"
                f"                              {r[4]:5s}"
                f"                    {r[5]:02d}{r[6]:02d}{r[7]:02d}"
                f"    {r[8]:1s}{r[9]:03d}{r[10]:017.0f}{r[11]:017.0f}{r[12]:016.0f}"
                f"{r[13]:017.0f}{r[14]:017.0f}{r[15]:017.0f}{r[16]:017.0f}{r[17]:017.0f}"
                f"{r[18]:017.0f}{r[19]:017.0f}{r[20]:017.0f}{r[21]:017.0f}{r[22]:017.0f}"
                f"{r[23]:017.0f}{r[24]:017.0f}{r[25]:017.0f}{r[26]:1s}{r[27]:05d}"
                f"      {r[28]:5s}\n")

print(f"PROVISION: {con.execute('SELECT COUNT(*) FROM provi_out').fetchone()[0]} records")

con.close()
print(f"\nCompleted: ACCOUNT, CREDIT, SUBACCOUNT, PROVISION files in {OUTPUT_DIR}")
