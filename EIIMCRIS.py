import duckdb
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# PFBCRFMT - Format Definitions (Islamic Banking)
PAYFQ = {'1':'M', '2':'Q', '3':'S', '4':'A', '5':'B', '9':'I'}
LNPRODC = {}  # Add your product code mappings

con = duckdb.connect()

# ============================================================================
# GET REPORTING DATE
# ============================================================================

reptdate = con.execute(f"SELECT reptdate FROM read_parquet('{INPUT_DIR}/loan/reptdate.parquet')").fetchone()[0]
day = reptdate.day
nowk = {8:'1', 15:'2', 22:'3'}.get(day, '4')
reptyear, reptmon, reptday = str(reptdate.year), f"{reptdate.month:02d}", f"{reptdate.day:02d}"
ryear = f"{reptdate.year % 100:02d}"
reptdte = reptday + reptmon + reptyear

prevmon = reptdate.month - 1 if reptdate.month > 1 else 12
prevmon_str = f"{prevmon:02d}"

print(f"Report: {reptday}/{reptmon}/{reptyear}, Week: {nowk}, PrevMonth: {prevmon_str}")

# Create format tables
con.executemany("CREATE TEMP TABLE payfq_fmt(k VARCHAR, v VARCHAR); INSERT INTO payfq_fmt VALUES(?,?)", 
                [(k,v) for k,v in PAYFQ.items()])
con.executemany("CREATE TEMP TABLE lnprodc_fmt(k INT, v VARCHAR); INSERT INTO lnprodc_fmt VALUES(?,?)", 
                [(k,v) for k,v in LNPRODC.items()])

# ============================================================================
# EXTRACT PERFORMING ACCOUNTS (PREVIOUS MONTH VS CURRENT)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE prev AS
    SELECT *, 'OLD' prevind FROM read_parquet('{INPUT_DIR}/npl/loan{prevmon_str}.parquet')
    WHERE loantype < 900
""")

con.execute(f"""
    CREATE TEMP TABLE curr AS
    SELECT *, 'NEW' prevind FROM read_parquet('{INPUT_DIR}/npl/loan{reptmon}.parquet')
""")

con.execute("""
    CREATE TEMP TABLE perform AS
    SELECT p.*, 0.0 marketvl
    FROM prev p JOIN curr c ON p.acctno = c.acctno
    WHERE p.prevind = 'OLD'
""")

# ============================================================================
# EXTRACT NON-PERFORMING ACCOUNTS (IIS + SP)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE iis AS
    SELECT ntbrch branch, acctno, noteno, borstat, totiis, iisp, suspend, 
           recover reciis, iispw, recc, oi oip, oisusp, oiw, oirecc, oirecv reciis2, days
    FROM read_parquet('{INPUT_DIR}/npl/iis.parquet')
""")

con.execute(f"""
    CREATE TEMP TABLE sp1 AS
    SELECT s1.*, s1o.riskopen
    FROM read_parquet('{INPUT_DIR}/npl/sp1.parquet') s1
    LEFT JOIN read_parquet('{INPUT_DIR}/npl/sp1open.parquet') s1o ON s1.acctno = s1o.acctno
""")

con.execute(f"""
    CREATE TEMP TABLE sp2 AS
    SELECT s2.*, s2o.riskopen, s1.spp1
    FROM read_parquet('{INPUT_DIR}/npl/sp2.parquet') s2
    LEFT JOIN read_parquet('{INPUT_DIR}/npl/sp2open.parquet') s2o ON s2.acctno = s2o.acctno
    LEFT JOIN (SELECT acctno, noteno, spp1 FROM read_parquet('{INPUT_DIR}/npl/sp1.parquet')) s1 
        ON s2.acctno = s1.acctno AND s2.noteno = s1.noteno
""")

con.execute("""
    CREATE TEMP TABLE sp AS
    SELECT acctno, noteno, osprin, COALESCE(marketvl,0) marketvl, spp2 spp, sppl, recover, sppw, sp,
           COALESCE(otherfee,0) otherfee,
           SUBSTR(CASE WHEN risk='SUBSTANDARD-1' THEN 'C' ELSE risk END, 1, 1) riskc,
           SUBSTR(COALESCE(riskopen,'P'), 1, 1) risko,
           sppl spmade, recover spwrback, COALESCE(spp1,0) spp1, COALESCE(spp2,0) spp2
    FROM sp2
""")

# Merge IIS with SP
con.execute("""
    CREATE TEMP TABLE npl AS
    SELECT i.*, s.osprin, s.marketvl, s.spp, s.sppl, s.recover, s.sppw, s.sp, s.otherfee,
           s.riskc, s.risko, s.spmade, s.spwrback, s.spp1, s.spp2,
           i.iisp + COALESCE(i.oip,0) iisp_new,
           i.suspend + COALESCE(i.oisusp,0) suspend_new,
           COALESCE(i.reciis,0) + COALESCE(i.reciis2,0) + COALESCE(i.recc,0) + COALESCE(i.oirecc,0) reciis_new,
           i.iispw + COALESCE(i.oiw,0) iispw_new,
           i.osprin - COALESCE(s.marketvl,0) classamt
    FROM iis i LEFT JOIN sp s ON i.acctno = s.acctno AND i.noteno = s.noteno
""")

# Add performing accounts
con.execute("""
    CREATE TEMP TABLE npl_all AS
    SELECT n.*, 'O' riskc_final FROM npl n
    UNION ALL
    SELECT p.*, 'O' riskc FROM perform p
""")

# ============================================================================
# MERGE WITH LOAN NOTE DATA
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE npl_merged AS
    SELECT n.*, 
           CASE WHEN ln.grantdt > 0 
                THEN TRY_STRPTIME(SUBSTRING(LPAD(CAST(ln.grantdt AS VARCHAR),11,'0'),1,8),'%m%d%Y')
                END apprdate,
           ln.loantype, ln.paidind, ln.borstat ln_borstat, ln.census7, ln.user5, ln.ntbrch,
           ln.lasttran, ln.mordayarr, ln.appldate, ln.lsttrncd
    FROM npl_all n
    LEFT JOIN read_parquet('{INPUT_DIR}/loan/lnnote.parquet') ln 
        ON n.acctno = ln.acctno AND n.noteno = ln.noteno
""")

# Apply risk classification logic
con.execute(f"""
    CREATE TEMP TABLE npl_final AS
    SELECT *,
           CASE WHEN loantype IN (983,993) THEN 'B'
                WHEN days < 90 AND totiis <= 0 AND sp <= 0 AND riskc IN ('S','C') AND paidind != 'P'
                     AND loantype IN (128,130,700,705,380,381) 
                     AND (ln_borstat NOT IN ('F','R','I') OR user5 != 'N') THEN 'P'
                WHEN days < 90 AND totiis <= 0 AND sp <= 0 AND riskc IN ('S','C') AND paidind != 'P'
                     AND census7 != '9' THEN 'P'
                WHEN paidind = 'P' THEN 'S'
                ELSE riskc END riskc_adj,
           CASE WHEN paidind = 'P' THEN
                TRY_STRPTIME(SUBSTRING(LPAD(CAST(lasttran AS VARCHAR),11,'0'),1,8),'%m%d%Y')
                END closedte,
           CASE WHEN mordayarr IS NOT NULL THEN mordayarr ELSE days END days_final
    FROM npl_merged
""")

# ============================================================================
# LOAD LOAN ADDRESSES
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE lnaddr AS
    SELECT n.acctno, n.taxno oldic, n.guarend newic, n.nameln1, n.nameln2, n.nameln3, n.nameln4, n.nameln5
    FROM read_parquet('{INPUT_DIR}/loan/lnnote.parquet') ln
    JOIN read_parquet('{INPUT_DIR}/name/lnname.parquet') n ON ln.acctno = n.acctno
""")

# ============================================================================
# LOAD TERM CHARGE FOR HP
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE termchg AS
    SELECT acctno, noteno, 
           CASE WHEN intamt > 0.1 THEN intamt + intearn2 ELSE 0 END termchg,
           vinno
    FROM read_parquet('{INPUT_DIR}/loan/lnnote.parquet')
    WHERE reversed != 'Y' AND noteno IS NOT NULL AND paidind != 'P' AND ntbrch IS NOT NULL
      AND loantype IN (128,130,700,705,380,381)
""")

# ============================================================================
# LOAD AND MERGE BNM LOANS (REGULAR + COMMITMENT)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE uloan_comm AS
    SELECT u.*, c.cmbrch, c.cappdate, u.commno noteno_final,
           CASE WHEN c.cappdate > 0 
                THEN TRY_STRPTIME(SUBSTRING(LPAD(CAST(c.cappdate AS VARCHAR),11,'0'),1,8),'%m%d%Y')
                END apprdate_comm,
           CASE WHEN u.branch = 0 OR u.branch IS NULL THEN c.cmbrch ELSE u.branch END branch_final
    FROM read_parquet('{INPUT_DIR}/bnm/uloan{reptmon}{nowk}.parquet') u
    LEFT JOIN read_parquet('{INPUT_DIR}/loan/lncomm.parquet') c ON u.acctno = c.acctno AND u.commno = c.commno
    WHERE u.apprlimt >= 1000000
""")

con.execute(f"""
    CREATE TEMP TABLE loan_base AS
    SELECT *, noteno noteno_final, 
           CASE WHEN product IN (128,130,700,705,380,381,709,710,750,752,760) THEN netproc ELSE orgbal END apprlimt_adj
    FROM read_parquet('{INPUT_DIR}/bnm/loan{reptmon}{nowk}.parquet')
    WHERE custcode NOT IN (1,2,3,10,11,12,81,91)
    UNION ALL
    SELECT *, commno noteno_final, apprlimt apprlimt_adj
    FROM uloan_comm
""")

# Aggregate limits
con.execute("CREATE TEMP TABLE temp AS SELECT acctno, SUM(apprlimt_adj) totlimt, COUNT(*) loancnt FROM loan_base GROUP BY 1")

# ============================================================================
# CALCULATE OSPRIN AND TOTIIS FOR HP/FL
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE loan AS
    SELECT l.*, t.totlimt, t.loancnt, n.*, tc.termchg, tc.vinno,
           '{reptdte}' reptdte, DATE'{reptdate}' reptdate_dt,
           CASE WHEN n.days_final IS NULL AND n.bldate > 0 THEN DATE'{reptdate}' - n.bldate
                WHEN n.days_final IS NULL AND n.bldate = 0 THEN 0
                ELSE n.days_final END days_calc,
           CASE WHEN l.branch IS NULL THEN n.ntbrch ELSE l.branch END branch_final
    FROM loan_base l
    JOIN temp t ON l.acctno = t.acctno
    LEFT JOIN npl_final n ON l.acctno = n.acctno AND l.noteno_final = n.noteno
    LEFT JOIN termchg tc ON l.acctno = tc.acctno AND l.noteno_final = tc.noteno
    WHERE n.acctno IS NOT NULL OR t.totlimt >= 1000000
""")

# Calculate OSPRIN for HP based on billing and term charges
con.execute("""
    CREATE TEMP TABLE loan_calc AS
    SELECT *,
           CASE WHEN loantype NOT IN (128,130,700,705,380,381) THEN curbal
                WHEN loantype IN (131,132) THEN curbal
                WHEN borstat = 'W' OR loantype IN (981,982,983,991,992,993) THEN 0
                ELSE GREATEST(COALESCE(curbal,0) - COALESCE(uhc,0) - COALESCE(totiis,0), 0) 
                END osprin_calc,
           osprin + totiis totbal
    FROM loan
""")

# ============================================================================
# PAGE 02: DIRECTOR INFO
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE dirinfo AS
    SELECT DISTINCT l.branch_final branch, l.acctno, l.reptdte, l.costctr,
           CASE WHEN REGEXP_REPLACE(c.dirnic,'[0 ]','')='' THEN ' ' ELSE c.dirnic END dirnic,
           CASE WHEN REGEXP_REPLACE(c.diroic,'[0 ]','')='' THEN ' ' ELSE c.diroic END diroic,
           COALESCE(c.dirname,'') dirname
    FROM (SELECT DISTINCT branch_final, acctno, reptdte, ntbrch, costctr FROM loan_calc) l
    LEFT JOIN read_parquet('{INPUT_DIR}/mnics/cis.parquet') c ON l.acctno = c.acctno
    ORDER BY 1,2
""")

con.execute(f"COPY dirinfo TO '{OUTPUT_DIR}/page02.parquet'")

with open(OUTPUT_DIR/'page02.txt','w') as f:
    for r in con.execute("SELECT * FROM dirinfo").fetchall():
        f.write(f"0234{r[0]:05d}{r[1]:010d}{r[2]}{r[4]:20s}{r[5]:20s}  {r[6]:60s}MY"
                f"N{r[3]:04d}\n")

print(f"PAGE02: {con.execute('SELECT COUNT(*) FROM dirinfo').fetchone()[0]} records")

# ============================================================================
# FINAL LOAN DATASET
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE save_loan AS
    SELECT l.*,
           CASE WHEN REGEXP_REPLACE(a.newic,'[0 ]','')='' THEN ' ' ELSE a.newic END newic,
           CASE WHEN REGEXP_REPLACE(a.oldic,'[0 ]','')='' THEN ' ' ELSE a.oldic END oldic,
           COALESCE(a.nameln2,'') addr1,
           COALESCE(TRIM(a.nameln3)||' '||TRIM(a.nameln4)||' '||a.nameln5,'') addr2,
           COALESCE(c.name,'') name, COALESCE(c.sic,0) sic,
           CASE WHEN l.product BETWEEN 100 AND 199 THEN 'I' ELSE 'C' END fitype,
           CASE WHEN l.loancnt > 1 THEN 'M' ELSE 'S' END loanopt,
           CASE WHEN l.apprdate > 0 THEN
                LPAD(CAST(EXTRACT(DAY FROM l.apprdate) AS VARCHAR),2,'0')||
                LPAD(CAST(EXTRACT(MONTH FROM l.apprdate) AS VARCHAR),2,'0')||
                CAST(EXTRACT(YEAR FROM l.apprdate) AS VARCHAR) END apprdte,
           CASE WHEN l.paidind = 'P' AND l.balance <= 0 AND l.closedte > 0 THEN
                LPAD(CAST(EXTRACT(DAY FROM l.closedte) AS VARCHAR),2,'0')||
                LPAD(CAST(EXTRACT(MONTH FROM l.closedte) AS VARCHAR),2,'0')||
                CAST(EXTRACT(YEAR FROM l.closedte) AS VARCHAR) END closdte,
           COALESCE(pc.v, LPAD(CAST(l.product AS VARCHAR),5,'0')) prodcd,
           CASE WHEN l.payfreq = ' ' THEN 'M' ELSE COALESCE(pf.v,'O') END payterm,
           CASE WHEN l.secure != 'S' THEN 'C' ELSE l.secure END secure,
           CASE WHEN l.borstat = 'S' THEN 'R' ELSE 'N' END restruc,
           CASE WHEN l.product IN (101,210,307,709,710,910) THEN 'C' ELSE 'N' END cagamas,
           CASE WHEN l.riskc_adj = ' ' THEN 'P' ELSE l.riskc_adj END riskc,
           FLOOR(l.days_calc / 30.00050) mtharr,
           CASE WHEN l.days_calc >= 90 OR l.borstat IN ('F','I','R') 
                OR l.loantype IN (983,993,678,679,698,699) OR l.user5 = 'N' 
                THEN 'Y' ELSE 'N' END impaired
    FROM loan_calc l
    LEFT JOIN lnaddr a ON l.acctno = a.acctno
    LEFT JOIN (SELECT DISTINCT acctno, name, sic FROM read_parquet('{INPUT_DIR}/mnics/cis.parquet')) c 
        ON l.acctno = c.acctno
    LEFT JOIN payfq_fmt pf ON CAST(l.payfreq AS VARCHAR) = pf.k
    LEFT JOIN lnprodc_fmt pc ON l.product = pc.k
    WHERE l.paidind != 'P' OR (l.paidind = 'P' AND 
          STRFTIME(l.closedte, '%Y%m') = '{reptyear}{reptmon}')
""")

# Filter with SUBA
con.execute(f"""
    CREATE TEMP TABLE save_loan_filtered AS
    SELECT s.* FROM save_loan s
    JOIN read_parquet('{INPUT_DIR}/suba/loan.parquet') sub 
        ON s.acctno = sub.acctno AND s.noteno_final = sub.noteno
""")

# Load ELDS and merge
con.execute(f"""
    CREATE TEMP TABLE elds AS
    SELECT SUBSTRING(LEFT(aano),1,13) aano, amount, aadate, reconame, reftype, 
           nmref1, nmref2, apvnme1, apvnme2, compliby, complidt, compligr
    FROM read_parquet('{INPUT_DIR}/elds2/elhpax.parquet')
    WHERE branch >= 3000
""")

con.execute("""
    CREATE TEMP TABLE final_loan AS
    SELECT s.*, e.amount apprlmaa, e.aadate, e.reconame, e.reftype,
           CASE WHEN s.loantype IN (128,131,380,700,720,983,993) THEN 34331
                WHEN s.loantype IN (130,132,381,705,725) THEN 34332 END facility,
           CASE WHEN s.loantype IN (700,720,128,131) THEN 3100
                WHEN s.loantype IN (705,725,130,132) THEN 3200
                ELSE s.crispurp END purposes
    FROM save_loan_filtered s
    LEFT JOIN elds e ON SUBSTRING(LEFT(s.vinno),1,13) = e.aano
""")

# Calculate FACCODE
con.execute("""
    CREATE TEMP TABLE final_loan2 AS
    SELECT *,
           CASE WHEN loantype IN (128,130,131,132,380,381,700,705,709,710,720,725,750,752,760,983,993,996) THEN
                CASE WHEN purposes IN ('3100','3101','3110','3120') THEN 34333
                     WHEN purposes IN ('3200','3201','3300','3900','3901','4100','5100','5200') THEN 34334
                     ELSE 0 END
                ELSE 0 END faccode_calc
    FROM final_loan
""")

# Load write-off back transactions
con.execute(f"""
    CREATE TEMP TABLE trans AS
    SELECT acctno, noteno, SUM(tranamt) tranamt
    FROM (
        SELECT * FROM read_parquet('{INPUT_DIR}/tran/lnbtran{ryear}{reptmon}01.parquet')
        UNION ALL SELECT * FROM read_parquet('{INPUT_DIR}/tran/lnbtran{ryear}{reptmon}02.parquet')
        UNION ALL SELECT * FROM read_parquet('{INPUT_DIR}/tran/lnbtran{ryear}{reptmon}03.parquet')
        UNION ALL SELECT * FROM read_parquet('{INPUT_DIR}/tran/lnbtran{ryear}{reptmon}04.parquet')
    )
    WHERE trancode IN (610,614,615,619,650,660,680)
      AND acctno IN (SELECT acctno FROM read_parquet('{INPUT_DIR}/loan/lnnote.parquet')
                     WHERE loantype IN (983,993) AND borstat != 'E')
    GROUP BY acctno, noteno
""")

con.execute("""
    CREATE TABLE final_output AS
    SELECT f.*, COALESCE(t.tranamt, 0) tranamt,
           CASE WHEN faccode_calc = 0 THEN facility ELSE faccode_calc END faccode
    FROM final_loan2 f
    LEFT JOIN trans t ON f.acctno = t.acctno AND f.noteno_final = t.noteno
""")

con.execute(f"COPY final_output TO '{OUTPUT_DIR}/save_loan.parquet'")

# ============================================================================
# PAGE 01: BORROWER PROFILE
# ============================================================================

with open(OUTPUT_DIR/'page01.txt','w') as f:
    for r in con.execute("""
        SELECT DISTINCT ON (branch_final, acctno) 
               branch_final, acctno, reptdte, newic, oldic, name, addr1, addr2,
               custcd, sic, fitype, loanopt, totlimt, loancnt, costctr
        FROM final_output ORDER BY branch_final, acctno
    """).fetchall():
        f.write(f"0234{r[0]:05d}{r[1]:010d}{r[2]}{r[3]:20s}{r[4]:20s}  {r[5]:60s}"
                f"{r[6]:45s}{r[7]:90s}{r[8]:2s}MY{r[9]:04d}NN{r[10]}{r[11]}"
                f"{r[12]:12.0f}{r[13]:02d}N{r[14]:04d}\n")

print(f"PAGE01: {con.execute('SELECT COUNT(DISTINCT acctno) FROM final_output').fetchone()[0]} records")

# ============================================================================
# PAGE 03: LOAN DETAILS (WITH PROV OUTPUT)
# ============================================================================

con.execute(f"""
    CREATE TEMP TABLE page03 AS
    SELECT *, 
           CASE WHEN riskc = 'O' THEN 'P' ELSE riskc END riskc_final,
           CASE WHEN impaired = 'Y' AND loantype NOT IN (983,993,698,699) THEN
                CASE WHEN ROUND(osprin_calc + totiis + otherfee, 2) != ROUND(balance, 2)
                     THEN balance - (osprin_calc + otherfee) ELSE totiis END
                ELSE totiis END totiis_adj,
           CASE WHEN loantype IN (983,993,698,699) AND paidind = 'P' THEN 0
                ELSE FLOOR(days_calc / 30.00050) END mth1aa
    FROM final_output
    WHERE riskc_adj != ' '
""")

with open(OUTPUT_DIR/'page03.txt','w') as f:
    for r in con.execute("""
        SELECT branch_final, acctno, noteno_final, reptdte, loantype, apprdte, closdte,
               sectorcd, noteterm, payterm, mtharr, riskc_final, secure, restruc, cagamas,
               osprin_calc, totiis_adj, otherfee, totbal, borstat, costctr, vinno, facility,
               0.0 total, tranamt, faccode, impaired
        FROM page03
    """).fetchall():
        f.write(f"0234{r[0]:05d}{r[1]:010d}{r[2]:05d}{r[3]}{r[4]:5d}{r[5]:8s}{r[6]:8s}          "
                f"{r[7]:4s}{r[8]:3d}{r[9]}{r[10]:3d}{r[10]:3d}{r[11]}{r[12]}N"
                f"N{r[13]}{r[14]}SNNNMYR{r[15]:12.2f}{r[16]:12.2f}{r[17]:12.2f}{r[18]:12.2f}"
                f"N{r[19]}{r[20]:04d}{r[21]:13s}{r[22]:5d}{r[23]:17.2f}{r[24]:17.2f}"
                f"{r[25]:5d}{r[26]}\n")

print(f"PAGE03: {con.execute('SELECT COUNT(*) FROM page03').fetchone()[0]} records")

# ============================================================================
# PAGE 04: NON-PERFORMING LOAN (QUARTERLY)
# ============================================================================

with open(OUTPUT_DIR/'page04.txt','w') as f:
    for r in con.execute("""
        SELECT branch_final, acctno, noteno_final, reptdte, marketvl, classamt,
               iisp_new, suspend_new, reciis_new, iispw_new, totiis,
               spp, spmade, spwrback, sppw, sp, costctr, vinno, facility,
               0.0 total, tranamt, faccode, impaired
        FROM page03
        WHERE riskc_final != 'P' OR riskc_adj = 'P'
    """).fetchall():
        f.write(f"0234{r[0]:05d}{r[1]:010d}{r[2]:05d}{r[3]}{r[4]:12.2f}{r[5]:12.2f}"
                f"{r[6]:12.2f}{r[7]:12.2f}{r[8]:12.2f}{r[9]:12.2f}{r[10]:12.2f}"
                f"{r[11]:12.2f}{r[12]:12.2f}{r[13]:12.2f}{r[14]:12.2f}{r[15]:12.2f}"
                f"            N{r[16]:04d}{r[17]:13s}{r[18]:5d}{r[19]:17.2f}"
                f"{r[20]:17.2f}{r[21]:5d}{r[22]}\n")

print(f"PAGE04: {con.execute('SELECT COUNT(*) FROM page03 WHERE riskc_final != \"P\" OR riskc_adj = \"P\"').fetchone()[0]} records")

con.close()
print(f"\nCompleted: PAGE01-04 generated in {OUTPUT_DIR}")
