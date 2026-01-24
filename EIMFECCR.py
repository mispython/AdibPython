import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'

# Input paths
EQUA_FEFCL_PREV = INPUT_DIR / 'equa' / 'fefcl{prevmon}.parquet'
EQUA_FEFCL_CURR = INPUT_DIR / 'equa' / 'fefcl{reptmon}.parquet'
LOAN_LNACCT = INPUT_DIR / 'loan' / 'lnacct.parquet'
LOAN_LNACC4 = INPUT_DIR / 'loan' / 'lnacc4.parquet'
CIS_LOAN = INPUT_DIR / 'cis' / 'loan.parquet'
BNMSUM_SUMM1 = INPUT_DIR / 'bnmsum' / 'summ1{rdate2}.parquet'
BRHFILE = INPUT_DIR / 'brhfile.txt'

# Output paths
EQUA_FECCR = OUTPUT_DIR / 'equa' / 'feccr.parquet'
SUBACRED_TXT = OUTPUT_DIR / 'subacred.txt'
CREDITPO_TXT = OUTPUT_DIR / 'creditpo.txt'
ACCTCRED_TXT = OUTPUT_DIR / 'acctcred.txt'

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
(OUTPUT_DIR / 'equa').mkdir(parents=True, exist_ok=True)

# ============================================================================
# Initialize and calculate dates
# ============================================================================
con = duckdb.connect()

today = datetime.now().date()
reptdate = today.replace(day=1) - timedelta(days=1)  # Last day of previous month
prevdate = reptdate.replace(day=1) - timedelta(days=1)  # Last day of month before

prevmon = f"{prevdate.month:02d}"
reptmon = f"{reptdate.month:02d}"
reptday = f"{reptdate.day:02d}"
reptdt = reptdate.strftime('%Y%m%d')
rdate1 = reptdate
rdate2 = (today - timedelta(days=1)).strftime('%y%m%d')

print(f"Current: {reptdate}, Previous: {prevdate}")
print(f"Report date: {reptdt}, RDATE2: {rdate2}")

# ============================================================================
# Load and process branch format for state codes
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE brhfmt AS
    SELECT 
        CAST(SUBSTRING(line, 2, 3) AS INTEGER) as branch,
        SUBSTRING(line, 6, 3) as brabbr,
        SUBSTRING(line, 45, 1) as statecd
    FROM read_csv('{BRHFILE}', columns={{'line': 'VARCHAR'}}, header=false)
""")

# ============================================================================
# Load and merge data
# ============================================================================
# Load FEFCL files
con.execute(f"""
    CREATE TEMP TABLE fefcl_prev AS
    SELECT acctno, fxlused as outst_prev
    FROM read_parquet('{str(EQUA_FEFCL_PREV).format(prevmon=prevmon)}')
    WHERE filetype IN ('A', 'C')
    ORDER BY acctno
""")

con.execute(f"""
    CREATE TEMP TABLE fefcl_curr AS
    SELECT *
    FROM read_parquet('{str(EQUA_FEFCL_CURR).format(reptmon=reptmon)}')
    WHERE filetype IN ('A', 'C')
    ORDER BY acctno
""")

# Merge FEFCL current and previous
con.execute("""
    CREATE TEMP TABLE fefcl_merged AS
    SELECT 
        COALESCE(c.acctno, p.acctno) as acctno,
        c.*,
        p.outst_prev,
        CASE WHEN p.acctno IS NOT NULL AND c.acctno IS NULL THEN 'S' ELSE 'O' END as acctstat
    FROM fefcl_curr c
    FULL OUTER JOIN fefcl_prev p ON c.acctno = p.acctno
    WHERE COALESCE(c.acctno, p.acctno) > 0
""")

# Merge with loan and CIS data
con.execute(f"""
    CREATE TEMP TABLE fefcl_full AS
    SELECT 
        f.*,
        l.custloc, l.branch as ln_branch, l.custcode, l.mniaplmt, l.mniapdte,
        l4.aano, l4.aano1, l4.acctyind, l4.faccode,
        c.seccust, c.sector_code,
        b.statecd,
        CAST(SUBSTRING(CAST(COALESCE(l.custloc, '') AS VARCHAR), 1, 3) AS INTEGER) as ficode_calc
    FROM fefcl_merged f
    LEFT JOIN read_parquet('{LOAN_LNACCT}') l ON f.acctno = l.acctno
    LEFT JOIN read_parquet('{LOAN_LNACC4}') l4 ON f.acctno = l4.acctno
    LEFT JOIN read_parquet('{CIS_LOAN}') c ON f.acctno = c.acctno AND c.seccust = '901'
    LEFT JOIN brhfmt b ON COALESCE(l.branch, f.ficode_calc) = b.branch
""")

# ============================================================================
# Apply business logic
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE fefcl_calc AS
    SELECT *,
        CASE WHEN ln_branch > 0 THEN ln_branch ELSE ficode_calc END as ficode,
        acctyind as apcode,
        COALESCE(faccode, '34861') as facility,
        'N' as syndicat,
        '00' as specialf,
        '5300' as fisspurp,
        99 as fconcept,
        12 as noteterm,
        '19' as payfreqc,
        'MYR' as curcode,
        0 as intrate,
        'N' as priorsec,
        '00000000' as issdte,
        '00000000' as matdate,
        CASE WHEN fxlused > 0 THEN 'Y' ELSE 'N' END as utilised,
        CASE WHEN mniaplmt > 0 THEN mniaplmt ELSE fxllimt END * 100 as apprlimt,
        fxlused * 100 as outstand,
        CASE WHEN mniapdte NOT IN (0, NULL) THEN
            TRY_STRPTIME(SUBSTRING(LPAD(CAST(mniapdte AS VARCHAR), 11, '0'), 1, 8), '%m%d%Y')
        END as apprdate_dt
    FROM fefcl_full
""")

con.execute("""
    CREATE TEMP TABLE fefcl_final AS
    SELECT *,
        apprlimt - outstand as undrawn_calc,
        (fxlused - COALESCE(outst_prev, 0)) * 100 as disbamtx,
        CASE 
            WHEN statecd = 'J' THEN 1
            WHEN statecd = 'K' THEN 2
            WHEN statecd = 'D' THEN 3
            WHEN statecd = 'M' THEN 4
            WHEN statecd = 'N' THEN 5
            WHEN statecd = 'C' THEN 6
            WHEN statecd = 'P' THEN 7
            WHEN statecd = 'A' THEN 8
            WHEN statecd = 'R' THEN 9
            WHEN statecd = 'S' THEN 10
            WHEN statecd = 'Q' THEN 11
            WHEN statecd = 'B' THEN 12
            WHEN statecd = 'T' THEN 13
            WHEN statecd IN ('W', 'L') THEN 14
            ELSE 0
        END as stateno,
        COALESCE(aano, aano1) as aano_final
    FROM fefcl_calc
""")

con.execute(f"""
    CREATE TEMP TABLE fefcl_amounts AS
    SELECT *,
        CASE WHEN undrawn_calc < 0 THEN 0 ELSE undrawn_calc END as undrawn,
        CASE WHEN disbamtx < 0 THEN 0 ELSE disbamtx END as disbamt,
        CASE WHEN disbamtx < 0 THEN disbamtx ELSE 0 END as repaymt,
        CASE WHEN disbamtx < 0 THEN '1200' ELSE '0000' END as repay_src,
        CASE WHEN disbamtx < 0 THEN '10' ELSE '00' END as repay_type,
        CASE WHEN apprdate_dt IS NOT NULL THEN STRFTIME(apprdate_dt, '%Y%m%d') ELSE '00000000' END as apprdate,
        CASE WHEN acctstat = 'S' THEN 0 ELSE outstand END as outstand_adj,
        CASE WHEN acctstat = 'S' THEN 0 ELSE undrawn_calc END as undrawn_adj
    FROM fefcl_final
    ORDER BY aano_final
""")

# ============================================================================
# Merge with SUMM1
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE summ1 AS
    SELECT DISTINCT aano, lu_add1, lu_add2, lu_add3, lu_add4, 
           lu_town_city, lu_postcode, lu_state_cd, lu_country_cd
    FROM read_parquet('{str(BNMSUM_SUMM1).format(rdate2=rdate2)}')
    WHERE date <= DATE '{rdate1}'
    ORDER BY aano
""")

con.execute("""
    CREATE TABLE fefcl_output AS
    SELECT f.*, s.lu_add1, s.lu_add2, s.lu_add3, s.lu_add4,
           s.lu_town_city, s.lu_postcode, s.lu_state_cd, s.lu_country_cd
    FROM fefcl_amounts f
    LEFT JOIN summ1 s ON f.aano_final = s.aano
""")

# Save parquet
con.execute(f"COPY fefcl_output TO '{EQUA_FECCR}'")

# ============================================================================
# Generate output files
# ============================================================================

# SUBACRED file
records = con.execute("SELECT * FROM fefcl_output ORDER BY aano_final").fetchall()
with open(SUBACRED_TXT, 'w') as f:
    for r in records:
        line = f"{r[0]:09d}{r[1]:03d}{r[2]:010d}{r[3]:13s}{reptdt}{r[4]:5s}"
        line += f"{r[5]:1s}{r[6]:2s}{r[7]:4s}{r[8]:02d}{r[9]:03d}{r[10]:2s}"
        line += f"{r[11]:3s}{r[12]:05d}{r[13]:10s}{r[14]:10s}{r[15]:1s}"
        line += f"{r[16]:1s}{r[17]:02d}{r[18]:02d}{r[19]:5s}"
        line += f"{r[20]:40s}{r[21]:40s}{r[22]:40s}{r[23]:40s}"
        line += f"{r[24]:20s}{r[25]:5s}{r[26]:2s}{r[27]:2s}\n"
        f.write(line)

# CREDITPO file
with open(CREDITPO_TXT, 'w') as f:
    for r in records:
        line = f"{r[0]:09d}{r[1]:03d}{r[2]:010d}{r[3]:13s}{reptdt}{r[4]:5s}"
        line += f"{r[28]:016d}{r[29]:016d}{r[30]:1s}{r[31]:016d}"
        line += f"{r[32]:016d}{r[33]:4s}{r[34]:2s}\n"
        f.write(line)

# ACCTCRED file (distinct by FICODE, ACCTNO)
acct_records = con.execute("""
    SELECT DISTINCT ON (ficode, acctno) *
    FROM fefcl_output
    ORDER BY ficode, acctno
""").fetchall()

with open(ACCTCRED_TXT, 'w') as f:
    for r in acct_records:
        line = f"{r[0]:09d}{r[1]:03d}{r[2]:010d}{r[3]:13s}{reptdt}"
        line += f"{r[11]:3s}{r[35]:016d}{r[36]:8s}\n"
        f.write(line)

con.close()

print(f"\nProcessing complete!")
print(f"Output files:")
print(f"  - {EQUA_FECCR}")
print(f"  - {SUBACRED_TXT}")
print(f"  - {CREDITPO_TXT}")
print(f"  - {ACCTCRED_TXT}")
