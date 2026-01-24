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

print(f"Processing FECCR (Islamic version)")
print(f"Current: {reptdate}, Previous: {prevdate}")
print(f"Report date: {reptdt}, RDATE2: {rdate2}")

# Build paths
fefcl_prev_path = str(EQUA_FEFCL_PREV).format(prevmon=prevmon)
fefcl_curr_path = str(EQUA_FEFCL_CURR).format(reptmon=reptmon)
summ1_path = str(BNMSUM_SUMM1).format(rdate2=rdate2)

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
# Load FEFCL data
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE fefcl_prev AS
    SELECT acctno, fxlused as outst_prev
    FROM read_parquet('{fefcl_prev_path}')
    WHERE filetype IN ('A', 'C')
""")

con.execute(f"""
    CREATE TEMP TABLE fefcl_curr AS
    SELECT *
    FROM read_parquet('{fefcl_curr_path}')
    WHERE filetype IN ('A', 'C')
""")

# Merge and set account status
con.execute("""
    CREATE TEMP TABLE fefcl_status AS
    SELECT 
        COALESCE(c.acctno, p.acctno) as acctno,
        c.*,
        p.outst_prev,
        CASE WHEN p.acctno IS NOT NULL AND c.acctno IS NULL THEN 'S' ELSE 'O' END as acctstat
    FROM fefcl_curr c
    FULL OUTER JOIN fefcl_prev p ON c.acctno = p.acctno
""")

# ============================================================================
# Merge with loan and CIS data, apply business logic
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE fefcl_processed AS
    SELECT 
        f.*,
        CASE WHEN l.branch > 0 THEN l.branch 
             ELSE CAST(SUBSTRING(CAST(l.custloc AS VARCHAR), 1, 3) AS INTEGER) END as ficode,
        l4.acctyind as apcode,
        COALESCE(l4.faccode, '34861') as facility,
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
        CASE WHEN f.fxlused > 0 THEN 'Y' ELSE 'N' END as utilised,
        CASE WHEN l.mniaplmt > 0 THEN l.mniaplmt ELSE f.fxllimt END * 100 as apprlimt,
        f.fxlused * 100 as outstand,
        (f.fxlused - COALESCE(f.outst_prev, 0)) * 100 as disbamtx,
        b.statecd,
        COALESCE(l4.aano, l4.aano1) as aano_final,
        COALESCE(l.custcode, 0) as custcode,
        c.sector_code,
        CASE WHEN l.mniapdte NOT IN (0, NULL) THEN
            TRY_STRPTIME(SUBSTRING(LPAD(CAST(l.mniapdte AS VARCHAR), 11, '0'), 1, 8), '%m%d%Y')
        END as apprdate_dt
    FROM fefcl_status f
    LEFT JOIN read_parquet('{LOAN_LNACCT}') l ON f.acctno = l.acctno
    LEFT JOIN read_parquet('{LOAN_LNACC4}') l4 ON f.acctno = l4.acctno
    LEFT JOIN read_parquet('{CIS_LOAN}') c ON f.acctno = c.acctno AND c.seccust = '901'
    LEFT JOIN brhfmt b ON CASE WHEN l.branch > 0 THEN l.branch 
                              ELSE CAST(SUBSTRING(CAST(l.custloc AS VARCHAR), 1, 3) AS INTEGER) END = b.branch
    WHERE f.acctno > 0
""")

# Calculate final amounts and state code
con.execute(f"""
    CREATE TEMP TABLE fefcl_final AS
    SELECT *,
        apprlimt - outstand as undrawn_raw,
        CASE WHEN disbamtx < 0 THEN 0 ELSE disbamtx END as disbamt,
        CASE WHEN disbamtx < 0 THEN disbamtx ELSE 0 END as repaymt,
        CASE WHEN disbamtx < 0 THEN '1200' ELSE '0000' END as repay_src,
        CASE WHEN disbamtx < 0 THEN '10' ELSE '00' END as repay_type,
        CASE WHEN apprdate_dt IS NOT NULL THEN STRFTIME(apprdate_dt, '%Y%m%d') ELSE '00000000' END as apprdate,
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
        END as stateno
    FROM fefcl_processed
""")

# Apply settled account adjustments
con.execute("""
    CREATE TEMP TABLE fefcl_adjusted AS
    SELECT *,
        CASE WHEN undrawn_raw < 0 THEN 0 ELSE undrawn_raw END as undrawn,
        CASE WHEN acctstat = 'S' THEN 0 ELSE outstand END as outstand_final,
        CASE WHEN acctstat = 'S' THEN 0 
             WHEN undrawn_raw < 0 THEN 0 
             ELSE undrawn_raw END as undrawn_final
    FROM fefcl_final
    ORDER BY aano_final
""")

# ============================================================================
# Merge with SUMM1
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE summ1 AS
    SELECT DISTINCT ON (aano) 
           aano, lu_add1, lu_add2, lu_add3, lu_add4, 
           lu_town_city, lu_postcode, lu_state_cd, lu_country_cd
    FROM read_parquet('{summ1_path}')
    WHERE date <= DATE '{rdate1}'
    ORDER BY aano, stage
""")

con.execute("""
    CREATE TABLE fefcl_output AS
    SELECT f.*, 
           COALESCE(s.lu_add1, '') as lu_add1,
           COALESCE(s.lu_add2, '') as lu_add2,
           COALESCE(s.lu_add3, '') as lu_add3,
           COALESCE(s.lu_add4, '') as lu_add4,
           COALESCE(s.lu_town_city, '') as lu_town_city,
           COALESCE(s.lu_postcode, '') as lu_postcode,
           COALESCE(s.lu_state_cd, '') as lu_state_cd,
           COALESCE(s.lu_country_cd, '') as lu_country_cd
    FROM fefcl_adjusted f
    LEFT JOIN summ1 s ON f.aano_final = s.aano
""")

con.execute(f"COPY fefcl_output TO '{EQUA_FECCR}'")

# ============================================================================
# Generate output files
# ============================================================================

# SUBACRED file
with open(SUBACRED_TXT, 'w') as f:
    for row in con.execute("SELECT * FROM fefcl_output ORDER BY aano_final").fetchall():
        ficode, apcode, acctno = row[0], row[1], row[2]
        aano, facility = row[3] or '', row[4] or ''
        syndicat, specialf, fisspurp = row[5] or '', row[6] or '', row[7] or ''
        fconcept, noteterm, payfreqc = row[8] or 0, row[9] or 0, row[10] or ''
        curcode, intrate = row[11] or '', row[12] or 0
        issdte, matdate, priorsec, utilised = row[13] or '', row[14] or '', row[15] or '', row[16] or ''
        stateno, custcode, sector_code = row[17] or 0, row[18] or 0, row[19] or ''
        lu_add1, lu_add2, lu_add3, lu_add4 = row[20] or '', row[21] or '', row[22] or '', row[23] or ''
        lu_town, lu_post, lu_state, lu_country = row[24] or '', row[25] or '', row[26] or '', row[27] or ''
        
        line = f"{ficode:09d}{apcode:03d}{acctno:010d}{aano:13s}{reptdt}"
        line += f"{facility:5s}{syndicat:1s}{specialf:2s}{fisspurp:4s}"
        line += f"{fconcept:02d}{noteterm:03d}{payfreqc:2s}{curcode:3s}"
        line += f"{intrate:05d}{issdte:10s}{matdate:10s}{priorsec:1s}"
        line += f"{utilised:1s}{stateno:02d}{custcode:02d}{sector_code:5s}"
        line += f"{lu_add1:40s}{lu_add2:40s}{lu_add3:40s}{lu_add4:40s}"
        line += f"{lu_town:20s}{lu_post:5s}{lu_state:2s}{lu_country:2s}\n"
        f.write(line)

# CREDITPO file
with open(CREDITPO_TXT, 'w') as f:
    for row in con.execute("SELECT * FROM fefcl_output").fetchall():
        ficode, apcode, acctno = row[0], row[1], row[2]
        aano, facility = row[3] or '', row[4] or ''
        outstand, undrawn, acctstat = row[28] or 0, row[29] or 0, row[30] or ''
        disbamt, repaymt = row[31] or 0, row[32] or 0
        repay_src, repay_type = row[33] or '', row[34] or ''
        
        line = f"{ficode:09d}{apcode:03d}{acctno:010d}{aano:13s}{reptdt}"
        line += f"{facility:5s}{int(outstand):016d}{int(undrawn):016d}"
        line += f"{acctstat:1s}{int(disbamt):016d}{int(repaymt):016d}"
        line += f"{repay_src:4s}{repay_type:2s}\n"
        f.write(line)

# ACCTCRED file
with open(ACCTCRED_TXT, 'w') as f:
    for row in con.execute("""
        SELECT DISTINCT ON (ficode, acctno) 
            ficode, apcode, acctno, aano_final, curcode, apprlimt, apprdate
        FROM fefcl_output
        ORDER BY ficode, acctno
    """).fetchall():
        ficode, apcode, acctno = row[0], row[1], row[2]
        aano, curcode = row[3] or '', row[4] or ''
        apprlimt, apprdate = row[5] or 0, row[6] or ''
        
        line = f"{ficode:09d}{apcode:03d}{acctno:010d}{aano:13s}{reptdt}"
        line += f"{curcode:3s}{int(apprlimt):016d}{apprdate:8s}\n"
        f.write(line)

con.close()

print(f"\nProcessing complete!")
print(f"Output files:")
print(f"  - {EQUA_FECCR}")
print(f"  - {SUBACRED_TXT}")
print(f"  - {CREDITPO_TXT}")
print(f"  - {ACCTCRED_TXT}")
