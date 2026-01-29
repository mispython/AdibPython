import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import sys

# ============================================================================
# PATH CONFIGURATION
# ============================================================================
BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'

# Input files (fixed-width)
ELDSRV1 = INPUT_DIR / 'eldsrv1.txt'
ELDSRV2 = INPUT_DIR / 'eldsrv2.txt'
ELDSRV3 = INPUT_DIR / 'eldsrv3.txt'
ELDSRV4 = INPUT_DIR / 'eldsrv4.txt'
ELDSRV5 = INPUT_DIR / 'eldsrv5.txt'
ELDSRVO_EREV = INPUT_DIR / 'eldsrvo' / 'erev.parquet'

# Output paths
ELDSRV_REPTDATE = OUTPUT_DIR / 'eldsrv' / 'reptdate.parquet'
ELDSRV_EREV = OUTPUT_DIR / 'eldsrv' / 'erev.parquet'
EREVFTP = OUTPUT_DIR / 'erevftp.parquet'

(OUTPUT_DIR / 'eldsrv').mkdir(parents=True, exist_ok=True)

# ============================================================================
# Calculate REPTDATE
# ============================================================================
today = datetime.now().date()
day = today.day

if 8 <= day <= 14:
    reptdate = datetime(today.year, today.month, 8).date()
    wk = '1'
elif 15 <= day <= 21:
    reptdate = datetime(today.year, today.month, 15).date()
    wk = '2'
elif 22 <= day <= 27:
    reptdate = datetime(today.year, today.month, 22).date()
    wk = '3'
else:
    first_of_month = today.replace(day=1)
    reptdate = first_of_month - timedelta(days=1)
    wk = '4'

nowk = wk
rdate = reptdate.strftime('%d%m%y')
reptmon = f"{reptdate.month:02d}"
reptyear = f"{reptdate.year % 100:02d}"

print(f"ELDS Review Processing - {reptdate.strftime('%d%b%Y')}")
print(f"Expected date: {rdate}")

# Save REPTDATE
reptdate_df = pl.DataFrame({'reptdate': [reptdate]})
reptdate_df.write_parquet(ELDSRV_REPTDATE)

# ============================================================================
# Check input file dates
# ============================================================================
def check_file_date(filepath, file_num):
    """Extract and validate date from first line of file"""
    if not Path(filepath).exists():
        print(f"Warning: {filepath} not found")
        return None
    
    with open(filepath, 'r') as f:
        first_line = f.readline()
        if len(first_line) < 50:
            print(f"Warning: {filepath} first line too short")
            return None
        
        try:
            dd = int(first_line[40:42])
            mm = int(first_line[43:45])
            yy = int(first_line[46:50])
            file_date = datetime(yy, mm, dd).date()
            file_date_str = file_date.strftime('%d%m%y')
            
            print(f"ELDSRV{file_num} date: {file_date.strftime('%d%b%Y')} ({file_date_str})")
            return file_date_str
        except Exception as e:
            print(f"Error reading date from {filepath}: {e}")
            return None

# Check all file dates
eldsdt1 = check_file_date(ELDSRV1, 1)
eldsdt2 = check_file_date(ELDSRV2, 2)
eldsdt3 = check_file_date(ELDSRV3, 3)
eldsdt4 = check_file_date(ELDSRV4, 4)
eldsdt5 = check_file_date(ELDSRV5, 5)

# Validate all dates match expected RDATE
all_dates_valid = all([
    eldsdt1 == rdate,
    eldsdt2 == rdate,
    eldsdt3 == rdate,
    eldsdt4 == rdate,
    eldsdt5 == rdate
])

if not all_dates_valid:
    print(f"\nERROR: File dates do not match expected date {rdate}")
    print("THE SAP.PBB.ELDS.NEWBNM.RV1.TEXT(0) NOT DATED", rdate)
    print("THE JOB IS NOT DONE !!")
    sys.exit(77)

print(f"\nAll file dates validated successfully. Processing...")

# ============================================================================
# Initialize DuckDB
# ============================================================================
con = duckdb.connect()

# ============================================================================
# Read ELN1 - Main review data
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE eln1 AS
    SELECT 
        SUBSTRING(line, 1, 12) as newid,
        UPPER(SUBSTRING(line, 16, 17)) as reviewno,
        UPPER(SUBSTRING(line, 36, 20)) as aano,
        UPPER(SUBSTRING(line, 59, 50)) as custname,
        CAST(SUBSTRING(line, 112, 2) AS INTEGER) as seqno,
        UPPER(SUBSTRING(line, 117, 19)) as revstat,
        UPPER(SUBSTRING(line, 139, 16)) as dservra,
        UPPER(SUBSTRING(line, 158, 19)) as networth,
        CAST(SUBSTRING(line, 180, 6) AS INTEGER) as maval,
        CAST(SUBSTRING(line, 207, 22) AS DECIMAL(22,2)) as yearc,
        UPPER(SUBSTRING(line, 232, 38)) as yearcr,
        CAST(SUBSTRING(line, 273, 26) AS DECIMAL(26,2)) as yearbs,
        UPPER(SUBSTRING(line, 302, 25)) as mgmexp,
        UPPER(SUBSTRING(line, 330, 10)) as indtype,
        UPPER(SUBSTRING(line, 343, 25)) as turnovr,
        UPPER(SUBSTRING(line, 371, 15)) as netproft,
        UPPER(SUBSTRING(line, 389, 15)) as tstratio,
        UPPER(SUBSTRING(line, 407, 11)) as leverge,
        UPPER(SUBSTRING(line, 421, 20)) as intcovr,
        UPPER(SUBSTRING(line, 444, 26)) as avgcper,
        UPPER(SUBSTRING(line, 473, 35)) as cashflw,
        UPPER(SUBSTRING(line, 511, 50)) as acctnod,
        UPPER(SUBSTRING(line, 564, 40)) as prholder,
        CAST(SUBSTRING(line, 607, 4) AS INTEGER) as expernc,
        CAST(SUBSTRING(line, 614, 2) AS INTEGER) as nrdd,
        CAST(SUBSTRING(line, 617, 2) AS INTEGER) as nrmm,
        CAST(SUBSTRING(line, 620, 4) AS INTEGER) as nryy,
        UPPER(SUBSTRING(line, 627, 3)) as cparti,
        UPPER(SUBSTRING(line, 633, 50)) as staffnm,
        UPPER(SUBSTRING(line, 686, 3)) as bodmem,
        CAST(SUBSTRING(line, 692, 5) AS INTEGER) as staffid,
        UPPER(SUBSTRING(line, 700, 30)) as sbranch,
        UPPER(SUBSTRING(line, 733, 50)) as relname,
        UPPER(SUBSTRING(line, 786, 50)) as relate,
        SUBSTRING(line, 839, 6) as totsval,
        UPPER(SUBSTRING(line, 848, 6)) as maodfl,
        UPPER(SUBSTRING(line, 857, 15)) as netexpo,
        UPPER(SUBSTRING(line, 875, 15)) as sblcbg,
        CAST(SUBSTRING(line, 893, 2) AS INTEGER) as sbdd,
        CAST(SUBSTRING(line, 896, 2) AS INTEGER) as sbmm,
        CAST(SUBSTRING(line, 899, 4) AS INTEGER) as sbyy,
        UPPER(SUBSTRING(line, 906, 15)) as cgcamt,
        CAST(SUBSTRING(line, 924, 2) AS INTEGER) as cgdd,
        CAST(SUBSTRING(line, 927, 2) AS INTEGER) as cgmm,
        CAST(SUBSTRING(line, 930, 4) AS INTEGER) as cgyy,
        UPPER(SUBSTRING(line, 937, 8)) as cgcnum,
        CAST(SUBSTRING(line, 1010, 2) AS INTEGER) as apdd,
        CAST(SUBSTRING(line, 1013, 2) AS INTEGER) as apmm,
        CAST(SUBSTRING(line, 1016, 4) AS INTEGER) as apyy,
        SUBSTRING(line, 1097, 5) as crr1,
        SUBSTRING(line, 1110, 5) as crr2,
        SUBSTRING(line, 1123, 5) as crr3,
        TRY_STRPTIME(LPAD(CAST(nrmm AS VARCHAR), 2, '0') || '/' || 
                     LPAD(CAST(nrdd AS VARCHAR), 2, '0') || '/' || 
                     CAST(nryy AS VARCHAR), '%m/%d/%Y') as nxrevdt,
        TRY_STRPTIME(LPAD(CAST(sbmm AS VARCHAR), 2, '0') || '/' || 
                     LPAD(CAST(sbdd AS VARCHAR), 2, '0') || '/' || 
                     CAST(sbyy AS VARCHAR), '%m/%d/%Y') as sblcxdt,
        TRY_STRPTIME(LPAD(CAST(cgmm AS VARCHAR), 2, '0') || '/' || 
                     LPAD(CAST(cgdd AS VARCHAR), 2, '0') || '/' || 
                     CAST(cgyy AS VARCHAR), '%m/%d/%Y') as cgcexdt,
        TRY_STRPTIME(LPAD(CAST(apmm AS VARCHAR), 2, '0') || '/' || 
                     LPAD(CAST(apdd AS VARCHAR), 2, '0') || '/' || 
                     CAST(apyy AS VARCHAR), '%m/%d/%Y') as apprdt
    FROM read_csv('{ELDSRV1}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY newid, reviewno
""")

# ============================================================================
# Read ELN2 - Customer details
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE eln2 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 12)) as newid,
        UPPER(SUBSTRING(line, 16, 17)) as reviewno,
        UPPER(SUBSTRING(line, 36, 140)) as custnme,
        UPPER(SUBSTRING(line, 179, 110)) as occpat,
        UPPER(SUBSTRING(line, 292, 40)) as identy,
        UPPER(SUBSTRING(line, 335, 50)) as custid,
        UPPER(SUBSTRING(line, 388, 50)) as dobdec,
        UPPER(SUBSTRING(line, 441, 80)) as aanos,
        UPPER(SUBSTRING(line, 524, 85)) as facilt,
        CAST(REPLACE(REPLACE(SUBSTRING(line, 612, 11), ',', ''), ' ', '') AS DECIMAL(13,2)) as appramt,
        CAST(REPLACE(REPLACE(SUBSTRING(line, 626, 11), ',', ''), ' ', '') AS DECIMAL(13,2)) as operlmt,
        CAST(REPLACE(REPLACE(SUBSTRING(line, 640, 11), ',', ''), ' ', '') AS DECIMAL(13,2)) as balance,
        CAST(REPLACE(REPLACE(SUBSTRING(line, 654, 11), ',', ''), ' ', '') AS DECIMAL(13,2)) as excsarr,
        UPPER(SUBSTRING(line, 668, 140)) as guarantr,
        UPPER(SUBSTRING(line, 811, 80)) as guaridno,
        UPPER(SUBSTRING(line, 894, 10)) as guarages,
        UPPER(SUBSTRING(line, 907, 50)) as guarnetw,
        CAST(SUBSTRING(line, 978, 12) AS BIGINT) as new_buss_reg_id
    FROM read_csv('{ELDSRV2}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY newid, reviewno
""")

# ============================================================================
# Read ELN3 - Property/Address
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE eln3 AS
    SELECT 
        SUBSTRING(line, 1, 12) as newid,
        UPPER(SUBSTRING(line, 16, 17)) as reviewno,
        UPPER(SUBSTRING(line, 36, 150)) as prptdes,
        UPPER(SUBSTRING(line, 189, 160)) as addrb01,
        UPPER(SUBSTRING(line, 349, 160)) as addrb02,
        UPPER(SUBSTRING(line, 509, 160)) as addrb03,
        UPPER(SUBSTRING(line, 669, 300)) as addrb04,
        UPPER(SUBSTRING(line, 972, 50)) as cmscode
    FROM read_csv('{ELDSRV3}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY newid, reviewno
""")

# ============================================================================
# Read ELN4 - Account conduct
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE eln4 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 12)) as newid,
        UPPER(SUBSTRING(line, 16, 17)) as reviewno,
        UPPER(SUBSTRING(line, 36, 43)) as odacct,
        UPPER(SUBSTRING(line, 82, 29)) as flacct,
        UPPER(SUBSTRING(line, 114, 29)) as tfacct,
        UPPER(SUBSTRING(line, 146, 29)) as bbacct,
        UPPER(SUBSTRING(line, 178, 70)) as bbreas,
        UPPER(SUBSTRING(line, 251, 56)) as ccacct,
        UPPER(SUBSTRING(line, 310, 70)) as ccreas,
        UPPER(SUBSTRING(line, 940, 6)) as bnmcd,
        UPPER(SUBSTRING(line, 390, 38)) as custcd,
        SUBSTRING(line, 431, 3) as smicd,
        UPPER(SUBSTRING(line, 437, 100)) as noncom1,
        UPPER(SUBSTRING(line, 537, 100)) as noncom2,
        UPPER(SUBSTRING(line, 637, 100)) as noncom3,
        UPPER(SUBSTRING(line, 737, 100)) as noncom4,
        UPPER(SUBSTRING(line, 837, 100)) as noncom5,
        UPPER(SUBSTRING(line, 940, 6)) as bnmsect
    FROM read_csv('{ELDSRV4}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY newid, reviewno
""")

# ============================================================================
# Read ELN5 - Financial data
# ============================================================================
con.execute(f"""
    CREATE TEMP TABLE eln5 AS
    SELECT 
        UPPER(SUBSTRING(line, 1, 12)) as newid,
        UPPER(SUBSTRING(line, 16, 17)) as reviewno,
        SUBSTRING(line, 36, 1) as joint,
        CAST(SUBSTRING(line, 40, 2) AS INTEGER) as lmdd,
        CAST(SUBSTRING(line, 43, 2) AS INTEGER) as lmmm,
        CAST(SUBSTRING(line, 46, 4) AS INTEGER) as lmyy,
        UPPER(SUBSTRING(line, 53, 72)) as reason,
        UPPER(SUBSTRING(line, 128, 71)) as reason1,
        UPPER(SUBSTRING(line, 202, 25)) as turnovr,
        UPPER(SUBSTRING(line, 230, 16)) as capital,
        UPPER(SUBSTRING(line, 249, 16)) as assets,
        UPPER(SUBSTRING(line, 268, 16)) as liabil,
        UPPER(SUBSTRING(line, 287, 16)) as fincost,
        UPPER(SUBSTRING(line, 306, 16)) as netprof,
        UPPER(SUBSTRING(line, 325, 16)) as stock,
        UPPER(SUBSTRING(line, 344, 3)) as tolscore,
        UPPER(SUBSTRING(line, 350, 3)) as ptolscore,
        UPPER(SUBSTRING(line, 356, 1)) as newspacct,
        UPPER(SUBSTRING(line, 360, 1)) as upspacct,
        UPPER(SUBSTRING(line, 364, 20)) as status,
        TRY_STRPTIME(LPAD(CAST(lmmm AS VARCHAR), 2, '0') || '/' || 
                     LPAD(CAST(lmdd AS VARCHAR), 2, '0') || '/' || 
                     CAST(lmyy AS VARCHAR), '%m/%d/%Y') as lastmdt
    FROM read_csv('{ELDSRV5}', columns={{'line': 'VARCHAR'}}, header=true, skip=1)
    ORDER BY newid, reviewno
""")

# ============================================================================
# Merge all ELN files
# ============================================================================
con.execute("""
    CREATE TABLE erev_new AS
    SELECT e1.*, e2.*, e3.*, e4.*, e5.*
    FROM eln1 e1
    LEFT JOIN eln2 e2 ON e1.newid = e2.newid AND e1.reviewno = e2.reviewno
    LEFT JOIN eln3 e3 ON e1.newid = e3.newid AND e1.reviewno = e3.reviewno
    LEFT JOIN eln4 e4 ON e1.newid = e4.newid AND e1.reviewno = e4.reviewno
    LEFT JOIN eln5 e5 ON e1.newid = e5.newid AND e1.reviewno = e5.reviewno
""")

# ============================================================================
# Append with previous data if exists
# ============================================================================
if Path(ELDSRVO_EREV).exists():
    con.execute(f"""
        CREATE TABLE erev_combined AS
        SELECT * FROM erev_new
        UNION ALL
        SELECT * FROM read_parquet('{ELDSRVO_EREV}')
    """)
else:
    con.execute("CREATE TABLE erev_combined AS SELECT * FROM erev_new")

# ============================================================================
# Filter out empty AANO records
# ============================================================================
con.execute("""
    CREATE TABLE erev_final AS
    SELECT * FROM erev_combined
    WHERE aano IS NOT NULL AND aano != ' '
    ORDER BY newid, reviewno
""")

# Save output
con.execute(f"COPY erev_final TO '{ELDSRV_EREV}'")
con.execute(f"COPY erev_final TO '{EREVFTP}'")

con.close()

print(f"\nProcessing complete!")
print(f"Output files:")
print(f"  - {ELDSRV_REPTDATE}")
print(f"  - {ELDSRV_EREV}")
print(f"  - {EREVFTP}")
print(f"\nRecords processed: {len(pl.read_parquet(ELDSRV_EREV))}")
