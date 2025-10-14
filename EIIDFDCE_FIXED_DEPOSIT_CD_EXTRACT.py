#!/usr/bin/env python3
"""
EIIDFDCE - FIXED DEPOSIT CD EXTRACT
1:1 CONVERSION FROM SAS TO PYTHON
EXTRACTS FIXED DEPOSIT CERTIFICATE DATA
PROCESSES DATE CONVERSIONS AND FILTERS BY ACCOUNT RANGE
"""

import duckdb
from pathlib import Path
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv

# INITIALIZE PATHS
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
DPDARPGS_DIR = INPUT_DIR / "dpdarpgs"
FIXEDCD_DIR = OUTPUT_DIR / "fixedcd"

# CREATE DIRECTORIES
FIXEDCD_DIR.mkdir(parents=True, exist_ok=True)

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("PROCESSING EIIDFDCE - FIXED DEPOSIT CD EXTRACT")

# ============================================================================
# DATA FIXEDCD.REPTDATE - READ REPORT DATE AND CALCULATE WEEK
# ============================================================================
REPTDATE_FILE = DPDARPGS_DIR / "REPTDATE.parquet"

if not REPTDATE_FILE.exists():
    print(f"ERROR: REPTDATE FILE NOT FOUND: {REPTDATE_FILE}")
    con.close()
    exit(1)

result = con.execute(f"""
    SELECT REPTDATE 
    FROM read_parquet('{REPTDATE_FILE}')
    LIMIT 1
""").fetchone()

REPTDATE = result[0]

# HANDLE DIFFERENT DATE FORMATS
if isinstance(REPTDATE, (int, float)):
    REPTDATE = datetime.strptime(str(int(REPTDATE)), '%Y%m%d').date()
elif isinstance(REPTDATE, str):
    REPTDATE = datetime.strptime(REPTDATE, '%Y-%m-%d').date()

# DETERMINE WEEK NUMBER
DAY_OF_MONTH = REPTDATE.day

if 1 <= DAY_OF_MONTH <= 8:
    NOWK = '1'
elif 9 <= DAY_OF_MONTH <= 15:
    NOWK = '2'
elif 16 <= DAY_OF_MONTH <= 22:
    NOWK = '3'
else:  # 23-31
    NOWK = '4'

# EXTRACT DATE COMPONENTS
REPTYEAR = str(REPTDATE.year)[-2:]  # YEAR2 FORMAT
REPTMON = str(REPTDATE.month).zfill(2)
REPTDAY = str(REPTDATE.day).zfill(2)
RDATE = REPTDATE.strftime('%d%m%Y')  # DDMMYY8 FORMAT

print(f"REPORT DATE: {REPTDATE}")
print(f"REPTYEAR: {REPTYEAR}")
print(f"REPTMON: {REPTMON}")
print(f"REPTDAY: {REPTDAY}")
print(f"WEEK: {NOWK}")
print(f"RDATE: {RDATE}")

# ============================================================================
# DATA FIXEDCD.IFDCD - PROCESS FIXED DEPOSIT DATA
# ============================================================================
print("\nPROCESSING FIXED DEPOSIT CD DATA...")

FD_FILE = DPDARPGS_DIR / "FD.parquet"

if not FD_FILE.exists():
    print(f"ERROR: FD FILE NOT FOUND: {FD_FILE}")
    con.close()
    exit(1)

# PROCESS FD DATA WITH DATE CONVERSIONS AND FILTERING
con.execute(f"""
    CREATE OR REPLACE TABLE IFDCD_PROCESSED AS
    SELECT 
        BRANCH,
        ACCTNO,
        CDNO,
        STATEC,
        CUSTCD,
        OPENIND,
        CURBAL,
        ORGDATE,
        MATDATE,
        RATE,
        RENEWAL,
        INTPLAN,
        LASTACTV,
        TERM,
        PURPOSE,
        ORIGAMT,
        INTPAY,
        LMATDATE,
        PAYMENT,
        DEPODTE,
        INTTFRACCT,
        INTFREQ,
        INTFREQID,
        MATID,
        ACCTTYPE AS PRODUCT,
        PENDINT AS PEND_INTPLAN,
        PRN_DISP_OPT,
        PRN_RENEW,
        PRN_TFR_ACCT
    FROM read_parquet('{FD_FILE}')
    WHERE OPENIND IN ('D', 'O')
""")

# EXPORT TO POLARS FOR DATE PROCESSING AND COMPLEX LOGIC
import polars as pl

DF = pl.from_arrow(con.execute("SELECT * FROM IFDCD_PROCESSED").arrow())

print(f"RECORDS AFTER OPENIND FILTER: {len(DF)}")

# ============================================================================
# DATE CONVERSIONS AND FILTERING
# ============================================================================
RESULT_ROWS = []

for row in DF.iter_rows(named=True):
    RECORD = dict(row)
    
    # CONVERT DEPODTE: EXTRACT FIRST 8 CHARS FROM 11-DIGIT NUMBER, PARSE AS MMDDYY8
    if RECORD.get('DEPODTE'):
        DEPODTE_STR = str(RECORD['DEPODTE']).zfill(11)[:8]
        try:
            DEPDATE = datetime.strptime(DEPODTE_STR, '%m%d%Y').date()
            RECORD['DEPDATE'] = DEPDATE
        except:
            RECORD['DEPDATE'] = None
    else:
        RECORD['DEPDATE'] = None
    
    # CONVERT ORGDATE: EXTRACT FIRST 6 CHARS FROM 9-DIGIT NUMBER, PARSE AS MMDDYY6
    if RECORD.get('ORGDATE'):
        ORGDATE_STR = str(RECORD['ORGDATE']).zfill(9)[:6]
        try:
            ORGDTE = datetime.strptime(ORGDATE_STR, '%m%d%y').date()
            RECORD['ORGDATE'] = ORGDTE
        except:
            pass
    
    # CONVERT MATDATE: EXTRACT 8 CHARS FROM MATDATE, PARSE AS YYMMDD8
    if RECORD.get('MATDATE'):
        MATDATE_STR = str(RECORD['MATDATE']).zfill(8)
        try:
            MDATES = datetime.strptime(MATDATE_STR, '%y%m%d').date()
            RECORD['MATDATE'] = MDATES
        except:
            pass
    
    # CONVERT LASTACTV: EXTRACT FIRST 6 CHARS FROM 9-DIGIT NUMBER, PARSE AS MMDDYY6
    if RECORD.get('LASTACTV'):
        LASTACTV_STR = str(RECORD['LASTACTV']).zfill(9)[:6]
        try:
            LDATES = datetime.strptime(LASTACTV_STR, '%m%d%y').date()
            RECORD['LASTACTV'] = LDATES
        except:
            pass
    
    # PURPOSE LOGIC
    PURPOSE1 = RECORD.get('PURPOSE')
    
    # IF PURPOSE = '4', CHANGE TO '1'
    if PURPOSE1 == '4':
        RECORD['PURPOSE'] = '1'
    
    # FILTER: EXCLUDE SPECIFIC ACCOUNT RANGES
    ACCTNO = RECORD.get('ACCTNO', 0)
    
    # EXCLUDE IF IN RANGES:
    # 1590000000-1599999999
    # 1689999999-1699999999  
    # 1789999999-1799999999
    if (1590000000 <= ACCTNO <= 1599999999) or \
       (1689999999 <= ACCTNO <= 1699999999) or \
       (1789999999 <= ACCTNO <= 1799999999):
        continue  # SKIP THIS RECORD
    
    RESULT_ROWS.append(RECORD)

print(f"RECORDS AFTER ACCOUNT RANGE FILTER: {len(RESULT_ROWS)}")

# ============================================================================
# CREATE OUTPUT DATAFRAME AND SAVE
# ============================================================================
if len(RESULT_ROWS) > 0:
    OUTPUT_DF = pl.DataFrame(RESULT_ROWS)
    
    # SELECT FINAL COLUMNS (FDCDVAR)
    FINAL_COLUMNS = [
        'BRANCH', 'ACCTNO', 'CDNO', 'STATEC', 'CUSTCD', 'OPENIND', 'CURBAL',
        'ORGDATE', 'MATDATE', 'RATE', 'RENEWAL', 'INTPLAN', 'LASTACTV',
        'TERM', 'PURPOSE', 'ORIGAMT', 'INTPAY', 'LMATDATE', 'PAYMENT', 'DEPDATE',
        'INTTFRACCT', 'INTFREQ', 'INTFREQID', 'MATID', 'PRODUCT',
        'PEND_INTPLAN', 'PRN_DISP_OPT', 'PRN_RENEW', 'PRN_TFR_ACCT'
    ]
    
    OUTPUT_DF = OUTPUT_DF.select(FINAL_COLUMNS)
    OUTPUT_ARROW = OUTPUT_DF.to_arrow()
    
    # OUTPUT FILE NAMES
    OUTPUT_FILENAME = f"IFDCD{REPTYEAR}{REPTMON}{REPTDAY}"
    OUTPUT_PARQUET = FIXEDCD_DIR / f"{OUTPUT_FILENAME}.parquet"
    OUTPUT_CSV = FIXEDCD_DIR / f"{OUTPUT_FILENAME}.csv"
    
    pq.write_table(OUTPUT_ARROW, OUTPUT_PARQUET)
    csv.write_csv(OUTPUT_ARROW, OUTPUT_CSV)
    
    print(f"\nSAVED: {OUTPUT_PARQUET}")
    print(f"SAVED: {OUTPUT_CSV}")
else:
    print("\nWARNING: NO RECORDS TO OUTPUT")

# ============================================================================
# SUMMARY REPORT
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"OUTPUT FILE      : IFDCD{REPTYEAR}{REPTMON}{REPTDAY}")
print(f"REPORT DATE      : {REPTDATE}")
print(f"WEEK NUMBER      : {NOWK}")
print(f"TOTAL RECORDS    : {len(RESULT_ROWS):,}")
print("="*80)

print("\nFILTER CRITERIA:")
print("  - OPENIND IN ('D', 'O')")
print("  - EXCLUDE ACCOUNT RANGES:")
print("    * 1590000000 - 1599999999")
print("    * 1689999999 - 1699999999")
print("    * 1789999999 - 1799999999")

print("\nDATE CONVERSIONS:")
print("  - DEPODTE  → DEPDATE  (MMDDYY8 FORMAT)")
print("  - ORGDATE  → ORGDATE  (MMDDYY6 FORMAT)")
print("  - MATDATE  → MATDATE  (YYMMDD8 FORMAT)")
print("  - LASTACTV → LASTACTV (MMDDYY6 FORMAT)")

print("\nFIELD RENAMES:")
print("  - ACCTTYPE → PRODUCT")
print("  - PENDINT  → PEND_INTPLAN")

print("\nPURPOSE LOGIC:")
print("  - IF PURPOSE = '4' THEN PURPOSE = '1'")

# ============================================================================
# SAMPLE RECORDS
# ============================================================================
if len(RESULT_ROWS) > 0:
    print("\n" + "="*80)
    print("SAMPLE RECORDS (FIRST 5)")
    print("="*80)
    
    OUTPUT_PL = pl.DataFrame(RESULT_ROWS)
    sample = OUTPUT_PL.select([
        'BRANCH', 'ACCTNO', 'CDNO', 'OPENIND', 'CURBAL', 
        'ORGDATE', 'MATDATE', 'RATE', 'PURPOSE', 'PRODUCT'
    ]).head(5)
    print(sample)

# CLOSE CONNECTION
con.close()
print("\nPROCESSING COMPLETE!")
