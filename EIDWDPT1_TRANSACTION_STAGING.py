#!/usr/bin/env python3
"""
EIDWDPT1 - TRANSACTION STAGING DATA PROCESSING
1:1 CONVERSION FROM SAS TO PYTHON
PROCESSES TRANSACTION DATA WITH COMPLEX POSITIONAL INPUT LOGIC
HANDLES MULTIPLE FORMAT CODES AND CONDITIONAL TRAIL FIELD PARSING
"""

import duckdb
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import polars as pl

# INITIALIZE PATHS
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")
STG_DIR = OUTPUT_DIR / "stg"

# CREATE DIRECTORIES
STG_DIR.mkdir(parents=True, exist_ok=True)

# INPUT FILES
DPPSTRG1_FILE = INPUT_DIR / "dppstrg1.parquet"

# OUTPUT FILES
OUTPUT_FILE = STG_DIR / "STG_DP_DPPSTRG1.parquet"
OUTPUT_CSV = STG_DIR / "STG_DP_DPPSTRG1.csv"

# CONNECT TO DUCKDB
con = duckdb.connect(":memory:")

print("PROCESSING EIDWDPT1 - TRANSACTION STAGING DATA")

# ============================================================================
# READ AND FILTER INITIAL DATA
# FILTER: BANKNO = 33 AND FMTCODE IN (1,2,6,8)
# ============================================================================

con.execute(f"""
    CREATE OR REPLACE TABLE DPPSTRG1_RAW AS
    SELECT *
    FROM read_parquet('{DPPSTRG1_FILE}')
    WHERE BANKNO = 33 
        AND FMTCODE IN (1, 2, 6, 8)
""")

TOTAL_RECORDS = con.execute("SELECT COUNT(*) FROM DPPSTRG1_RAW").fetchone()[0]
print(f"RECORDS AFTER INITIAL FILTER (BANKNO=33, FMTCODE IN 1,2,6,8): {TOTAL_RECORDS}")

# ============================================================================
# EXTRACT TO POLARS FOR COMPLEX POSITIONAL PROCESSING
# ============================================================================
DF = pl.from_arrow(con.execute("SELECT * FROM DPPSTRG1_RAW").arrow())

print(f"PROCESSING {len(DF)} RECORDS WITH COMPLEX POSITIONAL LOGIC...")

# INITIALIZE OUTPUT COLUMNS
RESULT_ROWS = []

# ============================================================================
# PROCESS EACH RECORD WITH CONDITIONAL POSITIONAL LOGIC
# ============================================================================
for row in DF.iter_rows(named=True):
    FMTCODE = row.get('FMTCODE')
    TRLIND = row.get('TRLIND')
    
    # BASE RECORD DATA (ALWAYS PRESENT)
    RECORD = {
        'BANKNO': row.get('BANKNO'),
        'FMTCODE': FMTCODE,
        'ACCTNO': row.get('ACCTNO'),
        'DATATYPE': row.get('DATATYPE'),
        'USERID': row.get('USERID'),
        'TRANCODE': row.get('TRANCODE'),
        'CHANNEL': row.get('CHANNEL'),
        'TIME': row.get('TIME'),
        'TRANAMT': row.get('TRANAMT'),
        'CHQNO': row.get('CHQNO'),
        'TRLIND': TRLIND,
        'PENALTY': row.get('PENALTY'),
        'TRACEBR': None,
        'ORIDT': None,
        'ORITXNCD': None,
        'STRAIL1': None,
        'STRAIL2': None,
        'STRAIL3': None,
        'STRAIL4': None,
        'STRAIL5': None,
        'STRAIL6': None,
        'LTRAIL1': None,
        'LTRAIL2': None,
        'LTRAIL3': None,
        'NUMTRL': None,
        'NEW_STRAIL1': None,
        'NEW_STRAIL2': None,
        'NEW_STRAIL3': None,
        'NEW_STRAIL4': None,
        'NEW_STRAIL5': None,
        'NEW_STRAIL6': None,
        'NEW_STRAIL7': None,
        'NEW_STRAIL8': None,
        'NEW_STRAIL9': None,
        'NEW_LTRAIL1': None,
        'NEW_LTRAIL2': None,
        'NEW_LTRAIL3': None,
        'NEW_LTRAIL4': None,
        'NEW_LTRAIL5': None,
        'NEW_LTRAIL6': None,
        'NEW_LTRAIL7': None,
        'NEW_LTRAIL8': None,
        'NEW_LTRAIL9': None,
        'NEW_LTRAIL10': None,
    }
    
    # ========================================================================
    # FMTCODE 6 OR 8: EXTRACT TRACEBR BASED ON TRLIND
    # ========================================================================
    if FMTCODE in [6, 8]:
        if TRLIND == '1':
            RECORD['TRACEBR'] = row.get('TRACEBR_POS61')
        elif TRLIND == '2':
            RECORD['TRACEBR'] = row.get('TRACEBR_POS89')
        elif TRLIND == '3':
            RECORD['TRACEBR'] = row.get('TRACEBR_POS63')
        elif TRLIND == 'L':
            RECORD['TRACEBR'] = row.get('TRACEBR_POS65')
    
    # ========================================================================
    # FMTCODE 2: EXTRACT ADDITIONAL FIELDS
    # ========================================================================
    if FMTCODE == 2:
        RECORD['ORIDT'] = row.get('ORIDT')
        RECORD['ORITXNCD'] = row.get('ORITXNCD')
        RECORD['TRACEBR'] = row.get('TRACEBR_POS71')
        RECORD['STRAIL1'] = row.get('STRAIL1_POS60')
        RECORD['STRAIL2'] = row.get('STRAIL2_POS78')
        RECORD['STRAIL3'] = row.get('STRAIL3_POS96')
        RECORD['STRAIL4'] = row.get('STRAIL4_POS114')
        RECORD['STRAIL5'] = row.get('STRAIL5_POS132')
        RECORD['STRAIL6'] = row.get('STRAIL6_POS150')
        RECORD['LTRAIL1'] = row.get('LTRAIL1_POS62')
        RECORD['LTRAIL2'] = row.get('LTRAIL2_POS112')
        RECORD['LTRAIL3'] = row.get('LTRAIL3_POS162')
    
    # ========================================================================
    # FMTCODE 1: EXTRACT TRAIL FIELDS AND TRACEBR
    # ========================================================================
    if FMTCODE == 1:
        RECORD['STRAIL1'] = row.get('STRAIL1_POS60')
        RECORD['STRAIL2'] = row.get('STRAIL2_POS78')
        RECORD['STRAIL3'] = row.get('STRAIL3_POS96')
        RECORD['STRAIL4'] = row.get('STRAIL4_POS114')
        RECORD['STRAIL5'] = row.get('STRAIL5_POS132')
        RECORD['STRAIL6'] = row.get('STRAIL6_POS150')
        RECORD['LTRAIL1'] = row.get('LTRAIL1_POS62')
        RECORD['LTRAIL2'] = row.get('LTRAIL2_POS112')
        RECORD['LTRAIL3'] = row.get('LTRAIL3_POS162')
        
        # EXTRACT TRACEBR BASED ON TRLIND
        if TRLIND == '1':
            RECORD['TRACEBR'] = row.get('TRACEBR_POS61')
        elif TRLIND == '2':
            RECORD['TRACEBR'] = row.get('TRACEBR_POS89')
        elif TRLIND == '3':
            RECORD['TRACEBR'] = row.get('TRACEBR_POS63')
        elif TRLIND == 'L':
            RECORD['TRACEBR'] = row.get('TRACEBR_POS65')
    
    # ========================================================================
    # FMTCODE 1 OR 2: EXTRACT NEW_TRAIL FIELDS WITH POSITION OFFSET
    # ========================================================================
    if FMTCODE in [1, 2]:
        POS = 0
        CURRENT_TRLIND = TRLIND
        
        # FOR FMTCODE 2, READ ADDITIONAL TRLIND AT POSITION 67 AND SET OFFSET
        if FMTCODE == 2:
            POS = 8
            CURRENT_TRLIND = row.get('TRLIND_POS67', TRLIND)
        
        # IF TRLIND = 'L', EXTRACT LONG TRAIL FIELDS (50 CHARS EACH)
        if CURRENT_TRLIND == 'L':
            RECORD['NUMTRL'] = row.get(f'NUMTRL_POS{60+POS}')
            RECORD['NEW_LTRAIL1'] = row.get(f'NEW_LTRAIL1_POS{62+POS}')
            RECORD['NEW_LTRAIL2'] = row.get(f'NEW_LTRAIL2_POS{112+POS}')
            RECORD['NEW_LTRAIL3'] = row.get(f'NEW_LTRAIL3_POS{162+POS}')
            RECORD['NEW_LTRAIL4'] = row.get(f'NEW_LTRAIL4_POS{212+POS}')
            RECORD['NEW_LTRAIL5'] = row.get(f'NEW_LTRAIL5_POS{262+POS}')
            RECORD['NEW_LTRAIL6'] = row.get(f'NEW_LTRAIL6_POS{312+POS}')
            RECORD['NEW_LTRAIL7'] = row.get(f'NEW_LTRAIL7_POS{362+POS}')
            RECORD['NEW_LTRAIL8'] = row.get(f'NEW_LTRAIL8_POS{412+POS}')
            RECORD['NEW_LTRAIL9'] = row.get(f'NEW_LTRAIL9_POS{462+POS}')
            RECORD['NEW_LTRAIL10'] = row.get(f'NEW_LTRAIL10_POS{512+POS}')
        else:
            # EXTRACT SHORT TRAIL FIELDS (18 CHARS EACH)
            RECORD['NEW_STRAIL1'] = row.get(f'NEW_STRAIL1_POS{60+POS}')
            RECORD['NEW_STRAIL2'] = row.get(f'NEW_STRAIL2_POS{78+POS}')
            RECORD['NEW_STRAIL3'] = row.get(f'NEW_STRAIL3_POS{96+POS}')
            RECORD['NEW_STRAIL4'] = row.get(f'NEW_STRAIL4_POS{114+POS}')
            RECORD['NEW_STRAIL5'] = row.get(f'NEW_STRAIL5_POS{132+POS}')
            RECORD['NEW_STRAIL6'] = row.get(f'NEW_STRAIL6_POS{150+POS}')
            RECORD['NEW_STRAIL7'] = row.get(f'NEW_STRAIL7_POS{168+POS}')
            RECORD['NEW_STRAIL8'] = row.get(f'NEW_STRAIL8_POS{186+POS}')
            RECORD['NEW_STRAIL9'] = row.get(f'NEW_STRAIL9_POS{204+POS}')
    
    RESULT_ROWS.append(RECORD)

# ============================================================================
# CREATE OUTPUT DATAFRAME AND SAVE
# ============================================================================
print(f"CREATING OUTPUT WITH {len(RESULT_ROWS)} RECORDS...")

# CREATE POLARS DATAFRAME
OUTPUT_DF = pl.DataFrame(RESULT_ROWS)

# CONVERT TO PYARROW AND SAVE
OUTPUT_ARROW = OUTPUT_DF.to_arrow()
pq.write_table(OUTPUT_ARROW, OUTPUT_FILE)
csv.write_csv(OUTPUT_ARROW, OUTPUT_CSV)

print(f"\nSAVED: {OUTPUT_FILE}")
print(f"SAVED: {OUTPUT_CSV}")

# ============================================================================
# SUMMARY REPORT
# ============================================================================
print("\n" + "="*80)
print("PROCESSING SUMMARY")
print("="*80)
print(f"TOTAL RECORDS PROCESSED: {len(RESULT_ROWS)}")
print("="*80)

# COUNT BY FMTCODE
FMTCODE_COUNTS = OUTPUT_DF.group_by('FMTCODE').agg(pl.count().alias('COUNT'))
print("\nRECORDS BY FMTCODE:")
for row in FMTCODE_COUNTS.iter_rows(named=True):
    print(f"  FMTCODE {row['FMTCODE']}: {row['COUNT']} RECORDS")

# COUNT BY TRLIND
TRLIND_COUNTS = OUTPUT_DF.group_by('TRLIND').agg(pl.count().alias('COUNT'))
print("\nRECORDS BY TRLIND:")
for row in TRLIND_COUNTS.iter_rows(named=True):
    print(f"  TRLIND '{row['TRLIND']}': {row['COUNT']} RECORDS")

print("\n" + "="*80)
print("FIELD EXTRACTION LOGIC:")
print("="*80)
print("FMTCODE 1:")
print("  - BASE FIELDS + TRAIL FIELDS + TRACEBR (BASED ON TRLIND)")
print("  - NEW_TRAIL FIELDS (SHORT OR LONG BASED ON TRLIND)")
print("\nFMTCODE 2:")
print("  - BASE FIELDS + ORIDT/ORITXNCD + TRAIL FIELDS")
print("  - NEW_TRAIL FIELDS WITH POS OFFSET=8")
print("\nFMTCODE 6/8:")
print("  - BASE FIELDS + TRACEBR (BASED ON TRLIND)")
print("\nTRLIND VALUES:")
print("  '1' -> TRACEBR FROM POSITION 61")
print("  '2' -> TRACEBR FROM POSITION 89")
print("  '3' -> TRACEBR FROM POSITION 63")
print("  'L' -> TRACEBR FROM POSITION 65 + LONG TRAIL FIELDS")
print("="*80)

# CLOSE CONNECTION
con.close()
print("\nPROCESSING COMPLETE!")
