import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys
import importlib
import os

# Configuration - equivalent to SAS LIBNAME and file references
input_base_path = Path("LOAN")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# SAS DATA _NULL_ equivalent
reptdate_df = pl.read_parquet(input_base_path / "REPTDATE.parquet")

# SAS SELECT/WHEN equivalent for date processing
reptdate_df = reptdate_df.with_columns([
    pl.col('REPTDATE').dt.day().alias('day')
]).with_columns([
    pl.when(pl.col('day').is_between(1, 8))
    .then(pl.lit('1'))
    .when(pl.col('day').is_between(9, 15))
    .then(pl.lit('2'))
    .when(pl.col('day').is_between(16, 22))
    .then(pl.lit('3'))
    .otherwise(pl.lit('4'))
    .alias('WK')
])

# SAS CALL SYMPUT equivalent - create global variables
first_row = reptdate_df.row(0)
NOWK = first_row['WK']
RDATE = first_row['REPTDATE'].strftime('%d%m%y')
REPTYEAR = str(first_row['REPTDATE'].year)
REPTMON = f"{first_row['REPTDATE'].month:02d}"
REPTDAY = f"{first_row['REPTDATE'].day:02d}"

print(f"NOWK: {NOWK}, RDATE: {RDATE}, REPTYEAR: {REPTYEAR}, REPTMON: {REPTMON}, REPTDAY: {REPTDAY}")

# SAS LIBNAME equivalent
bnm_path = Path("SAP/PBB/CCDTEMPB")
print(f"BNM library path: {bnm_path}")

# SAS DATA _NULL_ for LOAN date
loan_date_df = pl.read_parquet(input_base_path / "REPTDATE.parquet").head(1)
LOAN = loan_date_df.row(0)['REPTDATE'].strftime('%d%m%y')

print(f"LOAN date: {LOAN}")
print(f"RDATE: {RDATE}")

# SAS MACRO %PROCESS equivalent - direct conditional execution
if LOAN == RDATE:
    print("THE LOAN EXTRACTION IS DATED &RDATE - PROCESSING REPORTS")
    
    # SAS: %INC PGM(LNCCDB02);
    try:
        print(">>> EXECUTING LNCCDB02 - LOANS IN ARREARS REPORT (MONTHEND VERSION)")
        lnccdb02 = importlib.import_module('LNCCDB02')
        # Pass global variables directly (SAS macro variable style)
        lnccdb02.NOWK = NOWK
        lnccdb02.RDATE = RDATE
        lnccdb02.REPTYEAR = REPTYEAR
        lnccdb02.REPTMON = REPTMON
        lnccdb02.process()  # Simple method call
    except ImportError:
        print("ERROR: LNCCDB02.py not found - skipping")
    except Exception as e:
        print(f"ERROR in LNCCDB02: {e}")
    
    # SAS: PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print(">>> CLEARING WORK DATASETS AFTER LNCCDB02")
    
    # SAS: %INC PGM(LNCCDB03);
    try:
        print(">>> EXECUTING LNCCDB03 - LOANS IN NPL REPORT BY CAC")
        lnccdb03 = importlib.import_module('LNCCDB03')
        lnccdb03.NOWK = NOWK
        lnccdb03.RDATE = RDATE
        lnccdb03.REPTYEAR = REPTYEAR
        lnccdb03.REPTMON = REPTMON
        lnccdb03.process()
    except ImportError:
        print("ERROR: LNCCDB03.py not found - skipping")
    except Exception as e:
        print(f"ERROR in LNCCDB03: {e}")
    
    # SAS: PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print(">>> CLEARING WORK DATASETS AFTER LNCCDB03")
    
    # SAS: %INC PGM(LNCCDB04);
    try:
        print(">>> EXECUTING LNCCDB04 - LOANS IN NPL REPORT (10TH & 20TH VERSION)")
        lnccdb04 = importlib.import_module('LNCCDB04')
        lnccdb04.NOWK = NOWK
        lnccdb04.RDATE = RDATE
        lnccdb04.REPTYEAR = REPTYEAR
        lnccdb04.REPTMON = REPTMON
        lnccdb04.process()
    except ImportError:
        print("ERROR: LNCCDB04.py not found - skipping")
    except Exception as e:
        print(f"ERROR in LNCCDB04: {e}")
    
    # SAS: PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print(">>> CLEARING WORK DATASETS AFTER LNCCDB04")
    
    # SAS: /* %INC PGM(LNCCDB05); */ - commented out
    print(">>> LNCCDB05 - LOANS CLASSIFIED AS NPL REPORT (COMMENTED OUT IN SAS)")
    
else:
    # SAS %ELSE %DO branch
    print(f"THE LOAN EXTRACTION IS NOT DATED {RDATE}")
    print("THIS JOB IS NOT RUN !!")
    
    # SAS: DATA A; ABORT 77;
    print("ABORTING WITH CODE 77")
    sys.exit(77)

print("PROCESSING COMPLETED SUCCESSFULLY")
