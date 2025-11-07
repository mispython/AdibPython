import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys
import importlib

# Configuration
bnm_path = Path("BNM")
loan_path = Path("LOAN")
deposit_path = Path("DEPOSIT")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# DATA REPTDATE;
reptdate_df = pl.read_parquet(bnm_path / "REPTDATE.parquet")

# Process REPTDATE with SELECT/WHEN logic
processed_reptdate = reptdate_df.with_columns([
    pl.col('REPTDATE').dt.day().alias('day')
]).with_columns([
    pl.when(pl.col('day') == 8).then(pl.struct([
        pl.lit(1).alias('sdd'),
        pl.lit('1').alias('wk'),
        pl.lit('4').alias('wk1')
    ]))
    .when(pl.col('day') == 15).then(pl.struct([
        pl.lit(9).alias('sdd'),
        pl.lit('2').alias('wk'),
        pl.lit('1').alias('wk1')
    ]))
    .when(pl.col('day') == 22).then(pl.struct([
        pl.lit(16).alias('sdd'),
        pl.lit('3').alias('wk'),
        pl.lit('2').alias('wk1')
    ]))
    .otherwise(pl.struct([
        pl.lit(23).alias('sdd'),
        pl.lit('4').alias('wk'),
        pl.lit('3').alias('wk1')
    ])).alias('week_info')
]).with_columns([
    pl.col('week_info').struct.field('sdd').alias('SDD'),
    pl.col('week_info').struct.field('wk').alias('WK'),
    pl.col('week_info').struct.field('wk1').alias('WK1'),
    pl.col('REPTDATE').dt.month().alias('MM'),
    pl.when(pl.col('WK') == '1').then(
        pl.when(pl.col('REPTDATE').dt.month() == 1).then(12).otherwise(pl.col('REPTDATE').dt.month() - 1)
    ).otherwise(pl.col('REPTDATE').dt.month()).alias('MM1'),
    pl.datetime(pl.col('REPTDATE').dt.year(), pl.col('REPTDATE').dt.month(), pl.col('SDD')).alias('SDATE')
]).drop(['day', 'week_info'])

# Extract parameters - CALL SYMPUT equivalent
first_row = processed_reptdate.row(0)
NOWK = first_row['WK']
NOWK1 = first_row['WK1']
REPTMON = f"{first_row['MM']:02d}"
REPTMON1 = f"{first_row['MM1']:02d}"
REPTYEAR = str(first_row['REPTDATE'].year)
REPTYR = first_row['REPTDATE'].strftime('%y')  # YEAR2.
REPTDAY = f"{first_row['REPTDATE'].day:02d}"
RDATE = first_row['REPTDATE'].strftime('%d%m%y')
SDATE = first_row['SDATE'].strftime('%d%m%y')
TDATE = f"{first_row['REPTDATE'].day:05d}"  # Z5. format

# QTR logic
if first_row['MM'] in [3, 6, 9, 12] and first_row['REPTDATE'].day in [30, 31]:
    QTR = '1'
else:
    QTR = '0'

print(f"NOWK: {NOWK}, NOWK1: {NOWK1}, REPTMON: {REPTMON}, REPTMON1: {REPTMON1}")
print(f"REPTYEAR: {REPTYEAR}, REPTYR: {REPTYR}, REPTDAY: {REPTDAY}")
print(f"RDATE: {RDATE}, SDATE: {SDATE}, TDATE: {TDATE}, QTR: {QTR}")

# DATA _NULL_ for LOAN date
loan_date_df = pl.read_parquet(loan_path / "REPTDATE.parquet").head(1)
LOAN = loan_date_df.row(0)['REPTDATE'].strftime('%d%m%y')

print(f"LOAN date: {LOAN}")

# DATA _NULL_ for DEPOSIT date
deposit_date_df = pl.read_parquet(deposit_path / "REPTDATE.parquet").head(1)
DEPOSIT = deposit_date_df.row(0)['REPTDATE'].strftime('%d%m%y')

print(f"DEPOSIT date: {DEPOSIT}")
print(f"RDATE: {RDATE}")

# MACRO %PROCESS equivalent
if LOAN == RDATE and DEPOSIT == RDATE:
    print("PROCESSING WEEKLY REPORTS - BOTH LOAN AND DEPOSIT DATA ARE CURRENT")
    
    # SAS: %INC PGM(LALWPBBD);
    try:
        print(">>> EXECUTING LALWPBBD")
        lalw pbbd = importlib.import_module('LALWPBBD')
        # Pass all global variables
        lalw pbbd.NOWK = NOWK
        lalw pbbd.NOWK1 = NOWK1
        lalw pbbd.REPTMON = REPTMON
        lalw pbbd.REPTMON1 = REPTMON1
        lalw pbbd.REPTYEAR = REPTYEAR
        lalw pbbd.REPTYR = REPTYR
        lalw pbbd.REPTDAY = REPTDAY
        lalw pbbd.RDATE = RDATE
        lalw pbbd.SDATE = SDATE
        lalw pbbd.TDATE = TDATE
        lalw pbbd.QTR = QTR
        lalw pbbd.LOAN = LOAN
        lalw pbbd.DEPOSIT = DEPOSIT
        lalw pbbd.process()
    except ImportError:
        print("ERROR: LALWPBBD.py not found")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR in LALWPBBD: {e}")
        sys.exit(1)
    
    # SAS: PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print(">>> CLEARING WORK DATASETS AFTER LALWPBBD")
    
    # SAS: %INC PGM(LALWPBBI);
    try:
        print(">>> EXECUTING LALWPBBI")
        lalw pbbi = importlib.import_module('LALWPBBI')
        # Pass all global variables
        lalw pbbi.NOWK = NOWK
        lalw pbbi.NOWK1 = NOWK1
        lalw pbbi.REPTMON = REPTMON
        lalw pbbi.REPTMON1 = REPTMON1
        lalw pbbi.REPTYEAR = REPTYEAR
        lalw pbbi.REPTYR = REPTYR
        lalw pbbi.REPTDAY = REPTDAY
        lalw pbbi.RDATE = RDATE
        lalw pbbi.SDATE = SDATE
        lalw pbbi.TDATE = TDATE
        lalw pbbi.QTR = QTR
        lalw pbbi.LOAN = LOAN
        lalw pbbi.DEPOSIT = DEPOSIT
        lalw pbbi.process()
    except ImportError:
        print("ERROR: LALWPBBI.py not found")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR in LALWPBBI: {e}")
        sys.exit(1)
    
    # SAS: PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print(">>> CLEARING WORK DATASETS AFTER LALWPBBI")
    
    # SAS: %INC PGM(LALWPBBU);
    try:
        print(">>> EXECUTING LALWPBBU")
        lalw pbbu = importlib.import_module('LALWPBBU')
        # Pass all global variables
        lalw pbbu.NOWK = NOWK
        lalw pbbu.NOWK1 = NOWK1
        lalw pbbu.REPTMON = REPTMON
        lalw pbbu.REPTMON1 = REPTMON1
        lalw pbbu.REPTYEAR = REPTYEAR
        lalw pbbu.REPTYR = REPTYR
        lalw pbbu.REPTDAY = REPTDAY
        lalw pbbu.RDATE = RDATE
        lalw pbbu.SDATE = SDATE
        lalw pbbu.TDATE = TDATE
        lalw pbbu.QTR = QTR
        lalw pbbu.LOAN = LOAN
        lalw pbbu.DEPOSIT = DEPOSIT
        lalw pbbu.process()
    except ImportError:
        print("ERROR: LALWPBBU.py not found")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR in LALWPBBU: {e}")
        sys.exit(1)
    
    # SAS: PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print(">>> CLEARING WORK DATASETS AFTER LALWPBBU")
    
    # SAS: %INC PGM(LALWEIRC);
    try:
        print(">>> EXECUTING LALWEIRC")
        lalw eirc = importlib.import_module('LALWEIRC')
        # Pass all global variables
        lalw eirc.NOWK = NOWK
        lalw eirc.NOWK1 = NOWK1
        lalw eirc.REPTMON = REPTMON
        lalw eirc.REPTMON1 = REPTMON1
        lalw eirc.REPTYEAR = REPTYEAR
        lalw eirc.REPTYR = REPTYR
        lalw eirc.REPTDAY = REPTDAY
        lalw eirc.RDATE = RDATE
        lalw eirc.SDATE = SDATE
        lalw eirc.TDATE = TDATE
        lalw eirc.QTR = QTR
        lalw eirc.LOAN = LOAN
        lalw eirc.DEPOSIT = DEPOSIT
        lalw eirc.process()
    except ImportError:
        print("ERROR: LALWEIRC.py not found")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR in LALWEIRC: {e}")
        sys.exit(1)
    
    # SAS: PROC DATASETS LIB=WORK KILL NOLIST; RUN;
    print(">>> CLEARING WORK DATASETS AFTER LALWEIRC")
    
else:
    # %ELSE %DO;
    error_messages = []
    
    # %IF "&LOAN" NE "&RDATE" %THEN
    if LOAN != RDATE:
        error_message = f"THE LOAN EXTRACTION IS NOT DATED {RDATE}"
        print(error_message)
        error_messages.append(error_message)
    
    # %IF "&DEPOSIT" NE "&RDATE" %THEN  
    if DEPOSIT != RDATE:
        error_message = f"THE DEPOSIT EXTRACTION IS NOT DATED {RDATE}"
        print(error_message)
        error_messages.append(error_message)
    
    # %PUT THE JOB IS NOT DONE !!;
    print("THE JOB IS NOT DONE !!")
    
    # %DO; DATA A; ABORT 77; %END;
    print("ABORTING WITH CODE 77")
    sys.exit(77)

print("PROCESSING COMPLETED SUCCESSFULLY")
