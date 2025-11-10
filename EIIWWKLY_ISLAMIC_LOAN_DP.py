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

# Process REPTDATE with CORRECTED week logic using ranges
processed_reptdate = reptdate_df.with_columns([
    pl.col('REPTDATE').dt.day().alias('day')
]).with_columns([
    pl.when(pl.col('day') <= 8).then(pl.struct([
        pl.lit(1).alias('sdd'),
        pl.lit('1').alias('wk'),
        pl.lit('4').alias('wk1')
    ]))
    .when(pl.col('day') <= 15).then(pl.struct([
        pl.lit(9).alias('sdd'),
        pl.lit('2').alias('wk'),
        pl.lit('1').alias('wk1')
    ]))
    .when(pl.col('day') <= 22).then(pl.struct([
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
        lalwpbbd_module = importlib.import_module('LALWPBBD')
        # Pass all global variables
        lalwpbbd_module.NOWK = NOWK
        lalwpbbd_module.NOWK1 = NOWK1
        lalwpbbd_module.REPTMON = REPTMON
        lalwpbbd_module.REPTMON1 = REPTMON1
        lalwpbbd_module.REPTYEAR = REPTYEAR
        lalwpbbd_module.REPTYR = REPTYR
        lalwpbbd_module.REPTDAY = REPTDAY
        lalwpbbd_module.RDATE = RDATE
        lalwpbbd_module.SDATE = SDATE
        lalwpbbd_module.TDATE = TDATE
        lalwpbbd_module.QTR = QTR
        lalwpbbd_module.LOAN = LOAN
        lalwpbbd_module.DEPOSIT = DEPOSIT
        lalwpbbd_module.process()
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
        lalwpbbi_module = importlib.import_module('LALWPBBI')
        # Pass all global variables
        lalwpbbi_module.NOWK = NOWK
        lalwpbbi_module.NOWK1 = NOWK1
        lalwpbbi_module.REPTMON = REPTMON
        lalwpbbi_module.REPTMON1 = REPTMON1
        lalwpbbi_module.REPTYEAR = REPTYEAR
        lalwpbbi_module.REPTYR = REPTYR
        lalwpbbi_module.REPTDAY = REPTDAY
        lalwpbbi_module.RDATE = RDATE
        lalwpbbi_module.SDATE = SDATE
        lalwpbbi_module.TDATE = TDATE
        lalwpbbi_module.QTR = QTR
        lalwpbbi_module.LOAN = LOAN
        lalwpbbi_module.DEPOSIT = DEPOSIT
        lalwpbbi_module.process()
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
        lalwpbbu_module = importlib.import_module('LALWPBBU')
        # Pass all global variables
        lalwpbbu_module.NOWK = NOWK
        lalwpbbu_module.NOWK1 = NOWK1
        lalwpbbu_module.REPTMON = REPTMON
        lalwpbbu_module.REPTMON1 = REPTMON1
        lalwpbbu_module.REPTYEAR = REPTYEAR
        lalwpbbu_module.REPTYR = REPTYR
        lalwpbbu_module.REPTDAY = REPTDAY
        lalwpbbu_module.RDATE = RDATE
        lalwpbbu_module.SDATE = SDATE
        lalwpbbu_module.TDATE = TDATE
        lalwpbbu_module.QTR = QTR
        lalwpbbu_module.LOAN = LOAN
        lalwpbbu_module.DEPOSIT = DEPOSIT
        lalwpbbu_module.process()
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
        lalweirc_module = importlib.import_module('LALWEIRC')
        # Pass all global variables
        lalweirc_module.NOWK = NOWK
        lalweirc_module.NOWK1 = NOWK1
        lalweirc_module.REPTMON = REPTMON
        lalweirc_module.REPTMON1 = REPTMON1
        lalweirc_module.REPTYEAR = REPTYEAR
        lalweirc_module.REPTYR = REPTYR
        lalweirc_module.REPTDAY = REPTDAY
        lalweirc_module.RDATE = RDATE
        lalweirc_module.SDATE = SDATE
        lalweirc_module.TDATE = TDATE
        lalweirc_module.QTR = QTR
        lalweirc_module.LOAN = LOAN
        lalweirc_module.DEPOSIT = DEPOSIT
        lalweirc_module.process()
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
