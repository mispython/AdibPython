import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys
import importlib

# Configuration
input_base_path = Path("LOAN")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# SAS DATA _NULL_ equivalent for REPTDATE calculation
# REPTDATE=INPUT('01'||PUT(MONTH(TODAY()), Z2.)||PUT(YEAR(TODAY()), 4.), DDMMYY8.)-1;
today = datetime.date.today()
# '01' + month (z2) + year (4) = '01MMYYYY' then convert to date and subtract 1
date_string = f"01{today.month:02d}{today.year}"
reptdate = datetime.datetime.strptime(date_string, '%d%m%Y').date() - datetime.timedelta(days=1)

# SAS CALL SYMPUT equivalent
RDATE = reptdate.strftime('%d%m%y')  # DDMMYY8.
REPTYEAR = reptdate.strftime('%Y')   # YEAR4.
REPTMON = f"{reptdate.month:02d}"    # Z2.
REPTDAY = f"{reptdate.day:02d}"      # Z2.

print(f"RDATE: {RDATE}")
print(f"REPTYEAR: {REPTYEAR}") 
print(f"REPTMON: {REPTMON}")
print(f"REPTDAY: {REPTDAY}")

# SAS DATA _NULL_ for LOAN date
# SET LOAN.REPTDATE(OBS=1);
loan_date_df = pl.read_parquet(input_base_path / "REPTDATE.parquet").head(1)
LOAN = loan_date_df.row(0)['REPTDATE'].strftime('%d%m%y')  # DDMMYY8.

print(f"LOAN date: {LOAN}")
print(f"RDATE: {RDATE}")

# SAS MACRO %PROCESS equivalent 
# Note: The entire condition is commented out in SAS, so it always runs LNCCDB06
# but the structure should still be preserved

# SAS commented condition:
/*
%IF "&LOAN"="&RDATE" %THEN %DO;
*/
# Since condition is commented out, this block always executes:

# SAS: %INC PGM(LNCCDB06);
try:
    print(">>> EXECUTING LNCCDB06")
    lnccdb06 = importlib.import_module('LNCCDB06')
    # Pass all global variables that would be available in SAS
    lnccdb06.RDATE = RDATE
    lnccdb06.REPTYEAR = REPTYEAR 
    lnccdb06.REPTMON = REPTMON
    lnccdb06.REPTDAY = REPTDAY
    lnccdb06.LOAN = LOAN
    lnccdb06.process()
except ImportError:
    print("ERROR: LNCCDB06.py not found")
    sys.exit(1)
except Exception as e:
    print(f"ERROR in LNCCDB06: {e}")
    sys.exit(1)

# SAS: PROC DATASETS LIB=WORK KILL NOLIST; RUN;
print(">>> CLEARING WORK DATASETS AFTER LNCCDB06")
# In Python, we don't need to manually clear like SAS, but we log it

# SAS commented else block:
/*
%ELSE %DO;
    %PUT THE LOAN EXTRACTION IS NOT DATED &RDATE;
    %PUT THIS JOB IS NOT RUN !!;
    %DO;
        DATA A;
            ABORT 77;
    %END;
%END;
*/

print("PROCESSING COMPLETED SUCCESSFULLY")
