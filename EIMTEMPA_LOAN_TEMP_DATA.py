import polars as pl
import duckdb
from pathlib import Path
import datetime
import sys
import importlib

# Configuration
bnm_path = Path("BNM")
loan_path = Path("LOAN")
cap_path = Path("CAP")
ln_path = Path("LN")
output_path = Path("output")
output_path.mkdir(exist_ok=True)

# DATA BNM.REPTDATE;
today = datetime.date.today()
date_string = f"01{today.month:02d}{today.year}"
reptdate = datetime.datetime.strptime(date_string, '%d%m%Y').date() - datetime.timedelta(days=1)

# SELECT; WHEN conditions for NOWK
reptday = reptdate.day
if 8 <= reptday <= 14:
    NOWK = '1'
elif 15 <= reptday <= 21:
    NOWK = '2' 
elif 21 <= reptday <= 27:
    NOWK = '3'
else:
    NOWK = '4'

# PREVDATE = MDY(MONTH(REPTDATE),1,YEAR(REPTDATE))-1;
prevdate = datetime.date(reptdate.year, reptdate.month, 1) - datetime.timedelta(days=1)

# CALL SYMPUT equivalent
PREVMON = f"{prevdate.month:02d}"
PREVYEAR = prevdate.strftime('%y')  # YEAR2.
RDATE = reptdate.strftime('%d%m%y')  # DDMMYY8.
REPTYEAR = str(reptdate.year)  # YEAR4.
REPTMON = f"{reptdate.month:02d}"  # Z2.
REPTDAY = f"{reptdate.day:02d}"  # Z2.

print(f"NOWK: {NOWK}, PREVMON: {PREVMON}, PREVYEAR: {PREVYEAR}")
print(f"RDATE: {RDATE}, REPTYEAR: {REPTYEAR}, REPTMON: {REPTMON}, REPTDAY: {REPTDAY}")

# Save REPTDATE to BNM
reptdate_df = pl.DataFrame({'REPTDATE': [reptdate]})
reptdate_df.write_parquet(bnm_path / "REPTDATE.parquet")

# DATA _NULL_ for LOAN date
loan_date_df = pl.read_parquet(loan_path / "REPTDATE.parquet").head(1)
LOAN = loan_date_df.row(0)['REPTDATE'].strftime('%d%m%y')  # DDMMYY8.

print(f"LOAN date: {LOAN}")
print(f"RDATE: {RDATE}")

# MACRO %PROCESS equivalent
if LOAN == RDATE:
    print("PROCESSING LOAN DATA")
    
    # %INC PGM(PBBLNFMT);
    try:
        pbblnfmt = importlib.import_module('PBBLNFMT')
        pbblnfmt.process()
    except ImportError:
        print("NOTE: PBBLNFMT not available")
    
    # %LET LOAN= macro variable
    loan_columns = [
        'BRANCH', 'ACCTNO', 'NAME', 'NOTENO', 'PRODUCT', 'BALANCE',
        'BLDATE', 'ARREAR', 'ARREAR2', 'THISDATE', 'DAYDIFF', 'LOANSTAT', 
        'CHECKDT', 'CENSUS', 'COLLDESC', 'ISSDTE', 'FEEDUE', 'BORSTAT',
        'BILPAY', 'NOISTLPD', 'LASTRAN', 'MATURDT', 'LSTTRNAM',
        'LSTTRNCD', 'USER5', 'CAP', 'PAYAMT', 'ORGBAL', 'CURBAL', 
        'DELQCD', 'BILTOT', 'BILDUE'
    ]
    
    # PROC SORT DATA=LOAN.LNNOTE OUT=LNNOTE; BY ACCTNO COMMNO;
    lnnote_df = pl.read_parquet(loan_path / "LNNOTE.parquet").sort(['ACCTNO', 'COMMNO'])
    
    # PROC SORT DATA=LOAN.LNCOMM OUT=LNCOMM NODUPKEY; BY ACCTNO COMMNO;
    lncomm_df = pl.read_parquet(loan_path / "LNCOMM.parquet").unique(subset=['ACCTNO', 'COMMNO']).sort(['ACCTNO', 'COMMNO'])
    
    # DATA LNNOTE; MERGE LNNOTE(IN=A) LNCOMM(IN=B); BY ACCTNO COMMNO;
    lnnote_merged = lnnote_df.join(lncomm_df, on=['ACCTNO', 'COMMNO'], how='inner', suffix='_comm')
    
    # IF A AND PAIDIND NE 'P';
    lnnote_filtered = lnnote_merged.filter(pl.col('PAIDIND') != 'P')
    
    # PROC SORT DATA=LNNOTE; BY ACCTNO NOTENO;
    lnnote_sorted = lnnote_filtered.sort(['ACCTNO', 'NOTENO'])
    
    # PROC SORT DATA=CAP.CAP&PREVMON&PREVYEAR OUT=CAP&PREVMON&PREVYEAR;
    cap_filename = f"CAP{PREVMON}{PREVYEAR}.parquet"
    cap_df = pl.read_parquet(cap_path / cap_filename).select([
        'ACCTNO', 'NOTENO', 'CAP'
    ]).sort(['ACCTNO', 'NOTENO'])
    
    # DATA LNNOTE; MERGE CAP LNNOTE(IN=A); BY ACCTNO NOTENO;
    lnnote_with_cap = cap_df.join(lnnote_sorted, on=['ACCTNO', 'NOTENO'], how='right')
    
    # DATA SASLN; SET multiple datasets;
    loan_filename = f"LOAN{REPTMON}{NOWK}.parquet"
    wof_filename = f"LNWOF{REPTMON}{NOWK}.parquet" 
    wod_filename = f"LNWOD{REPTMON}{NOWK}.parquet"
    
    sasln_parts = []
    try:
        sasln_parts.append(pl.read_parquet(ln_path / loan_filename))
    except FileNotFoundError:
        print(f"NOTE: {loan_filename} not found")
    
    try:
        sasln_parts.append(pl.read_parquet(ln_path / wof_filename))
    except FileNotFoundError:
        print(f"NOTE: {wof_filename} not found")
        
    try:
        sasln_parts.append(pl.read_parquet(ln_path / wod_filename))
    except FileNotFoundError:
        print(f"NOTE: {wod_filename} not found")
    
    if sasln_parts:
        sasln_df = pl.concat(sasln_parts, how="diagonal")
        # PROC SORT DATA=SASLN; BY ACCTNO NOTENO;
        sasln_sorted = sasln_df.sort(['ACCTNO', 'NOTENO'])
    else:
        sasln_sorted = pl.DataFrame()
    
    # DATA BNM.LOANTEMP &LOAN;
    if not sasln_sorted.is_empty():
        loantemp_df = lnnote_with_cap.join(sasln_sorted, on=['ACCTNO', 'NOTENO'], how='inner', suffix='_sasln')
    else:
        loantemp_df = lnnote_with_cap
        
    # Apply filters and transformations
    loantemp_processed = loantemp_df.filter(
        pl.col('PAIDIND') != 'P'
    ).with_columns([
        # IF BRANCH = 0 THEN BRANCH = NTBRCH;
        pl.when(pl.col('BRANCH') == 0)
        .then(pl.col('NTBRCH'))
        .otherwise(pl.col('BRANCH'))
        .alias('BRANCH'),
        
        # NOISTLPD calculation
        pl.when((pl.col('ORGBAL') > 0) & (pl.col('CURBAL') > 0) & (pl.col('PAYAMT') > 0))
        .then(((pl.col('ORGBAL') - pl.col('CURBAL')) / pl.col('PAYAMT')).round(2))
        .otherwise(0.0)
        .alias('NOISTLPD_temp')
    ]).with_columns([
        # IF BORSTAT = 'F' THEN NOISTLPD = .;
        pl.when(pl.col('BORSTAT') == 'F')
        .then(None)
        .otherwise(pl.col('NOISTLPD_temp'))
        .alias('NOISTLPD')
    ]).with_columns([
        # LASTRAN date conversion
        pl.when(~pl.col('LASTTRAN').is_in([None, 0]))
        .then(pl.col('LASTTRAN').cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, '%m%d%Y'))
        .otherwise(pl.col('LASTTRAN'))
        .alias('LASTRAN'),
        
        # MATURDT date conversion  
        pl.when(~pl.col('MATUREDT').is_in([None, 0]))
        .then(pl.col('MATUREDT').cast(pl.Utf8).str.slice(0, 8).str.strptime(pl.Date, '%m%d%Y'))
        .otherwise(pl.col('MATUREDT'))
        .alias('MATURDT'),
        
        # THISDATE = INPUT("&RDATE", DDMMYY8.);
        pl.lit(datetime.datetime.strptime(RDATE, '%d%m%y')).alias('THISDATE'),
        
        # CHECKDT logic
        pl.when(pl.col('ISSDTE') >= datetime.date(1998, 1, 1))
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
        .alias('CHECKDT'),
        
        # DAYDIFF calculation
        pl.when(pl.col('BLDATE') > 0)
        .then(pl.col('THISDATE').dt.date() - pl.col('BLDATE').cast(pl.Date))
        .otherwise(0)
        .alias('DAYDIFF_temp')
    ]).with_columns([
        # OLDNOTEDAYARR logic (simplified)
        pl.when((pl.col('OLDNOTEDAYARR') > 0) & (pl.col('NOTENO').is_between(98000, 98999)))
        .then(
            pl.when(pl.col('DAYDIFF_temp') < 0)
            .then(0)
            .otherwise(pl.col('DAYDIFF_temp'))
            + pl.col('OLDNOTEDAYARR')
        )
        .otherwise(pl.col('DAYDIFF_temp'))
        .alias('DAYDIFF')
    ]).filter(
        pl.col('REVERSED') != 'Y'
    ).select(loan_columns)  # Apply &LOAN keep list
    
    # Save final output
    loantemp_processed.write_parquet(bnm_path / "LOANTEMP.parquet")
    loantemp_processed.write_csv(output_path / "LOANTEMP.csv")
    
    print("BNM.LOANTEMP created successfully")
    
else:
    # %ELSE %DO;
    print(f"THE LOAN EXTRACTION IS NOT DATED {RDATE}")
    print("THIS JOB IS NOT RUN !!")
    sys.exit(77)

print("PROCESSING COMPLETED SUCCESSFULLY")
