"""
EIIWOF07 - Bad Debt Write-Off Account Summary
Includes: PBBLNFMT format definitions

Key Purpose: Generate write-off summary from NPL.WOFFHP file
(pre-identified HP accounts for write-off)
"""

import polars as pl
from datetime import datetime
import sys

# Import format definition programs (%INC PGM equivalent)
sys.path.insert(0, '/mnt/user-data/outputs')
from pbblnfmt import get_branch_name, HPD
from pbbelf import LIBRARY_PATHS, format_mmddyy10

# Use library paths from PBBELF
LOAN_DIR = LIBRARY_PATHS['LOAN']
NPL_DIR = LIBRARY_PATHS['NPL6']  # NPL library
SASLN_DIR = 'data/sasln/'

OUTPUT_FILE1 = 'data/woffacct.txt'  # Fixed-width output
OUTPUT_FILE2 = 'data/woffacc2.txt'  # CSV output with headers

# Read report date
df_reptdate = pl.read_parquet(f'{LOAN_DIR}REPTDATE.parquet')
reptdate = df_reptdate['REPTDATE'][0]

day = reptdate.day
if day == 8:
    wk = '1'
    wk1 = '4'
elif day == 15:
    wk = '2'
    wk1 = '1'
elif day == 22:
    wk = '3'
    wk1 = '2'
else:
    wk = '4'
    wk1 = '3'

mm = reptdate.month
mm1 = mm - 1 if mm > 1 else 12

nowk = wk
nowks = '4'  # Always 4th week for previous month
nowk1 = wk1
reptmon = f'{mm:02d}'
reptmon1 = f'{mm1:02d}'
rdate = reptdate.strftime('%d/%m/%y')

print(f"Processing Bad Debt Write-Off Account Summary")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Week: {nowk}, Previous Month: {reptmon1}")

# Step 1: Get IIS data
df_iis = pl.read_parquet(f'{NPL_DIR}IIS.parquet').unique(subset=['ACCTNO'])

# Step 2: Get SP data
df_sp = pl.read_parquet(f'{NPL_DIR}SP2.parquet').unique(subset=['ACCTNO'])

# Step 3: KEY SOURCE - Get WOFFHP data (pre-identified HP write-off accounts)
df_woff = pl.read_parquet(f'{NPL_DIR}WOFFHP.parquet').sort('ACCTNO')

# Step 4: Merge SP, IIS, and WOFFHP
df_npl = df_sp.join(df_iis, on='ACCTNO', how='outer').join(
    df_woff, on='ACCTNO', how='inner', suffix='_woff'
).with_columns([
    pl.col('IIS').round(2),
    pl.col('OI').round(2),
    pl.col('SP').round(2),
    pl.col('MARKETVL').round(2),
    pl.col('BRANCH').str.slice(3, 4).alias('BRNO'),
    pl.col('BRANCH').str.slice(0, 3).alias('BRABBR')
]).select([
    'BRNO', 'BRABBR', 'NAME', 'ACCTNO', 'NOTENO', 'IIS', 'OI', 
    'TOTIIS', 'BORSTAT', 'SP', 'MARKETVL', 'DAYS', 'BRANCH'
])

# Step 5: Get loan data for HPD loan types (from PBBLNFMT)
df_loan_raw = pl.read_parquet(f'{LOAN_DIR}LNNOTE.parquet').filter(
    pl.col('LOANTYPE').is_in(HPD)
).unique(subset=['ACCTNO', 'NOTENO'])

# Merge NPL with LOAN data
df_loan = df_npl.join(
    df_loan_raw, on=['ACCTNO', 'NOTENO'], how='left', suffix='_loan'
).filter(pl.col('ACCTNO').is_not_null())

# Step 6: Calculate derived fields
loan_records = []
for row in df_loan.iter_rows(named=True):
    new_row = row.copy()
    
    # POSTAMT calculation
    feetotal = row.get('FEETOTAL', 0) or 0
    nfeeamt5 = row.get('NFEEAMT5', 0) or 0
    new_row['POSTAMT'] = feetotal + nfeeamt5
    
    # OTHERAMT calculation
    feeamt3 = row.get('FEEAMT3', 0) or 0
    new_row['OTHERAMT'] = feeamt3 - new_row['POSTAMT']
    
    # OIFEEAMT calculation
    feetot2 = row.get('FEETOT2', 0) or 0
    feeamta = row.get('FEEAMTA', 0) or 0
    feeamt5 = row.get('FEEAMT5', 0) or 0
    new_row['OIFEEAMT'] = feetot2 - feeamta + feeamt5
    
    # ECSRRSRV handling
    ecsrrsrv = row.get('ECSRRSRV', 0) or 0
    new_row['ECSRRSRV'] = 0 if ecsrrsrv <= 0 else ecsrrsrv
    
    # MATDATE formatting
    maturedt = row.get('MATUREDT')
    if maturedt:
        maturedt_str = str(int(maturedt)).zfill(11)[:8]
        try:
            matdate_dt = datetime.strptime(maturedt_str, '%m%d%Y')
            new_row['MATDATE'] = format_mmddyy10(matdate_dt)
            new_row['MATUREDT'] = matdate_dt.date()
        except:
            new_row['MATDATE'] = ''
    else:
        new_row['MATDATE'] = ''
    
    loan_records.append(new_row)

df_loan = pl.DataFrame(loan_records)

# Step 7: Get previous balance from SASLN
df_sasln = pl.read_parquet(f'{SASLN_DIR}LOAN{reptmon1}{nowks}.parquet').select([
    'ACCTNO', 'NOTENO', 'CURBAL'
]).rename({'CURBAL': 'PREVBAL'}).sort(['ACCTNO', 'NOTENO'])

df_sasln = df_sasln.join(
    df_npl.select(['ACCTNO', 'NOTENO']), 
    on=['ACCTNO', 'NOTENO'], 
    how='inner'
)

# Step 8: Merge SASLN, LOAN, and NPL
df_woff = df_sasln.join(df_loan, on=['ACCTNO', 'NOTENO'], how='outer').join(
    df_npl, on='ACCTNO', how='outer', suffix='_npl'
).with_columns([
    (pl.col('CURBAL') - pl.col('PREVBAL')).alias('PAYMENT'),
    ((pl.col('IIS') + pl.col('OIFEEAMT')).round(2)).alias('TOTIIS'),
    ((pl.col('IIS') + pl.col('OIFEEAMT') + pl.col('SP')).round(2)).alias('TOTAL')
]).sort('ACCTNO')

# Step 9: Get GUAREND (guarantor) from LNNOTE
df_lnnote = pl.read_parquet(f'{LOAN_DIR}LNNOTE.parquet').select([
    'ACCTNO', 'NOTENO', 'GUAREND'
]).sort(['ACCTNO', 'NOTENO'], descending=[False, True]).unique(subset=['ACCTNO'])

df_woff = df_woff.join(df_lnnote, on='ACCTNO', how='left', suffix='_guar')

print(f"\nBad Debt Write-Off Account Summary Complete")

# Step 10: Write fixed-width output file (WOFFACCT)
with open(OUTPUT_FILE1, 'w') as f:
    for row in df_woff.iter_rows(named=True):
        branch = (row.get('BRANCH', '') or '')[:7]
        name = (row.get('NAME', '') or '')[:24]
        acctno = row.get('ACCTNO', 0) or 0
        noteno = row.get('NOTENO', 0) or 0
        borstat = (row.get('BORSTAT', '') or '')[:1]
        iis = row.get('IIS', 0) or 0
        oifeeamt = row.get('OIFEEAMT', 0) or 0
        totiis = row.get('TOTIIS', 0) or 0
        sp = row.get('SP', 0) or 0
        total = row.get('TOTAL', 0) or 0
        curbal = row.get('CURBAL', 0) or 0
        prevbal = row.get('PREVBAL', 0) or 0
        payment = row.get('PAYMENT', 0) or 0
        ecsrrsrv = row.get('ECSRRSRV', 0) or 0
        postamt = row.get('POSTAMT', 0) or 0
        otheramt = row.get('OTHERAMT', 0) or 0
        matdate = (row.get('MATDATE', '') or '')[:10]
        loantype = row.get('LOANTYPE', 0) or 0
        intamt = row.get('INTAMT', 0) or 0
        postntrn = (row.get('POSTNTRN', '') or '')[:1]
        marketvl = row.get('MARKETVL', 0) or 0
        intearn4 = row.get('INTEARN4', 0) or 0
        days = row.get('DAYS', 0) or 0
        custcode = row.get('CUSTCODE', 0) or 0
        balance = row.get('BALANCE', 0) or 0
        guarend = (row.get('GUAREND', '') or '')[:12]
        
        # Write fixed-width record (positions as per original SAS PUT statement)
        f.write(f"{branch:<7}")                    # @1
        f.write(f"{name:<24}")                     # @9
        f.write(f"{acctno:>10.0f}")               # @34
        f.write(f"{noteno:>5.0f}")                # @45
        f.write(f"{borstat:1}")                   # @51
        f.write(f"{iis:>16.2f}")                  # @53
        f.write(f"{oifeeamt:>16.2f}")             # @69
        f.write(f"{totiis:>16.2f}")               # @85
        f.write(f"{sp:>16.2f}")                   # @101
        f.write(f"{total:>16.2f}")                # @117
        f.write(f"{curbal:>16.2f}")               # @133
        f.write(f"{prevbal:>16.2f}")              # @149
        f.write(f"{payment:>16.2f}")              # @165
        f.write(f"{ecsrrsrv:>16.2f}")             # @181
        f.write(f"{postamt:>16.2f}")              # @197
        f.write(f"{otheramt:>16.2f}")             # @213
        f.write(f"{matdate:10}")                  # @229
        f.write(f"{loantype:>3d}")                # @239
        f.write(f"{intamt:>16.2f}")               # @242
        f.write(f"{postntrn:1}")                  # @258
        f.write(f"{marketvl:>16.2f}")             # @262
        f.write(f"{intearn4:>16.2f}")             # @278
        f.write(f"{days:>6d}")                    # @294
        f.write(f"{custcode:>3d}")                # @301
        f.write(f"{balance:>16.2f}")              # @304
        f.write(f"{guarend:12}\n")                # @320

# Step 11: Write CSV output file with headers (WOFFACC2)
with open(OUTPUT_FILE2, 'w') as f:
    # Write header
    f.write("BRNO;BRABBR;NAME;ACCTNO;NOTENO;BORSTAT;IIS;OIFEEAMT;TOTIIS;SP;TOTAL;")
    f.write("CURBAL;PREVBAL;PAYMENT;ECSRRSRV;POSTAMT;OTHERAMT;MATDATE;LOANTYPE;")
    f.write("INTAMT;POSTNTRN;MARKETVL;INTEARN4;DAYS;CUSTCODE;BALANCE;I/C NO./BR NO.;\n")
    
    # Write data
    for row in df_woff.iter_rows(named=True):
        brno = row.get('BRNO', '')
        brabbr = row.get('BRABBR', '')
        name = row.get('NAME', '')
        acctno = row.get('ACCTNO', 0)
        noteno = row.get('NOTENO', 0)
        borstat = row.get('BORSTAT', '')
        iis = row.get('IIS', 0)
        oifeeamt = row.get('OIFEEAMT', 0)
        totiis = row.get('TOTIIS', 0)
        sp = row.get('SP', 0)
        total = row.get('TOTAL', 0)
        curbal = row.get('CURBAL', 0)
        prevbal = row.get('PREVBAL', 0)
        payment = row.get('PAYMENT', 0)
        ecsrrsrv = row.get('ECSRRSRV', 0)
        postamt = row.get('POSTAMT', 0)
        otheramt = row.get('OTHERAMT', 0)
        matdate = row.get('MATDATE', '')
        loantype = row.get('LOANTYPE', 0)
        intamt = row.get('INTAMT', 0)
        postntrn = row.get('POSTNTRN', '')
        marketvl = row.get('MARKETVL', 0)
        intearn4 = row.get('INTEARN4', 0)
        days = row.get('DAYS', 0)
        custcode = row.get('CUSTCODE', 0)
        balance = row.get('BALANCE', 0)
        guarend = row.get('GUAREND', '')
        
        f.write(f"{brno};{brabbr};{name};{acctno};{noteno};{borstat};")
        f.write(f"{iis};{oifeeamt};{totiis};{sp};{total};")
        f.write(f"{curbal};{prevbal};{payment};{ecsrrsrv};{postamt};{otheramt};")
        f.write(f"{matdate};{loantype};{intamt};{postntrn};")
        f.write(f"{marketvl};{intearn4};{days};{custcode};{balance};{guarend};\n")

print(f"\nOutput files generated:")
print(f"  {OUTPUT_FILE1} (Fixed-width format)")
print(f"  {OUTPUT_FILE2} (CSV format with headers)")
print(f"\nAccounts in write-off summary: {len(df_woff)}")
print(f"Total exposure: RM {df_woff['TOTAL'].sum():,.2f}")
print(f"\nKey metrics:")
print(f"  Total IIS: RM {df_woff['IIS'].sum():,.2f}")
print(f"  Total OIFEEAMT: RM {df_woff['OIFEEAMT'].sum():,.2f}")
print(f"  Total TOTIIS: RM {df_woff['TOTIIS'].sum():,.2f}")
print(f"  Total SP: RM {df_woff['SP'].sum():,.2f}")
print(f"  Total CURBAL: RM {df_woff['CURBAL'].sum():,.2f}")
print(f"  Total PREVBAL: RM {df_woff['PREVBAL'].sum():,.2f}")
print(f"  Total PAYMENT: RM {df_woff['PAYMENT'].sum():,.2f}")
print(f"\nSource: NPL.WOFFHP (pre-identified HP write-off accounts)")
