"""
EIFFTXT1 - Bad Debt Write-Off List (Conventional Banking)
Includes: PBBLNFMT, PBBELF format definitions

Key Differences from EIIFTXT1:
- RIND = 'D' (Domestic/Conventional) vs 'I' (Islamic)
- BIZTYPE = 'C' (Conventional) vs 'I' (Islamic)
- Uses CREDMSUBAC (not ICREDMSUBAC - no 'I' prefix)
"""

import polars as pl
from datetime import datetime
import sys

# Import format definition programs (%INC PGM equivalent)
sys.path.insert(0, '/mnt/user-data/outputs')
from pbblnfmt import get_branch_name, HPD, ndays_format
from pbbelf import LIBRARY_PATHS, format_ddmmyy10, format_mmddyy10

# Use library paths from PBBELF
LOAN_DIR = LIBRARY_PATHS['LOAN']
NPL_DIR = LIBRARY_PATHS['NPL6']
SASLN_DIR = 'data/sasln/'
CISNAME_DIR = 'data/cisname/'
CCRIS_DIR = 'data/ccris/'
BKCTRL_DIR = 'data/bkctrl/'

OUTPUT_FILE = 'data/wofftext.txt'
OUTPUT_FILE1 = 'data/wofftex1.txt'

# MTHPASS format - Days to Months Past Due (from PBBLNFMT NDAYS)
def mthpass_format(days):
    """Convert days to months past due - same as NDAYS"""
    return ndays_format(days)

# Additional formats loaded from BKCTRL.CISFMT (PROC FORMAT LIBRARY=WORK CNTLIN=BKCTRL.CISFMT)
DELQDES = {
    '01': 'RESIDENTIAL PROPERTY',
    '02': 'NON-RESIDENTIAL PROPERTY',
    '03': 'MOTOR VEHICLE',
    '04': 'OTHERS',
    '  ': 'NOT SPECIFIED'
}

OCCUPFMT = {
    '001': 'PROFESSIONAL',
    '002': 'BUSINESSMAN',
    '003': 'SELF EMPLOYED',
    '004': 'EMPLOYEE - PRIVATE',
    '005': 'EMPLOYEE - GOVERNMENT',
    '006': 'RETIRED',
    '999': 'OTHERS'
}

BGCFMT = {
    'B': 'BUSINESS',
    'G': 'GOVERNMENT',
    'C': 'CORPORATE',
    'I': 'INDIVIDUAL',
    '  ': 'NOT SPECIFIED'
}

def get_delq_desc(delqcd):
    """Get delinquency description"""
    return DELQDES.get(delqcd if delqcd else '  ', 'UNKNOWN')

def get_occup_desc(occupat):
    """Get occupation description"""
    return OCCUPFMT.get(occupat if occupat else '999', 'OTHERS')

def get_bgc_desc(bgc):
    """Get business/government code description"""
    return BGCFMT.get(bgc if bgc else '  ', 'NOT SPECIFIED')

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
nowks = '4'
nowk1 = wk1
reptmon = f'{mm:02d}'
reptmon1 = f'{mm1:02d}'
reptyear = f'{reptdate.year % 100:02d}'
rdate = reptdate.strftime('%d/%m/%y')

print(f"Processing Bad Debt Write-Off List (Conventional Banking)")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Week: {nowk}, Previous Month: {reptmon1}")

# Step 1: Create NPLA - Active accounts with BORSTAT='A'
df_npla = pl.read_parquet(f'{LOAN_DIR}LNNOTE.parquet').filter(
    (pl.col('BORSTAT') == 'A') &
    ~pl.col('LOANTYPE').is_in([983, 993, 678, 679, 698, 699])
).with_columns([
    pl.lit(0).alias('IIS'),
    (pl.col('FEEDUE') - pl.col('FEEDUEMS')).alias('OI'),
    (pl.lit(0) + (pl.col('FEEDUE') - pl.col('FEEDUEMS'))).alias('TOTIIS'),
    (pl.col('FEEDUEMS') + pl.col('FEEAMT16')).alias('SP')
])

# Apply BRCHCD format from PBBLNFMT
branch_list = []
for ntbrch in df_npla['NTBRCH'].to_list():
    branch_abbr = get_branch_name(ntbrch)
    branch_list.append(f"{branch_abbr} {ntbrch:03d}")

df_npla = df_npla.with_columns([
    pl.Series('BRANCH', branch_list)
]).select(['NAME', 'ACCTNO', 'NOTENO', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'])

# Step 2: Get IIS and SP data
df_iis = pl.read_parquet(f'{NPL_DIR}IIS.parquet').unique(subset=['ACCTNO', 'NOTENO'])
df_sp = pl.read_parquet(f'{NPL_DIR}SP2.parquet').unique(subset=['ACCTNO', 'NOTENO'])

# Merge IIS and SP
df_npl_data = df_sp.join(df_iis, on=['ACCTNO', 'NOTENO'], how='outer').select([
    'NAME', 'ACCTNO', 'NOTENO', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'
])

# Combine NPLA and NPL data
df_npl = pl.concat([df_npla, df_npl_data]).with_columns([
    pl.col('MARKETVL').round(2),
    pl.col('BRANCH').str.slice(3, 4).alias('BRNO'),
    pl.col('BRANCH').str.slice(0, 3).alias('BRABBR')
]).unique(subset=['ACCTNO', 'NOTENO'])

# Step 3: KEY DIFFERENCE - Get CCRIS credit submission data
# Uses CREDMSUBAC (Conventional) instead of ICREDMSUBAC (Islamic)
df_credsub = pl.read_parquet(f'{CCRIS_DIR}CREDMSUBAC{reptmon}{reptyear}.parquet').filter(
    pl.col('FACILITY').is_in(['34331', '34332'])
).rename({
    'ACCTNUM': 'ACCTNO',
    'DAYSARR': 'DAYS'
}).sort(['ACCTNO', 'NOTENO', 'DAYS'], descending=[False, False, True]).unique(
    subset=['ACCTNO', 'NOTENO']
).select(['ACCTNO', 'NOTENO', 'DAYS', 'FACILITY'])

# Step 4: Get loan data for HPD loan types (from PBBLNFMT)
df_loan_raw = pl.read_parquet(f'{LOAN_DIR}LNNOTE.parquet').filter(
    pl.col('LOANTYPE').is_in(HPD)
).unique(subset=['ACCTNO', 'NOTENO'])

# Merge NPL, CREDSUB, and LOAN
df_loan = df_npl.join(df_credsub, on=['ACCTNO', 'NOTENO'], how='left').join(
    df_loan_raw, on=['ACCTNO', 'NOTENO'], how='left', suffix='_loan'
).filter(pl.col('ACCTNO').is_not_null())

# Step 5: Calculate derived fields (same as EIIFTXT1)
loan_records = []
for row in df_loan.iter_rows(named=True):
    new_row = row.copy()
    
    new_row['POSTAMT'] = (row.get('FEETOTAL', 0) or 0) + (row.get('NFEEAMT5', 0) or 0)
    new_row['OTHERAMT'] = (row.get('FEEAMT3', 0) or 0) - new_row['POSTAMT']
    
    feetot2 = row.get('FEETOT2', 0) or 0
    feeamta = row.get('FEEAMTA', 0) or 0
    feeamt5 = row.get('FEEAMT5', 0) or 0
    new_row['OIFEEAMT'] = feetot2 - feeamta + feeamt5
    
    ecsrrsrv = row.get('ECSRRSRV', 0) or 0
    new_row['ECSRRSRV'] = 0 if ecsrrsrv <= 0 else ecsrrsrv
    
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
    
    lasttran = row.get('LASTTRAN')
    if lasttran:
        lasttran_str = str(int(lasttran)).zfill(11)[:8]
        try:
            lasttra1_dt = datetime.strptime(lasttran_str, '%m%d%Y')
            new_row['LASTTRA1'] = format_mmddyy10(lasttra1_dt)
        except:
            new_row['LASTTRA1'] = ''
    else:
        new_row['LASTTRA1'] = ''
    
    days = row.get('DAYS', 0) or 0
    mthpdue = mthpass_format(days)
    if mthpdue == 24:
        mthpdue = int((days / 365) * 12)
    new_row['MTHPDUE'] = mthpdue
    
    score2 = row.get('SCORE2', '') or ''
    contrtype = row.get('CONTRTYPE', '') or ''
    new_row['CRRGRADE'] = f"{score2}{contrtype}".strip()
    
    netproc = row.get('NETPROC', 0) or 0
    appvalue = row.get('APPVALUE', 0) or 0
    if appvalue > 0:
        new_row['MARGINFI'] = round(netproc / appvalue, 2)
    else:
        new_row['MARGINFI'] = 0
    
    birthdt = row.get('BIRTHDT', 0) or 0
    if birthdt and birthdt > 0:
        birthdt_str = str(int(birthdt)).zfill(11)[:8]
        try:
            dobmni_dt = datetime.strptime(birthdt_str, '%m%d%Y')
            new_row['DOBMNI'] = dobmni_dt.date()
        except:
            new_row['DOBMNI'] = None
    else:
        new_row['DOBMNI'] = None
    
    new_row['ECSRIND'] = 'Y' if new_row['ECSRRSRV'] > 0 else 'N'
    
    orgbal = row.get('ORGBAL', 0) or 0
    curbal = row.get('CURBAL', 0) or 0
    payamt = row.get('PAYAMT', 0) or 0
    if payamt > 0:
        new_row['BILPAID'] = int((orgbal - curbal) / payamt)
    else:
        new_row['BILPAID'] = 0
    
    nacospadt = row.get('NACOSPADT', 0) or 0
    if nacospadt > 0:
        new_row['PAY75PCT'] = 'Y'
        try:
            new_row['NACODATE'] = format_mmddyy10(nacospadt)
        except:
            new_row['NACODATE'] = ''
    else:
        new_row['PAY75PCT'] = 'N'
        new_row['NACODATE'] = ''
    
    loan_records.append(new_row)

df_loan = pl.DataFrame(loan_records)

# Step 6-8: Same as EIIFTXT1 - Customer names, Guarantors, Previous balance
df_cname = pl.read_parquet(f'{CISNAME_DIR}LOAN.parquet').filter(
    pl.col('SECCUST') == '901'
).select(['ACCTNO', 'CUSTNAM1', 'OCCUPAT', 'BGC']).unique(subset=['ACCTNO'])

df_liab = pl.read_parquet(f'{LOAN_DIR}LIAB.parquet').sort('LIABACCT')

df_liab = df_liab.join(
    df_cname.rename({'ACCTNO': 'LIABACCT', 'CUSTNAM1': 'GNAME'}),
    on='LIABACCT',
    how='left'
).with_columns([
    pl.when(pl.col('GNAME').is_null() | (pl.col('GNAME') == ''))
    .then(pl.col('LIABNAME'))
    .otherwise(pl.col('GNAME'))
    .alias('GNAME')
]).sort(['ACCTNO', 'NOTENO'])

guarantor_data = {}
for (acctno, noteno), group in df_liab.group_by(['ACCTNO', 'NOTENO']):
    gnames = group['GNAME'].to_list()
    guarantor_data[(acctno, noteno)] = {
        'GUARNAM1': gnames[0] if len(gnames) > 0 else '',
        'GUARNAM2': gnames[1] if len(gnames) > 1 else ''
    }

df_sasln = pl.read_parquet(f'{SASLN_DIR}LOAN{reptmon1}{nowks}.parquet').select([
    'ACCTNO', 'NOTENO', 'CURBAL'
]).rename({'CURBAL': 'PREVBAL'}).sort(['ACCTNO', 'NOTENO'])

df_sasln = df_sasln.join(df_npl.select(['ACCTNO', 'NOTENO']), on=['ACCTNO', 'NOTENO'], how='inner')

guarnam1_list_sasln = []
guarnam2_list_sasln = []
for row in df_sasln.iter_rows(named=True):
    key = (row['ACCTNO'], row['NOTENO'])
    gdata = guarantor_data.get(key, {'GUARNAM1': '', 'GUARNAM2': ''})
    guarnam1_list_sasln.append(gdata['GUARNAM1'])
    guarnam2_list_sasln.append(gdata['GUARNAM2'])

df_sasln = df_sasln.with_columns([
    pl.Series('GUARNAM1', guarnam1_list_sasln),
    pl.Series('GUARNAM2', guarnam2_list_sasln)
])

# Step 9: Merge all data
df_woff = df_sasln.join(df_loan, on=['ACCTNO', 'NOTENO'], how='outer').join(
    df_npl, on='ACCTNO', how='outer', suffix='_npl'
).with_columns([
    (pl.col('CURBAL') - pl.col('PREVBAL')).alias('PAYMENT'),
    (pl.col('TOTIIS') + pl.col('SP')).alias('TOTAL'),
    pl.lit('D').alias('RIND')  # KEY DIFFERENCE: 'D' for Conventional (vs 'I' for Islamic)
])

# Step 10: Filter for write-off candidates
df_woff = df_woff.filter(
    (
        ((pl.col('BORSTAT').is_in(['F', 'I'])) & (pl.col('DAYS') >= 334)) |
        (pl.col('DAYS') >= 334) |
        (
            (pl.col('BORSTAT') == 'A') &
            ~pl.col('LOANTYPE').is_in([983, 993, 678, 679, 698, 699]) &
            (pl.col('PAIDIND') != 'P')
        )
    ) &
    (pl.col('TOTAL') != 0)
).with_columns([
    pl.lit('Y').alias('CONFIRM')
]).sort('ACCTNO')

df_woff = df_woff.join(
    df_cname.rename({'CUSTNAM1': 'NAME'}),
    on='ACCTNO',
    how='left',
    suffix='_cname'
)

df_woff.write_parquet(f'{NPL_DIR}LIST.parquet')

print(f"\nBad Debt Write-Off List (Conventional) Generation Complete")

# Step 11: Write fixed-width output file (same format as EIIFTXT1)
with open(OUTPUT_FILE1, 'w') as f:
    for row in df_woff.iter_rows(named=True):
        branch = (row.get('BRANCH', '') or '')[:7]
        name = (row.get('NAME', '') or '')[:40]
        acctno = row.get('ACCTNO', 0) or 0
        noteno = row.get('NOTENO', 0) or 0
        borstat = (row.get('BORSTAT', '') or '')[:1]
        iis = row.get('IIS', 0) or 0
        oi = row.get('OI', 0) or 0
        totiis = row.get('TOTIIS', 0) or 0
        sp = row.get('SP', 0) or 0
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
        rind = (row.get('RIND', '') or '')[:1]
        oifeeamt = row.get('OIFEEAMT', 0) or 0
        lasttra1 = (row.get('LASTTRA1', '') or '')[:10]
        lsttrncd = row.get('LSTTRNCD', 0) or 0
        mthpdue = row.get('MTHPDUE', 0) or 0
        balance = row.get('BALANCE', 0) or 0
        guarend = (row.get('GUAREND', '') or '')[:20]
        guarnam1 = (row.get('GUARNAM1', '') or '')[:40]
        guarnam2 = (row.get('GUARNAM2', '') or '')[:40]
        
        issxdte = row.get('ISSXDTE', '')
        if issxdte:
            issxdte_str = format_mmddyy10(issxdte)[:10]
        else:
            issxdte_str = ' ' * 10
        
        netproc = row.get('NETPROC', 0) or 0
        colldesc = (row.get('COLLDESC', '') or '')[:70]
        collyear = row.get('COLLYEAR', 0) or 0
        bilpaid = row.get('BILPAID', 0) or 0
        crrgrade = (row.get('CRRGRADE', '') or '')[:5]
        marginfi = row.get('MARGINFI', 0) or 0
        noteterm = row.get('NOTETERM', 0) or 0
        payamt = row.get('PAYAMT', 0) or 0
        
        dobmni = row.get('DOBMNI', '')
        if dobmni:
            dobmni_str = format_mmddyy10(dobmni)[:10]
        else:
            dobmni_str = ' ' * 10
        
        ecsrind = (row.get('ECSRIND', '') or '')[:1]
        delqcd = (row.get('DELQCD', '') or '')[:2]
        occupat = (row.get('OCCUPAT', '') or '')[:3]
        bgc = (row.get('BGC', '') or '')[:2]
        pay75pct = (row.get('PAY75PCT', '') or '')[:1]
        nacodate = (row.get('NACODATE', '') or '')[:10]
        cp = (row.get('CP', '') or '')[:1]
        modeldes = (row.get('MODELDES', '') or '')[:6]
        akpk_status = (row.get('AKPK_STATUS', '') or '')[:9]
        
        f.write(f"{branch:<7}{name:<40}{acctno:>10.0f}{noteno:>5.0f}{borstat:1}")
        f.write(f"{iis:>16.2f}{oi:>16.2f}{totiis:>16.2f}{sp:>16.2f}")
        f.write(f"{curbal:>16.2f}{prevbal:>16.2f}{payment:>16.2f}")
        f.write(f"{ecsrrsrv:>16.2f}{postamt:>16.2f}{otheramt:>16.2f}")
        f.write(f"{matdate:10}{loantype:>3d}{intamt:>16.2f}{postntrn:1}")
        f.write(f"{marketvl:>16.2f}{intearn4:>16.2f}{days:>6d}{custcode:>3d}{rind:1}")
        f.write(f"{oifeeamt:>16.2f}{lasttra1:10}{lsttrncd:>3d}{mthpdue:>3d}")
        f.write(f"{balance:>16.2f}{guarend:20}{guarnam1:40}{guarnam2:40}")
        f.write(f"{issxdte_str:10}{netproc:>16.2f}{colldesc:70}{collyear:>4d}")
        f.write(f"{bilpaid:>3d}{crrgrade:5}{marginfi:>16.2f}{noteterm:>3d}")
        f.write(f"{payamt:>16.2f}{dobmni_str:10}{ecsrind:1}{delqcd:2}")
        f.write(f"{occupat:3}{bgc:2}{pay75pct:1}{nacodate:10}{cp:1}")
        f.write(f"{modeldes:6}{akpk_status:9}\n")

# Step 12-14: Same as EIIFTXT1 - Read, recalculate, write final
text_records = []
with open(OUTPUT_FILE1, 'r') as f:
    for line in f:
        record = {
            'BRANCH': line[0:7].strip(),
            'NAME': line[8:48].strip(),
            'ACCTNO': float(line[49:59]) if line[49:59].strip() else 0,
            'NOTENO': float(line[60:65]) if line[60:65].strip() else 0,
            'BORSTAT': line[66:67],
            'IIS': float(line[68:84]) if line[68:84].strip() else 0,
            'OI': float(line[84:100]) if line[84:100].strip() else 0,
            'TOTIIS': float(line[100:116]) if line[100:116].strip() else 0,
            'BALANCE': float(line[356:372]) if line[356:372].strip() else 0
        }
        
        record['SP'] = record['BALANCE'] - record['TOTIIS']
        record['TOTAL'] = record['TOTIIS'] + record['SP']
        record['_LINE'] = line
        text_records.append(record)

df_text = pl.DataFrame(text_records)

with open(OUTPUT_FILE, 'w') as f:
    for row in df_text.iter_rows(named=True):
        line = row['_LINE']
        
        delqcd = line[676:678]
        occupat = line[712:715]
        bgc = line[742:744]
        
        delqdes = get_delq_desc(delqcd)
        occupdes = get_occup_desc(occupat)
        bgcdes = get_bgc_desc(bgc)
        
        biztype = 'C'  # KEY DIFFERENCE: 'C' for Conventional (vs 'I' for Islamic)
        cap = 0.0
        latechg = row['OI']
        sp_calc = row['SP']
        total_calc = row['TOTAL']
        
        f.write(line[:116])
        f.write(f"{sp_calc:>16.2f}")
        f.write(f"{total_calc:>16.2f}")
        f.write(line[148:373])
        f.write(f"{cap:>16.2f}")
        f.write(f"{latechg:>16.2f}")
        f.write(line[407:679])
        f.write(f"{delqdes:30}")
        f.write(f"{biztype:1}")
        f.write(line[712:715])
        f.write(f"{occupdes:25}")
        f.write(line[742:744])
        f.write(f"{bgcdes:20}")
        f.write(line[766:])

df_text.write_parquet(f'{NPL_DIR}WOFFTXT.parquet')

print(f"\nOutput files generated:")
print(f"  {OUTPUT_FILE} (Final formatted output)")
print(f"  {OUTPUT_FILE1} (Intermediate output)")
print(f"  {NPL_DIR}LIST.parquet (Data file)")
print(f"  {NPL_DIR}WOFFTXT.parquet (Final dataset)")
print(f"\nAccounts identified for write-off: {len(df_woff)}")
print(f"Total exposure: RM {df_woff['TOTAL'].sum():,.2f}")
print(f"\nKey Differences from EIIFTXT1 (Islamic):")
print(f"  - RIND = 'D' (Domestic/Conventional) vs 'I' (Islamic)")
print(f"  - BIZTYPE = 'C' (Conventional) vs 'I' (Islamic)")
print(f"  - Uses CREDMSUBAC vs ICREDMSUBAC (CCRIS)")
