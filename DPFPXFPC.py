"""
EIIFTXT1 - Bad Debt Write-Off List Generation
"""

import polars as pl
import pyreadstat
from datetime import datetime, timedelta
import sys
import os

# Import format definition programs (%INC PGM equivalent)
sys.path.insert(0, '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS')
from PBBLNFMT import (
    HP_ALL, HP_ACTIVE, AITAB, MORE_PLAN, MORE_ISLAM,
    HOME_ISLAMIC, HOME_CONVENTIONAL, SWIFT_ISLAMIC, SWIFT_CONVENTIONAL,
    FCY_PRODUCTS,
    format_mthpass, format_ndays, format_lnprod, format_lndenom,
    format_odprod, format_oddenom, format_collcd, format_riskcd,
    format_delqdes, format_statecd
)

# Since get_branch_name is not in PBBLNFMT, define it here
# This is based on the BRCHCD format that would typically be in PBBLNFMT
def get_branch_name(branch_code):
    """
    Get branch abbreviation from branch code.
    This is a simplified version - in production, this should come from a
    branch code format dataset.
    """
    # This is a placeholder - you'll need to implement the actual branch name mapping
    # based on your organization's branch codes
    branch_map = {
        1: 'HQ', 2: 'KL', 3: 'PJ', 4: 'JB', 5: 'PG',
        6: 'IP', 7: 'KK', 8: 'KU', 9: 'MK', 10: 'SB',
        # Add more branch codes as needed
    }
    return branch_map.get(branch_code, 'BR')

# Use library paths from PBBELF
try:
    from PBBELF import LIBRARY_PATHS, format_ddmmyy10, format_mmddyy10
except ImportError:
    # Fallback definitions if PBBELF not available
    LIBRARY_PATHS = {
        'LOAN': '/sas/data/loan/',
        'NPL6': '/sas/data/npl/',
    }
    def format_ddmmyy10(date_obj):
        return date_obj.strftime('%d/%m/%Y') if date_obj else ''
    def format_mmddyy10(date_obj):
        return date_obj.strftime('%m/%d/%Y') if date_obj else ''

LOAN_DIR = LIBRARY_PATHS.get('LOAN', '/sas/data/loan/')
NPL_DIR = LIBRARY_PATHS.get('NPL6', '/sas/data/npl/')
SASLN_DIR = '/sas/data/sasln/'
CISNAME_DIR = '/sas/data/cisname/'
CCRIS_DIR = '/sas/data/ccris/'
BKCTRL_DIR = '/sas/data/bkctrl/'

OUTPUT_FILE = '/sas/data/output/wofftext.txt'
OUTPUT_FILE1 = '/sas/data/output/wofftex1.txt'

# HPD from PBBLNFMT (Hire Purchase Active)
HPD = HP_ACTIVE  # This is defined in PBBLNFMT

# Additional formats loaded from BKCTRL.CISFMT
# DELQDES - Delinquency Description (already imported from PBBLNFMT)
# OCCUPFMT - Occupation Format
OCCUPFMT = {
    '001': 'PROFESSIONAL',
    '002': 'BUSINESSMAN',
    '003': 'SELF EMPLOYED',
    '004': 'EMPLOYEE - PRIVATE',
    '005': 'EMPLOYEE - GOVERNMENT',
    '006': 'RETIRED',
    '999': 'OTHERS'
}

# BGCFMT - Business/Government Code Format
BGCFMT = {
    'B': 'BUSINESS',
    'G': 'GOVERNMENT',
    'C': 'CORPORATE',
    'I': 'INDIVIDUAL',
    '  ': 'NOT SPECIFIED'
}

def read_sas7bdat(filepath):
    """Read SAS dataset using pyreadstat and return polars DataFrame"""
    try:
        if not os.path.exists(filepath):
            print(f"Warning: File not found: {filepath}")
            return None
        df, meta = pyreadstat.read_sas7bdat(filepath)
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def write_sas7bdat(df, filepath):
    """Write DataFrame to SAS dataset using pyreadstat"""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        pyreadstat.write_sas7bdat(df.to_pandas(), filepath)
        print(f"Successfully wrote: {filepath}")
    except Exception as e:
        print(f"Error writing {filepath}: {e}")

def get_delq_desc(delqcd):
    """Get delinquency description"""
    return format_delqdes(delqcd) if delqcd else 'NO LEGAL ACTION TAKEN'

def get_occup_desc(occupat):
    """Get occupation description"""
    return OCCUPFMT.get(occupat if occupat else '999', 'OTHERS')

def get_bgc_desc(bgc):
    """Get business/government code description"""
    return BGCFMT.get(bgc if bgc else '  ', 'NOT SPECIFIED')

# Set report date to yesterday (using datetime.timedelta)
reptdate = datetime.now().date() - timedelta(days=1)
print(f"Processing Bad Debt Write-Off List")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")

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
reptyear = f'{reptdate.year % 100:02d}'
rdate = reptdate.strftime('%d/%m/%y')

print(f"Week: {nowk}, Previous Month: {reptmon1}")

# Step 1: Create NPLA - Active accounts with borrower status 'A'
df_npla_raw = read_sas7bdat(f'{LOAN_DIR}LNNOTE.sas7bdat')
if df_npla_raw is not None:
    df_npla = df_npla_raw.filter(
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
else:
    raise Exception("Failed to read LOAN.LNNOTE")

# Step 2: Get IIS and SP data
df_iis = read_sas7bdat(f'{NPL_DIR}IIS.sas7bdat')
if df_iis is not None:
    df_iis = df_iis.unique(subset=['ACCTNO', 'NOTENO'])
else:
    df_iis = pl.DataFrame()

df_sp = read_sas7bdat(f'{NPL_DIR}SP2.sas7bdat')
if df_sp is not None:
    df_sp = df_sp.unique(subset=['ACCTNO', 'NOTENO'])
else:
    df_sp = pl.DataFrame()

# Merge IIS and SP
if df_iis.height > 0 and df_sp.height > 0:
    df_npl_data = df_sp.join(df_iis, on=['ACCTNO', 'NOTENO'], how='outer').select([
        'NAME', 'ACCTNO', 'NOTENO', 'IIS', 'OI', 'TOTIIS', 'SP', 'MARKETVL', 'BRANCH'
    ])
else:
    df_npl_data = pl.DataFrame(schema={
        'NAME': pl.Utf8, 'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
        'IIS': pl.Float64, 'OI': pl.Float64, 'TOTIIS': pl.Float64,
        'SP': pl.Float64, 'MARKETVL': pl.Float64, 'BRANCH': pl.Utf8
    })

# Combine NPLA and NPL data
df_npl = pl.concat([df_npla, df_npl_data]).with_columns([
    pl.col('MARKETVL').round(2),
    pl.col('BRANCH').str.slice(3, 4).alias('BRNO'),
    pl.col('BRANCH').str.slice(0, 3).alias('BRABBR')
]).unique(subset=['ACCTNO', 'NOTENO'])

# Step 3: Get CCRIS credit submission data
ccris_file = f'{CCRIS_DIR}ICREDMSUBAC{reptmon}{reptyear}.sas7bdat'
df_credsub = read_sas7bdat(ccris_file)
if df_credsub is not None:
    df_credsub = df_credsub.filter(
        pl.col('FACILITY').is_in(['34331', '34332'])
    ).rename({
        'ACCTNUM': 'ACCTNO',
        'DAYSARR': 'DAYS'
    }).sort(['ACCTNO', 'NOTENO', 'DAYS'], descending=[False, False, True]).unique(
        subset=['ACCTNO', 'NOTENO']
    ).select(['ACCTNO', 'NOTENO', 'DAYS', 'FACILITY'])
else:
    df_credsub = pl.DataFrame(schema={
        'ACCTNO': pl.Float64, 'NOTENO': pl.Float64,
        'DAYS': pl.Float64, 'FACILITY': pl.Utf8
    })

# Step 4: Get loan data for HPD loan types (from PBBLNFMT)
df_loan_raw = read_sas7bdat(f'{LOAN_DIR}LNNOTE.sas7bdat')
if df_loan_raw is not None:
    df_loan_raw = df_loan_raw.filter(
        pl.col('LOANTYPE').is_in(HPD)
    ).unique(subset=['ACCTNO', 'NOTENO'])
else:
    df_loan_raw = pl.DataFrame()

# Merge NPL, CREDSUB, and LOAN
if df_npl.height > 0 and df_loan_raw.height > 0:
    df_loan = df_npl.join(df_credsub, on=['ACCTNO', 'NOTENO'], how='left').join(
        df_loan_raw, on=['ACCTNO', 'NOTENO'], how='left', suffix='_loan'
    ).filter(pl.col('ACCTNO').is_not_null())
else:
    df_loan = df_npl.clone() if df_npl.height > 0 else pl.DataFrame()

# Step 5: Calculate derived fields
if df_loan.height > 0:
    loan_records = []
    for row in df_loan.iter_rows(named=True):
        new_row = row.copy()
        
        # POSTAMT calculation
        new_row['POSTAMT'] = (row.get('FEETOTAL', 0) or 0) + (row.get('NFEEAMT5', 0) or 0)
        
        # OTHERAMT calculation
        new_row['OTHERAMT'] = (row.get('FEEAMT3', 0) or 0) - new_row['POSTAMT']
        
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
        if maturedt and maturedt > 0:
            maturedt_str = str(int(maturedt)).zfill(11)[:8]
            try:
                matdate_dt = datetime.strptime(maturedt_str, '%m%d%Y')
                new_row['MATDATE'] = format_mmddyy10(matdate_dt)
                new_row['MATUREDT'] = matdate_dt.date()
            except:
                new_row['MATDATE'] = ''
        else:
            new_row['MATDATE'] = ''
        
        # LASTTRA1 formatting
        lasttran = row.get('LASTTRAN')
        if lasttran and lasttran > 0:
            lasttran_str = str(int(lasttran)).zfill(11)[:8]
            try:
                lasttra1_dt = datetime.strptime(lasttran_str, '%m%d%Y')
                new_row['LASTTRA1'] = format_mmddyy10(lasttra1_dt)
            except:
                new_row['LASTTRA1'] = ''
        else:
            new_row['LASTTRA1'] = ''
        
        # MTHPDUE calculation using MTHPASS format (NDAYS)
        days = row.get('DAYS', 0) or 0
        if days > 0:
            mthpdue = format_mthpass(days)
            if mthpdue == 24:
                mthpdue = int((days / 365) * 12)
        else:
            mthpdue = 0
        new_row['MTHPDUE'] = mthpdue
        
        # CRRGRADE calculation
        score2 = row.get('SCORE2', '') or ''
        contrtype = row.get('CONTRTYPE', '') or ''
        new_row['CRRGRADE'] = f"{score2}{contrtype}".strip()
        
        # MARGINFI calculation
        netproc = row.get('NETPROC', 0) or 0
        appvalue = row.get('APPVALUE', 0) or 0
        if appvalue > 0:
            new_row['MARGINFI'] = round(netproc / appvalue, 2)
        else:
            new_row['MARGINFI'] = 0
        
        # DOBMNI calculation
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
        
        # ECSRIND indicator
        new_row['ECSRIND'] = 'Y' if new_row['ECSRRSRV'] > 0 else 'N'
        
        # BILPAID calculation
        orgbal = row.get('ORGBAL', 0) or 0
        curbal = row.get('CURBAL', 0) or 0
        payamt = row.get('PAYAMT', 0) or 0
        if payamt > 0:
            new_row['BILPAID'] = int((orgbal - curbal) / payamt)
        else:
            new_row['BILPAID'] = 0
        
        # PAY75PCT and NACODATE
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
else:
    df_loan = pl.DataFrame()

# Step 6: Get customer names from CISNAME
df_cname = read_sas7bdat(f'{CISNAME_DIR}LOAN.sas7bdat')
if df_cname is not None:
    df_cname = df_cname.filter(
        pl.col('SECCUST') == '901'
    ).select(['ACCTNO', 'CUSTNAM1', 'OCCUPAT', 'BGC']).unique(subset=['ACCTNO'])
else:
    df_cname = pl.DataFrame()

# Step 7: Get guarantor information from LIAB
df_liab = read_sas7bdat(f'{LOAN_DIR}LIAB.sas7bdat')
guarantor_data = {}
if df_liab is not None and df_cname.height > 0:
    df_liab = df_liab.sort('LIABACCT')
    
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
    
    for (acctno, noteno), group in df_liab.group_by(['ACCTNO', 'NOTENO']):
        gnames = group['GNAME'].to_list()
        guarantor_data[(acctno, noteno)] = {
            'GUARNAM1': gnames[0] if len(gnames) > 0 else '',
            'GUARNAM2': gnames[1] if len(gnames) > 1 else ''
        }

# Add guarantor names to loan data
guarnam1_list = []
guarnam2_list = []
if df_loan.height > 0:
    for row in df_loan.iter_rows(named=True):
        key = (row['ACCTNO'], row['NOTENO'])
        gdata = guarantor_data.get(key, {'GUARNAM1': '', 'GUARNAM2': ''})
        guarnam1_list.append(gdata['GUARNAM1'])
        guarnam2_list.append(gdata['GUARNAM2'])
    
    df_loan = df_loan.with_columns([
        pl.Series('GUARNAM1', guarnam1_list),
        pl.Series('GUARNAM2', guarnam2_list)
    ])

# Step 8: Get previous balance from SASLN
sasln_file = f'{SASLN_DIR}LOAN{reptmon1}{nowks}.sas7bdat'
df_sasln = read_sas7bdat(sasln_file)
if df_sasln is not None:
    df_sasln = df_sasln.select([
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
else:
    df_sasln = pl.DataFrame()

# Step 9: Final merge and calculations
if df_sasln.height > 0 and df_loan.height > 0:
    df_woff = df_sasln.join(df_loan, on=['ACCTNO', 'NOTENO'], how='outer')
    if 'BRANCH' not in df_woff.columns:
        df_woff = df_woff.join(df_npl.select(['ACCTNO', 'BRANCH', 'MARKETVL']), on='ACCTNO', how='left')
    
    df_woff = df_woff.with_columns([
        (pl.col('CURBAL') - pl.col('PREVBAL')).alias('PAYMENT'),
        (pl.col('TOTIIS') + pl.col('SP')).alias('TOTAL'),
        pl.lit('I').alias('RIND')
    ])
else:
    df_woff = pl.DataFrame()

# Step 10: Filter for write-off candidates
if df_woff.height > 0:
    required_cols = ['BORSTAT', 'DAYS', 'LOANTYPE', 'PAIDIND', 'TOTAL']
    for col in required_cols:
        if col not in df_woff.columns:
            df_woff = df_woff.with_columns(pl.lit(None).alias(col))
    
    df_woff = df_woff.with_columns([
        pl.col('BORSTAT').cast(pl.Utf8),
        pl.col('DAYS').cast(pl.Float64),
        pl.col('LOANTYPE').cast(pl.Float64),
        pl.col('PAIDIND').cast(pl.Utf8),
        pl.col('TOTAL').cast(pl.Float64)
    ])
    
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
    
    if 'NAME' not in df_woff.columns:
        df_woff = df_woff.join(
            df_cname.rename({'CUSTNAM1': 'NAME'}),
            on='ACCTNO',
            how='left'
        )
else:
    df_woff = pl.DataFrame()

# Save to NPL.LIST
if df_woff.height > 0:
    write_sas7bdat(df_woff, f'{NPL_DIR}LIST.sas7bdat')
    print(f"\nAccounts identified for write-off: {len(df_woff)}")
    print(f"Total exposure: RM {df_woff['TOTAL'].sum():,.2f}")
else:
    print("\nNo accounts identified for write-off")
    df_woff = pl.DataFrame(schema={
        'BRANCH': pl.Utf8, 'NAME': pl.Utf8, 'ACCTNO': pl.Float64,
        'NOTENO': pl.Float64, 'BORSTAT': pl.Utf8, 'IIS': pl.Float64,
        'OI': pl.Float64, 'TOTIIS': pl.Float64, 'SP': pl.Float64,
        'TOTAL': pl.Float64, 'CURBAL': pl.Float64, 'PREVBAL': pl.Float64
    })

print(f"\nBad Debt Write-Off List Generation Complete")

# Step 11: Write fixed-width output file (WOFFTEX1)
if df_woff.height > 0:
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
                try:
                    issxdte_str = format_mmddyy10(issxdte)[:10]
                except:
                    issxdte_str = ' ' * 10
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
                try:
                    dobmni_str = format_mmddyy10(dobmni)[:10]
                except:
                    dobmni_str = ' ' * 10
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
            
            # Write fixed-width record
            f.write(f"{branch:<7}{name:<40}{acctno:>10.0f}{noteno:>5.0f}{borstat:1}")
            f.write(f"{iis:>16.2f}{oi:>16.2f}{totiis:>16.2f}{sp:>16.2f}")
            f.write(f"{curbal:>16.2f}{prevbal:>16.2f}{payment:>16.2f}")
            f.write(f"{ecsrrsrv:>16.2f}{postamt:>16.2f}{otheramt:>16.2f}")
            f.write(f"{matdate:10}{loantype:>3.0f}{intamt:>16.2f}{postntrn:1}")
            f.write(f"{marketvl:>16.2f}{intearn4:>16.2f}{days:>6.0f}{custcode:>3.0f}{rind:1}")
            f.write(f"{oifeeamt:>16.2f}{lasttra1:10}{lsttrncd:>3.0f}{mthpdue:>3.0f}")
            f.write(f"{balance:>16.2f}{guarend:20}{guarnam1:40}{guarnam2:40}")
            f.write(f"{issxdte_str:10}{netproc:>16.2f}{colldesc:70}{collyear:>4.0f}")
            f.write(f"{bilpaid:>3.0f}{crrgrade:5}{marginfi:>16.2f}{noteterm:>3.0f}")
            f.write(f"{payamt:>16.2f}{dobmni_str:10}{ecsrind:1}{delqcd:2}")
            f.write(f"{occupat:3}{bgc:2}{pay75pct:1}{nacodate:10}{cp:1}")
            f.write(f"{modeldes:6}{akpk_status:9}\n")

    # Step 12: Re-read and recalculate SP
    text_records = []
    try:
        with open(OUTPUT_FILE1, 'r') as f:
            for line in f:
                if len(line) >= 372:
                    record = {
                        'BRANCH': line[0:7].strip(),
                        'NAME': line[8:48].strip(),
                        'ACCTNO': float(line[49:59]) if line[49:59].strip() else 0,
                        'NOTENO': float(line[60:65]) if line[60:65].strip() else 0,
                        'BORSTAT': line[66:67] if len(line) > 66 else '',
                        'IIS': float(line[68:84]) if len(line) > 84 and line[68:84].strip() else 0,
                        'OI': float(line[84:100]) if len(line) > 100 and line[84:100].strip() else 0,
                        'TOTIIS': float(line[100:116]) if len(line) > 116 and line[100:116].strip() else 0,
                        'BALANCE': float(line[356:372]) if len(line) > 372 and line[356:372].strip() else 0
                    }
                    
                    record['SP'] = record['BALANCE'] - record['TOTIIS']
                    record['TOTAL'] = record['TOTIIS'] + record['SP']
                    record['_LINE'] = line
                    text_records.append(record)
        
        if text_records:
            df_text = pl.DataFrame(text_records)
            
            with open(OUTPUT_FILE, 'w') as f:
                for row in df_text.iter_rows(named=True):
                    line = row['_LINE']
                    
                    delqcd = line[676:678] if len(line) > 678 else '  '
                    occupat = line[712:715] if len(line) > 715 else '999'
                    bgc = line[742:744] if len(line) > 744 else '  '
                    
                    delqdes = get_delq_desc(delqcd)
                    occupdes = get_occup_desc(occupat)
                    bgcdes = get_bgc_desc(bgc)
                    
                    biztype = 'I'
                    cap = 0.0
                    latechg = row['OI']
                    sp_calc = row['SP']
                    total_calc = row['TOTAL']
                    
                    if len(line) >= 116:
                        f.write(line[:116])
                        f.write(f"{sp_calc:>16.2f}")
                        f.write(f"{total_calc:>16.2f}")
                        
                        if len(line) > 373:
                            f.write(line[148:373])
                            f.write(f"{cap:>16.2f}")
                            f.write(f"{latechg:>16.2f}")
                            f.write(line[407:679] if len(line) > 679 else line[407:])
                            f.write(f"{delqdes:30}")
                            f.write(f"{biztype:1}")
                            f.write(line[712:715] if len(line) > 715 else '   ')
                            f.write(f"{occupdes:25}")
                            f.write(line[742:744] if len(line) > 744 else '  ')
                            f.write(f"{bgcdes:20}")
                            f.write(line[766:] if len(line) > 766 else '')
                            f.write('\n')
            
            write_sas7bdat(df_text, f'{NPL_DIR}WOFFTXT.sas7bdat')
    except Exception as e:
        print(f"Warning: Error processing output files: {e}")

print(f"\nOutput files generated:")
print(f"  {OUTPUT_FILE} (Final formatted output)")
print(f"  {OUTPUT_FILE1} (Intermediate output)")
if df_woff.height > 0:
    print(f"  {NPL_DIR}LIST.sas7bdat (Data file)")
    print(f"  {NPL_DIR}WOFFTXT.sas7bdat (Final dataset)")
