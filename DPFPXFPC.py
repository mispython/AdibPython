"""
EIFFTXT1 - Bad Debt Write-Off List (Conventional Banking) - OPTIMIZED VERSION
Includes: PBBLNFMT, PBBELF format definitions

Key Differences from EIIFTXT1:
- RIND = 'D' (Domestic/Conventional) vs 'I' (Islamic)
- BIZTYPE = 'C' (Conventional) vs 'I' (Islamic)
- Uses CREDMSUBAC (not ICREDMSUBAC - no 'I' prefix)
"""

import pandas as pd
import pyreadstat
from datetime import datetime, timedelta
import sys
import os
import gc
import numpy as np

# Input directory paths (all lowercase)
LOAN_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'
NPL_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'
SASLN_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'
CISNAME_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'
CCRIS_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'
BKCTRL_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIFFTXT1/'

OUTPUT_FILE = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFFTXT1/wofftext.txt'
OUTPUT_FILE1 = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIFFTXT1/wofftex1.txt'

# HPD loan types (from PBBLNFMT)
HPD = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
       201, 202, 203, 204, 205, 206, 207, 208, 209, 210,
       301, 302, 303, 304, 305, 306, 307, 308, 309, 310]

# ===== FUNCTION DEFINITIONS (moved to top) =====
def get_branch_name(branch_code):
    """Get branch abbreviation - simplified version"""
    branch_map = {
        1: 'KL', 2: 'PJ', 3: 'JB', 4: 'PG', 5: 'IP',
    }
    return branch_map.get(branch_code, 'UNK')

def ndays_format(days):
    """Convert days to months past due"""
    if days <= 0:
        return 0
    elif days <= 30:
        return 1
    elif days <= 60:
        return 2
    elif days <= 90:
        return 3
    elif days <= 120:
        return 4
    elif days <= 150:
        return 5
    elif days <= 180:
        return 6
    elif days <= 210:
        return 7
    elif days <= 240:
        return 8
    elif days <= 270:
        return 9
    elif days <= 300:
        return 10
    elif days <= 330:
        return 11
    elif days <= 365:
        return 12
    else:
        return int(days / 30)

def format_ddmmyy10(date_obj):
    """Format date as DD/MM/YYYY"""
    if pd.isna(date_obj) or date_obj is None:
        return ''
    return date_obj.strftime('%d/%m/%Y')

def format_mmddyy10(date_obj):
    """Format date as MM/DD/YYYY"""
    if pd.isna(date_obj) or date_obj is None:
        return ''
    return date_obj.strftime('%m/%d/%Y')

def mthpass_format(days):
    """Convert days to months past due - same as NDAYS"""
    return ndays_format(days)

def safe_format_date(date_val, fmt_func):
    """Safely format date values"""
    if pd.isna(date_val) or date_val is None or date_val == 0:
        return ''
    try:
        date_str = str(int(date_val)).zfill(8)
        if len(date_str) >= 8:
            dt = datetime.strptime(date_str[:8], '%m%d%Y')
            return fmt_func(dt)
    except:
        pass
    return ''

def safe_get_date(date_val):
    """Safely get date object"""
    if pd.isna(date_val) or date_val is None or date_val == 0:
        return None
    try:
        date_str = str(int(date_val)).zfill(8)
        if len(date_str) >= 8:
            return datetime.strptime(date_str[:8], '%m%d%Y').date()
    except:
        pass
    return None

def create_fixed_width_line(row):
    """Create fixed-width line from row data"""
    branch = str(row.get('branch', '') or '')[:7]
    name = str(row.get('name', '') or '')[:40]
    acctno = row.get('acctno', 0) or 0
    noteno = row.get('noteno', 0) or 0
    borstat = str(row.get('borstat', '') or '')[:1]
    iis = row.get('iis', 0) or 0
    oi = row.get('oi', 0) or 0
    totiis = row.get('totiis', 0) or 0
    sp = row.get('sp', 0) or 0
    curbal = row.get('curbal', 0) or 0
    prevbal = row.get('prevbal', 0) or 0
    payment = row.get('payment', 0) or 0
    ecsrrsrv = row.get('ecsrrsrv', 0) or 0
    postamt = row.get('postamt', 0) or 0
    otheramt = row.get('otheramt', 0) or 0
    matdate = str(row.get('matdate', '') or '')[:10]
    loantype = row.get('loantype', 0) or 0
    intamt = row.get('intamt', 0) or 0
    postntrn = str(row.get('postntrn', '') or '')[:1]
    marketvl = row.get('marketvl', 0) or 0
    intearn4 = row.get('intearn4', 0) or 0
    days = row.get('days', 0) or 0
    custcode = row.get('custcode', 0) or 0
    rind = str(row.get('rind', '') or '')[:1]
    oifeeamt = row.get('oifeeamt', 0) or 0
    lasttra1 = str(row.get('lasttra1', '') or '')[:10]
    lsttrncd = row.get('lsttrncd', 0) or 0
    mthpdue = row.get('mthpdue', 0) or 0
    balance = row.get('balance', 0) or 0
    guarend = str(row.get('guarend', '') or '')[:20]
    guarnam1 = str(row.get('guarnam1', '') or '')[:40]
    guarnam2 = str(row.get('guarnam2', '') or '')[:40]
    
    issxdte = row.get('issxdte', '')
    if pd.notna(issxdte) and issxdte:
        try:
            issxdte_str = format_mmddyy10(issxdte)[:10]
        except:
            issxdte_str = ' ' * 10
    else:
        issxdte_str = ' ' * 10
    
    netproc = row.get('netproc', 0) or 0
    colldesc = str(row.get('colldesc', '') or '')[:70]
    collyear = row.get('collyear', 0) or 0
    bilpaid = row.get('bilpaid', 0) or 0
    crrgrade = str(row.get('crrgrade', '') or '')[:5]
    marginfi = row.get('marginfi', 0) or 0
    noteterm = row.get('noteterm', 0) or 0
    payamt = row.get('payamt', 0) or 0
    
    dobmni = row.get('dobmni', '')
    if pd.notna(dobmni) and dobmni:
        try:
            dobmni_str = format_mmddyy10(dobmni)[:10]
        except:
            dobmni_str = ' ' * 10
    else:
        dobmni_str = ' ' * 10
    
    ecsrind = str(row.get('ecsrind', '') or '')[:1]
    delqcd = str(row.get('delqcd', '') or '')[:2]
    occupat = str(row.get('occupat', '') or '')[:3]
    bgc = str(row.get('bgc', '') or '')[:2]
    pay75pct = str(row.get('pay75pct', '') or '')[:1]
    nacodate = str(row.get('nacodate', '') or '')[:10]
    cp = str(row.get('cp', '') or '')[:1]
    modeldes = str(row.get('modeldes', '') or '')[:6]
    akpk_status = str(row.get('akpk_status', '') or '')[:9]
    
    line = f"{branch:<7} {name:<40}{acctno:>10.0f}{noteno:>5.0f}{borstat:1}"
    line += f"{iis:>16.2f}{oi:>16.2f}{totiis:>16.2f}{sp:>16.2f}"
    line += f"{curbal:>16.2f}{prevbal:>16.2f}{payment:>16.2f}"
    line += f"{ecsrrsrv:>16.2f}{postamt:>16.2f}{otheramt:>16.2f}"
    line += f"{matdate:<10}{int(loantype):>3d}{intamt:>16.2f}{postntrn:1}"
    line += f"{marketvl:>16.2f}{intearn4:>16.2f}{int(days):>6d}{int(custcode):>3d}{rind:1}"
    line += f"{oifeeamt:>16.2f}{lasttra1:<10}{int(lsttrncd):>3d}{int(mthpdue):>3d}"
    line += f"{balance:>16.2f}{guarend:<20}{guarnam1:<40}{guarnam2:<40}"
    line += f"{issxdte_str:<10}{netproc:>16.2f}{colldesc:<70}{int(collyear):>4d}"
    line += f"{int(bilpaid):>3d}{crrgrade:<5}{marginfi:>16.2f}{int(noteterm):>3d}"
    line += f"{payamt:>16.2f}{dobmni_str:<10}{ecsrind:1}{delqcd:<2}"
    line += f"{occupat:<3}{bgc:<2}{pay75pct:1}{nacodate:<10}{cp:1}"
    line += f"{modeldes:<6}{akpk_status:<9}\n"
    
    return line

# Additional formats
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
    return DELQDES.get(str(delqcd).strip() if delqcd else '  ', 'UNKNOWN')

def get_occup_desc(occupat):
    return OCCUPFMT.get(str(occupat).strip() if occupat else '999', 'OTHERS')

def get_bgc_desc(bgc):
    return BGCFMT.get(str(bgc).strip() if bgc else '  ', 'NOT SPECIFIED')

# ===== MAIN PROCESSING =====
# Calculate report date (yesterday)
reptdate = datetime.now() - timedelta(days=1)

day = reptdate.day
if day <= 7:
    wk = '4'
    wk1 = '3'
elif day <= 14:
    wk = '1'
    wk1 = '4'
elif day <= 21:
    wk = '2'
    wk1 = '1'
elif day <= 28:
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

# ===== OPTIMIZATION 1: Read only necessary columns =====
LNNOTE_COLS_NEEDED = [
    'borstat', 'loantype', 'feedue', 'feeduems', 'feeamt16',
    'name', 'acctno', 'noteno', 'marketvl', 'ntbrch',
    'feetotal', 'nfeeamt5', 'feeamt3', 'feetot2', 'feeamta',
    'feeamt5', 'ecsrrsrv', 'maturedt', 'lasttran', 'days',
    'score2', 'contrtype', 'netproc', 'appvalue', 'birthdt',
    'orgbal', 'curbal', 'payamt', 'nacospadt', 'intamt',
    'postntrn', 'intearn4', 'custcode', 'lsttrncd', 'balance',
    'guarend', 'issxdte', 'colldesc', 'collyear', 'noteterm',
    'delqcd', 'cp', 'modeldes', 'akpk_status', 'paidind'
]

# ===== OPTIMIZATION 2: Read with column selection =====
print("Reading LNNOTE (optimized)...")
try:
    # Try pyreadstat first with column selection
    df_lnnote, meta = pyreadstat.read_sas7bdat(
        f'{LOAN_DIR}lnnote.sas7bdat',
        usecols=LNNOTE_COLS_NEEDED,
        disable_datetime_conversion=True,
        encoding='latin1'
    )
    print(f"Successfully read {len(df_lnnote)} records from LNNOTE")
except Exception as e:
    print(f"pyreadstat failed: {e}")
    print("Trying pandas SAS reader...")
    try:
        # Fallback to pandas
        df_lnnote = pd.read_sas(
            f'{LOAN_DIR}lnnote.sas7bdat',
            format='sas7bdat',
            encoding='latin1'
        )
        # Keep only needed columns
        existing_cols = [col for col in LNNOTE_COLS_NEEDED if col in df_lnnote.columns]
        df_lnnote = df_lnnote[existing_cols]
        print(f"Successfully read {len(df_lnnote)} records using pandas")
    except Exception as e2:
        print(f"All readers failed. Error: {e2}")
        sys.exit(1)

# Step 1: Create NPLA - Active accounts with BORSTAT='A'
print("Step 1: Creating NPLA...")
df_npla = df_lnnote.query(
    "borstat == 'A' and loantype not in [983, 993, 678, 679, 698, 699]"
).copy()

df_npla['iis'] = 0
df_npla['oi'] = df_npla['feedue'] - df_npla['feeduems']
df_npla['totiis'] = df_npla['oi']
df_npla['sp'] = df_npla['feeduems'] + df_npla['feeamt16']

df_npla['branch'] = df_npla['ntbrch'].apply(
    lambda x: f"{get_branch_name(x)} {x:03d}"
)

df_npla = df_npla[['name', 'acctno', 'noteno', 'iis', 'oi', 'totiis', 'sp', 'marketvl', 'branch']]

del df_lnnote
gc.collect()

# Step 2: Get IIS and SP data
print("Step 2: Reading IIS and SP data...")
try:
    df_iis, _ = pyreadstat.read_sas7bdat(
        f'{NPL_DIR}iis.sas7bdat',
        usecols=['acctno', 'noteno', 'iis', 'oi', 'totiis', 'name', 'sp', 'marketvl', 'branch']
    )
except:
    df_iis = pd.DataFrame(columns=['acctno', 'noteno', 'iis', 'oi', 'totiis', 'name', 'sp', 'marketvl', 'branch'])

try:
    df_sp, _ = pyreadstat.read_sas7bdat(
        f'{NPL_DIR}sp2.sas7bdat',
        usecols=['acctno', 'noteno', 'sp', 'name', 'marketvl', 'branch']
    )
except:
    df_sp = pd.DataFrame(columns=['acctno', 'noteno', 'sp', 'name', 'marketvl', 'branch'])

df_iis = df_iis.drop_duplicates(subset=['acctno', 'noteno'])
df_sp = df_sp.drop_duplicates(subset=['acctno', 'noteno'])

df_npl_data = df_sp.merge(df_iis, on=['acctno', 'noteno'], how='outer')
cols_needed = ['name', 'acctno', 'noteno', 'iis', 'oi', 'totiis', 'sp', 'marketvl', 'branch']
existing_cols = [col for col in cols_needed if col in df_npl_data.columns]
df_npl_data = df_npl_data[existing_cols]

df_npl = pd.concat([df_npla, df_npl_data], ignore_index=True)
df_npl['marketvl'] = df_npl['marketvl'].round(2)
df_npl['brno'] = df_npl['branch'].str[3:7]
df_npl['brabr'] = df_npl['branch'].str[0:3]
df_npl = df_npl.drop_duplicates(subset=['acctno', 'noteno'])

print(f"NPL records: {len(df_npl)}")

# Step 3: KEY DIFFERENCE - Get CCRIS credit submission data
print("Step 3: Reading CREDMSUBAC...")
credmsubac_file = f'{CCRIS_DIR}credmsubac{reptmon}{reptyear}.sas7bdat'
if os.path.exists(credmsubac_file):
    try:
        df_credsub, _ = pyreadstat.read_sas7bdat(
            credmsubac_file,
            usecols=['facility', 'acctnum', 'daysarr', 'noteno']
        )
        df_credsub = df_credsub[
            df_credsub['facility'].isin(['34331', '34332'])
        ].rename(columns={'acctnum': 'acctno', 'daysarr': 'days'})
        
        df_credsub = df_credsub.sort_values(
            ['acctno', 'noteno', 'days'], 
            ascending=[True, True, False]
        )
        df_credsub = df_credsub.drop_duplicates(subset=['acctno', 'noteno'])
        df_credsub = df_credsub[['acctno', 'noteno', 'days', 'facility']]
        print(f"CREDMSUBAC records: {len(df_credsub)}")
    except Exception as e:
        print(f"Error reading CREDMSUBAC: {e}")
        df_credsub = pd.DataFrame(columns=['acctno', 'noteno', 'days', 'facility'])
else:
    print(f"Warning: {credmsubac_file} not found.")
    df_credsub = pd.DataFrame(columns=['acctno', 'noteno', 'days', 'facility'])

# Step 4: Get loan data for HPD loan types
print("Step 4: Re-reading LNNOTE for HPD loans...")
try:
    # Only read the additional columns needed
    hpd_cols = ['acctno', 'noteno', 'loantype'] + LNNOTE_COLS_NEEDED[10:]
    df_loan_raw, _ = pyreadstat.read_sas7bdat(
        f'{LOAN_DIR}lnnote.sas7bdat',
        usecols=hpd_cols,
        disable_datetime_conversion=True
    )
    df_loan_raw = df_loan_raw[df_loan_raw['loantype'].isin(HPD)].copy()
    df_loan_raw = df_loan_raw.drop_duplicates(subset=['acctno', 'noteno'])
    print(f"HPD loan records: {len(df_loan_raw)}")
except Exception as e:
    print(f"Error reading HPD loans: {e}")
    df_loan_raw = pd.DataFrame(columns=['acctno', 'noteno'])

# Merge NPL, CREDSUB, and LOAN
print("Merging NPL, CREDSUB, and LOAN data...")
df_loan = df_npl.merge(df_credsub, on=['acctno', 'noteno'], how='left')
df_loan = df_loan.merge(df_loan_raw, on=['acctno', 'noteno'], how='left', suffixes=('', '_loan'))
df_loan = df_loan[df_loan['acctno'].notna()]

print(f"Merged loan records: {len(df_loan)}")

# Step 5: Calculate derived fields (vectorized)
print("Step 5: Calculating derived fields (vectorized)...")
df_loan['postamt'] = df_loan['feetotal'].fillna(0) + df_loan['nfeeamt5'].fillna(0)
df_loan['otheramt'] = df_loan['feeamt3'].fillna(0) - df_loan['postamt']
df_loan['oifeeamt'] = df_loan['feetot2'].fillna(0) - df_loan['feeamta'].fillna(0) + df_loan['feeamt5'].fillna(0)
df_loan['ecsrrsrv'] = df_loan['ecsrrsrv'].apply(lambda x: 0 if pd.isna(x) or x <= 0 else x)

# Date formatting
df_loan['matdate'] = df_loan['maturedt'].apply(lambda x: safe_format_date(x, format_mmddyy10))
df_loan['lasttra1'] = df_loan['lasttran'].apply(lambda x: safe_format_date(x, format_mmddyy10))

# Months past due
df_loan['days'] = df_loan['days'].fillna(0).astype(int)
df_loan['mthpdue'] = df_loan['days'].apply(mthpass_format)
mask = df_loan['mthpdue'] == 24
df_loan.loc[mask, 'mthpdue'] = (df_loan.loc[mask, 'days'] / 365 * 12).astype(int)

# Credit grade
df_loan['score2'] = df_loan['score2'].fillna('').astype(str)
df_loan['contrtype'] = df_loan['contrtype'].fillna('').astype(str)
df_loan['crrgrade'] = (df_loan['score2'] + df_loan['contrtype']).str.strip()

# Margin of financing
df_loan['netproc'] = df_loan['netproc'].fillna(0)
df_loan['appvalue'] = df_loan['appvalue'].fillna(0)
df_loan['marginfi'] = np.where(
    df_loan['appvalue'] > 0,
    (df_loan['netproc'] / df_loan['appvalue']).round(2),
    0
)

# Date of birth
df_loan['dobmni'] = df_loan['birthdt'].apply(safe_get_date)

# ECSR indicator
df_loan['ecsrind'] = np.where(df_loan['ecsrrsrv'] > 0, 'Y', 'N')

# Bills paid
df_loan['orgbal'] = df_loan['orgbal'].fillna(0)
df_loan['curbal'] = df_loan['curbal'].fillna(0)
df_loan['payamt'] = df_loan['payamt'].fillna(0)
df_loan['bilpaid'] = np.where(
    df_loan['payamt'] > 0,
    ((df_loan['orgbal'] - df_loan['curbal']) / df_loan['payamt']).astype(int),
    0
)

# NACO special attention
df_loan['pay75pct'] = np.where(df_loan['nacospadt'].fillna(0) > 0, 'Y', 'N')
df_loan['nacodate'] = df_loan['nacospadt'].apply(lambda x: safe_format_date(x, format_mmddyy10))

print("Derived fields calculated")

# Step 6: Get customer names
print("Step 6: Reading customer names...")
try:
    df_cname, _ = pyreadstat.read_sas7bdat(
        f'{CISNAME_DIR}loan.sas7bdat',
        usecols=['acctno', 'custnam1', 'occupat', 'bgc', 'seccust']
    )
    df_cname = df_cname[df_cname['seccust'] == '901']
    df_cname = df_cname[['acctno', 'custnam1', 'occupat', 'bgc']].drop_duplicates(subset=['acctno'])
    print(f"Customer records: {len(df_cname)}")
except Exception as e:
    print(f"Error reading customer names: {e}")
    df_cname = pd.DataFrame(columns=['acctno', 'custnam1', 'occupat', 'bgc'])

# Step 7: Get guarantors
print("Step 7: Reading liability data...")
try:
    df_liab, _ = pyreadstat.read_sas7bdat(
        f'{LOAN_DIR}liab.sas7bdat',
        usecols=['acctno', 'noteno', 'liabacct', 'liabname']
    )
    df_liab = df_liab.sort_values('liabacct')
    
    df_liab = df_liab.merge(
        df_cname.rename(columns={'acctno': 'liabacct', 'custnam1': 'gname'}),
        on='liabacct',
        how='left'
    )
    
    df_liab['gname'] = df_liab['gname'].fillna(df_liab['liabname'])
    df_liab = df_liab.sort_values(['acctno', 'noteno'])
    
    guarantor_data = {}
    for (acctno, noteno), group in df_liab.groupby(['acctno', 'noteno']):
        gnames = group['gname'].tolist()
        guarantor_data[(acctno, noteno)] = {
            'guarnam1': gnames[0] if len(gnames) > 0 else '',
            'guarnam2': gnames[1] if len(gnames) > 1 else ''
        }
    print(f"Guarantor records processed: {len(guarantor_data)}")
except Exception as e:
    print(f"Error reading liability data: {e}")
    guarantor_data = {}
    df_liab = pd.DataFrame()

# Step 8: Get previous month balance
print("Step 8: Reading previous month balance...")
sasln_file = f'{SASLN_DIR}loan{reptmon1}{nowks}.sas7bdat'
if os.path.exists(sasln_file):
    try:
        df_sasln, _ = pyreadstat.read_sas7bdat(
            sasln_file,
            usecols=['acctno', 'noteno', 'curbal']
        )
        df_sasln = df_sasln.rename(columns={'curbal': 'prevbal'})
        df_sasln = df_sasln.sort_values(['acctno', 'noteno'])
        print(f"Previous balance records: {len(df_sasln)}")
    except Exception as e:
        print(f"Error reading {sasln_file}: {e}")
        df_sasln = pd.DataFrame(columns=['acctno', 'noteno', 'prevbal'])
else:
    print(f"Warning: {sasln_file} not found.")
    df_sasln = pd.DataFrame(columns=['acctno', 'noteno', 'prevbal'])

# Merge with NPL to get only relevant accounts
df_sasln = df_sasln.merge(df_npl[['acctno', 'noteno']], on=['acctno', 'noteno'], how='inner')

# Add guarantor names
df_sasln['guarnam1'] = df_sasln.apply(
    lambda row: guarantor_data.get((row['acctno'], row['noteno']), {}).get('guarnam1', ''),
    axis=1
)
df_sasln['guarnam2'] = df_sasln.apply(
    lambda row: guarantor_data.get((row['acctno'], row['noteno']), {}).get('guarnam2', ''),
    axis=1
)

# Step 9: Merge all data
print("Step 9: Merging all data...")
df_woff = df_sasln.merge(df_loan, on=['acctno', 'noteno'], how='outer')
df_woff = df_woff.merge(df_npl, on='acctno', how='outer', suffixes=('', '_npl'))

df_woff['payment'] = df_woff['curbal'].fillna(0) - df_woff['prevbal'].fillna(0)
df_woff['total'] = df_woff['totiis'].fillna(0) + df_woff['sp'].fillna(0)
df_woff['rind'] = 'D'

gc.collect()

# Step 10: Filter for write-off candidates
print("Step 10: Filtering write-off candidates...")
df_woff = df_woff[
    (
        ((df_woff['borstat'].isin(['F', 'I'])) & (df_woff['days'] >= 334)) |
        (df_woff['days'] >= 334) |
        (
            (df_woff['borstat'] == 'A') &
            (~df_woff['loantype'].isin([983, 993, 678, 679, 698, 699])) &
            (df_woff['paidind'] != 'P')
        )
    ) &
    (df_woff['total'] != 0)
]

df_woff['confirm'] = 'Y'
df_woff = df_woff.sort_values('acctno')

df_woff = df_woff.merge(
    df_cname.rename(columns={'custnam1': 'name'}),
    on='acctno',
    how='left',
    suffixes=('', '_cname')
)

print(f"Write-off candidates: {len(df_woff)}")

# Save to parquet
os.makedirs(os.path.dirname(f'{NPL_DIR}list.parquet'), exist_ok=True)
df_woff.to_parquet(f'{NPL_DIR}list.parquet', index=False)

print(f"\nBad Debt Write-Off List (Conventional) Generation Complete")

# Step 11: Write fixed-width output file
print("Step 11: Writing fixed-width output file...")
os.makedirs(os.path.dirname(OUTPUT_FILE1), exist_ok=True)

with open(OUTPUT_FILE1, 'w', buffering=8192*1024) as f:
    lines = []
    for _, row in df_woff.iterrows():
        lines.append(create_fixed_width_line(row))
        
        if len(lines) >= 1000:
            f.writelines(lines)
            lines = []
    
    if lines:
        f.writelines(lines)

# Step 12-14: Read, recalculate, write final output
print("Step 12-14: Writing final formatted output...")
with open(OUTPUT_FILE1, 'r', buffering=8192*1024) as f_in, \
     open(OUTPUT_FILE, 'w', buffering=8192*1024) as f_out:
    
    for line in f_in:
        totiis = float(line[100:116]) if line[100:116].strip() else 0
        balance = float(line[356:372]) if line[356:372].strip() else 0
        oi = float(line[84:100]) if line[84:100].strip() else 0
        
        sp_calc = balance - totiis
        total_calc = totiis + sp_calc
        
        delqcd = line[676:678]
        occupat = line[712:715]
        bgc = line[742:744]
        
        delqdes = get_delq_desc(delqcd)
        occupdes = get_occup_desc(occupat)
        bgcdes = get_bgc_desc(bgc)
        
        biztype = 'C'
        cap = 0.0
        latechg = oi
        
        f_out.write(line[:116])
        f_out.write(f"{sp_calc:>16.2f}")
        f_out.write(f"{total_calc:>16.2f}")
        f_out.write(line[148:373])
        f_out.write(f"{cap:>16.2f}")
        f_out.write(f"{latechg:>16.2f}")
        f_out.write(line[407:679])
        f_out.write(f"{delqdes:<30}")
        f_out.write(f"{biztype:1}")
        f_out.write(line[712:715])
        f_out.write(f"{occupdes:<25}")
        f_out.write(line[742:744])
        f_out.write(f"{bgcdes:<20}")
        f_out.write(line[766:])

print(f"\nOutput files generated:")
print(f"  {OUTPUT_FILE} (Final formatted output)")
print(f"  {OUTPUT_FILE1} (Intermediate output)")
print(f"  {NPL_DIR}list.parquet (Data file)")
print(f"\nAccounts identified for write-off: {len(df_woff)}")
if len(df_woff) > 0:
    print(f"Total exposure: RM {df_woff['total'].sum():,.2f}")
print(f"\nKey Differences from EIIFTXT1 (Islamic):")
print(f"  - RIND = 'D' (Domestic/Conventional) vs 'I' (Islamic)")
print(f"  - BIZTYPE = 'C' (Conventional) vs 'I' (Islamic)")
print(f"  - Uses CREDMSUBAC vs ICREDMSUBAC (CCRIS)")

Processing Bad Debt Write-Off List (Conventional Banking)
Report Date: 27/07/2026
Week: 3, Previous Month: 06
Reading LNNOTE (optimized)...
Successfully read 0 records from LNNOTE
Step 1: Creating NPLA...
Traceback (most recent call last):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/scope.py", line 231, in resolve
    return self.resolvers[key]
  File "/usr/lib64/python3.9/collections/__init__.py", line 941, in __getitem__
    return self.__missing__(key)            # support subclasses that define __missing__
  File "/usr/lib64/python3.9/collections/__init__.py", line 933, in __missing__
    raise KeyError(key)
KeyError: 'borstat'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/scope.py", line 242, in resolve
    return self.temps[key]
KeyError: 'borstat'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIFFTXT1.py", line 319, in <module>
    df_npla = df_lnnote.query(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4823, in query
    res = self.eval(expr, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4949, in eval
    return _eval(expr, inplace=inplace, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/eval.py", line 336, in eval
    parsed_expr = Expr(expr, engine=engine, parser=parser, env=env)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 805, in __init__
    self.terms = self.parse()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 824, in parse
    return self._visitor.visit(self.expr)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 411, in visit
    return visitor(node, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 417, in visit_Module
    return self.visit(expr, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 411, in visit
    return visitor(node, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 420, in visit_Expr
    return self.visit(node.value, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 411, in visit
    return visitor(node, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 742, in visit_BoolOp
    return reduce(visitor, operands)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 735, in visitor
    lhs = self._try_visit_binop(x)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 731, in _try_visit_binop
    return self.visit(bop)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 411, in visit
    return visitor(node, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 715, in visit_Compare
    return self.visit(binop)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 411, in visit
    return visitor(node, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 531, in visit_BinOp
    op, op_class, left, right = self._maybe_transform_eq_ne(node)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 451, in _maybe_transform_eq_ne
    left = self.visit(node.left, side="left")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 411, in visit
    return visitor(node, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/expr.py", line 541, in visit_Name
    return self.term_type(node.id, self.env, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/ops.py", line 91, in __init__
    self._value = self._resolve_name()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/ops.py", line 115, in _resolve_name
    res = self.env.resolve(local_name, is_local=is_local)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/computation/scope.py", line 244, in resolve
    raise UndefinedVariableError(key, is_local) from err
pandas.errors.UndefinedVariableError: name 'borstat' is not defined
