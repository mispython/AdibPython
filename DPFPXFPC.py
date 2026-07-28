"""
EIFFTXT1 - Bad Debt Write-Off List (Conventional Banking)
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

# Input directory paths (all lowercase)
LOAN_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eifftxt1/'
NPL_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eifftxt1/'
SASLN_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eifftxt1/'
CISNAME_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eifftxt1/'
CCRIS_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eifftxt1/'
BKCTRL_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eifftxt1/'

OUTPUT_FILE = 'data/wofftext.txt'
OUTPUT_FILE1 = 'data/wofftex1.txt'

# HPD loan types (from PBBLNFMT)
HPD = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110,
       201, 202, 203, 204, 205, 206, 207, 208, 209, 210,
       301, 302, 303, 304, 305, 306, 307, 308, 309, 310]

# Format definitions
def get_branch_name(branch_code):
    """Get branch abbreviation - simplified version"""
    branch_map = {
        1: 'KL', 2: 'PJ', 3: 'JB', 4: 'PG', 5: 'IP',
        # Add more branch codes as needed
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

# Additional formats loaded from BKCTRL.CISFMT
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
    return DELQDES.get(str(delqcd).strip() if delqcd else '  ', 'UNKNOWN')

def get_occup_desc(occupat):
    """Get occupation description"""
    return OCCUPFMT.get(str(occupat).strip() if occupat else '999', 'OTHERS')

def get_bgc_desc(bgc):
    """Get business/government code description"""
    return BGCFMT.get(str(bgc).strip() if bgc else '  ', 'NOT SPECIFIED')

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

# Step 1: Create NPLA - Active accounts with BORSTAT='A'
print("Reading LNNOTE...")
df_lnnote, meta = pyreadstat.read_sas7bdat(f'{LOAN_DIR}lnnote.sas7bdat')
df_npla = df_lnnote[
    (df_lnnote['borstat'] == 'A') &
    (~df_lnnote['loantype'].isin([983, 993, 678, 679, 698, 699]))
].copy()

df_npla['iis'] = 0
df_npla['oi'] = df_npla['feedue'] - df_npla['feeduems']
df_npla['totiis'] = 0 + (df_npla['feedue'] - df_npla['feeduems'])
df_npla['sp'] = df_npla['feeduems'] + df_npla['feeamt16']

# Apply BRCHCD format
branch_list = []
for ntbrch in df_npla['ntbrch']:
    branch_abbr = get_branch_name(ntbrch)
    branch_list.append(f"{branch_abbr} {ntbrch:03d}")

df_npla['branch'] = branch_list
df_npla = df_npla[['name', 'acctno', 'noteno', 'iis', 'oi', 'totiis', 'sp', 'marketvl', 'branch']]

# Step 2: Get IIS and SP data
print("Reading IIS and SP data...")
df_iis, _ = pyreadstat.read_sas7bdat(f'{NPL_DIR}iis.sas7bdat')
df_sp, _ = pyreadstat.read_sas7bdat(f'{NPL_DIR}sp2.sas7bdat')

df_iis = df_iis.drop_duplicates(subset=['acctno', 'noteno'])
df_sp = df_sp.drop_duplicates(subset=['acctno', 'noteno'])

# Merge IIS and SP
df_npl_data = df_sp.merge(df_iis, on=['acctno', 'noteno'], how='outer')
df_npl_data = df_npl_data[['name', 'acctno', 'noteno', 'iis', 'oi', 'totiis', 'sp', 'marketvl', 'branch']]

# Combine NPLA and NPL data
df_npl = pd.concat([df_npla, df_npl_data], ignore_index=True)
df_npl['marketvl'] = df_npl['marketvl'].round(2)
df_npl['brno'] = df_npl['branch'].str[3:7]
df_npl['brabr'] = df_npl['branch'].str[0:3]
df_npl = df_npl.drop_duplicates(subset=['acctno', 'noteno'])

# Step 3: KEY DIFFERENCE - Get CCRIS credit submission data
print("Reading CREDMSUBAC...")
credmsubac_file = f'{CCRIS_DIR}credmsubac{reptmon}{reptyear}.sas7bdat'
if os.path.exists(credmsubac_file):
    df_credsub, _ = pyreadstat.read_sas7bdat(credmsubac_file)
    df_credsub = df_credsub[
        df_credsub['facility'].isin(['34331', '34332'])
    ].rename(columns={'acctnum': 'acctno', 'daysarr': 'days'})
    
    df_credsub = df_credsub.sort_values(['acctno', 'noteno', 'days'], ascending=[True, True, False])
    df_credsub = df_credsub.drop_duplicates(subset=['acctno', 'noteno'])
    df_credsub = df_credsub[['acctno', 'noteno', 'days', 'facility']]
else:
    print(f"Warning: {credmsubac_file} not found. Creating empty DataFrame.")
    df_credsub = pd.DataFrame(columns=['acctno', 'noteno', 'days', 'facility'])

# Step 4: Get loan data for HPD loan types
print("Reading LNNOTE for HPD loans...")
df_loan_raw = df_lnnote[df_lnnote['loantype'].isin(HPD)].copy()
df_loan_raw = df_loan_raw.drop_duplicates(subset=['acctno', 'noteno'])

# Merge NPL, CREDSUB, and LOAN
df_loan = df_npl.merge(df_credsub, on=['acctno', 'noteno'], how='left')
df_loan = df_loan.merge(df_loan_raw, on=['acctno', 'noteno'], how='left', suffixes=('', '_loan'))
df_loan = df_loan[df_loan['acctno'].notna()]

# Step 5: Calculate derived fields
print("Calculating derived fields...")
loan_records = []
for _, row in df_loan.iterrows():
    new_row = row.to_dict()
    
    # Post amount
    new_row['postamt'] = (row.get('feetotal', 0) or 0) + (row.get('nfeeamt5', 0) or 0)
    new_row['otheramt'] = (row.get('feeamt3', 0) or 0) - new_row['postamt']
    
    # OI fee amount
    feetot2 = row.get('feetot2', 0) or 0
    feeamta = row.get('feeamta', 0) or 0
    feeamt5 = row.get('feeamt5', 0) or 0
    new_row['oifeeamt'] = feetot2 - feeamta + feeamt5
    
    # ECSR reserve
    ecsrrsrv = row.get('ecsrrsrv', 0) or 0
    new_row['ecsrrsrv'] = 0 if ecsrrsrv <= 0 else ecsrrsrv
    
    # Maturity date
    maturedt = row.get('maturedt')
    if pd.notna(maturedt) and maturedt:
        try:
            maturedt_str = str(int(maturedt)).zfill(8)
            if len(maturedt_str) >= 8:
                matdate_dt = datetime.strptime(maturedt_str[:8], '%m%d%Y')
                new_row['matdate'] = format_mmddyy10(matdate_dt)
                new_row['maturedt_date'] = matdate_dt.date()
            else:
                new_row['matdate'] = ''
        except:
            new_row['matdate'] = ''
    else:
        new_row['matdate'] = ''
    
    # Last transaction date
    lasttran = row.get('lasttran')
    if pd.notna(lasttran) and lasttran:
        try:
            lasttran_str = str(int(lasttran)).zfill(8)
            if len(lasttran_str) >= 8:
                lasttra1_dt = datetime.strptime(lasttran_str[:8], '%m%d%Y')
                new_row['lasttra1'] = format_mmddyy10(lasttra1_dt)
            else:
                new_row['lasttra1'] = ''
        except:
            new_row['lasttra1'] = ''
    else:
        new_row['lasttra1'] = ''
    
    # Months past due
    days = row.get('days', 0) or 0
    mthpdue = mthpass_format(days)
    if mthpdue == 24:
        mthpdue = int((days / 365) * 12)
    new_row['mthpdue'] = mthpdue
    
    # Credit grade
    score2 = str(row.get('score2', '')) if pd.notna(row.get('score2')) else ''
    contrtype = str(row.get('contrtype', '')) if pd.notna(row.get('contrtype')) else ''
    new_row['crrgrade'] = f"{score2}{contrtype}".strip()
    
    # Margin of financing
    netproc = row.get('netproc', 0) or 0
    appvalue = row.get('appvalue', 0) or 0
    if appvalue > 0:
        new_row['marginfi'] = round(netproc / appvalue, 2)
    else:
        new_row['marginfi'] = 0
    
    # Date of birth
    birthdt = row.get('birthdt', 0) or 0
    if pd.notna(birthdt) and birthdt > 0:
        try:
            birthdt_str = str(int(birthdt)).zfill(8)
            if len(birthdt_str) >= 8:
                dobmni_dt = datetime.strptime(birthdt_str[:8], '%m%d%Y')
                new_row['dobmni'] = dobmni_dt.date()
            else:
                new_row['dobmni'] = None
        except:
            new_row['dobmni'] = None
    else:
        new_row['dobmni'] = None
    
    # ECSR indicator
    new_row['ecsrind'] = 'Y' if new_row['ecsrrsrv'] > 0 else 'N'
    
    # Bills paid
    orgbal = row.get('orgbal', 0) or 0
    curbal = row.get('curbal', 0) or 0
    payamt = row.get('payamt', 0) or 0
    if payamt > 0:
        new_row['bilpaid'] = int((orgbal - curbal) / payamt)
    else:
        new_row['bilpaid'] = 0
    
    # NACO special attention date
    nacospadt = row.get('nacospadt', 0) or 0
    if pd.notna(nacospadt) and nacospadt > 0:
        new_row['pay75pct'] = 'Y'
        try:
            nacospadt_str = str(int(nacospadt)).zfill(8)
            if len(nacospadt_str) >= 8:
                new_row['nacodate'] = format_mmddyy10(datetime.strptime(nacospadt_str[:8], '%m%d%Y'))
            else:
                new_row['nacodate'] = ''
        except:
            new_row['nacodate'] = ''
    else:
        new_row['pay75pct'] = 'N'
        new_row['nacodate'] = ''
    
    loan_records.append(new_row)

df_loan = pd.DataFrame(loan_records)

# Step 6: Get customer names
print("Reading customer names...")
df_cname, _ = pyreadstat.read_sas7bdat(f'{CISNAME_DIR}loan.sas7bdat')
df_cname = df_cname[df_cname['seccust'] == '901']
df_cname = df_cname[['acctno', 'custnam1', 'occupat', 'bgc']].drop_duplicates(subset=['acctno'])

# Step 7: Get guarantors
print("Reading liability data...")
df_liab, _ = pyreadstat.read_sas7bdat(f'{LOAN_DIR}liab.sas7bdat')
df_liab = df_liab.sort_values('liabacct')

# Merge with customer names for guarantors
df_liab = df_liab.merge(
    df_cname.rename(columns={'acctno': 'liabacct', 'custnam1': 'gname'}),
    on='liabacct',
    how='left'
)

# Use LIABNAME if GNAME is null
df_liab['gname'] = df_liab['gname'].fillna(df_liab['liabname'])
df_liab = df_liab.sort_values(['acctno', 'noteno'])

# Create guarantor dictionary
guarantor_data = {}
for (acctno, noteno), group in df_liab.groupby(['acctno', 'noteno']):
    gnames = group['gname'].tolist()
    guarantor_data[(acctno, noteno)] = {
        'guarnam1': gnames[0] if len(gnames) > 0 else '',
        'guarnam2': gnames[1] if len(gnames) > 1 else ''
    }

# Step 8: Get previous month balance
print("Reading previous month balance...")
sasln_file = f'{SASLN_DIR}loan{reptmon1}{nowks}.sas7bdat'
if os.path.exists(sasln_file):
    df_sasln, _ = pyreadstat.read_sas7bdat(sasln_file)
    df_sasln = df_sasln[['acctno', 'noteno', 'curbal']].rename(columns={'curbal': 'prevbal'})
    df_sasln = df_sasln.sort_values(['acctno', 'noteno'])
else:
    print(f"Warning: {sasln_file} not found. Creating empty DataFrame.")
    df_sasln = pd.DataFrame(columns=['acctno', 'noteno', 'prevbal'])

# Merge with NPL to get only relevant accounts
df_sasln = df_sasln.merge(df_npl[['acctno', 'noteno']], on=['acctno', 'noteno'], how='inner')

# Add guarantor names
guarnam1_list = []
guarnam2_list = []
for _, row in df_sasln.iterrows():
    key = (row['acctno'], row['noteno'])
    gdata = guarantor_data.get(key, {'guarnam1': '', 'guarnam2': ''})
    guarnam1_list.append(gdata['guarnam1'])
    guarnam2_list.append(gdata['guarnam2'])

df_sasln['guarnam1'] = guarnam1_list
df_sasln['guarnam2'] = guarnam2_list

# Step 9: Merge all data
print("Merging all data...")
df_woff = df_sasln.merge(df_loan, on=['acctno', 'noteno'], how='outer')
df_woff = df_woff.merge(df_npl, on='acctno', how='outer', suffixes=('', '_npl'))

# Calculate payment and total
df_woff['payment'] = df_woff['curbal'] - df_woff['prevbal']
df_woff['total'] = df_woff['totiis'] + df_woff['sp']
df_woff['rind'] = 'D'  # KEY DIFFERENCE: 'D' for Conventional (vs 'I' for Islamic)

# Step 10: Filter for write-off candidates
print("Filtering write-off candidates...")
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

# Merge with customer names
df_woff = df_woff.merge(
    df_cname.rename(columns={'custnam1': 'name'}),
    on='acctno',
    how='left',
    suffixes=('', '_cname')
)

# Save to parquet (keeping this for compatibility)
df_woff.to_parquet(f'{NPL_DIR}list.parquet', index=False)

print(f"\nBad Debt Write-Off List (Conventional) Generation Complete")

# Step 11: Write fixed-width output file
print("Writing fixed-width output file...")
os.makedirs(os.path.dirname(OUTPUT_FILE1), exist_ok=True)

with open(OUTPUT_FILE1, 'w') as f:
    for _, row in df_woff.iterrows():
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
        
        # Issue date
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
        
        # Date of birth
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
        
        # Write fixed-width record
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
        
        f.write(line)

# Step 12-14: Read, recalculate, write final output
print("Writing final formatted output...")
with open(OUTPUT_FILE1, 'r') as f_in, open(OUTPUT_FILE, 'w') as f_out:
    for line in f_in:
        # Parse fields from fixed-width format
        branch = line[0:7].strip()
        name = line[8:48].strip()
        acctno = float(line[49:59]) if line[49:59].strip() else 0
        noteno = float(line[60:65]) if line[60:65].strip() else 0
        borstat = line[66:67]
        iis = float(line[68:84]) if line[68:84].strip() else 0
        oi = float(line[84:100]) if line[84:100].strip() else 0
        totiis = float(line[100:116]) if line[100:116].strip() else 0
        balance = float(line[356:372]) if line[356:372].strip() else 0
        
        sp_calc = balance - totiis
        total_calc = totiis + sp_calc
        
        delqcd = line[676:678]
        occupat = line[712:715]
        bgc = line[742:744]
        
        delqdes = get_delq_desc(delqcd)
        occupdes = get_occup_desc(occupat)
        bgcdes = get_bgc_desc(bgc)
        
        biztype = 'C'  # KEY DIFFERENCE: 'C' for Conventional (vs 'I' for Islamic)
        cap = 0.0
        latechg = oi
        
        # Write reformatted line
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

# Save final dataset
print("Saving final dataset...")
# Create a DataFrame from the final output file
final_records = []
with open(OUTPUT_FILE, 'r') as f:
    for line in f:
        record = {
            'branch': line[0:7].strip(),
            'name': line[8:48].strip(),
            'acctno': float(line[49:59]) if line[49:59].strip() else 0,
            'noteno': float(line[60:65]) if line[60:65].strip() else 0,
            'borstat': line[66:67],
            'iis': float(line[68:84]) if line[68:84].strip() else 0,
            'oi': float(line[84:100]) if line[84:100].strip() else 0,
            'totiis': float(line[100:116]) if line[100:116].strip() else 0,
            'sp': float(line[116:132]) if line[116:132].strip() else 0,
            'total': float(line[132:148]) if line[132:148].strip() else 0
        }
        final_records.append(record)

if final_records:
    df_final = pd.DataFrame(final_records)
    df_final.to_parquet(f'{NPL_DIR}wofftxt.parquet', index=False)

print(f"\nOutput files generated:")
print(f"  {OUTPUT_FILE} (Final formatted output)")
print(f"  {OUTPUT_FILE1} (Intermediate output)")
print(f"  {NPL_DIR}list.parquet (Data file)")
print(f"  {NPL_DIR}wofftxt.parquet (Final dataset)")
print(f"\nAccounts identified for write-off: {len(df_woff)}")
if len(df_woff) > 0:
    print(f"Total exposure: RM {df_woff['total'].sum():,.2f}")
print(f"\nKey Differences from EIIFTXT1 (Islamic):")
print(f"  - RIND = 'D' (Domestic/Conventional) vs 'I' (Islamic)")
print(f"  - BIZTYPE = 'C' (Conventional) vs 'I' (Islamic)")
print(f"  - Uses CREDMSUBAC vs ICREDMSUBAC (CCRIS)")
