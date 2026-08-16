"""
EIBDLCRM - BNM LCR (Liquidity Coverage Ratio) Reporting for Conventional Banking
Consolidates deposits & treasury positions for BNM LCR reporting.
Includes DCI (Dual Currency Investments) and full treasury processing.
Outputs: LCR reports with customer categorization (08/19/29/39/49/59)
"""

import polars as pl
import pyreadstat
from datetime import datetime, date, timedelta
import os
from pathlib import Path
import calendar
import glob

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'lcr': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibdlcrm/lcr/',
    'lcrm': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibdlcrm/lcrm/',
    'forate': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibdlcrm/',
    'cisdp': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibdlcrm/cisdp/',
    'cisca': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibdlcrm/cisca/',
    'cis': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibdlcrm/cis/',
    'dciwh': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibdlcrm/dciwh/',
    'equa': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibdlcrm/equa/',
    'bnmk': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibdlcrm/bnmk/',
    'list': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibdlcrm/list/',
    'output': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/eibdlcrm/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

inst = 'PBB'  # Institution code

# Customer category mappings (LCR)
cust_map = {
    '08': [76, 77, 78, 95, 96],  # Central banks/governments
    '19': [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69],  # SME
    '29': [0,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99],  # Other retail
    '39': [1,71,72,73,74,90,91,92],  # Sovereign funds
    '49': [2,3,7,12,81,82,83,84],  # Financial institutions
    '59': [4,5,6,13,20] + list(range(30,41)) + [17]  # Corporate
}

# Special customers
special_cust = {
    '39': ['kwsp', 'kwap', 'kwan', 'lemtab'],
    '49': ['aim', 'pbl', 'pbleur', 'pblnid', 'pblusd', 'pivmyr', 'ipbb']
}

# Hardcoded FX rates (replaces FOFMT)
FX_RATES = {
    'MYR': 1.0,
    'USD': 4.0,
    'SGD': 3.0,
    'HKD': 0.5,
    'AUD': 3.0,
    'JPY': 0.03,
    'XAU': 200.0,
    'GBP': 5.0,
    'EUR': 4.5,
    'CNY': 0.6
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def get_report_date():
    """Get report date as yesterday's date"""
    reptdate = date.today() - timedelta(days=1)
    
    day = reptdate.day
    nowk = '1' if day <= 8 else '2' if day <= 15 else '3' if day <= 22 else '4'
    
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if reptdate.year % 4 == 0:
        days_in_month[1] = 29
    
    return {
        'date': reptdate,
        'nowk': nowk,
        'mon': f"{reptdate.month:02d}",
        'day': f"{reptdate.day:02d}",
        'rdate': reptdate.strftime('%d%m%y'),
        'rptdt': reptdate.strftime('%y%m%d'),
        'year': reptdate.year,
        'month': reptdate.month,
        'day_of_month': day,
        'days_in_month': days_in_month,
        'days_in_cur_month': days_in_month[reptdate.month - 1]
    }

def normalize_column_names(df):
    """Convert all column names to lowercase for consistency"""
    if df is not None:
        new_cols = {col: col.lower() for col in df.columns}
        return df.rename(new_cols)
    return df

def read_sas_file(filepath, columns=None):
    """Read SAS dataset using pyreadstat and return polars DataFrame with lowercase columns"""
    try:
        if columns:
            df, meta = pyreadstat.read_sas7bdat(filepath, usecols=columns)
        else:
            df, meta = pyreadstat.read_sas7bdat(filepath)
        df_pl = pl.from_pandas(df)
        # Convert column names to lowercase
        df_pl = normalize_column_names(df_pl)
        print(f"    Successfully read: {os.path.basename(filepath)} ({len(df_pl)} rows, {len(df_pl.columns)} columns)")
        return df_pl
    except Exception as e:
        print(f"    Warning: Could not read {filepath}: {e}")
        return None

def read_parquet_file(filepath):
    """Read parquet file and return polars DataFrame with lowercase columns"""
    try:
        df = pl.read_parquet(filepath)
        df = normalize_column_names(df)
        print(f"    Successfully read: {os.path.basename(filepath)} ({len(df)} rows, {len(df.columns)} columns)")
        return df
    except Exception as e:
        print(f"    Warning: Could not read {filepath}: {e}")
        return None

def read_walk_file(filepath):
    """Read WALK.TXT file (fixed width format) - with error handling for non-numeric values"""
    records = []
    try:
        with open(filepath, 'r') as f:
            for line_num, line in enumerate(f, 1):
                if len(line) >= 18:
                    try:
                        acctno_str = line[0:11].strip()
                        custno_str = line[11:18].strip()
                        
                        # Only add if both are numeric
                        if acctno_str and acctno_str.isdigit() and custno_str and custno_str.isdigit():
                            records.append({
                                'acctno': int(acctno_str),
                                'custno': int(custno_str)
                            })
                        else:
                            # Skip non-numeric lines (like header or invalid data)
                            pass
                    except ValueError:
                        # Skip lines that can't be parsed
                        pass
        print(f"    Read {len(records)} valid records from {os.path.basename(filepath)}")
    except Exception as e:
        print(f"    Warning: Could not read {filepath}: {e}")
    return records

def read_templ_file(filepath):
    """Read TEMPL.TXT file (fixed width format)"""
    records = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if len(line) >= 14:
                    records.append({
                        'tag': line[0:2].strip(),
                        'desc': line[2:14].strip()
                    })
        print(f"    Read {len(records)} records from {os.path.basename(filepath)}")
    except Exception as e:
        print(f"    Warning: Could not read {filepath}: {e}")
    return records

def get_customer_category(code, mapping, special=None, is_custno=False):
    """Get customer category from code"""
    if is_custno and special and code in special:
        return next((cat for cat, vals in special.items() if code in vals), '29')
    
    for cat, codes in mapping.items():
        if code in codes:
            return cat
    return '29'

def calculate_remaining_months(matdt, reptdate, days_in_month):
    """Calculate REMMTH and REM30D (equivalent to %REMMTH macro)"""
    if matdt <= reptdate:
        return 0.1, 0
    
    rp_year = reptdate.year
    rp_month = reptdate.month
    rp_day = reptdate.day
    
    md_year = matdt.year
    md_month = matdt.month
    md_day = matdt.day
    
    days_in_target_month = days_in_month[md_month - 1]
    if md_day > days_in_target_month:
        md_day = days_in_target_month
    
    rem_years = md_year - rp_year
    rem_months = md_month - rp_month
    rem_days = md_day - rp_day
    
    remmth = rem_years * 12 + rem_months + rem_days / days_in_month[rp_month - 1]
    rem30d = (matdt - reptdate).days / 30
    
    return remmth, rem30d

def format_mth_bucket(months):
    """Format months into bucket (01-10)"""
    if months <= 1: return '01'
    if months <= 3: return '02'
    if months <= 6: return '03'
    if months <= 9: return '04'
    if months <= 12: return '05'
    if months <= 24: return '06'
    if months <= 36: return '07'
    if months <= 60: return '08'
    if months <= 120: return '09'
    return '10'

def format_day_bucket(days):
    """Format days into bucket (01=<=30, 02=>30)"""
    return '01' if days <= 1 else '02'

# =============================================================================
# KALMLIQ LOGIC - Process K1TBL and K3TBL from BNMK source
# =============================================================================
def find_k1tbl_file(rep_date):
    """Find K1TBL file"""
    base_path = PATHS['bnmk']
    month = rep_date['mon']
    week = rep_date['nowk']
    
    if not os.path.exists(base_path):
        return None
    
    possible_names = [
        f"k1tbl{month}{week}.sas7bdat",
        f"k1tbl{month}0{week}.sas7bdat",
    ]
    
    for name in possible_names:
        full_path = os.path.join(base_path, name)
        if os.path.exists(full_path):
            return full_path
    
    # Try wildcard
    matches = glob.glob(os.path.join(base_path, f"*k1tbl*{month}*.sas7bdat"))
    if matches:
        return matches[0]
    
    return None

def find_k3tbl_file(rep_date):
    """Find K3TBL file"""
    base_path = PATHS['bnmk']
    month = rep_date['mon']
    week = rep_date['nowk']
    
    if not os.path.exists(base_path):
        return None
    
    possible_names = [
        f"k3tbl{month}{week}.sas7bdat",
        f"k3tbl{month}0{week}.sas7bdat",
    ]
    
    for name in possible_names:
        full_path = os.path.join(base_path, name)
        if os.path.exists(full_path):
            return full_path
    
    # Try wildcard
    matches = glob.glob(os.path.join(base_path, f"*k3tbl*{month}*.sas7bdat"))
    if matches:
        return matches[0]
    
    return None

def process_k1tbl(rep_date):
    """Process K1TBL from BNMK.K1TBL{REPTMON}{NOWK}"""
    records = []
    
    try:
        k1_filepath = find_k1tbl_file(rep_date)
        
        if k1_filepath is None:
            print(f"  No K1TBL file found")
            return records
        
        print(f"  Using K1TBL file: {k1_filepath}")
        df = read_sas_file(k1_filepath)
        
        if df is None:
            return records
        
        print(f"  Processing K1TBL with {len(df)} rows...")
        
        # Filter: GWMVT = 'P' (case insensitive)
        df = df.filter(pl.col('gwmvt').str.to_uppercase() == 'P')
        print(f"    After GWMVT='P' filter: {len(df)} rows")
        
        # Filter out XAU/XAT
        df = df.filter(
            ~pl.col('gwocy').is_in(['XAU', 'XAT']) & 
            ~pl.col('gwccy').is_in(['XAU', 'XAT'])
        )
        print(f"    After XAU/XAT filter: {len(df)} rows")
        
        # Process each row
        for row in df.iter_rows(named=True):
            gwccy = str(row.get('gwccy', 'MYR')).upper()
            gwmpts = str(row.get('gwmpts', '')).upper()
            gwctp = str(row.get('gwctp', '')).upper()
            gwdlp = str(row.get('gwdlp', '')).upper()
            
            matdt = row.get('gwmdt')
            issdt = row.get('gwsdt')
            amount = row.get('gwbalc', 0) or 0
            gwhsn = row.get('gwhsn', '') or ''
            gwc2r = row.get('gwc2r', 0) or 0
            gwdlr = row.get('gwdlr', '') or ''
            
            if gwccy == 'MYR':
                part = '95'
                amtusd = 0
                amtsgd = 0
                
                if gwmpts == 'M':
                    # Check for item 830
                    if gwdlp in ['BCD', 'BCI', 'BCS', 'BCQ', 'BCT', 'BCW', 'BQD']:
                        records.append({
                            'part': part, 'item': '830', 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })
                    
                    # Check for items 610 and 810 based on GWCTP
                    if gwctp and gwctp[0] == 'B':
                        if gwdlp in ['LO', 'LC', 'LF', 'LS', 'LOI', 'LSI', 'LSC', 'LSW',
                                     'FDA', 'FDB', 'FDS', 'FDL', 'LOC', 'LOW']:
                            records.append({
                                'part': part, 'item': '610', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        elif gwdlp in ['BO', 'BF', 'BOI', 'BFI', 'BSC', 'BSW', 'BOC', 'BOW']:
                            records.append({
                                'part': part, 'item': '810', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                    
                    # Check for items 820 and 620 based on GWDLP[1:3]
                    if len(gwdlp) >= 2 and gwdlp[1:3] in ['MI', 'MT']:
                        records.append({
                            'part': part, 'item': '820', 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })
                    if len(gwdlp) >= 2 and gwdlp[1:3] in ['XI', 'XT']:
                        records.append({
                            'part': part, 'item': '620', 'matdt': matdt, 'issdt': issdt,
                            'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                            'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                            'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                        })
            else:
                part = '96'
                amtusd = amount if gwccy == 'USD' else 0
                amtsgd = amount if gwccy == 'SGD' else 0
                
                if gwmpts == 'M':
                    if gwctp and gwctp[0] == 'B' and gwctp != 'BW':
                        if gwdlp in ['LO', 'LC', 'LS', 'LF', 'LOI', 'LSI', 'LSC', 'LOC',
                                     'FDA', 'FDB', 'FDS', 'FDL', 'LOW', 'LSW']:
                            records.append({
                                'part': part, 'item': '610', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
                        elif gwdlp in ['BC', 'BF', 'BO', 'BSC', 'BOW', 'BSW']:
                            if gwhsn[:6] != 'FCY-FD':
                                records.append({
                                    'part': part, 'item': '810', 'matdt': matdt, 'issdt': issdt,
                                    'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                    'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                                    'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                                })
                        elif gwdlp == 'BOC':
                            records.append({
                                'part': part, 'item': '810', 'matdt': matdt, 'issdt': issdt,
                                'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                                'gwccy': gwccy, 'gwhsn': gwhsn, 'gwc2r': gwc2r,
                                'gwdlp': gwdlp, 'gwdlr': gwdlr, 'src': 'k1tbl'
                            })
        
        print(f"  K1TBL records with item assigned: {len(records):,}")
        
    except Exception as e:
        print(f"  K1TBL warning: {e}")
        import traceback
        traceback.print_exc()
    
    return records

def process_k3tbl(rep_date):
    """Process K3TBL from BNMK.K3TBL{REPTMON}{NOWK}"""
    records = []
    
    try:
        k3_filepath = find_k3tbl_file(rep_date)
        
        if k3_filepath is None:
            print(f"  No K3TBL file found")
            return records
        
        print(f"  Using K3TBL file: {k3_filepath}")
        df = read_sas_file(k3_filepath)
        
        if df is None:
            return records
        
        print(f"  Processing K3TBL with {len(df)} rows...")
        
        # Filter for relevant UTREF values
        valid_utref = ['INV', 'DRI', 'DLG', 'AFSLIQ', 'AFSBOND', 'IAFSLIQ', 'AFS', 'IAFS',
                       'PFD', 'PLD', 'PSD', 'PZD', 'PDC', 'IINV', 'IDRI', 'IDLG']
        df = df.filter(pl.col('utref').str.to_uppercase().is_in(valid_utref))
        print(f"    After UTREF filter: {len(df)} rows")
        
        # Process each row
        for row in df.iter_rows(named=True):
            utamoc = row.get('utamoc', 0) or 0
            utdpf = row.get('utdpf', 0) or 0
            amount = utamoc - utdpf
            
            utsty = str(row.get('utsty', '')).upper()
            if utsty == 'IDC':
                amount = utamoc + utdpf
            
            utccy = str(row.get('utccy', 'MYR')).upper()
            utcus = row.get('utcus', '') or ''
            utctp = row.get('utctp', 0) or 0
            utdlr = row.get('utdlr', '') or ''
            utdlp = str(row.get('utdlp', '')).upper()
            utref = str(row.get('utref', '')).upper()
            utaict = row.get('utaict', 0) or 0
            utpcp = row.get('utpcp', 0) or 0
            utdpey = row.get('utdpey', 0) or 0
            utdpe = row.get('utdpe', 0) or 0
            utaicy = row.get('utaicy', 0) or 0
            utait = row.get('utait', 0) or 0
            utmm1 = str(row.get('utmm1', '')).upper()
            matdt = row.get('utmat', None)
            
            part = '95'
            amtusd = amount if utccy == 'USD' else 0
            amtsgd = amount if utccy == 'SGD' else 0
            
            item = None
            
            # Process based on UTREF
            if utref in ['INV', 'DRI', 'DLG', 'AFSLIQ', 'AFSBOND', 'IAFSLIQ', 'AFS', 'IAFS']:
                if utsty in ['CB1', 'CB2', 'CF1', 'CF2', 'CNT', 'MGS', 'MTB', 'BNB', 'BNN',
                            'ITB', 'SAC', 'BMN', 'BMC', 'BMF', 'SCD', 'SCM',
                            'CMB', 'MGI', 'SMC']:
                    item = '631'
                    if inst == 'PBB':
                        amount = amount + utaict
                elif utsty in ['SDC', 'LDC', 'SLD', 'SSD', 'SFD', 'SZD']:
                    item = '632'
                    if utsty == 'SDC' and inst == 'PBB':
                        amount = (utamoc * (utpcp / 100)) + utdpey + utdpe
                    elif utsty in ['SLD', 'SSD'] and inst == 'PBB':
                        amount = (utamoc * (utpcp / 100)) + utaicy + utait
                    elif inst == 'PBB':
                        amount = amount + utaict
                elif utsty == 'SBA' and utdlp not in ['MOS', 'MSS']:
                    item = '633'
                elif utsty in ['ISB', 'DHB', 'KHA', 'PNB']:
                    item = '636'
                elif utsty in ['IDS', 'DMB', 'GRL', 'MTL', 'RUL']:
                    item = '635'
                elif utsty == 'DBD':
                    item = '634'
                elif utsty == 'PBA' and utdlp in ['MOS', 'MSS']:
                    item = '850'
            
            elif utref in ['PFD', 'PLD', 'PSD', 'PZD', 'PDC']:
                if utsty in ['IFD', 'ILD', 'ISD', 'IZD', 'IDC', 'IDP', 'IZP']:
                    item = '840'
            
            elif utref in ['IINV', 'IDRI', 'IDLG']:
                if utsty == 'SBA' and utdlp == 'IOP':
                    item = '633'
                elif utsty in ['SDC', 'LDC']:
                    item = '632'
                elif utsty in ['CB1', 'CB2', 'CF1', 'CF2', 'CNT', 'MGI',
                               'ITB', 'SAC', 'BMN', 'BMC', 'BMF', 'SCD', 'SCM',
                               'MGS', 'MTB', 'BNB', 'BNN', 'CMB', 'SMC']:
                    item = '631'
                    if inst == 'PBB':
                        amount = amount + utaict
                elif utsty in ['ISB', 'IDS', 'IBZ', 'ICN']:
                    item = '636' if utmm1 == 'GGB' else '635'
                    amount = amount + utaict
                elif utsty in ['DHB', 'KHA']:
                    item = '636'
                elif utsty == 'DBD':
                    item = '634'
            
            elif utsty == 'SIP':
                item = '610'
            
            if item:
                records.append({
                    'part': part, 'item': item, 'matdt': matdt,
                    'amount': amount, 'amtusd': amtusd, 'amtsgd': amtsgd,
                    'utccy': utccy, 'utcus': utcus, 'utctp': utctp,
                    'utdlr': utdlr, 'utdlp': utdlp, 'src': 'k3tbl'
                })
        
        print(f"  K3TBL records with item assigned: {len(records):,}")
        
    except Exception as e:
        print(f"  K3TBL warning: {e}")
        import traceback
        traceback.print_exc()
    
    return records

def build_ktblall(k1_records, k3_records, rep_date):
    """Build KTBLALL from K1 and K3 records"""
    all_records = []
    
    for r in k1_records + k3_records:
        if r.get('item') and r.get('matdt'):
            matdt = r['matdt']
            issdt = r.get('issdt', matdt)
            
            if (matdt - rep_date['date']).days < 8:
                remmth = 0.1
            else:
                remmth, rem30d = calculate_remaining_months(
                    matdt, rep_date['date'], rep_date['days_in_month']
                )
            
            if issdt and (matdt - issdt).days < 8:
                ori30d = 0.1
            else:
                ori30d = (matdt - issdt).days / 30 if issdt else 0
            
            part = r['part']
            item = r['item']
            bnmcode = f"{part}{item}00{format_mth_bucket(remmth)}0000Y"
            
            cur = r.get('gwccy', r.get('utccy', 'MYR'))
            custfiss = r.get('gwc2r', r.get('utctp', 0))
            custno = r.get('utcus', None)
            dealtype = r.get('gwdlp', r.get('utdlp', ''))
            dealref = r.get('gwdlr', r.get('utdlr', ''))
            
            all_records.append({
                'src': r['src'],
                'bnmcode': bnmcode,
                'part': part,
                'item': item,
                'cur': cur,
                'amt': r['amount'],
                'amtusd': r.get('amtusd', 0),
                'amtsgd': r.get('amtsgd', 0),
                'custfiss': custfiss,
                'custno': custno,
                'dealtype': dealtype,
                'dealref': dealref,
                'remmth': remmth,
                'rem30d': rem30d if 'rem30d' in r else remmth,
                'ori30d': ori30d,
                'matdt': matdt
            })
            
            # Duplicate for PART 1 (93/94)
            if part == '95':
                new_part = '93'
            elif part == '96':
                new_part = '94'
            else:
                continue
            
            bnmcode2 = f"{new_part}{item}00{format_mth_bucket(remmth)}0000Y"
            all_records.append({
                'src': r['src'] + '_part1',
                'bnmcode': bnmcode2,
                'part': new_part,
                'item': item,
                'cur': cur,
                'amt': r['amount'],
                'amtusd': r.get('amtusd', 0),
                'amtsgd': r.get('amtsgd', 0),
                'custfiss': custfiss,
                'custno': custno,
                'dealtype': dealtype,
                'dealref': dealref,
                'remmth': remmth,
                'rem30d': rem30d if 'rem30d' in r else remmth,
                'ori30d': ori30d,
                'matdt': matdt
            })
    
    return all_records

# =============================================================================
# DCI PROCESSING
# =============================================================================
def process_dci(rep_date):
    """Process DCI (Dual Currency Investments)"""
    records = []
    
    try:
        dci_pattern = f"{PATHS['dciwh']}dcid*.sas7bdat"
        dci_files = glob.glob(dci_pattern)
        if not dci_files:
            print(f"  No DCI files found")
            return records
        
        dci_file = max(dci_files)
        print(f"  Using DCI file: {os.path.basename(dci_file)}")
        df = read_sas_file(dci_file)
        
        if df is None:
            return records
        
        # Check if we have the required columns
        required_cols = ['matdt', 'startdt']
        for col in required_cols:
            if col not in df.columns:
                print(f"  DCI: Required column '{col}' not found")
                return records
        
        for row in df.iter_rows(named=True):
            matdt = row.get('matdt')
            startdt = row.get('startdt')
            
            if matdt and startdt and matdt > rep_date['date'] and startdt <= rep_date['date']:
                if (matdt - rep_date['date']).days < 8:
                    remmth = 0.1
                else:
                    remmth, rem30d = calculate_remaining_months(
                        matdt, rep_date['date'], rep_date['days_in_month']
                    )
                
                invamt = row.get('invamt', 0) or 0
                invccy = str(row.get('invcurr', 'MYR')).upper()
                spotrt = FX_RATES.get(invccy, 1.0)
                
                if invccy == 'JPY':
                    invamt = round(invamt)
                else:
                    invamt = round(invamt, 2)
                
                amount = invamt * spotrt
                remth_bucket = format_mth_bucket(remmth)
                custcode = row.get('custcode', 0) or 0
                
                if invccy == 'MYR':
                    bnmcode = f"9532900{remth_bucket}0000Y"
                else:
                    bnmcode = f"9632900{remth_bucket}0000Y"
                
                records.append({
                    'src': 'dci',
                    'bnmcode': bnmcode,
                    'cur': invccy,
                    'amt': amount,
                    'custfiss': f"{int(custcode):02d}" if custcode else '00',
                    'dealtype': row.get('product', '') or '',
                    'dealref': row.get('ticketno', '') or '',
                    'remmth': remmth,
                    'rem30d': rem30d if 'rem30d' in locals() else remmth,
                    'ori30d': 0
                })
        
        print(f"  DCI records: {len(records):,}")
        
    except Exception as e:
        print(f"  DCI warning: {e}")
        import traceback
        traceback.print_exc()
    
    return records

# =============================================================================
# UTSAS PROCESSING
# =============================================================================
def process_utsas(rep_date):
    """Process UTSAS from EQUA tables"""
    records = []
    
    try:
        # The columns we need - these will be converted to lowercase
        needed_cols = ['dealref', 'dealtype', 'custfiss', 'custno', 'custname', 'custeqno', 'custid']
        
        for prefix in ['utms', 'utfx', 'utrp']:
            file_pattern = f"{PATHS['equa']}{prefix}{rep_date['rptdt']}.sas7bdat"
            files = glob.glob(file_pattern)
            
            if not files:
                # Try with different pattern
                file_pattern2 = f"{PATHS['equa']}{prefix}*.sas7bdat"
                files = glob.glob(file_pattern2)
            
            for filepath in files:
                # Extract prefix from filename
                base = os.path.basename(filepath).lower()
                if prefix not in base:
                    continue
                    
                df = read_sas_file(filepath)
                if df is None:
                    continue
                
                # Get available columns
                available_cols = [c for c in needed_cols if c in df.columns]
                
                if available_cols:
                    df = df.select(available_cols)
                    
                    # Rename custeqno to acctno if it exists
                    if 'custeqno' in df.columns:
                        df = df.rename({'custeqno': 'acctno'})
                    
                    # Convert to records
                    for row in df.rows(named=True):
                        records.append(row)
        
        print(f"  UTSAS records: {len(records):,}")
        
    except Exception as e:
        print(f"  UTSAS warning: {e}")
        import traceback
        traceback.print_exc()
    
    return records

# =============================================================================
# CIS EQUITY PROCESSING (UPDATED WITH UPPERCASE COLUMNS)
# =============================================================================
def process_cis_equity():
    """Process CIS equity data from parquet file"""
    records = []
    
    try:
        cis_pattern = f"{PATHS['cis']}*.parquet"
        cis_files = glob.glob(cis_pattern)
        if not cis_files:
            print(f"  No CIS parquet files found")
            return records
        
        cis_file = max(cis_files)
        print(f"  Using CIS file: {os.path.basename(cis_file)}")
        df = read_parquet_file(cis_file)
        
        if df is None:
            return records
        
        # Columns are now lowercase due to normalize_column_names
        # Filter for equity accounts
        if 'acctcode' in df.columns and 'prisec' in df.columns:
            df = df.filter((pl.col('acctcode') == 'EQC') & (pl.col('prisec') == 901))
            print(f"    After EQC/901 filter: {len(df)} rows")
        else:
            print(f"    Columns 'acctcode' or 'prisec' not found")
            return records
        
        for row in df.iter_rows(named=True):
            newic = row.get('newic', '') or ''
            if not newic or (len(str(newic)) >= 5 and str(newic)[:5] == '99999'):
                icno = f"{row.get('aliaskey', '') or ''}{row.get('custno', 0)}".replace(' ', '')
            else:
                icno = f"{row.get('aliaskey', '') or ''}{row.get('alias', '')}".replace(' ', '')
            
            records.append({
                'acctno': row.get('acctno'),
                'custno': row.get('custno'),
                'cisno': row.get('custno'),
                'cisname': row.get('custname', '') or '',
                'icno': icno
            })
        
        print(f"  CIS equity records: {len(records):,}")
        
    except Exception as e:
        print(f"  CIS equity warning: {e}")
        import traceback
        traceback.print_exc()
    
    return records

# =============================================================================
# CORE BANKING PROCESSING
# =============================================================================
def process_core_banking(rep_date):
    """Process core banking data: FD, SA, CA, FCYCA"""
    records = []
    
    try:
        for tbl in ['fd', 'sa', 'ca', 'fcyca']:
            file_pattern = f"{PATHS['lcr']}{tbl}*.sas7bdat"
            files = glob.glob(file_pattern)
            
            for filepath in files:
                df = read_sas_file(filepath)
                if df is None:
                    continue
                
                # Check for required columns
                if 'bnmcode' not in df.columns:
                    print(f"    Warning: 'bnmcode' column not found in {os.path.basename(filepath)}")
                    continue
                
                for row in df.iter_rows(named=True):
                    custcd = row.get('custcd', 0) or 0
                    if tbl == 'fd':
                        custcd = row.get('custcdx', 0) or 0
                    
                    cust = get_customer_category(custcd, cust_map)
                    
                    rem30d = row.get('rem30d', row.get('remmth', 1))
                    remmth = row.get('remmth', 1)
                    
                    if rem30d is None:
                        rem30d = remmth
                    
                    bic = str(row.get('bnmcode', '95311'))[:5]
                    
                    amount = row.get('amount', 0) or 0
                    
                    records.append({
                        'src': f'banking_{tbl}',
                        'bic': bic,
                        'bnmcode': f"{bic}{cust}020000Y",
                        'cmmcode': f"{bic}{cust}{format_mth_bucket(remmth)}0000Y",
                        'cur': str(row.get('curcode', 'MYR')).upper(),
                        'amt': amount,
                        'acctno': row.get('acctno', 0) or 0,
                        'custno': row.get('custno', 0) or 0,
                        'custcd': custcd,
                        'rem30d': rem30d,
                        'remmth': remmth,
                        'ecp': '00',
                        'product': row.get('product', 0) or 0,
                        'billerind': str(row.get('billerind', 'N')).upper(),
                        'pbmerch': str(row.get('pbmerch', 'N')).upper(),
                        'intrate': row.get('intrate', 0) or 0,
                        'oprrate': row.get('oprrate', 0) or 0,
                        'source': str(row.get('source', '') or ''),
                        'dtsigned': row.get('dtsigned'),
                        'intplan': row.get('intplan', 0) or 0,
                        'sme_tag': str(row.get('sme_tag', '') or ''),
                        'fdhold': str(row.get('fdhold', 'N')).upper(),
                        'trx': row.get('trx', 0) or 0,
                        'sign': ''
                    })
        
        print(f"  Banking records: {len(records):,}")
        
    except Exception as e:
        print(f"  Core banking warning: {e}")
        import traceback
        traceback.print_exc()
    
    return records

def process_cis_info():
    """Process CIS info from CISDP.DEPOSIT and CISCA.DEPOSIT"""
    records = {}
    try:
        for deptype in ['cisdp', 'cisca']:
            file_pattern = f"{PATHS[deptype]}*.sas7bdat"
            files = glob.glob(file_pattern)
            for filepath in files:
                df = read_sas_file(filepath)
                if df is not None:
                    # Check for seccust column
                    if 'seccust' in df.columns:
                        df = df.filter(pl.col('seccust') == 901)
                    for row in df.rows(named=True):
                        if row.get('acctno'):
                            records[row['acctno']] = row
    except Exception as e:
        print(f"  CIS info warning: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"  CIS info records: {len(records):,}")
    return records

def process_ecp():
    """Process LCR_ECP from LIST.LCR_ECP"""
    records = {}
    try:
        file_pattern = f"{PATHS['list']}lcr_ecp*.sas7bdat"
        files = glob.glob(file_pattern)
        for filepath in files:
            df = read_sas_file(filepath)
            if df is not None:
                if 'acctno' in df.columns and 'ecp' in df.columns:
                    for row in df.rows(named=True):
                        if row.get('acctno'):
                            records[row['acctno']] = row.get('ecp', '00')
                else:
                    print(f"    Warning: 'acctno' or 'ecp' column not found in {os.path.basename(filepath)}")
    except Exception as e:
        print(f"  ECP warning: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"  ECP records: {len(records):,}")
    return records

def read_walk_and_templ():
    """Read WALK.TXT and TEMPL.TXT files"""
    walk_records = []
    templ_records = []
    
    walk_files = glob.glob(f"{PATHS['list']}walk*.txt")
    if walk_files:
        walk_records = read_walk_file(walk_files[0])
    else:
        print(f"  No WALK.TXT files found in {PATHS['list']}")
    
    templ_files = glob.glob(f"{PATHS['list']}templ*.txt")
    if templ_files:
        templ_records = read_templ_file(templ_files[0])
    else:
        print(f"  No TEMPL.TXT files found in {PATHS['list']}")
    
    return walk_records, templ_records

# =============================================================================
# INSURED/UNINSURED SPLIT
# =============================================================================
def apply_insurance_split(records, walk_records, templ_records):
    """Split insured/uninsured portions for amounts > 250K"""
    result = []
    
    walk_dict = {r['acctno']: r for r in walk_records if r.get('acctno')}
    
    # Group by ICGRP to get totals
    icgrp_totals = {}
    for r in records:
        icgrp = r.get('icgrp', '') or ''
        if icgrp:
            icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + (r['amt'] or 0)
    
    for r in records:
        icgrp = r.get('icgrp', '') or ''
        toticbal = icgrp_totals.get(icgrp, 0)
        
        acctno = r.get('acctno')
        if acctno in walk_dict:
            r['walk_custno'] = walk_dict[acctno].get('custno')
        
        amt = r.get('amt', 0) or 0
        
        if toticbal > 250000 and r.get('bic') not in ['9531X']:
            curbal = amt
            insured_amt = (curbal / toticbal) * 250000
            uninsured_amt = curbal - insured_amt
            
            if r.get('bnmcode', '')[:2] in ['29', '39'] and r.get('ecp') != '01':
                r1 = r.copy()
                r1['amt'] = curbal
                r1['bnmcode'] = r['bnmcode'][:7] + '10' + r['bnmcode'][10:15]
                result.append(r1)
            else:
                r1 = r.copy()
                r1['amt'] = insured_amt
                result.append(r1)
                
                r2 = r.copy()
                r2['amt'] = uninsured_amt
                r2['bnmcode'] = r['bnmcode'][:7] + '10' + r['bnmcode'][10:15]
                result.append(r2)
        else:
            result.append(r)
    
    return result

# =============================================================================
# CONSOLIDATION AND REPORTING
# =============================================================================
def consolidate_data(all_records):
    """Consolidate all records into summary by BNMCODE"""
    if not all_records:
        return pl.DataFrame()
    
    df = pl.DataFrame(all_records)
    df = df.with_columns([
        (pl.col('amt') / 1000).round(2).alias('amt_k')
    ])
    
    summary = df.group_by(['bnmcode', 'cur']).agg([
        pl.col('amt_k').sum()
    ])
    
    return summary

def apply_column_mapping(row, is_banking):
    """Apply column mapping logic"""
    bnmcode = row['bnmcode']
    bic = bnmcode[:5]
    
    col_map = {
        '95311': 'fd95311rm',
        '95312': 'sa95312rm',
        '95313': 'ca95313rm',
        '95830': 'std95830',
        '95840': 'nid95840',
        '9x810': 'ibb9x810',
        '9x329': 'dci9x329',
        '95820': 'ibr95820',
        '95850': 'bap95850',
        '9531x': 'gld9531x'
    }
    colname = col_map.get(bic[:5].lower(), '')
    
    if is_banking:
        ecp = bnmcode[9:11]
        if bic.lower() in ['95313', '96313'] and ecp == '01':
            item = bnmcode[5:9]
        else:
            item = bnmcode[5:9]
        remmth = bnmcode[9:11]
    else:
        item = bnmcode[5:7]
        if bic == '95820':
            item = 'C1.11'
        remmth = bnmcode[7:9]
        orimth = bnmcode[9:11]
        if item == 'B3.30' and orimth == '02':
            item = 'B6.30'
    
    if colname[:3].lower() in ['fd9', 'std']:
        colname = f"{colname}{'1' if remmth == '1' else '2'}"
    elif colname[:3].lower() in ['nid', 'dci', 'ibb', 'ibr', 'bap']:
        for i in range(1, 7):
            if str(i) == remmth:
                colname = f"{colname}v{i}"
                break
    
    return item, colname, row['amt_k']

def write_text_report(report_data, rep_date):
    """Write report to text files"""
    if not report_data:
        print("  No report data to write")
        return
    
    output_dir = PATHS['output']
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    report_df = pl.DataFrame(report_data)
    final = report_df.group_by(['item', 'colname']).agg([
        pl.col('amount').sum()
    ])
    
    items = sorted(final['item'].unique().to_list())
    columns = sorted(final['colname'].unique().to_list())
    
    filename = f"lcr{rep_date['day']}.txt"
    filepath = f"{output_dir}{filename}"
    
    with open(filepath, 'w') as f:
        f.write("item\t" + "\t".join(columns) + "\n")
        for item in items:
            row_data = [item]
            for col in columns:
                mask = (final['item'] == item) & (final['colname'] == col)
                if mask.any():
                    amount = final.filter(mask)['amount'].sum()
                    row_data.append(f"{amount:.2f}")
                else:
                    row_data.append("0.00")
            f.write("\t".join(row_data) + "\n")
    
    print(f"  ✓ {filename}: {len(items)} items x {len(columns)} columns")

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBDLCRM - BNM LCR Reporting (Conventional Banking)")
    print("=" * 60)
    print("\nNOTE: KALMLIQ logic integrated directly")
    print("      - Reading from BNMK.K1TBL{mon}{week} and BNMK.K3TBL{mon}{week}")
    print("      - Using hardcoded FX rates")
    print("=" * 60)
    
    rep_date = get_report_date()
    print(f"\nReport Date: {rep_date['date'].strftime('%d/%m/%Y')}")
    print(f"Week: {rep_date['nowk']}, Month: {rep_date['mon']}")
    
    # Load all inputs
    print("\n" + "=" * 60)
    print("LOADING INPUTS")
    print("=" * 60)
    
    # 1. FX rates - HARDCODED
    print("\n1. FX Rates (HARDCODED)...")
    print(f"  Loaded {len(FX_RATES)} currencies")
    
    # 2. WALK.TXT and TEMPL.TXT
    print("\n2. Loading WALK.TXT and TEMPL.TXT...")
    walk_records, templ_records = read_walk_and_templ()
    print(f"  WALK: {len(walk_records)} records")
    print(f"  TEMPL: {len(templ_records)} records")
    
    # 3. KALMLIQ - Process K1TBL and K3TBL
    print("\n3. Processing KALMLIQ (K1TBL and K3TBL)...")
    k1_records = process_k1tbl(rep_date)
    print(f"  K1TBL records: {len(k1_records):,}")
    k3_records = process_k3tbl(rep_date)
    print(f"  K3TBL records: {len(k3_records):,}")
    
    # Build KTBLALL
    treasury_records = build_ktblall(k1_records, k3_records, rep_date)
    print(f"  Total treasury records: {len(treasury_records):,}")
    
    # 4. DCIWH.DCID - DCI data
    print("\n4. Processing DCIWH.DCID...")
    dci_records = process_dci(rep_date)
    print(f"  DCI records: {len(dci_records):,}")
    
    # 5. CIS.CUSTDLY - CIS equity (parquet)
    print("\n5. Processing CIS.CUSTDLY (parquet)...")
    cis_records = process_cis_equity()
    cis_dict = {r['acctno']: r for r in cis_records if r.get('acctno')}
    print(f"  CIS records: {len(cis_dict):,}")
    
    # 6. EQUA.UTMS/UTFX/UTRP - UTSAS data
    print("\n6. Processing EQUA.UTMS/UTFX/UTRP...")
    utsas_records = process_utsas(rep_date)
    utsas_dict = {r['dealref']: r for r in utsas_records if r.get('dealref')}
    print(f"  UTSAS records: {len(utsas_dict):,}")
    
    # 7. LCR.FD/SA/CA/FCYCA - Core banking
    print("\n7. Processing LCR.FD/SA/CA/FCYCA...")
    banking_records = process_core_banking(rep_date)
    print(f"  Banking records: {len(banking_records):,}")
    
    # 8. CISDP/CISCA.DEPOSIT - CIS info
    print("\n8. Processing CISDP/CISCA.DEPOSIT...")
    cis_info_dict = process_cis_info()
    print(f"  CIS info records: {len(cis_info_dict):,}")
    
    # 9. LIST.LCR_ECP - ECP list
    print("\n9. Processing LIST.LCR_ECP...")
    ecp_dict = process_ecp()
    print(f"  ECP records: {len(ecp_dict):,}")
    
    print("\n" + "=" * 60)
    print("PROCESSING DATA")
    print("=" * 60)
    
    # Combine treasury and DCI
    all_treasury = treasury_records + dci_records
    print(f"\nCombined treasury + DCI: {len(all_treasury):,} records")
    
    # Apply UTSAS and CIS to treasury
    enhanced_treasury = []
    for r in all_treasury:
        dealref = r.get('dealref')
        if dealref and dealref in utsas_dict:
            ut = utsas_dict[dealref]
            r.update(ut)
        
        acctno = r.get('acctno') or r.get('custeqno')
        if acctno and acctno in cis_dict:
            ci = cis_dict[acctno]
            r['cisno'] = ci.get('cisno')
            r['cisname'] = ci.get('cisname')
            r['icno'] = ci.get('icno')
        
        custfiss = r.get('custfiss', 0) or 0
        if custfiss:
            try:
                custfiss = int(custfiss)
            except:
                custfiss = 0
        
        custno = r.get('custno', '') or ''
        cust = get_customer_category(custfiss, cust_map, special_cust, 
                                     is_custno=(custno in special_cust.get('39', [])))
        
        bic = r['bnmcode'][:5]
        if bic == '95830' and r.get('dealtype') in ['BCQ', 'BCT', 'BCW']:
            bic = '9583X'
        
        rem30d = r.get('rem30d', r.get('remmth', 1))
        remmth = r.get('remmth', 1)
        
        if rem30d is None:
            rem30d = remmth
        
        bnmcode = f"{bic}{cust}{format_day_bucket(rem30d)}0000Y"
        cmmcode = f"{bic}{cust}{format_mth_bucket(remmth)}0000Y"
        
        if custno in special_cust.get('49', []) and cust == '49' and bic in ['95840', '96840']:
            ori30d = r.get('ori30d', 0) or 0
            if format_day_bucket(ori30d) > '05' and format_day_bucket(rem30d) > '01':
                bnmcode = bnmcode[:9] + '0200Y'
        
        icgrp = r.get('custid', r.get('icno', '')).replace(' ', '') or ''
        
        enhanced_treasury.append({
            'src': r['src'],
            'bic': bic,
            'bnmcode': bnmcode,
            'cmmcode': cmmcode,
            'cur': r.get('cur', 'MYR'),
            'amt': r.get('amt', 0) or 0,
            'dealref': dealref,
            'custno': custno,
            'icgrp': icgrp,
            'rem30d': rem30d,
            'remmth': remmth,
            'acctno': acctno,
            'ori30d': r.get('ori30d', 0) or 0
        })
    
    print(f"Enhanced treasury: {len(enhanced_treasury):,} records")
    
    # Process Core Banking with CIS info and ECP
    enhanced_banking = []
    for r in banking_records:
        acctno = r.get('acctno')
        
        if acctno and acctno in cis_info_dict:
            ci = cis_info_dict[acctno]
            r['newic'] = ci.get('newic')
            r['oldic'] = ci.get('oldic')
            r['custname'] = ci.get('custname')
        
        if acctno and acctno in ecp_dict:
            r['ecp'] = ecp_dict[acctno]
        
        if r['ecp'] == '':
            r['ecp'] = '00'
        if r['ecp'] == '01':
            if r['intrate'] < r['oprrate']:
                r['ecp'] = '01'
            else:
                r['ecp'] = '00'
        if r['billerind'] == 'Y' or r['pbmerch'] == 'Y':
            r['ecp'] = '01'
        
        product_list = [106, 151, 158, 97, 164, 201, 215]
        intplan_ranges = list(range(400,420)) + list(range(600,659)) + \
                         list(range(720,741)) + list(range(864,891)) + \
                         list(range(941,968))
        
        if (r['product'] in product_list or 
            r['intplan'] in intplan_ranges or
            (r['source'] != 'PGD' and r['dtsigned'] and 
             r['dtsigned'] > 0 and 
             (rep_date['date'] - r['dtsigned']).days >= 365)):
            r['sign'] = 'R '
        
        special_39 = [4391161,2115999,12579649,13468207,14300254,
                     14675929,15327497,17104931,12677444,3703533,
                     5978659,16185090,2558344,10819745]
        
        if r['custno'] in special_39:
            r['cust'] = '39'
        
        if r['cur'] == 'XAU':
            r['bic'] = '9531X'
            r['bnmcode'] = f"9531X{r['cust']}100000Y"
            r['cmmcode'] = f"9531X{r['cust']}{format_mth_bucket(r['remmth'])}0000Y"
            r['amt'] = r['amt'] * FX_RATES.get('XAU', 200.0)
            r['cur'] = 'MYR'
        
        # Ensure amount is numeric
        r['amt'] = r.get('amt', 0) or 0
        
        enhanced_banking.append(r)
    
    print(f"Enhanced banking: {len(enhanced_banking):,} records")
    
    # Calculate ICGRP totals for banking
    icgrp_totals = {}
    for r in enhanced_banking:
        icgrp = r.get('newic', r.get('oldic', '')).replace(' ', '') or ''
        r['icgrp'] = icgrp
        icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + (r['amt'] or 0)
    
    exclude_cust = [14094942,16557696,3728510,11335374,16265490,
                    3523050,11880426,16771972,15241330,16500538]
    
    for r in enhanced_banking:
        icgrp = r.get('icgrp', '') or ''
        toticbal = icgrp_totals.get(icgrp, 0)
        r['toticbal'] = toticbal
        
        if (r['custno'] not in exclude_cust and r['bnmcode'][5:7] == '29') or r['custcd'] in [72,73,74]:
            totdpbal = toticbal + 0
            if totdpbal < 5000000:
                r['bnmcode'] = f"{r['bic']}19{r['bnmcode'][7:]}"
                r['cmmcode'] = f"{r['bic']}19{r['cmmcode'][7:]}"
        elif r['bnmcode'][5:7] == '19' and r.get('sme_tag') == 'N':
            totdpbal = toticbal + 0
            if totdpbal >= 5000000:
                r['bnmcode'] = f"{r['bic']}29{r['bnmcode'][7:]}"
                r['cmmcode'] = f"{r['bic']}29{r['cmmcode'][7:]}"
        
        if r['bnmcode'][5:7] in ['08', '19'] and r['bic'] != '9531X':
            if r.get('trx') == 1:
                tag = '01'
            elif r.get('sign') in ['R', 'R ']:
                tag = '02'
            else:
                tag = '03'
            r['bnmcode'] = r['bnmcode'][:7] + tag + '0000Y'
        
        if r['bic'] in ['95313', '96313']:
            r['bnmcode'] = r['bnmcode'][:9] + r['ecp'] + '00Y'
            r['cmmcode'] = r['cmmcode'][:9] + r['ecp'] + '00Y'
    
    print("\nApplying insurance split...")
    banking_split = apply_insurance_split(enhanced_banking, walk_records, templ_records)
    print(f"Banking after insurance split: {len(banking_split):,} records")
    
    # Combine all sources
    all_data = enhanced_treasury + banking_split
    print(f"\nTotal records before consolidation: {len(all_data):,}")
    
    print("\nConsolidating...")
    summary = consolidate_data(all_data)
    print(f"  Consolidated to {len(summary):,} BNM code x currency combinations")
    
    print("\nGenerating LCR report (text format)...")
    report_data = []
    for row in summary.rows(named=True):
        is_banking = row['bnmcode'][5] != '9'
        item, colname, amount = apply_column_mapping(row, is_banking)
        report_data.append({
            'item': item,
            'colname': colname,
            'amount': amount,
            'cur': row['cur']
        })
    
    if report_data:
        write_text_report(report_data, rep_date)
    else:
        print("  No report data to write")
    
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if all_data:
        df_all = pl.DataFrame(all_data)
        total = df_all['amt'].sum() / 1000
        by_src = df_all.group_by('src').agg([(pl.col('amt').sum() / 1000).alias('amt_k')])
        
        print(f"\nTotal: RM {total:,.0f}K")
        print(f"\nBy Source:")
        for row in by_src.sort('amt_k', descending=True).iter_rows():
            print(f"  {row[0]}: RM {row[1]:,.0f}K")
    else:
        print("\n  No data processed!")
    
    print("\n" + "=" * 60)
    print("✓ EIBDLCRM Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
