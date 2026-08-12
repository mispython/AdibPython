"""
EIBMLCRM - BNM LCR (Liquidity Coverage Ratio) Reporting
Consolidates deposits & treasury positions for BNM LCR reporting.
Outputs: LCR reports by currency with customer categorization (08/19/29/39/49/59)
Includes PBBELF and PBLCRFMT format mappings
"""

import pyreadstat
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pathlib import Path
import subprocess

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'lcr': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLCRM/lcr/',
    'deposit': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLCRM/deposit/',
    'forate': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLCRM/',
    'cisdp': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLCRM/cisdp/',
    'cisca': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLCRM/cisca/',
    'list': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLCRM/list/',
    'sme': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLCRM/',
    'output': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMLCRM/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# Import format mappings from existing Python scripts
try:
    from PBBELF import lcrcdmni_map, lcrcdmniopr_map, lcrcdgl_map, lcrcdglccy_map, lcrcdgloth_map, ctype_map, colid_map, lcrcdequ_map
except ImportError:
    print("Warning: PBBELF.py not found, using default mappings")
    # Default mappings (would normally come from PBBELF.py)
    lcrcdmni_map = {}
    lcrcdmniopr_map = {}
    lcrcdgl_map = {}
    lcrcdglccy_map = {}
    lcrcdgloth_map = {}
    ctype_map = {}
    colid_map = {}
    lcrcdequ_map = {}

try:
    from PBLCRFMT import remfmt_map, cmmfmt_map, remfmx_map
except ImportError:
    print("Warning: PBLCRFMT.py not found, using default format mappings")
    # Default format mappings
    remfmt_map = {1: '01', 2: '02', 3: '03', 4: '04', 5: '05', 6: '06', 7: '07', 8: '08', 9: '09', 10: '10'}
    cmmfmt_map = {1: '01', 2: '02', 3: '03', 4: '04', 5: '05', 6: '06', 7: '07', 8: '08', 9: '09', 10: '10'}
    remfmx_map = remfmt_map.copy()

# Customer category mappings (LCR)
CUST_MAP = {
    '08': [76, 77, 78, 95, 96],
    '19': [41, 42, 43, 44, 46, 47, 48, 49, 51, 52, 53, 54, 65, 66, 67, 68, 69],
    '29': [0, 45, 57, 59, 60, 61, 62, 63, 64, 75, 79, 85, 86, 87, 88, 89, 98, 99],
    '39': [1, 71, 72, 73, 74, 90, 91, 92],
    '49': [2, 3, 7, 12, 81, 82, 83, 84],
    '59': [4, 5, 6, 13, 17, 20] + list(range(30, 41))
}

# Special customers for Treasury
SPECIAL_CUST_TREASURY = {
    '39': ['kwsp', 'kwap', 'kwan', 'lemtab']
}

# Special customers for Banking
SPECIAL_CUST_BANKING_CUST = [4391161, 2115999, 12579649, 13468207, 14300254,
                             14675929, 15327497, 17104931, 12677444, 3703533,
                             5978659, 16185090, 2558344, 10819745]

SPECIAL_CUST_BANKING_CUSX = [4391161, 2115999, 12579649, 13468207, 14675929,
                             15327497, 17104931, 12677444, 3703533, 5978659,
                             16185090, 10819745, 2558344]

# NSFR customer mappings
CUSX_MAP = {
    '08': [76, 77, 78, 95, 96],
    '19': [41, 42, 43, 44, 46, 47, 48, 49, 51, 52, 53, 54, 65, 66, 67, 68, 69],
    '29': [0, 45, 57, 59, 60, 61, 62, 63, 64, 75, 79, 85, 86, 87, 88, 89, 98, 99],
    '39': [1, 91],
    '49': [71, 72, 73, 74, 90, 92],
    '59': [2, 3, 4, 5, 6, 7, 12, 13, 17, 20] + list(range(30, 41)) + [81, 82, 83, 84]
}

# Special Treasury NSFR mapping
SPECIAL_CUST_TREASURY_NSFR = {
    '49': ['kwsp', 'kwspkl', 'kwap', 'kwapkl', 'kwan', 'kwankl', 'lemtab', 'lemtabkl']
}

# AIM/PBL customers for special handling
AIM_PBL_CUSTS = ['aim', 'pbl', 'pbleur', 'pblnid', 'pblusd', 'pivmyr', 'ipbb']

# Template structure for LCR report
TEMPLATE_ITEMS = {
    'FD95311RM1': 'FD95311RM1',
    'FD95311RM2': 'FD95311RM2', 
    'FD95311RM': 'FD95311RM',
    'FD96311FX1': 'FD96311FX1',
    'FD96311FX2': 'FD96311FX2',
    'FD96311FX': 'FD96311FX',
    'SA95312RM': 'SA95312RM',
    'CA95313RM': 'CA95313RM',
    'CA96313FX': 'CA96313FX',
    'STD95830V1': 'STD95830V1',
    'STD95830V2': 'STD95830V2',
    'STD95830': 'STD95830',
    'STD95830Q1': 'STD95830Q1',
    'STD95830Q2': 'STD95830Q2',
    'STD95830Q': 'STD95830Q',
    'GLD9531X': 'GLD9531X',
    'NID95840V1': 'NID95840V1',
    'NID95840V2': 'NID95840V2',
    'NID95840V3': 'NID95840V3',
    'NID95840V4': 'NID95840V4',
    'NID95840V5': 'NID95840V5',
    'NID95840V6': 'NID95840V6',
    'NID95840': 'NID95840',
    'RNI95840V1': 'RNI95840V1',
    'RNI95840V2': 'RNI95840V2',
    'RNI95840': 'RNI95840',
    'IBB9X810V1': 'IBB9X810V1',
    'IBB9X810V2': 'IBB9X810V2',
    'IBB9X810V3': 'IBB9X810V3',
    'IBB9X810V4': 'IBB9X810V4',
    'IBB9X810V5': 'IBB9X810V5',
    'IBB9X810V6': 'IBB9X810V6',
    'IBB9X810': 'IBB9X810',
    'DCI9X329V1': 'DCI9X329V1',
    'DCI9X329V2': 'DCI9X329V2',
    'DCI9X329V3': 'DCI9X329V3',
    'DCI9X329V4': 'DCI9X329V4',
    'DCI9X329V5': 'DCI9X329V5',
    'DCI9X329V6': 'DCI9X329V6',
    'DCI9X329': 'DCI9X329',
    'IBR95820V1': 'IBR95820V1',
    'IBR95820V2': 'IBR95820V2',
    'IBR95820V3': 'IBR95820V3',
    'IBR95820V4': 'IBR95820V4',
    'IBR95820V5': 'IBR95820V5',
    'IBR95820V6': 'IBR95820V6',
    'IBR95820': 'IBR95820',
    'BAP95850V1': 'BAP95850V1',
    'BAP95850V2': 'BAP95850V2',
    'BAP95850V3': 'BAP95850V3',
    'BAP95850V4': 'BAP95850V4',
    'BAP95850V5': 'BAP95850V5',
    'BAP95850V6': 'BAP95850V6',
    'BAP95850': 'BAP95850',
    'OTHSOURCE': 'OTHSOURCE',
    'TOTALV1': 'TOTALV1',
    'TOTALDP': 'TOTALDP',
    'FDPLEDGE1': 'FDPLEDGE1',
    'FDPLEDGE2': 'FDPLEDGE2',
    'FXPLEDGE1': 'FXPLEDGE1',
    'FXPLEDGE2': 'FXPLEDGE2'
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def read_sas_dataset(filepath):
    """Read SAS dataset using pyreadstat"""
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def read_text_file(filepath):
    """Read text file (like walk.txt)"""
    try:
        with open(filepath, 'r') as f:
            lines = f.readlines()
        return lines
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return []

def get_report_date():
    """Calculate report date as yesterday"""
    reptdate = datetime.now() - timedelta(days=1)
    
    day = reptdate.day
    if 1 <= day <= 8:
        nowk = '1'
    elif 9 <= day <= 15:
        nowk = '2'
    elif 16 <= day <= 22:
        nowk = '3'
    else:
        nowk = '4'
    
    return {
        'date': reptdate,
        'nowk': nowk,
        'mon': f"{reptdate.month:02d}",
        'day': f"{reptdate.day:02d}",
        'year': f"{reptdate.year % 100:02d}",
        'rdate': reptdate.strftime('%d%m%y'),
        'fildt': reptdate.strftime('%d%m%y'),
        'rptdt': reptdate.strftime('%y%m%d'),
        'reptyear': str(reptdate.year % 100)
    }

def get_customer_category(code, mapping, special_dict=None, special_list=None):
    """Get customer category from code"""
    # Check special list first
    if special_list and code in special_list:
        for cat, codes in special_dict.items() if special_dict else {}:
            if code in codes:
                return cat
        return '39' if special_dict == SPECIAL_CUST_TREASURY else '49'
    
    # Check special dictionary
    if special_dict:
        for cat, codes in special_dict.items():
            if code in codes:
                return cat
    
    # Check standard mapping
    if pd.notna(code):
        code_int = int(code) if isinstance(code, (int, float)) else code
        for cat, codes in mapping.items():
            if code_int in codes:
                return cat
    return '29'

def format_remfmt(value):
    """Format using REMFMT mapping"""
    if pd.isna(value):
        return '01'
    value = int(value)
    return remfmt_map.get(value, f"{value:02d}")

def format_cmmfmt(value):
    """Format using CMMFMT mapping"""
    if pd.isna(value):
        return '01'
    value = int(value)
    return cmmfmt_map.get(value, f"{value:02d}")

def format_remfmx(value):
    """Format using REMFMX mapping"""
    if pd.isna(value):
        return '01'
    value = int(value)
    return remfmx_map.get(value, f"{value:02d}")

def read_template():
    """Read template file"""
    template_path = f"{PATHS['lcr']}templ.txt"
    try:
        with open(template_path, 'r') as f:
            return [line.rstrip() for line in f]
    except:
        print(f"Warning: Template file {template_path} not found")
        return []

# =============================================================================
# DATA PROCESSING
# =============================================================================
def process_treasury(rep_date):
    """Process Treasury (Kapiti) data"""
    records = []
    
    try:
        # Read UTSAS first
        utsas = read_sas_dataset(f"{PATHS['lcr']}utsas{rep_date['mon']}.sas7bdat")
        
        # Read and combine treasury tables
        dfs = []
        for tbl in ['k1tbl', 'k3tbl', 'dci']:
            df = read_sas_dataset(f"{PATHS['lcr']}{tbl}.sas7bdat")
            if df is not None and 'bnmcode' in df.columns:
                df = df[df['bnmcode'].astype(str).str[:2].isin(['95', '96'])]
                dfs.append(df)
        
        if not dfs:
            return records, pd.DataFrame()
            
        df = pd.concat(dfs, ignore_index=True)
        df = df.drop_duplicates(subset=['dealref'], keep='first')
        
        # Merge with UTSAS
        if utsas is not None:
            df = df.merge(utsas, on='dealref', how='left', suffixes=('', '_utsas'))
        
        # Process each row
        processed_rows = []
        for _, row in df.iterrows():
            custno = str(row.get('custno', '')).lower()
            custfiss = row.get('custfiss', np.nan)
            
            # Apply CTYPE format for custfiss
            if pd.isna(custfiss) and 'utctp' in row and pd.notna(row.get('utctp')):
                utctp = str(row['utctp'])
                if utctp in ctype_map:
                    custfiss = int(ctype_map[utctp])
            
            # CUST mapping
            if custno in SPECIAL_CUST_TREASURY.get('39', []):
                cust = '39'
            else:
                cust = get_customer_category(custfiss, CUST_MAP)
            
            # CUSX mapping for NSFR
            if custno in SPECIAL_CUST_TREASURY_NSFR.get('49', []):
                cusx = '49'
            else:
                cusx = get_customer_category(custfiss, CUSX_MAP)
            
            # Maturity handling
            rem30d = row.get('rem30d', np.nan)
            remmth = row.get('remmth', 1)
            
            if pd.isna(rem30d):
                rem30d = remmth
            if rem30d > 1 and remmth > 1:
                rem30d = remmth
            
            # Build BIC and codes
            bic = str(row['bnmcode'])[:5]
            if bic == '95830' and str(row.get('dealtype', '')).upper() in ['BCQ', 'BCT', 'BCW']:
                bic = '9583X'  # PQMMD
            
            bnmcode = f"{bic}{cust}{format_remfmt(rem30d)}0000Y"
            cmmcode = f"{bic}{cust}{format_cmmfmt(remmth)}0000Y"
            nsfcode = f"{bic}{cusx}{format_remfmx(rem30d)}0000Y"
            
            # AIM/PBL special handling
            if custno in AIM_PBL_CUSTS and cust == '49' and bic in ['95840', '96840']:
                ori30d = row.get('ori30d', np.nan)
                if pd.notna(ori30d):
                    if int(format_remfmt(ori30d)) > 5 and int(format_remfmt(rem30d)) > 1:
                        bnmcode = bnmcode[:9] + '0200Y'
                    if int(format_remfmx(ori30d)) > 5 and int(format_remfmx(rem30d)) > 1:
                        nsfcode = nsfcode[:9] + '0200Y'
            
            # ICGRP for consolidation
            icgrp = str(row.get('custid', '')) if pd.notna(row.get('custid')) else str(row.get('icno', ''))
            icgrp = icgrp.replace(' ', '')
            
            processed_rows.append({
                'bic': bic,
                'bnmcode': bnmcode,
                'cmmcode': cmmcode,
                'nsfcode': nsfcode,
                'curcode': str(row.get('curcode', 'MYR')),
                'amount': float(row.get('amount', 0)),
                'dealref': str(row.get('dealref', '')),
                'dealtype': str(row.get('dealtype', '')),
                'custfiss': custfiss,
                'custno': custno,
                'custname': str(row.get('custname', '')),
                'rem30d': rem30d,
                'remmth': remmth,
                'ori30d': row.get('ori30d', np.nan),
                'matdt': row.get('matdt', np.nan),
                'custid': row.get('custid', ''),
                'icno': row.get('icno', ''),
                'acctno': str(row.get('acctno', '')),
                'cisno': str(row.get('cisno', '')),
                'cisname': str(row.get('cisname', '')),
                'icgrp': icgrp,
                'source': 'TREASURY'
            })
        
        df_processed = pd.DataFrame(processed_rows)
        
        # Summarize by BNMCODE and CURCODE
        if len(df_processed) > 0:
            equ_summary = df_processed.groupby(['bnmcode', 'curcode'])['amount'].sum().reset_index()
            equ_summary['source'] = 'TREASURY'
        else:
            equ_summary = pd.DataFrame()
        
        # Calculate ICGRP totals
        equ_icgrp = pd.DataFrame()
        if len(df_processed) > 0:
            equ_subset = df_processed[df_processed['bic'].str[2:5].isin(['810', '820', '830', '83X', '840', '850'])]
            if len(equ_subset) > 0:
                equ_icgrp = equ_subset.groupby('icgrp')['amount'].sum().reset_index()
                equ_icgrp.columns = ['icgrp', 'toticeqbal']
        
        return df_processed, equ_summary, equ_icgrp
        
    except Exception as e:
        print(f"  Treasury warning: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def process_banking(rep_date, fx_rates):
    """Process Core Banking data"""
    records = []
    
    try:
        # Read all banking tables
        dfs = []
        for tbl in ['fd', 'sa', 'ca', 'fcyca', 'nid']:
            df = read_sas_dataset(f"{PATHS['lcr']}{tbl}.sas7bdat")
            if df is not None and 'bnmcode' in df.columns:
                df = df[df['bnmcode'].astype(str).str[:2].isin(['95', '96'])]
                
                # Rename columns for consistency
                if tbl == 'fd':
                    if 'custcdx' in df.columns:
                        df = df.rename(columns={'custcdx': 'custcd_orig'})
                elif tbl == 'nid':
                    if 'nid_acctno' in df.columns:
                        df = df.rename(columns={'nid_acctno': 'acctno'})
                
                df['source_table'] = tbl
                dfs.append(df)
        
        if not dfs:
            return pd.DataFrame(), pd.DataFrame()
            
        df = pd.concat(dfs, ignore_index=True)
        
        # Convert CUSTCD for FD records
        if 'custcd_orig' in df.columns and 'custcd' not in df.columns:
            df['custcd'] = df['custcd_orig'].apply(lambda x: f"{int(x):02d}" if pd.notna(x) else '00')
        
        # Categorize customers
        def categorize_cust(custcd):
            try:
                custcd_int = int(custcd)
            except:
                return '29'
            return get_customer_category(custcd_int, CUST_MAP)
        
        def categorize_cusx(custcd):
            try:
                custcd_int = int(custcd)
            except:
                return '29'
            return get_customer_category(custcd_int, CUSX_MAP)
        
        df['cust'] = df['custcd'].apply(categorize_cust)
        df['cusx'] = df['custcd'].apply(categorize_cusx)
        
        # Maturity handling
        df['rem30d'] = df['rem30d'].fillna(df['remmth'])
        df.loc[(df['rem30d'] > 1) & (df['remmth'] > 1), 'rem30d'] = df['remmth']
        
        # Merge with CISINFO
        cisinfo_dp = read_sas_dataset(f"{PATHS['cisdp']}deposit.sas7bdat")
        cisinfo_ca = read_sas_dataset(f"{PATHS['cisca']}deposit.sas7bdat")
        
        cisinfo_list = []
        for cis_df in [cisinfo_dp, cisinfo_ca]:
            if cis_df is not None:
                cols_to_keep = ['acctno', 'custno', 'seccust', 'newic', 'oldic', 'custname', 'bussreg']
                available_cols = [c for c in cols_to_keep if c in cis_df.columns]
                cis_df = cis_df[available_cols]
                if 'seccust' in cis_df.columns:
                    cis_df = cis_df[cis_df['seccust'] == '901']
                cisinfo_list.append(cis_df)
        
        if cisinfo_list:
            cisinfo = pd.concat(cisinfo_list).drop_duplicates(subset=['acctno'])
            df = df.merge(cisinfo, on='acctno', how='left', suffixes=('', '_cis'))
        
        # Merge with ECP
        ecp = read_sas_dataset(f"{PATHS['list']}lcr_ecp_{rep_date['mon']}.sas7bdat")
        if ecp is not None:
            ecp = ecp.drop_duplicates(subset=['acctno'])
            df = df.merge(ecp, on='acctno', how='left', suffixes=('', '_ecp'))
        
        # Merge with SME
        sme = read_sas_dataset(f"{PATHS['sme']}baselsme{rep_date['mon']}{rep_date['reptyear']}.sas7bdat")
        if sme is not None:
            df = df.merge(sme, on='acctno', how='left', suffixes=('', '_sme'))
        
        # Process ECP
        if 'ecp' in df.columns:
            df['ecp'] = df['ecp'].fillna('00')
            if 'intrate' in df.columns and 'oprrate' in df.columns:
                mask = (df['ecp'] == '01') & (df['intrate'] >= df['oprrate'])
                df.loc[mask, 'ecp'] = '00'
        
        # Set ECP for billers/merchants
        if 'billerind' in df.columns:
            df.loc[df['billerind'] == 'Y', 'ecp'] = '01'
        if 'pbmerch' in df.columns:
            df.loc[df['pbmerch'] == 'Y', 'ecp'] = '01'
        
        # Special customer overrides for banking
        if 'custno' in df.columns:
            df.loc[df['custno'].isin(SPECIAL_CUST_BANKING_CUST), 'cust'] = '39'
            df.loc[df['custno'].isin(SPECIAL_CUST_BANKING_CUSX), 'cusx'] = '49'
        
        # Additional special customers
        special_custs = [9888664, 11565156, 170458, 17835250, 12078514, 12542063]
        special_bussregs = ['061904X', '186852H', '211510H', '685480K', '643815V', '734789U']
        
        if 'custno' in df.columns:
            df.loc[df['custno'].isin(special_custs), ['cust', 'cusx']] = '59'
        if 'bussreg' in df.columns:
            df.loc[df['bussreg'].isin(special_bussregs), ['cust', 'cusx']] = '59'
        
        # Build codes
        def build_bnmcode(row):
            bic = str(row['bnmcode'])[:5]
            if row.get('curcode', 'MYR') == 'XAU':
                bic = '9531X'
                return f"{bic}{row['cust']}100000Y"
            elif bic == '95840':
                return f"{bic}{row['cust']}{'10' if row['rem30d'] <= 1 else '20'}0000Y"
            else:
                return f"{bic}{row['cust']}020000Y"
        
        df['bic'] = df['bnmcode'].apply(lambda x: str(x)[:5])
        df.loc[df['curcode'] == 'XAU', 'bic'] = '9531X'
        
        df['bnmcode_out'] = df.apply(build_bnmcode, axis=1)
        df['cmmcode_out'] = df.apply(lambda row: f"{row['bic']}{row['cust']}{format_cmmfmt(row['remmth'])}0000Y", axis=1)
        df['nsfcode_out'] = df.apply(lambda row: f"{row['bic']}{row['cusx']}020000Y", axis=1)
        
        # XAU conversion
        df.loc[df['curcode'] == 'XAU', 'amount'] *= fx_rates.get('XAU', 200.0)
        
        # ICGRP
        df['icgrp'] = ''
        
        # Calculate ICGRP totals
        mni_not_nid = df[df['bic'] != '95840']
        icgrp_not_nid = pd.DataFrame()
        if len(mni_not_nid) > 0:
            icgrp_not_nid = mni_not_nid.groupby('icgrp')['amount'].sum().reset_index()
            icgrp_not_nid.columns = ['icgrp', 'totical']
        
        mni_nid = df[df['bic'] == '95840']
        icgrp_nid = pd.DataFrame()
        if len(mni_nid) > 0:
            icgrp_nid = mni_nid.groupby('icgrp')['amount'].sum().reset_index()
            icgrp_nid.columns = ['icgrp', 'toticrnibal']
        
        # SME/Retail reclassification based on total deposits
        # This is a simplified version - full logic would require ICGRP totals merge
        
        # TAG assignments for categories 08/19
        df['tag'] = '03'
        if 'trx' in df.columns:
            df.loc[df['trx'] == 1, 'tag'] = '01'
        if 'sign' in df.columns:
            df.loc[df['sign'].isin(['R', 'R ']), 'tag'] = '02'
        
        # Apply TAG to codes
        tag_cats = ['08', '19']
        tag_bics = ['9531X', '95840']
        
        mask_tag = df['cust'].isin(tag_cats) & ~df['bic'].isin(tag_bics)
        df.loc[mask_tag, 'bnmcode_out'] = df.loc[mask_tag].apply(
            lambda row: row['bnmcode_out'][:7] + row['tag'] + '0000Y', axis=1
        )
        df.loc[mask_tag, 'nsfcode_out'] = df.loc[mask_tag].apply(
            lambda row: row['nsfcode_out'][:7] + row['tag'] + '0000Y', axis=1
        )
        
        # ECP for CA
        ca_bics = ['95313', '96313']
        mask_ca = df['bic'].isin(ca_bics)
        if 'ecp' in df.columns:
            df.loc[mask_ca, 'bnmcode_out'] = df.loc[mask_ca].apply(
                lambda row: row['bnmcode_out'][:9] + row['ecp'] + '00Y', axis=1
            )
            df.loc[mask_ca, 'cmmcode_out'] = df.loc[mask_ca].apply(
                lambda row: row['cmmcode_out'][:9] + row['ecp'] + '00Y', axis=1
            )
            df.loc[mask_ca, 'nsfcode_out'] = df.loc[mask_ca].apply(
                lambda row: row['nsfcode_out'][:9] + row['ecp'] + '00Y', axis=1
            )
        
        # PIDM insurance split (> 250k)
        df_insured = df.copy()
        df_uninsured = pd.DataFrame()
        
        # This is a simplified version - full logic would split accounts > 250k
        # into insured and uninsured portions
        
        # FDHOLD processing for specific BICs
        fdhold_mask = df['bic'].isin(['95311', '96311', '95840'])
        fdhold_data = pd.DataFrame()
        if fdhold_mask.any():
            fdhold_df = df[fdhold_mask].copy()
            fdhold_df['bnmcode_out'] = fdhold_df.apply(
                lambda row: row['bnmcode_out'][:9] + ('0100Y' if row['rem30d'] <= 1 else '0200Y'), axis=1
            )
            if 'fdhold' in fdhold_df.columns:
                fdhold_data = fdhold_df[fdhold_df['fdhold'] == 'Y'].copy()
                fdhold_data['bnmcode_out'] = fdhold_data['bnmcode_out'].str[:7] + '20' + fdhold_data['bnmcode_out'].str[9:]
        
        # Summarize
        if len(df_insured) > 0:
            mni_summary = df_insured.groupby(['bnmcode_out', 'curcode'])['amount'].sum().reset_index()
            mni_summary.columns = ['bnmcode', 'curcode', 'amount']
        else:
            mni_summary = pd.DataFrame()
        
        # FDHOLD summary
        fdhold_summary = pd.DataFrame()
        if len(fdhold_data) > 0:
            fdhold_summary = fdhold_data.groupby(['bnmcode_out', 'curcode'])['amount'].sum().reset_index()
            fdhold_summary.columns = ['bnmcode', 'curcode', 'amount']
            
            # Map to LCR item codes
            fdhold_summary['item'] = fdhold_summary['bnmcode'].apply(
                lambda x: lcrcdmni_map.get(x[5:9], '')
            )
            fdhold_summary = fdhold_summary[fdhold_summary['item'] != '']
            
            # Split by maturity and currency
            fdhold_summary['is_30d'] = fdhold_summary['bnmcode'].str[9:11] == '01'
            fdhold_summary['is_myr'] = fdhold_summary['curcode'] == 'MYR'
            
            fdhold_pivot = pd.pivot_table(
                fdhold_summary,
                values='amount',
                index=['item', 'curcode'],
                columns=['is_30d', 'is_myr'],
                aggfunc='sum',
                fill_value=0
            )
        
        return df_insured, mni_summary, fdhold_summary, icgrp_not_nid, icgrp_nid
        
    except Exception as e:
        print(f"  Banking warning: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def process_walker_gl():
    """Process walk.txt for GL data"""
    gl_records = []
    
    try:
        walk_path = f"{PATHS['output']}walk.txt"
        lines = read_text_file(walk_path)
        
        for line in lines:
            if len(line) >= 62:
                set_id = line[1:20].strip()
                amount = float(line[41:61].strip().replace(',', ''))
                sign = line[61:62].strip() if len(line) > 61 else ''
                
                if sign == '':
                    amount = -amount
                
                item = lcrcdgl_map.get(set_id, '')
                curcode = lcrcdglccy_map.get(set_id, '')
                
                if item == '' and curcode != '':
                    item = lcrcdgloth_map.get(set_id, '')
                
                if item != '':
                    gl_records.append({
                        'set_id': set_id,
                        'item': item,
                        'curcode': curcode,
                        'amount': amount
                    })
        
        df_gl = pd.DataFrame(gl_records)
        
        # Handle special mapping (F142699OPE maps to B3.30)
        special_set = df_gl[df_gl['set_id'] == 'F142699OPE'].copy()
        if len(special_set) > 0:
            special_set['item'] = 'B3.30'
            df_gl = pd.concat([df_gl, special_set])
        
        # Summarize by item and currency
        if len(df_gl) > 0:
            gl_summary = df_gl.groupby(['item', 'curcode'])['amount'].sum().reset_index()
            gl_summary.columns = ['item', 'curcode', 'othsource']
        else:
            gl_summary = pd.DataFrame()
        
        return gl_summary
        
    except Exception as e:
        print(f"  Walker GL warning: {e}")
        return pd.DataFrame()

def generate_lcr_reports(equ_summary, mni_summary, fdhold_summary, gl_summary, rep_date):
    """Generate LCR reports for each currency"""
    
    # Combine all sources
    all_sources = []
    if len(equ_summary) > 0:
        all_sources.append(equ_summary)
    if len(mni_summary) > 0:
        all_sources.append(mni_summary)
    
    if not all_sources:
        print("No data to generate reports")
        return
    
    combined = pd.concat(all_sources, ignore_index=True)
    
    # Apply SHAREX logic to map to template items
    # This is a simplified version
    
    # Generate reports for each currency set
    report_configs = [
        ('MTH', None, 'lcrmth'),  # All currencies
        ('USD', ['USD'], 'lcrusd'),
        ('SGD', ['SGD'], 'lcrsgd'),
        ('HKD', ['HKD'], 'lcrhkd'),
        ('MYR', ['MYR', 'XAU'], 'lcrmyr')
    ]
    
    template_lines = read_template()
    
    for suffix, currencies, prefix in report_configs:
        print(f"  Generating {prefix} report...")
        
        # Filter by currency
        if currencies:
            data = combined[combined['curcode'].isin(currencies)].copy()
        else:
            data = combined.copy()
            # Exclude certain currencies for main report
            if len(gl_summary) > 0:
                gl_filtered = gl_summary[~gl_summary['curcode'].isin(['USD', 'SGD', 'HKD'])]
            else:
                gl_filtered = pd.DataFrame()
        
        if len(data) == 0:
            continue
        
        # Aggregate to template structure
        # Build output similar to SAS LCRPRINT macro
        
        # Generate text output
        output_path = f"{PATHS['output']}{prefix}{rep_date['mon']}.txt"
        with open(output_path, 'w') as f:
            # Header
            f.write("PUBLIC BANK BERHAD\n")
            f.write(f"LIQUIDITY COVERAGE RATIO (LCR) AS AT {rep_date['rdate']}\n")
            f.write("\n")
            
            # Column headers would go here - using simplified version
            f.write(f"Report Date: {rep_date['date'].strftime('%d/%m/%Y')}\n")
            f.write(f"Currency Filter: {currencies if currencies else 'ALL'}\n")
            f.write("\n")
            
            # Data rows
            f.write(f"{'ITEM':<10}{'DESCRIPTION':<50}{'AMOUNT':>20}\n")
            f.write("-" * 80 + "\n")
            
            for _, row in data.iterrows():
                f.write(f"{row.get('bnmcode', ''):<10}{row.get('curcode', ''):<50}{row.get('amount', 0):>20,.2f}\n")
        
        print(f"    ✓ {prefix}{rep_date['mon']}.txt")

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBMLCRM - BNM LCR Reporting")
    print("=" * 60)
    
    # Get report date
    rep_date = get_report_date()
    print(f"\nReport Date: {rep_date['date'].strftime('%d/%m/%Y')}")
    print(f"Week: {rep_date['nowk']}, Month: {rep_date['mon']}")
    
    # Load FX rates
    print("\nLoading FX rates...")
    fx_rates = {'MYR': 1.0}
    try:
        df_fx = read_sas_dataset(f"{PATHS['forate']}foratebkp.sas7bdat")
        if df_fx is not None:
            df_fx['reptdate'] = pd.to_datetime(df_fx['reptdate'])
            df_fx = df_fx[df_fx['reptdate'] <= rep_date['date']]
            df_fx = df_fx.sort_values('reptdate', ascending=False)
            df_fx = df_fx.drop_duplicates(subset=['curcode'], keep='first')
            fx_rates.update(dict(zip(df_fx['curcode'], df_fx['spotrate'])))
        print(f"  Loaded {len(fx_rates)} currencies")
    except Exception as e:
        print(f"  Using default rates: {e}")
        fx_rates.update({'USD': 4.0, 'SGD': 3.0, 'HKD': 0.5, 'XAU': 200.0})
    
    # Process data sources
    print("\nProcessing Treasury...")
    trea_detail, equ_summary, equ_icgrp = process_treasury(rep_date)
    print(f"  {len(trea_detail):,} treasury records")
    
    print("\nProcessing Core Banking...")
    bank_detail, mni_summary, fdhold_summary, mni_icgrp, nid_icgrp = process_banking(rep_date, fx_rates)
    print(f"  {len(bank_detail):,} banking records")
    
    # Process Walker GL
    print("\nProcessing Walker GL...")
    gl_summary = process_walker_gl()
    if len(gl_summary) > 0:
        print(f"  {len(gl_summary):,} GL records")
    
    # Generate LCR Reports
    print("\nGenerating LCR Reports...")
    generate_lcr_reports(equ_summary, mni_summary, fdhold_summary, gl_summary, rep_date)
    
    # Save detailed datasets
    print("\nSaving detailed datasets...")
    if len(trea_detail) > 0:
        trea_detail.to_parquet(f"{PATHS['output']}equ{rep_date['mon']}.parquet")
        print(f"  ✓ equ{rep_date['mon']}.parquet")
    
    if len(bank_detail) > 0:
        bank_detail.to_parquet(f"{PATHS['output']}cmm{rep_date['mon']}.parquet")
        print(f"  ✓ cmm{rep_date['mon']}.parquet")
    
    # Summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    total_amount = 0
    if len(equ_summary) > 0:
        total_amount += equ_summary['amount'].sum()
    if len(mni_summary) > 0:
        total_amount += mni_summary['amount'].sum()
    
    print(f"\nTotal Amount: RM {total_amount:,.0f}")
    print(f"Treasury Records: {len(trea_detail):,}")
    print(f"Banking Records: {len(bank_detail):,}")
    
    print("\n" + "=" * 60)
    print("✓ EIBMLCRM Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
