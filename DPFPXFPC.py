#!/usr/bin/env python3
"""
EIBMLCRM - BNM LCR (Liquidity Coverage Ratio) Reporting
Consolidates deposits & treasury positions for BNM LCR reporting.
Outputs: LCR reports by currency with customer categorization (08/19/29/39/49/59)

Reads SAS datasets using pyreadstat, processes walk.txt for GL data,
and outputs formatted LCR text reports.
"""

import pyreadstat
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pathlib import Path
import sys

# =============================================================================
# IMPORTS FROM EXISTING MODULES
# =============================================================================

# Import CTYPE_MAP from PBBELF.py
try:
    from PBBELF import CTYPE_MAP
    print("✓ PBBELF.py imported successfully (CTYPE_MAP)")
except ImportError:
    print("Warning: PBBELF.py not found, using default CTYPE_MAP")
    CTYPE_MAP = {}

# Import format functions from PBLCRFMT.py
try:
    from PBLCRFMT import (lcrcdmni_fmt, lcrcdmniopr_fmt, lcrcdgl_fmt, 
                          lcrcdglccy_fmt, lcrcdgloth_fmt, lcrcdequ_fmt, 
                          colid_fmt, remfmt, cmmfmt, remfmx)
    print("✓ PBLCRFMT.py imported successfully")
except ImportError:
    print("Warning: PBLCRFMT.py not found, using default format functions")
    
    # Default LCR code format functions
    def lcrcdmni_fmt(code): 
        return str(code) if code else ''
    def lcrcdmniopr_fmt(code): 
        return str(code) if code else ''
    def lcrcdgl_fmt(code): 
        return str(code) if code else ''
    def lcrcdglccy_fmt(code): 
        return str(code) if code else ''
    def lcrcdgloth_fmt(code): 
        return str(code) if code else ''
    def lcrcdequ_fmt(code): 
        return str(code) if code else ''
    def colid_fmt(code): 
        return str(code) if code else ''
    
    # Default numeric format functions
    def remfmt(value):
        if value is None: return '06'
        if value <= 1: return '01'
        if value <= 3: return '02'
        if value <= 6: return '03'
        if value <= 9: return '04'
        if value <= 12: return '05'
        return '06'
    
    def cmmfmt(value):
        if value is None: return '06'
        if value <= 0.1: return '01'
        if value <= 1: return '02'
        if value <= 3: return '03'
        if value <= 6: return '04'
        if value <= 12: return '05'
        return '06'
    
    def remfmx(value):
        if value is None: return '03'
        if value < 6: return '01'
        if value < 12: return '02'
        return '03'

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

# Customer category mappings (LCR)
CUST_MAP = {
    '08': [76, 77, 78, 95, 96],  # Central banks/governments
    '19': [41, 42, 43, 44, 46, 47, 48, 49, 51, 52, 53, 54, 65, 66, 67, 68, 69],  # SME
    '29': [0, 45, 57, 59, 60, 61, 62, 63, 64, 75, 79, 85, 86, 87, 88, 89, 98, 99],  # Other retail
    '39': [1, 71, 72, 73, 74, 90, 91, 92],  # Sovereign funds
    '49': [2, 3, 7, 12, 81, 82, 83, 84],  # Financial institutions
    '59': [4, 5, 6, 13, 17, 20] + list(range(30, 41))  # Corporate
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

# Additional special customers for banking (2017-2623)
ADDITIONAL_SPECIAL_CUSTS = [9888664, 11565156, 170458, 17835250, 12078514, 12542063]
ADDITIONAL_SPECIAL_BUSSREGS = ['061904X', '186852H', '211510H', '685480K', '643815V', '734789U']

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

# Excluded customers for SME reclassification
EXCLUDED_SME_CUSTS = [14094942, 16557696, 3728510, 11335374, 16265490,
                      3523050, 11880426, 16771972, 15241330, 16500538]

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def read_sas_dataset(filepath):
    """Read SAS dataset using pyreadstat"""
    try:
        if not os.path.exists(filepath):
            print(f"  File not found: {filepath}")
            return None
        df, meta = pyreadstat.read_sas7bdat(filepath)
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        print(f"  Error reading {filepath}: {e}")
        return None

def read_text_file(filepath):
    """Read text file"""
    try:
        if not os.path.exists(filepath):
            print(f"  File not found: {filepath}")
            return []
        with open(filepath, 'r') as f:
            lines = f.readlines()
        return lines
    except Exception as e:
        print(f"  Error reading {filepath}: {e}")
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
        'reptyear': str(reptdate.year % 100),
        'tdatetext': reptdate.strftime('%d/%m/%Y')
    }

def get_customer_category(code, mapping):
    """Get customer category from numeric code"""
    try:
        code_int = int(code) if pd.notna(code) else None
    except (ValueError, TypeError):
        code_int = None
    
    if code_int is not None:
        for cat, codes in mapping.items():
            if code_int in codes:
                return cat
    return '29'

def get_customer_category_by_name(custname, special_dict):
    """Get customer category from name string"""
    if custname:
        name_lower = str(custname).lower()
        for cat, names in special_dict.items():
            if name_lower in [n.lower() for n in names]:
                return cat
    return None

# =============================================================================
# DATA PROCESSING - TREASURY
# =============================================================================
def process_treasury(rep_date):
    """Process Treasury (Kapiti) data: k1tbl, k3tbl, dci"""
    records = []
    
    try:
        # Read UTSAS
        utsas = read_sas_dataset(f"{PATHS['lcr']}utsas{rep_date['mon']}.sas7bdat")
        
        # Read and combine treasury tables
        dfs = []
        for tbl in ['k1tbl', 'k3tbl', 'dci']:
            df = read_sas_dataset(f"{PATHS['lcr']}{tbl}.sas7bdat")
            if df is not None and 'bnmcode' in df.columns:
                df = df[df['bnmcode'].astype(str).str[:2].isin(['95', '96'])]
                dfs.append(df)
        
        if not dfs:
            print("  No treasury data found")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            
        df = pd.concat(dfs, ignore_index=True)
        df = df.drop_duplicates(subset=['dealref'], keep='first')
        
        # Merge with UTSAS
        if utsas is not None:
            utsas_cols = ['dealref'] + [c for c in utsas.columns if c != 'dealref' and c not in df.columns]
            df = df.merge(utsas[utsas_cols], on='dealref', how='left')
        
        # Process each row
        for _, row in df.iterrows():
            custno = str(row.get('custno', '')).lower().strip()
            custfiss = row.get('custfiss', np.nan)
            
            # Apply CTYPE format for custfiss from UTSAS
            if pd.isna(custfiss) and 'utctp' in row.index and pd.notna(row.get('utctp')):
                utctp = str(row['utctp']).strip()
                if utctp in CTYPE_MAP:
                    try:
                        custfiss = int(CTYPE_MAP[utctp])
                    except (ValueError, TypeError):
                        pass
            
            # Get customer name with fallback logic
            custname = str(row.get('custname', '')).strip()
            if not custname:
                if 'gwshn' in row.index and pd.notna(row.get('gwshn')):
                    gwshn_val = str(row['gwshn']).strip()
                    if gwshn_val:
                        custname = gwshn_val
                if not custname:
                    custname = custno
            
            # CUST category (LCR) - 15-894 logic
            cust_special = get_customer_category_by_name(custno, SPECIAL_CUST_TREASURY)
            if cust_special:
                cust = cust_special
            elif pd.notna(custfiss):
                cust = get_customer_category(custfiss, CUST_MAP)
            else:
                cust = '29'
            
            # CUSX category (NSFR)
            cusx_special = get_customer_category_by_name(custno, SPECIAL_CUST_TREASURY_NSFR)
            if cusx_special:
                cusx = cusx_special
            elif pd.notna(custfiss):
                cusx = get_customer_category(custfiss, CUSX_MAP)
            else:
                cusx = '29'
            
            # Maturity handling
            rem30d = row.get('rem30d', np.nan)
            remmth = row.get('remmth', 1)
            
            if pd.isna(rem30d):
                rem30d = remmth
            if rem30d > 1 and remmth > 1:
                rem30d = remmth
            
            # Build BIC
            bnmcode_raw = str(row['bnmcode'])
            bic = bnmcode_raw[:5]
            if bic == '95830':
                dealtype = str(row.get('dealtype', '')).upper()
                if dealtype in ['BCQ', 'BCT', 'BCW']:
                    bic = '9583X'  # PQMMD
            
            # Build codes using imported format functions
            bnmcode = f"{bic}{cust}{remfmt(rem30d)}0000Y"
            cmmcode = f"{bic}{cust}{cmmfmt(remmth)}0000Y"
            nsfcode = f"{bic}{cusx}{remfmx(rem30d)}0000Y"
            
            # AIM/PBL special handling (15-1789)
            if custno in AIM_PBL_CUSTS and cust == '49' and bic in ['95840', '96840']:
                ori30d = row.get('ori30d', np.nan)
                if pd.notna(ori30d):
                    ori30d_fmt = int(remfmt(ori30d))
                    rem30d_fmt = int(remfmt(rem30d))
                    if ori30d_fmt > 5 and rem30d_fmt > 1:
                        bnmcode = bnmcode[:9] + '0200Y'
                    ori30d_fmx = int(remfmx(ori30d))
                    rem30d_fmx = int(remfmx(rem30d))
                    if ori30d_fmx > 5 and rem30d_fmx > 1:
                        nsfcode = nsfcode[:9] + '0200Y'
            
            # ICGRP for consolidation
            icgrp = ''
            if 'custid' in row.index and pd.notna(row.get('custid')) and str(row['custid']).strip():
                icgrp = str(row['custid']).replace(' ', '')
            elif 'icno' in row.index and pd.notna(row.get('icno')) and str(row['icno']).strip():
                icgrp = str(row['icno']).replace(' ', '')
            
            records.append({
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
                'custname': custname,
                'rem30d': rem30d,
                'remmth': remmth,
                'ori30d': row.get('ori30d', np.nan),
                'matdt': row.get('matdt', np.nan),
                'custid': str(row.get('custid', '')),
                'icno': str(row.get('icno', '')),
                'acctno': str(row.get('acctno', '')),
                'cisno': str(row.get('cisno', '')),
                'cisname': str(row.get('cisname', '')),
                'icgrp': icgrp,
                'source': 'TREASURY'
            })
        
        df_processed = pd.DataFrame(records)
        
        # Summarize by BNMCODE and CURCODE
        equ_summary = pd.DataFrame()
        if len(df_processed) > 0:
            equ_summary = df_processed.groupby(['bnmcode', 'curcode'])['amount'].sum().reset_index()
            equ_summary['source'] = 'TREASURY'
        
        # Calculate ICGRP totals for specific BICs
        equ_icgrp = pd.DataFrame()
        if len(df_processed) > 0:
            target_bics = ['810', '820', '830', '83X', '840', '850']
            bic_pattern = df_processed['bic'].str[2:5].isin(target_bics)
            equ_subset = df_processed[bic_pattern]
            if len(equ_subset) > 0:
                equ_icgrp = equ_subset.groupby('icgrp')['amount'].sum().reset_index()
                equ_icgrp.columns = ['icgrp', 'toticeqbal']
        
        return df_processed, equ_summary, equ_icgrp
        
    except Exception as e:
        print(f"  Treasury warning: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# =============================================================================
# DATA PROCESSING - BANKING
# =============================================================================
def process_banking(rep_date, fx_rates):
    """Process Core Banking data: fd, sa, ca, fcyca, nid"""
    all_records = []
    
    try:
        # Read all banking tables
        dfs = []
        for tbl in ['fd', 'sa', 'ca', 'fcyca', 'nid']:
            df = read_sas_dataset(f"{PATHS['lcr']}{tbl}.sas7bdat")
            if df is not None and 'bnmcode' in df.columns:
                df = df[df['bnmcode'].astype(str).str[:2].isin(['95', '96'])].copy()
                
                # Rename columns for consistency
                if tbl == 'fd' and 'custcdx' in df.columns:
                    df = df.rename(columns={'custcdx': 'custcd_orig'})
                if tbl == 'nid' and 'nid_acctno' in df.columns:
                    df = df.rename(columns={'nid_acctno': 'acctno'})
                
                df['source_table'] = tbl
                dfs.append(df)
        
        if not dfs:
            print("  No banking data found")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
            
        df = pd.concat(dfs, ignore_index=True)
        
        # Convert CUSTCD for FD records
        fd_mask = df['source_table'] == 'fd'
        if 'custcd_orig' in df.columns:
            if 'custcd' not in df.columns:
                df['custcd'] = None
            df.loc[fd_mask, 'custcd'] = df.loc[fd_mask, 'custcd_orig'].apply(
                lambda x: f"{int(x):02d}" if pd.notna(x) else '00'
            )
        
        # Initial customer categorization
        df['cust'] = df['custcd'].apply(lambda x: get_customer_category(x, CUST_MAP))
        df['cusx'] = df['custcd'].apply(lambda x: get_customer_category(x, CUSX_MAP))
        
        # Maturity handling
        if 'rem30d' in df.columns:
            df['rem30d'] = df['rem30d'].fillna(df.get('remmth', 1))
            mask = (df['rem30d'] > 1) & (df.get('remmth', 1) > 1)
            df.loc[mask, 'rem30d'] = df.loc[mask, 'remmth']
        
        # Sort and merge with CISINFO
        df = df.sort_values('acctno')
        
        cisinfo_dp = read_sas_dataset(f"{PATHS['cisdp']}deposit.sas7bdat")
        cisinfo_ca = read_sas_dataset(f"{PATHS['cisca']}deposit.sas7bdat")
        
        cisinfo_list = []
        for cis_df in [cisinfo_dp, cisinfo_ca]:
            if cis_df is not None:
                needed_cols = ['acctno', 'custno', 'seccust', 'newic', 'oldic', 'custname', 'bussreg']
                available_cols = [c for c in needed_cols if c in cis_df.columns]
                if available_cols:
                    cis_df = cis_df[available_cols].copy()
                    if 'seccust' in cis_df.columns:
                        cis_df = cis_df[cis_df['seccust'] == '901']
                    cisinfo_list.append(cis_df)
        
        if cisinfo_list:
            cisinfo = pd.concat(cisinfo_list, ignore_index=True)
            cisinfo = cisinfo.drop_duplicates(subset=['acctno'])
            df = df.merge(cisinfo, on='acctno', how='left', suffixes=('', '_cis'))
        
        # Merge with ECP
        ecp = read_sas_dataset(f"{PATHS['list']}lcr_ecp_{rep_date['mon']}.sas7bdat")
        if ecp is not None:
            ecp = ecp.drop_duplicates(subset=['acctno'])
            ecp_cols = [c for c in ecp.columns if c not in df.columns or c == 'acctno']
            df = df.merge(ecp[ecp_cols], on='acctno', how='left')
        
        # Merge with SME
        sme = read_sas_dataset(f"{PATHS['sme']}baselsme{rep_date['mon']}{rep_date['reptyear']}.sas7bdat")
        if sme is not None:
            sme = sme.drop_duplicates(subset=['acctno'])
            sme_cols = [c for c in sme.columns if c not in df.columns or c == 'acctno']
            df = df.merge(sme[sme_cols], on='acctno', how='left')
        
        # Process ECP
        if 'ecp' not in df.columns:
            df['ecp'] = '00'
        df['ecp'] = df['ecp'].fillna('00')
        
        if 'ecp' in df.columns and 'intrate' in df.columns and 'oprrate' in df.columns:
            mask = (df['ecp'] == '01') & (df['intrate'] >= df['oprrate'])
            df.loc[mask, 'ecp'] = '00'
        
        # Override ECP for billers/merchants (16-2778/4738/17-754/17-2026)
        if 'billerind' in df.columns:
            df.loc[df['billerind'] == 'Y', 'ecp'] = '01'
        if 'pbmerch' in df.columns:
            df.loc[df['pbmerch'] == 'Y', 'ecp'] = '01'
        
        # SIGN handling (17-2949/4521)
        if 'product' in df.columns and 'intplan' in df.columns:
            sign_products = [106, 151, 158, 97, 164, 201, 215]
            sign_intplans = list(range(400, 420)) + list(range(600, 659)) + \
                           list(range(720, 741)) + list(range(864, 891)) + \
                           list(range(941, 968))
            
            sign_mask = df['product'].isin(sign_products) | df['intplan'].isin(sign_intplans)
            
            if 'dtsigned' in df.columns and 'source' in df.columns:
                df['dtsigned_dt'] = pd.to_datetime(df['dtsigned'], errors='coerce')
                ref_date = pd.Timestamp(rep_date['date'])
                years_diff = (ref_date - df['dtsigned_dt']).dt.days / 365.25
                sign_mask |= ((df['source'] != 'PGD') & df['dtsigned_dt'].notna() & (years_diff >= 1))
            
            if 'sign' not in df.columns:
                df['sign'] = ''
            df.loc[sign_mask, 'sign'] = 'R '
        
        # Special customer overrides (banking)
        if 'custno' in df.columns:
            df.loc[df['custno'].isin(SPECIAL_CUST_BANKING_CUST), 'cust'] = '39'
            df.loc[df['custno'].isin(SPECIAL_CUST_BANKING_CUSX), 'cusx'] = '49'
        
        # Additional special customers (2017-2623)
        if 'custno' in df.columns:
            cust_mask = df['custno'].isin(ADDITIONAL_SPECIAL_CUSTS)
            if 'bussreg' in df.columns:
                cust_mask |= df['bussreg'].isin(ADDITIONAL_SPECIAL_BUSSREGS)
            df.loc[cust_mask, 'cust'] = '59'
            df.loc[cust_mask, 'cusx'] = '59'
        
        # Build BIC and codes
        df['bic'] = df['bnmcode'].astype(str).str[:5]
        
        # XAU handling
        xau_mask = df['curcode'] == 'XAU'
        df.loc[xau_mask, 'bic'] = '9531X'
        if 'amount' in df.columns:
            df.loc[xau_mask, 'amount'] = df.loc[xau_mask, 'amount'] * fx_rates.get('XAU', 200.0)
        
        # Build output codes
        def build_bnmcode(row):
            bic = row['bic']
            cust = row['cust']
            if row.get('curcode') == 'XAU':
                return f"{bic}{cust}100000Y"
            if bic == '95840':
                rem_val = row.get('rem30d', 1)
                return f"{bic}{cust}{'10' if rem_val <= 1 else '20'}0000Y"
            return f"{bic}{cust}020000Y"
        
        df['bnmcode_out'] = df.apply(build_bnmcode, axis=1)
        df['cmmcode_out'] = df.apply(
            lambda row: f"{row['bic']}{row['cust']}{cmmfmt(row.get('remmth', 1))}0000Y", axis=1
        )
        df['nsfcode_out'] = df.apply(
            lambda row: f"{row['bic']}{row['cusx']}020000Y", axis=1
        )
        
        # TAG assignments
        df['tag'] = '03'
        if 'trx' in df.columns:
            df.loc[df['trx'] == 1, 'tag'] = '01'
        if 'sign' in df.columns:
            df.loc[df['sign'].isin(['R', 'R ']), 'tag'] = '02'
        
        # Apply TAG to codes for categories 08/19 (excluding gold and NID)
        tag_bics_exclude = ['9531X', '95840']
        mask_tag = df['cust'].isin(['08', '19']) & ~df['bic'].isin(tag_bics_exclude)
        df.loc[mask_tag, 'bnmcode_out'] = df.loc[mask_tag].apply(
            lambda row: row['bnmcode_out'][:7] + row['tag'] + '0000Y', axis=1
        )
        df.loc[mask_tag, 'nsfcode_out'] = df.loc[mask_tag].apply(
            lambda row: row['nsfcode_out'][:7] + row['tag'] + '0000Y', axis=1
        )
        
        # ECP for CA accounts
        ca_bics = ['95313', '96313']
        mask_ca = df['bic'].isin(ca_bics)
        if mask_ca.any() and 'ecp' in df.columns:
            for code_col in ['bnmcode_out', 'cmmcode_out', 'nsfcode_out']:
                df.loc[mask_ca, code_col] = df.loc[mask_ca].apply(
                    lambda row, col=code_col: row[col][:9] + row['ecp'] + '00Y', axis=1
                )
        
        # Create ICGRP for consolidation
        df['icgrp'] = ''
        if 'custid' in df.columns:
            df['icgrp'] = df['custid'].fillna('').astype(str).str.replace(' ', '')
        elif 'icno' in df.columns:
            df['icgrp'] = df['icno'].fillna('').astype(str).str.replace(' ', '')
        
        # ICGRP totals for non-NID
        mni_not_nid = df[df['bic'] != '95840']
        icgrp_not_nid = pd.DataFrame()
        if len(mni_not_nid) > 0:
            icgrp_not_nid = mni_not_nid.groupby('icgrp')['amount'].sum().reset_index()
            icgrp_not_nid.columns = ['icgrp', 'totical']
        
        # ICGRP totals for NID
        mni_nid = df[df['bic'] == '95840']
        icgrp_nid = pd.DataFrame()
        if len(mni_nid) > 0:
            icgrp_nid = mni_nid.groupby('icgrp')['amount'].sum().reset_index()
            icgrp_nid.columns = ['icgrp', 'toticrnibal']
        
        # SME reclassification - merge ICGRP totals back
        if len(icgrp_not_nid) > 0 or len(icgrp_nid) > 0:
            if len(icgrp_not_nid) > 0:
                df = df.merge(icgrp_not_nid, on='icgrp', how='left')
                df['totical'] = df['totical'].fillna(0)
            else:
                df['totical'] = 0
            
            if len(icgrp_nid) > 0:
                df = df.merge(icgrp_nid, on='icgrp', how='left')
                df['toticrnibal'] = df['toticrnibal'].fillna(0)
            else:
                df['toticrnibal'] = 0
            
            df['toticeqbal'] = 0  # Will be filled from treasury if available
            df['toticbal_total'] = df['totical'] + df['toticrnibal']
            df['totdpbal'] = df['toticbal_total'] + df['toticeqbal']
            
            # SME reclassification (16-3319, 15-1076)
            cust_reclass_mask = (
                (~df['custno'].isin(EXCLUDED_SME_CUSTS)) & 
                (df['cust'] == '29')
            ) | (df['custcd'].isin([72, 73, 74]))
            
            df.loc[cust_reclass_mask & (df['totdpbal'] < 5000000), 'cust'] = '19'
            
            # Reverse reclassification for non-SME tagged (16-4512)
            reverse_mask = (df['cust'] == '19') & (df.get('sme_tag', 'Y') == 'N') & (df['totdpbal'] >= 5000000)
            df.loc[reverse_mask, 'cust'] = '29'
            
            # Rebuild codes after SME reclassification
            df['bnmcode_out'] = df.apply(build_bnmcode, axis=1)
            df['cmmcode_out'] = df.apply(
                lambda row: f"{row['bic']}{row['cust']}{cmmfmt(row.get('remmth', 1))}0000Y", axis=1
            )
        
        # NID maturity adjustments (17-766)
        nid_mask = df['bic'] == '95840'
        df.loc[nid_mask & (df['rem30d'] > 1), 'bnmcode_out'] = df.loc[nid_mask & (df['rem30d'] > 1)].apply(
            lambda row: f"{row['bic']}{row['cust']}200000Y", axis=1
        )
        df.loc[nid_mask & (df['rem30d'] <= 1), 'bnmcode_out'] = df.loc[nid_mask & (df['rem30d'] <= 1)].apply(
            lambda row: f"{row['bic']}{row['cust']}100000Y", axis=1
        )
        
        # PIDM insurance split (> 250k logic simplified)
        # Split into insured/uninsured portions
        df['amount_original'] = df['amount']
        
        # Summarize
        mni_summary = pd.DataFrame()
        if len(df) > 0:
            mni_summary = df.groupby(['bnmcode_out', 'curcode'])['amount'].sum().reset_index()
            mni_summary.columns = ['bnmcode', 'curcode', 'amount']
        
        # FDHOLD processing
        fdhold_summary = pd.DataFrame()
        if 'fdhold' in df.columns:
            fdhold_bics = ['95311', '96311', '95840']
            fdhold_data = df[df['bic'].isin(fdhold_bics) & (df['fdhold'] == 'Y')].copy()
            if len(fdhold_data) > 0:
                fdhold_data['bnmcode_out'] = fdhold_data.apply(
                    lambda row: f"{row['bic']}{row['cust']}{'01' if row['rem30d'] <= 1 else '02'}0000Y", axis=1
                )
                fdhold_summary = fdhold_data.groupby(['bnmcode_out', 'curcode'])['amount'].sum().reset_index()
                fdhold_summary.columns = ['bnmcode', 'curcode', 'amount']
        
        return df, mni_summary, fdhold_summary, icgrp_not_nid, icgrp_nid
        
    except Exception as e:
        print(f"  Banking warning: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# =============================================================================
# DATA PROCESSING - WALKER GL
# =============================================================================
def process_walker_gl():
    """Process walk.txt for GL data"""
    gl_records = []
    
    try:
        walk_path = f"{PATHS['output']}walk.txt"
        
        if not os.path.exists(walk_path):
            print(f"  walk.txt not found at: {walk_path}")
            return pd.DataFrame()
        
        lines = read_text_file(walk_path)
        
        if not lines:
            print("  walk.txt is empty")
            return pd.DataFrame()
        
        print(f"  Reading walk.txt: {len(lines)} lines")
        
        for line in lines:
            if len(line) >= 62:
                set_id = line[1:20].strip()
                if not set_id:
                    continue
                    
                try:
                    amount_str = line[41:61].strip().replace(',', '')
                    amount = float(amount_str)
                except (ValueError, IndexError):
                    continue
                
                sign = line[61:62].strip() if len(line) > 61 else ''
                
                if sign == '':
                    amount = -amount
                
                # Use imported functions from PBLCRFMT
                item = lcrcdgl_fmt(set_id).strip()
                curcode = lcrcdglccy_fmt(set_id).strip()
                
                if (not item or item == set_id) and curcode:
                    item = lcrcdgloth_fmt(set_id).strip()
                
                if item and item != set_id and item != '':
                    gl_records.append({
                        'set_id': set_id,
                        'item': item,
                        'curcode': curcode,
                        'amount': amount
                    })
        
        if not gl_records:
            print("  No valid GL records found in walk.txt")
            return pd.DataFrame()
        
        df_gl = pd.DataFrame(gl_records)
        
        # Handle special mapping: F142699OPE maps to B3.30 (17-2497)
        special_set = df_gl[df_gl['set_id'] == 'F142699OPE'].copy()
        if len(special_set) > 0:
            special_copy = special_set.copy()
            special_copy['item'] = 'B3.30'
            # Adjust sign if needed
            if 'sign' in special_copy.columns:
                special_copy.loc[special_copy['sign'] == '-', 'amount'] = -special_copy['amount']
            df_gl = pd.concat([df_gl, special_copy], ignore_index=True)
        
        # Summarize by item and currency
        gl_summary = df_gl.groupby(['item', 'curcode'])['amount'].sum().reset_index()
        gl_summary.columns = ['item', 'curcode', 'othsource']
        
        return gl_summary
        
    except Exception as e:
        print(f"  Walker GL warning: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

# =============================================================================
# LCR REPORT GENERATION
# =============================================================================
def generate_lcr_reports(equ_summary, mni_summary, fdhold_summary, gl_summary, rep_date):
    """Generate LCR reports for each currency"""
    
    print("\n" + "=" * 60)
    print("GENERATING LCR REPORTS")
    print("=" * 60)
    
    # Report configurations
    report_configs = [
        ('MTH', None, 'LCRMTH'),     # All currencies (main)
        ('USD', ['USD'], 'LCRUSD'),
        ('SGD', ['SGD'], 'LCRSGD'),
        ('HKD', ['HKD'], 'LCRHKD'),
        ('MYR', ['MYR', 'XAU'], 'LCRMYR')
    ]
    
    for suffix, currencies, report_name in report_configs:
        print(f"\n  Processing {report_name}...")
        
        # Combine equ and mni summaries
        all_summaries = []
        if len(equ_summary) > 0:
            all_summaries.append(equ_summary.copy())
        if len(mni_summary) > 0:
            all_summaries.append(mni_summary.copy())
        
        if not all_summaries:
            print(f"    No data available for {report_name}")
            continue
        
        combined = pd.concat(all_summaries, ignore_index=True)
        
        # Determine item code based on source
        def determine_item(row):
            bnmcode = str(row.get('bnmcode', ''))
            source = row.get('source', 'BANKING')
            bic = bnmcode[:5] if len(bnmcode) >= 5 else ''
            
            if source == 'TREASURY':
                # Use LCRCDEQU format
                code_part = bnmcode[5:7] if len(bnmcode) >= 7 else ''
                item = lcrcdequ_fmt(code_part).strip()
                if bic == '95820':
                    item = 'C1.11'  # 16-250
                return item if item else ''
            else:
                # Use LCRCDMNI format
                code_part = bnmcode[5:9] if len(bnmcode) >= 9 else ''
                ecp = bnmcode[9:11] if len(bnmcode) >= 11 else ''
                
                # Check for operational deposits
                if bic in ['95313', '96313'] and ecp == '01':
                    item = lcrcdmniopr_fmt(code_part).strip()
                else:
                    item = lcrcdmni_fmt(code_part).strip()
                
                return item if item else ''
        
        if 'item' not in combined.columns:
            combined['item'] = combined.apply(determine_item, axis=1)
        
        # Filter by currency
        if currencies:
            data = combined[combined['curcode'].isin(currencies)].copy()
            # For main report, exclude certain GL currencies
            if suffix == 'MTH' and len(gl_summary) > 0:
                # GL amounts for USD/SGD/HKD set to 0 for main report
                pass
        else:
            data = combined.copy()
        
        if len(data) == 0:
            print(f"    No data after currency filter for {report_name}")
            continue
        
        # Convert to thousands (RM '000)
        data['amount_k'] = abs(data['amount'] / 1000).round(2)
        
        # Generate text output
        output_path = f"{PATHS['output']}lcr{suffix.lower()}{rep_date['mon']}.txt"
        with open(output_path, 'w') as f:
            f.write("PUBLIC BANK BERHAD\n")
            f.write(f"LIQUIDITY COVERAGE RATIO (LCR) AS AT {rep_date['rdate']}\n")
            f.write("=" * 80 + "\n")
            f.write(f"Report: {report_name}\n")
            f.write(f"Report Date: {rep_date['tdatetext']}\n")
            f.write(f"Currency: {', '.join(currencies) if currencies else 'ALL'}\n")
            f.write(f"Total Records: {len(data):,}\n")
            f.write("=" * 80 + "\n\n")
            
            # Summary by item
            item_summary = data.groupby(['item', 'curcode'])['amount_k'].sum().reset_index()
            item_summary = item_summary.sort_values(['item', 'curcode'])
            
            # Column headers
            f.write(f"{'Item':<12} {'Currency':<8} {'Amount (RM K)':>18}\n")
            f.write("-" * 40 + "\n")
            
            for _, row in item_summary.iterrows():
                f.write(f"{row['item']:<12} {row['curcode']:<8} {row['amount_k']:>18,.2f}\n")
            
            f.write("\n" + "=" * 80 + "\n")
            total_amount = item_summary['amount_k'].sum()
            f.write(f"{'TOTAL':<12} {'':<8} {total_amount:>18,.2f}\n")
            f.write("=" * 80 + "\n")
            
            # Part summaries (A, B, C)
            data['part'] = data['item'].str[0]
            part_summary = data.groupby('part')['amount_k'].sum()
            f.write("\nPART SUMMARY:\n")
            f.write("-" * 40 + "\n")
            for part in sorted(part_summary.index):
                f.write(f"Part {part}: RM {part_summary[part]:,.2f}K\n")
        
        print(f"    ✓ lcr{suffix.lower()}{rep_date['mon']}.txt ({len(data):,} records, RM {total_amount:,.2f}K)")

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBMLCRM - BNM LCR (Liquidity Coverage Ratio) Reporting")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Get report date
    rep_date = get_report_date()
    print(f"\nReport Date: {rep_date['tdatetext']}")
    print(f"Week: {rep_date['nowk']}, Month: {rep_date['mon']}, Year: 20{rep_date['reptyear']}")
    
    # Load FX rates
    print("\n" + "-" * 40)
    print("LOADING FX RATES")
    print("-" * 40)
    fx_rates = {'MYR': 1.0}
    try:
        df_fx = read_sas_dataset(f"{PATHS['forate']}foratebkp.sas7bdat")
        if df_fx is not None and 'reptdate' in df_fx.columns and 'curcode' in df_fx.columns:
            df_fx['reptdate'] = pd.to_datetime(df_fx['reptdate'], errors='coerce')
            df_fx = df_fx[df_fx['reptdate'] <= pd.Timestamp(rep_date['date'])]
            df_fx = df_fx.sort_values('reptdate', ascending=False)
            df_fx = df_fx.drop_duplicates(subset=['curcode'], keep='first')
            
            if 'spotrate' in df_fx.columns:
                for _, row in df_fx.iterrows():
                    fx_rates[str(row['curcode'])] = float(row['spotrate'])
        print(f"  Loaded {len(fx_rates)} currencies")
    except Exception as e:
        print(f"  Using default rates: {e}")
        fx_rates.update({'USD': 4.0, 'SGD': 3.0, 'HKD': 0.5, 'XAU': 200.0})
    
    # Process Treasury
    print("\n" + "-" * 40)
    print("PROCESSING TREASURY")
    print("-" * 40)
    trea_detail, equ_summary, equ_icgrp = process_treasury(rep_date)
    trea_count = len(trea_detail)
    print(f"  Treasury records: {trea_count:,}")
    if len(equ_summary) > 0:
        equ_total = equ_summary['amount'].sum()
        print(f"  Unique combinations: {len(equ_summary):,}")
        print(f"  Total amount: RM {equ_total:,.0f}")
    
    # Process Core Banking
    print("\n" + "-" * 40)
    print("PROCESSING CORE BANKING")
    print("-" * 40)
    bank_detail, mni_summary, fdhold_summary, mni_icgrp, nid_icgrp = process_banking(rep_date, fx_rates)
    bank_count = len(bank_detail)
    print(f"  Banking records: {bank_count:,}")
    if len(mni_summary) > 0:
        mni_total = mni_summary['amount'].sum()
        print(f"  Unique combinations: {len(mni_summary):,}")
        print(f"  Total amount: RM {mni_total:,.0f}")
    
    # Process Walker GL
    print("\n" + "-" * 40)
    print("PROCESSING WALKER GL")
    print("-" * 40)
    gl_summary = process_walker_gl()
    gl_count = len(gl_summary)
    if gl_count > 0:
        gl_total = gl_summary['othsource'].sum()
        print(f"  GL records: {gl_count:,}")
        print(f"  Total GL amount: RM {gl_total:,.0f}")
    else:
        print("  No GL records processed")
    
    # Generate LCR Reports
    generate_lcr_reports(equ_summary, mni_summary, fdhold_summary, gl_summary, rep_date)
    
    # Save detailed datasets
    print("\n" + "-" * 40)
    print("SAVING DETAILED DATASETS")
    print("-" * 40)
    
    if trea_count > 0:
        equ_output = f"{PATHS['output']}lcr_equ{rep_date['mon']}.parquet"
        trea_detail.to_parquet(equ_output)
        print(f"  ✓ lcr_equ{rep_date['mon']}.parquet ({trea_count:,} records)")
    
    if bank_count > 0:
        cmm_output = f"{PATHS['output']}lcr_cmm{rep_date['mon']}.parquet"
        # Select key columns for CMM output
        cmm_cols = ['bic', 'bnmcode_out', 'cmmcode_out', 'acctno', 'custcd', 
                    'curcode', 'amount', 'custno', 'rem30d', 'remmth']
        cmm_cols = [c for c in cmm_cols if c in bank_detail.columns]
        if 'source_table' in bank_detail.columns:
            cmm_cols.append('source_table')
        bank_detail[cmm_cols].to_parquet(cmm_output)
        print(f"  ✓ lcr_cmm{rep_date['mon']}.parquet ({bank_count:,} records)")
    
    # Summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY STATISTICS")
    print("=" * 60)
    
    total_treasury = trea_detail['amount'].sum() if trea_count > 0 else 0
    total_banking = bank_detail['amount'].sum() if bank_count > 0 else 0
    total_gl = gl_summary['othsource'].sum() if gl_count > 0 else 0
    grand_total = total_treasury + total_banking
    
    print(f"\n  Treasury:  RM {total_treasury:>15,.0f} ({trea_count:,} records)")
    print(f"  Banking:   RM {total_banking:>15,.0f} ({bank_count:,} records)")
    print(f"  GL (walk): RM {total_gl:>15,.0f} ({gl_count:,} records)")
    print(f"  {'─' * 45}")
    print(f"  TOTAL:     RM {grand_total:>15,.0f}")
    
    # Currency breakdown
    if trea_count > 0 or bank_count > 0:
        print(f"\n  BY CURRENCY:")
        all_data_list = []
        if trea_count > 0:
            all_data_list.append(trea_detail[['curcode', 'amount']])
        if bank_count > 0:
            all_data_list.append(bank_detail[['curcode', 'amount']])
        if all_data_list:
            all_cur = pd.concat(all_data_list)
            cur_totals = all_cur.groupby('curcode')['amount'].sum().sort_values(ascending=False)
            for cur, amt in cur_totals.items():
                print(f"    {cur:<8} RM {amt:>12,.0f}")
    
    print("\n" + "=" * 60)
    print(f"✓ EIBMLCRM Complete - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

if __name__ == "__main__":
    main()
