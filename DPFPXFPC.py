"""
EIIMLCRM - BNM LCR (Liquidity Coverage Ratio) Reporting for Islamic Banking
Consolidates Islamic deposits & treasury positions for BNM LCR reporting.
Outputs: LCR reports by currency with customer categorization (08/19/29/39/49/59)

Python conversion of SAS EIIMLCRM program
- Uses pyreadstat to read .sas7bdat files
- Uses PBBELF.py and PBLCRFMT.py for format definitions
- Reports calculated from datetime.now() - timedelta(days=1)
- Outputs tab-delimited text files
"""

import pyreadstat
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import os
from pathlib import Path

# Import from existing format libraries
from PBBELF import format_ctype, format_brchcd
from PBLCRFMT import (
    remfmt, cmmfmt, remfmx,
    lcrcdequ_fmt, lcrcdmni_fmt, lcrcdmniopr_fmt,
    lcrcdgl_fmt, lcrcdglccy_fmt, lcrcdgloth_fmt,
    lcrcdigl_fmt, lcrcdiglccy_fmt, colid_fmt, bnmcd_fmt
)

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'LCR': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/lcr/',
    'CISDP': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/cisdp/',
    'CISCA': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/cisca/',
    'LIST': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/list/',
    'SME': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/',
    'OUTPUT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMLCRM/',
    'TEMPLATE': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# =============================================================================
# CUSTOMER CATEGORY MAPPINGS (from original SAS code)
# =============================================================================
# LCR Customer mappings (CUST)
CUST_MAP = [
    (['KWSP', 'KWAP', 'KWAN', 'LEMTAB'], None, '39'),  # Special treasury
    ([76, 77, 78, 95, 96], None, '08'),
    ([41, 42, 43, 44, 46, 47, 48, 49, 51, 52, 53, 54, 65, 66, 67, 68, 69], None, '19'),
    ([0, 45, 57, 59, 60, 61, 62, 63, 64, 75, 79, 85, 86, 87, 88, 89, 98, 99], None, '29'),
    ([1, 71, 72, 73, 74, 90, 91, 92], None, '39'),
    ([2, 3, 7, 12, 81, 82, 83, 84], None, '49'),
    ([4, 5, 6, 13, 20] + list(range(30, 41)) + [17], None, '59'),
]

# NSFR Customer mappings (CUSX)
CUSX_MAP = [
    (['KWSP', 'KWSPKL', 'KWAP', 'KWAPKL', 'KWAN', 'KWANKL', 'LEMTAB', 'LEMTABKL'], None, '49'),
    ([76, 77, 78, 95, 96], None, '08'),
    ([41, 42, 43, 44, 46, 47, 48, 49, 51, 52, 53, 54, 65, 66, 67, 68, 69], None, '19'),
    ([0, 17, 45, 57, 59, 60, 61, 62, 63, 64, 75, 79, 85, 86, 87, 88, 89, 98, 99], None, '29'),
    ([1, 91], None, '39'),
    ([71, 72, 73, 74, 90, 92], None, '49'),
    ([2, 3, 4, 5, 6, 7, 12, 13, 20] + list(range(30, 41)) + [81, 82, 83, 84], None, '59'),
]

MGIA_PRODUCTS = [302, 315, 394, 396]

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def get_report_date():
    """Calculate report date as yesterday (SAS equivalent of REPTDATE)"""
    reptdate = datetime.now() - timedelta(days=1)
    day = reptdate.day
    
    if day <= 8:
        nowk = '1'
    elif day <= 15:
        nowk = '2'
    elif day <= 22:
        nowk = '3'
    else:
        nowk = '4'
    
    return {
        'date': reptdate,
        'nowk': nowk,
        'mon': f"{reptdate.month:02d}",
        'day': f"{reptdate.day:02d}",
        'year': str(reptdate.year % 100).zfill(2),
        'rdate': reptdate.strftime('%d%m%y'),
        'fildt': reptdate.strftime('%d%m%y'),
        'reptmon': f"{reptdate.month:02d}",
        'reptyear': str(reptdate.year % 100).zfill(2),
        'tdatetime': reptdate  # SAS &TDATE equivalent
    }

def read_sas7bdat(filepath):
    """Read SAS dataset using pyreadstat"""
    try:
        if not os.path.exists(filepath):
            print(f"  File not found: {filepath}")
            return pd.DataFrame()
        df, meta = pyreadstat.read_sas7bdat(filepath)
        df.columns = [c.upper() for c in df.columns]
        return df
    except Exception as e:
        print(f"  Warning: Cannot read {filepath}: {e}")
        return pd.DataFrame()

def read_walk_file(filepath):
    """Read WALK.TXT file matching SAS INFILE WALK logic"""
    records = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if len(line) >= 62:
                    set_id = line[1:20].strip()       # @002 SET_ID $19.
                    amount_str = line[41:61].strip()   # @042 AMOUNT COMMA20.2
                    sign = line[61:62].strip() if len(line) > 61 else ''  # @062 SIGN $1.
                    
                    try:
                        amount = float(amount_str.replace(',', ''))
                    except:
                        amount = 0
                    
                    if sign == '':
                        amount = -amount
                    
                    records.append({
                        'SET_ID': set_id,
                        'AMOUNT': amount,
                        'SIGN': sign
                    })
    except Exception as e:
        print(f"  Warning: Cannot read WALK.TXT: {e}")
    
    return pd.DataFrame(records)

def get_cust_category(custno, custfiss, mapping, is_treasury=False):
    """
    Apply customer category mapping matching SAS IF/ELSE logic.
    Checks special customer names first, then custfiss codes.
    """
    custno_upper = str(custno).upper().strip() if custno else ''
    
    try:
        custfiss_int = int(float(custfiss)) if pd.notna(custfiss) else -1
    except (ValueError, TypeError):
        custfiss_int = -1
    
    for names_or_codes, _, category in mapping:
        if names_or_codes and isinstance(names_or_codes[0], str):
            # Special customer name check
            if custno_upper in [n.upper() for n in names_or_codes]:
                return category
        elif custfiss_int in names_or_codes:
            return category
    
    return '29'  # Default

def format_day_bucket_sas(days):
    """Matching SAS REMFMT for day bucket"""
    try:
        d = float(days)
    except:
        d = 0
    return '01' if d <= 1 else '02'

def format_mth_bucket_sas(months):
    """Matching SAS CMMFMT for month bucket"""
    try:
        m = float(months)
    except:
        m = 0
    
    if m <= 1:
        return '01'
    elif m <= 3:
        return '02'
    elif m <= 6:
        return '03'
    elif m <= 9:
        return '04'
    elif m <= 12:
        return '05'
    else:
        return '10'

def convert_sas_date_to_python(sas_date):
    """Convert SAS date (days since 1960-01-01) to Python date"""
    if pd.isna(sas_date):
        return None
    try:
        if isinstance(sas_date, (datetime, date)):
            return sas_date
        if isinstance(sas_date, (int, float)):
            return datetime(1960, 1, 1) + timedelta(days=int(sas_date))
        return None
    except:
        return None

def is_valid_date(value):
    """Check if value is a valid date (not NaN, not zero)"""
    if pd.isna(value):
        return False
    if isinstance(value, (int, float)) and value <= 0:
        return False
    return True

# =============================================================================
# TREASURY PROCESSING - Matching SAS ALLEQU DATA step
# =============================================================================
def process_treasury(rep_date):
    """
    Process Treasury data.
    
    SAS equivalent:
    DATA ALLEQU;
       SET LCR.K1TBL LCR.K3TBL;
    RUN;
    PROC SORT NODUPKEY; BY DEALREF; RUN;
    
    DATA ALLEQU LCR.EQU&REPTMON;
       MERGE ALLEQU(IN=A) UTSAS;
       BY DEALREF;
       IF A;
       ...
    RUN;
    """
    print("Processing Treasury (ALLEQU)...")
    
    # Read UTSAS first (PROC SORT NODUPKEY; BY DEALREF;)
    utsas_file = f"{PATHS['LCR']}utsas{rep_date['reptmon']}.sas7bdat"
    utsas = read_sas7bdat(utsas_file)
    if not utsas.empty:
        utsas = utsas.drop_duplicates(subset=['DEALREF'], keep='first')
        utsas = utsas.sort_values('DEALREF')
        print(f"  UTSAS: {len(utsas)} records")
    
    # Read K1TBL and K3TBL (SET LCR.K1TBL LCR.K3TBL)
    dfs = []
    for tbl in ['k1tbl', 'k3tbl']:
        df = read_sas7bdat(f"{PATHS['LCR']}{tbl}.sas7bdat")
        if not df.empty:
            dfs.append(df)
    
    if not dfs:
        print("  No treasury data found")
        return pd.DataFrame()
    
    allequ = pd.concat(dfs, ignore_index=True)
    
    # PROC SORT NODUPKEY; BY DEALREF;
    allequ = allequ.drop_duplicates(subset=['DEALREF'], keep='first')
    allequ = allequ.sort_values('DEALREF')
    print(f"  Combined K1TBL+K3TBL: {len(allequ)} records")
    
    # MERGE ALLEQU(IN=A) UTSAS; BY DEALREF; IF A;
    if not utsas.empty:
        allequ = allequ.merge(utsas, on='DEALREF', how='left', suffixes=('', '_UTSAS'))
    
    # Process each record matching SAS DATA step logic
    records = []
    for _, row in allequ.iterrows():
        custno = str(row.get('CUSTNO', '')).strip()
        custfiss = row.get('CUSTFISS', np.nan)
        
        # IF CUSTFISS = . AND UTCTP NE '' THEN CUSTFISS=PUT(UTCTP,$CTYPE.);
        if pd.isna(custfiss) and 'UTCTP' in row.index:
            utctp = str(row.get('UTCTP', '')).strip()
            if utctp:
                formatted = format_ctype(utctp)
                try:
                    custfiss = int(formatted.strip())
                except:
                    custfiss = 0
        
        # CUSTNAME handling
        custname = str(row.get('CUSTNAME', '')).strip()
        if custname == '':
            gwsname = str(row.get('GWSHN', '')).strip()
            if gwsname:
                custname = gwsname
            else:
                custname = custno
        
        # Customer categories (LCR - 15-894)
        cust = get_cust_category(custno, custfiss, CUST_MAP, is_treasury=True)
        
        # Customer categories (NSFR)
        cusx = get_cust_category(custno, custfiss, CUSX_MAP, is_treasury=True)
        
        # Maturity handling: IF REM30D = . THEN REM30D = REMMTH;
        rem30d = row.get('REM30D', np.nan)
        remmth = row.get('REMMTH', 1)
        ori30d = row.get('ORI30D', np.nan)
        
        if pd.isna(rem30d):
            rem30d = remmth
        
        # IF REM30D > 1 AND REMMTH > 1 THEN REM30D = REMMTH;
        if rem30d > 1 and remmth > 1:
            rem30d = remmth
        
        # Deal type: IF DEALTYPE = 'BQD' THEN DLTYPE = '01';
        dltype = '01' if str(row.get('DEALTYPE', '')).strip() == 'BQD' else '00'
        
        # BIC: BIC = SUBSTR(BNMCODE,1,5);
        bnmcode_raw = str(row.get('BNMCODE', ''))
        bic = bnmcode_raw[:5] if len(bnmcode_raw) >= 5 else bnmcode_raw
        
        rem30d_bucket = format_day_bucket_sas(rem30d)
        
        # Special handling 15-1789
        if (custno.upper() in ['AIM','PBL','PBLEUR','PBLNID','PBLUSD','PIVMYR','PBB',
                                'PBBMYR','PBBUSD','CUST'] and 
            cust == '49' and bic in ['95840','96840']):
            
            if not pd.isna(ori30d):
                ori30d_bucket = format_day_bucket_sas(ori30d)
                if ori30d_bucket > '05' and rem30d_bucket > '01':
                    bnmcode = f"{bic}{cust}020200Y"
                    nsfcode = f"{bic}{cusx}020200Y"
                else:
                    bnmcode = f"{bic}{cust}{rem30d_bucket}00{dltype}Y"
                    nsfcode = f"{bic}{cusx}{rem30d_bucket}00{dltype}Y"
            else:
                bnmcode = f"{bic}{cust}{rem30d_bucket}00{dltype}Y"
                nsfcode = f"{bic}{cusx}{rem30d_bucket}00{dltype}Y"
        else:
            bnmcode = f"{bic}{cust}{rem30d_bucket}00{dltype}Y"
            nsfcode = f"{bic}{cusx}{rem30d_bucket}00{dltype}Y"
        
        cmmcode = f"{bic}{cust}{format_mth_bucket_sas(remmth)}00{dltype}Y"
        
        # ICGRP: IF CUSTID NE '' THEN ICGRP = COMPRESS(CUSTID); ELSE ICGRP = COMPRESS(ICNO);
        custid = str(row.get('CUSTID', '')).replace(' ', '')
        icno = str(row.get('ICNO', '')).replace(' ', '')
        icgrp = custid if custid != '' and custid != 'nan' else icno
        
        records.append({
            'BIC': bic,
            'BNMCODE': bnmcode,
            'CMMCODE': cmmcode,
            'CURCODE': str(row.get('CURCODE', 'MYR')).strip(),
            'AMOUNT': float(row.get('AMOUNT', 0)),
            'DEALREF': str(row.get('DEALREF', '')).strip(),
            'DEALTYPE': dltype,
            'CUSTFISS': custfiss if not pd.isna(custfiss) else 0,
            'CUSTNO': custno,
            'CUSTNAME': custname,
            'REM30D': rem30d,
            'REMMTH': remmth,
            'ORI30D': ori30d if not pd.isna(ori30d) else 0,
            'MATDT': str(row.get('MATDT', '')).strip(),
            'CUSTID': custid,
            'ICNO': icno,
            'ACCTNO': str(row.get('ACCTNO', '')).strip(),
            'CISNO': str(row.get('CISNO', '')).strip(),
            'CISNAME': str(row.get('CISNAME', '')).strip(),
            'ICGRP': icgrp,
            'NSFCODE': nsfcode,
            'SRC': 'TREASURY'
        })
    
    df_result = pd.DataFrame(records)
    print(f"  Treasury processed: {len(df_result)} records")
    return df_result

# =============================================================================
# BANKING PROCESSING - Matching SAS ALLMNI DATA step
# =============================================================================
def process_banking(rep_date):
    """
    Process Core Banking data.
    
    SAS equivalent:
    DATA ALLMNI;
       SET LCR.FD(RENAME=(CUSTCD=CUSTCDX) IN=FD)
           LCR.SA
           LCR.CA
           LCR.FCYCA;
       IF FD THEN CUSTCD = PUT(CUSTCDX,Z2.);
       ...
    RUN;
    """
    print("Processing Core Banking (ALLMNI)...")
    
    # Read banking tables
    dfs = []
    for tbl in ['fd', 'sa', 'ca', 'fcyca']:
        filepath = f"{PATHS['LCR']}{tbl}.sas7bdat"
        df = read_sas7bdat(filepath)
        if not df.empty:
            if tbl == 'fd' and 'CUSTCD' in df.columns:
                # IF FD THEN CUSTCD = PUT(CUSTCDX,Z2.);
                df['CUSTCDX'] = df['CUSTCD']
                df['CUSTCD'] = df['CUSTCDX'].apply(
                    lambda x: str(int(x)).zfill(2) if pd.notna(x) else '00'
                )
            dfs.append(df)
    
    if not dfs:
        print("  No banking data found")
        return pd.DataFrame()
    
    allmni = pd.concat(dfs, ignore_index=True)
    print(f"  Combined banking: {len(allmni)} records")
    
    # Ensure CUSTCD is string
    if 'CUSTCD' in allmni.columns:
        allmni['CUSTCD'] = allmni['CUSTCD'].astype(str).str.zfill(2)
    else:
        allmni['CUSTCD'] = '00'
    
    # REM30D handling: IF REM30D = . THEN REM30D = REMMTH;
    allmni['REM30D'] = allmni['REM30D'].fillna(allmni['REMMTH'])
    mask = (allmni['REM30D'] > 1) & (allmni['REMMTH'] > 1)
    allmni.loc[mask, 'REM30D'] = allmni.loc[mask, 'REMMTH']
    
    # Sort by ACCTNO for merging
    allmni = allmni.sort_values('ACCTNO')
    
    # Read CIS info: SET CISDP.DEPOSIT CISCA.DEPOSIT; WHERE SECCUST='901';
    cis_dfs = []
    for path_key in ['CISDP', 'CISCA']:
        cis_file = f"{PATHS[path_key]}deposit.sas7bdat"
        if os.path.exists(cis_file):
            cis = read_sas7bdat(cis_file)
            if not cis.empty:
                keep_cols = ['ACCTNO', 'CUSTNO', 'SECCUST', 'NEWIC', 'OLDIC', 'CUSTNAME', 'BUSSREG']
                available = [c for c in keep_cols if c in cis.columns]
                cis_dfs.append(cis[available])
    
    cisinfo = pd.DataFrame()
    if cis_dfs:
        cisinfo = pd.concat(cis_dfs, ignore_index=True)
        if 'SECCUST' in cisinfo.columns:
            cisinfo = cisinfo[cisinfo['SECCUST'] == '901']
        cisinfo = cisinfo.drop_duplicates(subset=['ACCTNO'], keep='first')
        print(f"  CIS info: {len(cisinfo)} accounts")
    
    # Read ECP
    ecp_df = pd.DataFrame()
    ecp_file = f"{PATHS['LIST']}lcr_ecp_{rep_date['reptmon']}.sas7bdat"
    if os.path.exists(ecp_file):
        ecp_df = read_sas7bdat(ecp_file)
        if not ecp_df.empty:
            ecp_df = ecp_df.drop_duplicates(subset=['ACCTNO'], keep='first')
    
    # Read SME
    sme_df = pd.DataFrame()
    sme_file = f"{PATHS['SME']}ibaselsme{rep_date['reptmon']}{rep_date['reptyear']}.sas7bdat"
    if os.path.exists(sme_file):
        sme_df = read_sas7bdat(sme_file)
        if not sme_df.empty:
            sme_df = sme_df.drop_duplicates(subset=['ACCTNO'], keep='first')
    
    # Merge all: MERGE ALLMNI(IN=A) LCR.CISINFO LCR.TRNSCISIC ECP LCR.SME; BY ACCTNO; IF A;
    print("  Merging CIS/ECP/SME data...")
    allmni = allmni.merge(cisinfo, on='ACCTNO', how='left', suffixes=('', '_CIS'))
    if not ecp_df.empty:
        allmni = allmni.merge(ecp_df, on='ACCTNO', how='left', suffixes=('', '_ECP'))
    if not sme_df.empty:
        allmni = allmni.merge(sme_df, on='ACCTNO', how='left', suffixes=('', '_SME'))
    
    print("  Processing records...")
    
    # Special customer lists from SAS
    special_39 = [4391161, 2115999, 12579649, 13468207, 14300254,
                  14675929, 15327497, 17104931, 12677444, 3703533,
                  5978659, 16185090, 2558344, 10819745]
    special_49 = [4391161, 2115999, 12579649, 13468207, 14675929,
                  15327497, 17104931, 12677444, 3703533, 5978659,
                  16185090, 10819745, 2558344]
    special_59 = [9888664, 11565156, 170458, 17835250, 12078514, 12542063]
    special_reg = ['061904X', '186852H', '211510H', '685480K', '643815V', '734789U']
    
    # Process records matching SAS logic
    records = []
    for idx, row in allmni.iterrows():
        custcd_str = str(row.get('CUSTCD', '00'))
        try:
            custcd_int = int(float(custcd_str))
        except:
            custcd_int = 0
        
        # Customer categorization matching SAS IF/ELSE
        if custcd_int in [76,77,78,95,96]:
            cust = '08'
        elif custcd_int in [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69]:
            cust = '19'
        elif custcd_int in [0,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99]:
            cust = '29'
        elif custcd_int in [1,71,72,73,74,90,91,92]:
            cust = '39'
        elif custcd_int in [2,3,7,12,81,82,83,84]:
            cust = '49'
        elif custcd_int in [4,5,6,13,20] + list(range(30,41)) + [17]:
            cust = '59'
        else:
            cust = '29'
        
        # NSFR categorization
        if custcd_int in [76,77,78,95,96]:
            cusx = '08'
        elif custcd_int in [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69]:
            cusx = '19'
        elif custcd_int in [0,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99]:
            cusx = '29'
        elif custcd_int in [1,91]:
            cusx = '39'
        elif custcd_int in [71,72,73,74,90,92]:
            cusx = '49'
        elif custcd_int in [2,3,4,5,6,7,12,13,17,20] + list(range(30,41)) + [81,82,83,84]:
            cusx = '59'
        else:
            cusx = '29'
        
        custno = row.get('CUSTNO', 0)
        
        # Special customer overrides (matching SAS exactly)
        if custno in special_39:
            cust = '39'
        if custno in special_49:
            cusx = '49'
        if custno in special_59 or str(row.get('BUSSREG', '')).strip() in special_reg:
            cust = '59'
            cusx = '59'
        
        # ECP logic matching SAS
        ecp_val = str(row.get('ECP', '00')).strip()
        if ecp_val == '' or ecp_val == 'nan':
            ecp_val = '00'
        
        if ecp_val == '01':
            intrate = row.get('INTRATE', 0)
            oprrate = row.get('OPRRATE', 0)
            intrate_val = float(intrate) if pd.notna(intrate) else 0
            oprrate_val = float(oprrate) if pd.notna(oprrate) else 0
            if intrate_val < oprrate_val:
                ecp_val = '01'
            else:
                ecp_val = '00'
        
        billerind = str(row.get('BILLERIND', '')).strip()
        pbmerch = str(row.get('PBMERCH', '')).strip()
        if billerind == 'Y' or pbmerch == 'Y':
            ecp_val = '01'
        
        # SIGN logic matching SAS
        sign = ''
        product = row.get('PRODUCT', 0)
        intplan = row.get('INTPLAN', 0)
        source = str(row.get('SOURCE', '')).strip()
        dtsigned = row.get('DTSIGNED', None)
        
        if (product in [106,151,158,97,164,201,215] or
            (pd.notna(intplan) and (
                (400 <= intplan <= 419) or
                (600 <= intplan <= 658) or
                (720 <= intplan <= 740) or
                (864 <= intplan <= 890) or
                (941 <= intplan <= 967)
            ))):
            sign = 'R '
        elif source != 'PGD' and dtsigned is not None and pd.notna(dtsigned):
            # Handle both SAS numeric dates and Python date objects
            try:
                if isinstance(dtsigned, (datetime, date)):
                    dtsigned_date = dtsigned
                elif isinstance(dtsigned, (int, float)) and dtsigned > 0:
                    dtsigned_date = datetime(1960, 1, 1) + timedelta(days=int(dtsigned))
                else:
                    dtsigned_date = None
                
                if dtsigned_date:
                    years_diff = (rep_date['tdatetime'] - dtsigned_date).days / 365.25
                    if years_diff >= 1:
                        sign = 'R '
            except:
                pass
        
        # BIC: IF BIC = '95317' AND PRODUCT IN (302,315,394,396) THEN BIC = '95315';
        bnmcode_raw = str(row.get('BNMCODE', ''))
        bic = bnmcode_raw[:5] if len(bnmcode_raw) >= 5 else bnmcode_raw
        if bic == '95317' and product in MGIA_PRODUCTS:
            bic = '95315'
        
        remmth = row.get('REMMTH', 1)
        
        # Build codes
        bnmcode = f"{bic}{cust}020000Y"
        cmmcode = f"{bic}{cust}{format_mth_bucket_sas(remmth)}0000Y"
        nsfcode = f"{bic}{cusx}020000Y"
        
        # ICGRP from NEWIC/OLDIC
        newic = str(row.get('NEWIC', '')).strip()
        oldic = str(row.get('OLDIC', '')).strip()
        icgrp = newic.replace(' ', '') if newic and newic != 'nan' else oldic.replace(' ', '')
        
        records.append({
            'BIC': bic,
            'BNMCODE': bnmcode,
            'CMMCODE': cmmcode,
            'BRANCH': str(row.get('BRANCH', '')).strip(),
            'ACCTNO': str(row.get('ACCTNO', '')).strip(),
            'CUSTCD': custcd_str,
            'PRODUCT': product,
            'CURCODE': str(row.get('CURCODE', 'MYR')).strip(),
            'AMOUNT': float(row.get('AMOUNT', 0)),
            'CUSTNO': custno,
            'NEWIC': newic,
            'OLDIC': oldic,
            'CUSTNAME': str(row.get('CUSTNAME', '')).strip(),
            'REM30D': row.get('REM30D', 1),
            'REMMTH': remmth,
            'ECP': ecp_val,
            'CDNO': str(row.get('CDNO', '')).strip(),
            'MATDT': str(row.get('MATDT', '')).strip(),
            'BILLERIND': billerind,
            'SME_TAG': str(row.get('SME_TAG', '')).strip(),
            'PBMERCH': pbmerch,
            'INTPLAN': intplan,
            'ICGRP': icgrp,
            'FDHOLD': str(row.get('FDHOLD', 'N')).strip(),
            'SIGN': sign,
            'NSFCODE': nsfcode,
            'SRC': 'BANKING',
            'SOURCE': source,
            'DTSIGNED': dtsigned,
            'TRX': row.get('TRX', 0)
        })
        
        # Progress indicator
        if (idx + 1) % 500000 == 0:
            print(f"    Processed {idx + 1:,} records...")
    
    df_result = pd.DataFrame(records)
    print(f"  Banking processed: {len(df_result)} records")
    return df_result

# =============================================================================
# SME RECLASSIFICATION AND INSURANCE SPLIT
# Matching SAS: DATA ALLMNI / MERGE ALLMNI TOTMNI TOTEQU; BY ICGRP;
# =============================================================================
def apply_sme_reclassification_and_insurance(df):
    """
    Apply SME reclassification and insurance split.
    """
    print("Applying SME reclassification and insurance split...")
    
    # Calculate TOTICBAL from banking records
    banking = df[df['SRC'] == 'BANKING']
    toticbal = banking.groupby('ICGRP')['AMOUNT'].sum().reset_index()
    toticbal.columns = ['ICGRP', 'TOTICBAL']
    
    # Calculate TOTICEQBAL from treasury records WHERE SUBSTR(BIC,3,3) IN ('810','820','830','83X','840','850')
    treasury = df[df['SRC'] == 'TREASURY']
    bic_condition = treasury['BIC'].str[2:5].isin(['810', '820', '830', '83X', '840', '850'])
    toticeqbal = treasury[bic_condition].groupby('ICGRP')['AMOUNT'].sum().reset_index()
    toticeqbal.columns = ['ICGRP', 'TOTICEQBAL']
    
    # Merge totals into main dataframe
    df = df.merge(toticbal, on='ICGRP', how='left')
    df = df.merge(toticeqbal, on='ICGRP', how='left')
    df['TOTICBAL'] = df['TOTICBAL'].fillna(0)
    df['TOTICEQBAL'] = df['TOTICEQBAL'].fillna(0)
    
    special_custnos = [14094942, 16557696, 3728510, 11335374, 16265490,
                      3523050, 11880426, 16771972, 15241330, 16500538]
    
    result = []
    for _, row in df.iterrows():
        r = row.to_dict()
        
        if r['SRC'] != 'BANKING':
            result.append(r)
            continue
        
        bnmcode = r['BNMCODE']
        bic = r['BIC']
        custno = r['CUSTNO']
        custcd = r.get('CUSTCD', '00')
        sme_tag = r.get('SME_TAG', '')
        toticbal = r.get('TOTICBAL', 0)
        toticeqbal = r.get('TOTICEQBAL', 0)
        totdpbal = toticbal + toticeqbal
        
        # Reclassify retail to SME if total deposits < 5M
        if (custno not in special_custnos and bnmcode[5:7] == '29') or custcd in ['72', '73', '74']:
            if totdpbal < 5000000:
                r['BNMCODE'] = bic + '19' + bnmcode[7:]
                r['CMMCODE'] = bic + '19' + r.get('CMMCODE', '')[7:]
                r['NSFCODE'] = bic + '19' + r.get('NSFCODE', '')[7:]
        
        # Reclassify SME to retail if total deposits >= 5M and not SME tagged
        elif bnmcode[5:7] == '19' and sme_tag == 'N':
            if totdpbal >= 5000000:
                r['BNMCODE'] = bic + '29' + bnmcode[7:]
                r['CMMCODE'] = bic + '29' + r.get('CMMCODE', '')[7:]
                r['NSFCODE'] = bic + '29' + r.get('NSFCODE', '')[7:]
        
        # Apply TAG for 08/19 categories
        if r['BNMCODE'][5:7] in ['08', '19']:
            trx = r.get('TRX', 0)
            sign = r.get('SIGN', '')
            
            if trx == 1:
                tag = '01'
            elif sign in ['R', 'R ']:
                tag = '02'
            else:
                tag = '03'
            
            r['BNMCODE'] = r['BNMCODE'][:7] + tag + '0000Y'
            r['NSFCODE'] = r['NSFCODE'][:7] + tag + '0000Y'
        
        # Apply ECP for CA accounts
        ecp = r.get('ECP', '00')
        if bic in ['95313', '96313']:
            r['BNMCODE'] = r['BNMCODE'][:9] + ecp + '00Y'
            r['CMMCODE'] = r['CMMCODE'][:9] + ecp + '00Y'
            r['NSFCODE'] = r['NSFCODE'][:9] + ecp + '00Y'
        
        # Insurance split logic
        if toticbal > 250000:
            curbal = r['AMOUNT']
            bnm = r['BNMCODE']
            nsf = r['NSFCODE']
            
            # IF SUBSTR(BNMCODE,6,2) IN ('29','39') AND ECP NE '01' THEN DO;
            if bnm[5:7] in ['29', '39'] and ecp != '01':
                # Not fully covered
                r['BNMCODE'] = bnm[:7] + '10' + bnm[10:]
                r['NSFCODE'] = nsf[:7] + '10' + nsf[10:]
                result.append(r)
            else:
                # Insured portion
                insured_amt = (curbal / toticbal) * 250000
                r1 = r.copy()
                r1['AMOUNT'] = insured_amt
                
                # IF SUBSTR(BNMCODE,6,2) IN ('49') AND ECP NE '01' THEN NSFCODE = ...;
                if bnm[5:7] in ['49'] and ecp != '01':
                    r1['NSFCODE'] = nsf[:7] + '10' + nsf[10:]
                
                result.append(r1)
                
                # Uninsured portion
                uninsured_amt = curbal - insured_amt
                r2 = r.copy()
                r2['AMOUNT'] = uninsured_amt
                r2['BNMCODE'] = bnm[:7] + '10' + bnm[10:]
                r2['NSFCODE'] = nsf[:7] + '10' + nsf[10:]
                result.append(r2)
        else:
            result.append(r)
    
    df_result = pd.DataFrame(result)
    print(f"  After reclassification/split: {len(df_result)} records")
    return df_result

# =============================================================================
# NSFR AND FD HOLD PROCESSING
# =============================================================================
def process_nsfr_fdhold(df):
    """
    Process NSFR codes and FD hold flags.
    """
    print("Processing NSFR and FD hold...")
    
    fdhold_records = []
    all_records = []
    
    for _, row in df.iterrows():
        r = row.to_dict()
        bic = r['BIC']
        
        # Only process banking records with BIC 95315/95317
        if bic in ['95315', '95317'] and r['SRC'] == 'BANKING':
            rem30d = r.get('REM30D', 1)
            remmth = r.get('REMMTH', 1)
            fdhold = str(r.get('FDHOLD', 'N')).strip().upper()
            
            # NSFR processing
            r['NSFCODE'] = r['NSFCODE'][:9] + format_mth_bucket_sas(remmth) + '00Y'
            
            if fdhold == 'Y':
                r['NSFCODE'] = r['NSFCODE'][:7] + '20' + r['NSFCODE'][10:]
            
            # LCR processing
            if rem30d <= 1:
                r['BNMCODE'] = r['BNMCODE'][:9] + '0100Y'
            else:
                r['BNMCODE'] = r['BNMCODE'][:9] + '0200Y'
            
            # FDHOLD extraction
            if fdhold == 'Y':
                fdhold_records.append({
                    'BNMCODE': r['BNMCODE'],
                    'CURCODE': r['CURCODE'],
                    'AMOUNT': r['AMOUNT'],
                    'BIC': bic
                })
                r['BNMCODE'] = r['BNMCODE'][:7] + '20' + r['BNMCODE'][10:]
        
        all_records.append(r)
    
    # Process FDHOLD records
    fdhold_processed = []
    if fdhold_records:
        fdhold_df = pd.DataFrame(fdhold_records)
        fdhold_grouped = fdhold_df.groupby(['BNMCODE', 'CURCODE', 'BIC'])['AMOUNT'].sum().reset_index()
        
        for _, row in fdhold_grouped.iterrows():
            bnmcode = row['BNMCODE']
            item_raw = lcrcdmni_fmt(bnmcode[5:9])
            item = item_raw.strip() if item_raw else ''
            
            if item:
                bic = row['BIC']
                amount = row['AMOUNT']
                
                if len(bnmcode) >= 11 and bnmcode[9:11] == '01':
                    if bic == '95315':
                        fdhold_processed.append({
                            'ITEM': item, 'CURCODE': row['CURCODE'],
                            'FDPLEDGE1': amount, 'FDPLEDGE2': 0,
                            'TDPLEDGE1': 0, 'TDPLEDGE2': 0
                        })
                    else:
                        fdhold_processed.append({
                            'ITEM': item, 'CURCODE': row['CURCODE'],
                            'FDPLEDGE1': 0, 'FDPLEDGE2': 0,
                            'TDPLEDGE1': amount, 'TDPLEDGE2': 0
                        })
                else:
                    if bic == '95315':
                        fdhold_processed.append({
                            'ITEM': item, 'CURCODE': row['CURCODE'],
                            'FDPLEDGE1': 0, 'FDPLEDGE2': amount,
                            'TDPLEDGE1': 0, 'TDPLEDGE2': 0
                        })
                    else:
                        fdhold_processed.append({
                            'ITEM': item, 'CURCODE': row['CURCODE'],
                            'FDPLEDGE1': 0, 'FDPLEDGE2': 0,
                            'TDPLEDGE1': 0, 'TDPLEDGE2': amount
                        })
    
    return pd.DataFrame(all_records), fdhold_processed

# =============================================================================
# SHAREX FORMATTING
# =============================================================================
def apply_sharex_format(df):
    """
    Apply SHAREX macro logic.
    """
    print("Applying SHAREX format...")
    
    report_records = []
    
    for _, row in df.iterrows():
        bic = row['BIC']
        bnmcode = row['BNMCODE']
        curcode = row['CURCODE']
        amount = abs(round(row['AMOUNT'] / 1000, 2))
        
        if row['SRC'] == 'BANKING':
            colname_raw = colid_fmt(bic)
            colname = colname_raw.strip() if colname_raw else ''
            
            ecp = bnmcode[9:11] if len(bnmcode) > 11 else '00'
            
            if bic in ['95313', '96313'] and ecp == '01':
                item_raw = lcrcdmniopr_fmt(bnmcode[5:9])
            else:
                item_raw = ''
            
            item = item_raw.strip() if item_raw else ''
            
            if not item:
                item_raw = lcrcdmni_fmt(bnmcode[5:9])
                item = item_raw.strip() if item_raw else ''
            
            remmth = bnmcode[9:11] if len(bnmcode) > 11 else '00'
            
        else:  # TREASURY
            dltype = bnmcode[11:13] if len(bnmcode) > 12 else '00'
            if dltype == '01':
                colname = 'STQ95830'
            else:
                colname_raw = colid_fmt(bic)
                colname = colname_raw.strip() if colname_raw else ''
            
            item_raw = lcrcdequ_fmt(bnmcode[5:7])
            item = item_raw.strip() if item_raw else ''
            
            remmth = bnmcode[7:9] if len(bnmcode) > 9 else '00'
            orimth = bnmcode[9:11] if len(bnmcode) > 10 else '00'
            
            if item == 'B3.30' and orimth == '02':
                item = 'B6.30'
        
        if colname and item:
            if colname[:2] == 'FD' or colname[:3] in ['STD', 'STQ']:
                if remmth == '01':
                    colname = colname + '1'
                else:
                    colname = colname + '2'
            elif colname[:3] in ['NID', 'IBB']:
                try:
                    rem_idx = int(remmth)
                    if 1 <= rem_idx <= 6:
                        colname = colname + f'V{rem_idx}'
                except:
                    pass
            
            report_records.append({
                'ITEM': item,
                'CURCODE': curcode,
                'COLNAME': colname,
                'AMOUNT': amount
            })
    
    return pd.DataFrame(report_records)

# =============================================================================
# GL PROCESSING
# =============================================================================
def process_gl():
    """
    Process WALK.TXT for GL data.
    """
    print("Processing GL data (WALK.TXT)...")
    
    gl_file = f"{PATHS['TEMPLATE']}walk.txt"
    if not os.path.exists(gl_file):
        print(f"  WALK.TXT not found")
        return pd.DataFrame()
    
    gl = read_walk_file(gl_file)
    if gl.empty:
        print("  No GL data")
        return pd.DataFrame()
    
    gl['ITEM'] = gl['SET_ID'].apply(lambda x: lcrcdigl_fmt(x).strip())
    gl['CURCODE'] = gl['SET_ID'].apply(lambda x: lcrcdiglccy_fmt(x).strip())
    
    mask = (gl['ITEM'] == '') & (gl['CURCODE'] != '')
    gl.loc[mask, 'ITEM'] = gl.loc[mask, 'SET_ID'].apply(lambda x: lcrcdgloth_fmt(x).strip())
    
    mask_still = gl['ITEM'] == ''
    gl.loc[mask_still, 'ITEM'] = gl.loc[mask_still, 'SET_ID'].apply(lambda x: lcrcdgl_fmt(x).strip())
    
    gl = gl[gl['ITEM'] != '']
    gl = gl.drop_duplicates(subset=['SET_ID'], keep='first')
    gl = gl.sort_values(['ITEM', 'CURCODE'])
    
    if not gl.empty:
        gl_summary = gl.groupby(['ITEM', 'CURCODE'])['AMOUNT'].sum().reset_index()
        gl_summary.rename(columns={'AMOUNT': 'OTHSOURCE'}, inplace=True)
        print(f"  GL records: {len(gl_summary)}")
        return gl_summary
    
    return pd.DataFrame()

# =============================================================================
# REPORT GENERATION
# =============================================================================
def generate_reports(sharex_df, fdhold_data, gl_data, rep_date):
    """
    Generate LCR reports matching SAS %LCRPRINT macro.
    """
    print("Generating LCR reports...")
    
    if sharex_df.empty:
        print("  No SHAREX data")
        return
    
    deposit = sharex_df.groupby(['ITEM', 'CURCODE', 'COLNAME'])['AMOUNT'].sum().reset_index()
    
    deposit_wide = deposit.pivot_table(
        index=['ITEM', 'CURCODE'],
        columns='COLNAME',
        values='AMOUNT',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    
    if fdhold_data:
        fdhold_df = pd.DataFrame(fdhold_data)
        if not fdhold_df.empty:
            fdhold_summary = fdhold_df.groupby(['ITEM', 'CURCODE']).sum().reset_index()
            deposit_wide = deposit_wide.merge(fdhold_summary, on=['ITEM', 'CURCODE'], how='left')
    
    for col in ['FDPLEDGE1', 'FDPLEDGE2', 'TDPLEDGE1', 'TDPLEDGE2']:
        if col not in deposit_wide.columns:
            deposit_wide[col] = 0
        deposit_wide[col] = deposit_wide[col].fillna(0)
    
    if not gl_data.empty:
        deposit_wide = deposit_wide.merge(gl_data, on=['ITEM', 'CURCODE'], how='outer')
    if 'OTHSOURCE' not in deposit_wide.columns:
        deposit_wide['OTHSOURCE'] = 0
    deposit_wide['OTHSOURCE'] = deposit_wide['OTHSOURCE'].fillna(0)
    
    configs = [
        ('MTH', 'LCRMTH', None),
        ('USD', 'LCRUSD', ['USD']),
        ('SGD', 'LCRSGD', ['SGD']),
        ('MYR', 'LCRMYR', ['MYR'])
    ]
    
    for suffix, prefix, currencies in configs:
        if currencies:
            df_curr = deposit_wide[deposit_wide['CURCODE'].isin(currencies)].copy()
        else:
            df_curr = deposit_wide.copy()
        
        if suffix == 'MTH':
            df_curr.loc[df_curr['CURCODE'].isin(['USD', 'SGD']), 'OTHSOURCE'] = 0
        
        write_report(df_curr, suffix, prefix, rep_date)
    
    print("  Reports generated successfully")

def write_report(df_data, suffix, prefix, rep_date):
    """Write tab-delimited report file matching SAS %LCRPRINT output"""
    
    output_file = f"{PATHS['OUTPUT']}{prefix}{rep_date['mon']}.txt"
    
    template_file = f"{PATHS['TEMPLATE']}templ.txt"
    template_items = []
    try:
        with open(template_file, 'r') as f:
            for line in f:
                if len(line) >= 8:
                    item = line[0:5].strip()
                    idesc = line[7:127].strip() if len(line) > 7 else ''
                    template_items.append({'ITEM': item, 'IDESC': idesc})
    except:
        print(f"  Template not found: {template_file}")
        template_items = []
    
    columns = [
        'FD95315RM1', 'FD95315RM2', 'FD95315RM',
        'FD95317RM1', 'FD95317RM2', 'FD95317RM',
        None, None, None,
        'SA95312RM', 'CA95313RM', 'CA96313FX',
        'STD95830V1', 'STD95830V2', 'STD95830',
        'STQ95830V1', 'STQ95830V2', 'STQ95830',
        'NID95840V1', 'NID95840V2', 'NID95840V3', 'NID95840V4', 'NID95840V5', 'NID95840V6', 'NID95840',
        'IBB9X810V1', 'IBB9X810V2', 'IBB9X810V3', 'IBB9X810V4', 'IBB9X810V5', 'IBB9X810V6', 'IBB9X810',
        None, None, None, None, None, None, None,
        None, None, None, None, None, None, None,
        'OTHSOURCE', 'TOTALV1', 'TOTALDP',
        'FDPLEDGE1', 'FDPLEDGE2', 'TDPLEDGE1', 'TDPLEDGE2'
    ]
    
    delim = '\t'
    
    with open(output_file, 'w') as f:
        f.write(f'PUBLIC ISLAMIC BANK BERHAD\n')
        f.write(f'LIQUIDITY COVERAGE RATIO (LCR) AS AT {rep_date["rdate"]}\n')
        
        for template_row in template_items:
            item = template_row['ITEM']
            idesc = template_row['IDESC']
            
            if idesc.upper().startswith('B)'):
                f.write('\n')
            
            item_data = df_data[df_data['ITEM'] == item]
            
            values = []
            for col in columns:
                if col is None:
                    values.append('')
                elif col in df_data.columns and not item_data.empty:
                    val = item_data[col].sum()
                    if col == 'OTHSOURCE':
                        val = abs(round(val / 1000, 2))
                    if pd.notna(val) and val != 0:
                        values.append(f'{val:,.2f}')
                    else:
                        values.append('')
                else:
                    values.append('')
            
            f.write(f'{idesc}{delim}')
            f.write(delim.join(values))
            f.write('\n')
    
    print(f"  Generated: {output_file}")

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 70)
    print("EIIMLCRM - BNM LCR Reporting (Islamic Banking)")
    print("Python conversion of SAS EIIMLCRM program")
    print("=" * 70)
    
    rep_date = get_report_date()
    print(f"\nReport Date: {rep_date['date'].strftime('%d/%m/%Y')}")
    print(f"Week of Month: {rep_date['nowk']}")
    print(f"Month: {rep_date['mon']}")
    print(f"Year: {rep_date['year']}")
    
    treasury_df = process_treasury(rep_date)
    banking_df = process_banking(rep_date)
    
    if treasury_df.empty and banking_df.empty:
        print("\nNo data found!")
        return
    
    all_data = pd.concat([treasury_df, banking_df], ignore_index=True)
    print(f"\nTotal combined records: {len(all_data):,}")
    
    all_data = apply_sme_reclassification_and_insurance(all_data)
    
    all_data, fdhold_data = process_nsfr_fdhold(all_data)
    
    sharex_df = apply_sharex_format(all_data)
    
    gl_data = process_gl()
    
    generate_reports(sharex_df, fdhold_data, gl_data, rep_date)
    
    print("\n" + "=" * 70)
    print("PROCESSING SUMMARY")
    print("=" * 70)
    
    total = all_data['AMOUNT'].sum() / 1000
    print(f"\nTotal Amount (RM'000): {total:,.2f}")
    
    by_src = all_data.groupby('SRC')['AMOUNT'].sum()
    print("\nBy Source:")
    for src, amt in by_src.items():
        print(f"  {src}: RM {amt/1000:,.2f}K")
    
    by_cur = all_data.groupby('CURCODE')['AMOUNT'].sum()
    print("\nBy Currency:")
    for cur, amt in by_cur.items():
        print(f"  {cur}: RM {amt/1000:,.2f}K")
    
    print("\n" + "=" * 70)
    print("EIIMLCRM Complete")
    print("=" * 70)

if __name__ == "__main__":
    main()
