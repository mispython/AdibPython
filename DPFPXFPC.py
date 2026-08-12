"""
EIIMLCRM - BNM LCR (Liquidity Coverage Ratio) Reporting for Islamic Banking
Consolidates Islamic deposits & treasury positions for BNM LCR reporting.
Outputs: LCR reports by currency with customer categorization (08/19/29/39/49/59)
"""

import pyreadstat
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pathlib import Path
import sys

# Import format definitions from existing programs
from PBBELF import (
    get_customer_category, format_mth_bucket, format_day_bucket,
    CUST_MAP, CUSX_MAP, SPECIAL_CUST, SPECIAL_CUST_NSFR,
    MGIA_PRODUCTS
)
from PBLCRFMT import (
    LCRCDMNI, LCRCDEQU, LCRCDMNIOPR, COLID, 
    LCRCDIGL, LCRCDIGLCCY, LCRCDGLOTH, CTYPE, REMFMT, CMMFMT, REMFMX
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
# UTILITY FUNCTIONS
# =============================================================================
def get_report_date():
    """Calculate report date as yesterday"""
    reptdate = datetime.now() - timedelta(days=1)
    
    # Week of month (1-4)
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
        'reptdate_full': reptdate
    }

def read_sas7bdat(filepath):
    """Read SAS dataset using pyreadstat"""
    try:
        if not os.path.exists(filepath):
            print(f"  File not found: {filepath}")
            return pd.DataFrame()
        df, meta = pyreadstat.read_sas7bdat(filepath)
        # Convert column names to uppercase for consistency with SAS
        df.columns = [col.upper() for col in df.columns]
        return df
    except Exception as e:
        print(f"  Warning: Cannot read {filepath}: {e}")
        return pd.DataFrame()

def read_walk_file(filepath):
    """Read WALK.TXT file for GL data"""
    records = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if len(line) >= 62:
                    set_id = line[1:20].strip()
                    amount_str = line[41:61].strip()
                    sign = line[61:62].strip() if len(line) > 61 else ''
                    
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

# =============================================================================
# DATA PROCESSING
# =============================================================================
def process_treasury(rep_date):
    """Process Treasury (Kapiti) data: K1TBL, K3TBL"""
    print("Processing Treasury...")
    records = []
    
    try:
        # Read treasury tables
        dfs = []
        for tbl in ['k1tbl', 'k3tbl']:
            filepath = f"{PATHS['LCR']}{tbl}.sas7bdat"
            df = read_sas7bdat(filepath)
            if not df.empty:
                dfs.append(df)
        
        if not dfs:
            print("  No treasury data found")
            return records
        
        df = pd.concat(dfs, ignore_index=True)
        df = df.drop_duplicates(subset=['DEALREF'], keep='first')
        print(f"  Read {len(df)} treasury records")
        
        # Merge with UTSAS if available
        utsas_file = f"{PATHS['LCR']}utsas{rep_date['reptmon']}.sas7bdat"
        if os.path.exists(utsas_file):
            utsas = read_sas7bdat(utsas_file)
            if not utsas.empty:
                utsas = utsas.drop_duplicates(subset=['DEALREF'], keep='first')
                df = df.merge(utsas, on='DEALREF', how='left', suffixes=('', '_UTSAS'))
                print(f"  Merged with UTSAS: {len(utsas)} records")
        
        for idx, row in df.iterrows():
            custno = str(row.get('CUSTNO', '')).strip()
            custfiss = row.get('CUSTFISS', 0)
            
            # Handle missing CUSTFISS with UTCTP
            if pd.isna(custfiss) or custfiss == 0:
                utctp = str(row.get('UTCTP', '')).strip()
                if utctp and utctp != '':
                    custfiss = CTYPE.get(utctp, 0)
                else:
                    custfiss = 0
            
            # Customer name handling
            custname = str(row.get('CUSTNAME', '')).strip()
            if custname == '':
                gwsname = str(row.get('GWSHN', '')).strip()
                if gwsname != '':
                    custname = gwsname
                else:
                    custname = custno
            
            # Customer categories - LCR
            cust = get_customer_category(custno, custfiss, CUST_MAP, SPECIAL_CUST, is_treasury=True)
            
            # Customer categories - NSFR
            cusx = get_customer_category(custno, custfiss, CUSX_MAP, SPECIAL_CUST_NSFR, is_treasury=True)
            
            # Maturity
            rem30d = row.get('REM30D', np.nan)
            remmth = row.get('REMMTH', 1)
            
            if pd.isna(rem30d) or rem30d == 0:
                rem30d = remmth
            if rem30d > 1 and remmth > 1:
                rem30d = remmth
            
            # Deal type for BQD
            dltype = '01' if str(row.get('DEALTYPE', '')).strip() == 'BQD' else '00'
            
            # Build codes
            bic = str(row['BNMCODE'])[:5]
            
            # Special handling for certain counterparties (15-1789)
            ori30d = row.get('ORI30D', rem30d)
            rem30d_bucket = format_day_bucket(rem30d)
            
            if custno.upper() in ['AIM','PBL','PBLEUR','PBLNID','PBLUSD','PIVMYR','PBB',
                                   'PBBMYR','PBBUSD','CUST'] and cust == '49' and bic in ['95840','96840']:
                if format_day_bucket(ori30d) > '05' and format_day_bucket(rem30d) > '01':
                    rem30d_bucket = '02'
            
            bnmcode = f"{bic}{cust}{rem30d_bucket}00{dltype}Y"
            cmmcode = f"{bic}{cust}{format_mth_bucket(remmth)}00{dltype}Y"
            nsfcode = f"{bic}{cusx}{rem30d_bucket}00{dltype}Y"
            
            # ICGRP
            custid = str(row.get('CUSTID', '')).replace(' ', '')
            icno = str(row.get('ICNO', '')).replace(' ', '')
            icgrp = custid if custid != '' else icno
            
            records.append({
                'SRC': 'TREASURY',
                'BIC': bic,
                'BNMCODE': bnmcode,
                'CMMCODE': cmmcode,
                'NSFCODE': nsfcode,
                'CURCODE': str(row.get('CURCODE', 'MYR')).strip(),
                'AMOUNT': float(row.get('AMOUNT', 0)),
                'DEALREF': str(row.get('DEALREF', '')).strip(),
                'DEALTYPE': str(row.get('DEALTYPE', '')).strip(),
                'CUSTFISS': custfiss,
                'CUSTNO': custno,
                'CUSTNAME': custname,
                'REM30D': rem30d,
                'REMMTH': remmth,
                'ORI30D': ori30d,
                'MATDT': str(row.get('MATDT', '')).strip(),
                'CUSTID': custid,
                'ICNO': icno,
                'ACCTNO': str(row.get('ACCTNO', '')).strip(),
                'CISNO': str(row.get('CISNO', '')).strip(),
                'CISNAME': str(row.get('CISNAME', '')).strip(),
                'ICGRP': icgrp,
                'ECP': '00',
                'FDHOLD': 'N',
                'SIGN': '',
                'TRX': 0,
                'SME_TAG': '',
                'PRODUCT': 0,
                'INTPLAN': 0,
                'SOURCE': '',
                'DTSIGNED': 0
            })
        
        print(f"  Processed {len(records)} treasury records")
    except Exception as e:
        print(f"  Treasury error: {e}")
        import traceback
        traceback.print_exc()
    
    return records

def process_banking(rep_date):
    """Process Core Banking data: FD, SA, CA, FCYCA"""
    print("Processing Core Banking...")
    records = []
    
    try:
        # Read banking tables
        dfs = []
        for tbl in ['fd', 'sa', 'ca', 'fcyca']:
            filepath = f"{PATHS['LCR']}{tbl}.sas7bdat"
            df = read_sas7bdat(filepath)
            if not df.empty:
                if tbl == 'fd' and 'CUSTCD' in df.columns:
                    df = df.rename(columns={'CUSTCD': 'CUSTCDX'})
                df['_TBL'] = tbl.upper()
                dfs.append(df)
        
        if not dfs:
            print("  No banking data found")
            return records
        
        df = pd.concat(dfs, ignore_index=True)
        print(f"  Read {len(df)} banking records")
        
        # Handle FD specific CUSTCD conversion
        if 'CUSTCDX' in df.columns:
            df['CUSTCD'] = df['CUSTCDX'].apply(
                lambda x: str(int(x)).zfill(2) if pd.notna(x) and x != '' else '00'
            )
        else:
            df['CUSTCD'] = df.get('CUSTCD', '00').astype(str).str.zfill(2)
        
        # Initial customer categorization
        def safe_int_custcd(x):
            try:
                return int(float(x))
            except:
                return 0
        
        df['CUSTCD_INT'] = df['CUSTCD'].apply(safe_int_custcd)
        df['CUST'] = df['CUSTCD_INT'].apply(lambda x: get_customer_category('', x, CUST_MAP))
        df['CUSX'] = df['CUSTCD_INT'].apply(lambda x: get_customer_category('', x, CUSX_MAP))
        
        # Handle missing REM30D
        df['REM30D'] = df['REM30D'].fillna(df['REMMTH'])
        mask = (df['REM30D'] > 1) & (df['REMMTH'] > 1)
        df.loc[mask, 'REM30D'] = df.loc[mask, 'REMMTH']
        
        # Merge with CIS info
        cis_dfs = []
        for prefix, path_key in [('cisdp', 'CISDP'), ('cisca', 'CISCA')]:
            filepath = f"{PATHS[path_key]}deposit.sas7bdat"
            if os.path.exists(filepath):
                cis = read_sas7bdat(filepath)
                if not cis.empty:
                    cis_cols = ['ACCTNO', 'CUSTNO', 'SECCUST', 'NEWIC', 'OLDIC', 'CUSTNAME', 'BUSSREG']
                    available_cols = [c for c in cis_cols if c in cis.columns]
                    cis_dfs.append(cis[available_cols])
        
        if cis_dfs:
            cis_info = pd.concat(cis_dfs, ignore_index=True)
            if 'SECCUST' in cis_info.columns:
                cis_info = cis_info[cis_info['SECCUST'] == '901']
            cis_info = cis_info.drop_duplicates(subset=['ACCTNO'], keep='first')
            df = df.merge(cis_info, on='ACCTNO', how='left', suffixes=('', '_CIS'))
            print(f"  Merged CIS info: {len(cis_info)} accounts")
        
        # Merge with ECP
        ecp_file = f"{PATHS['LIST']}lcr_ecp_{rep_date['reptmon']}.sas7bdat"
        if os.path.exists(ecp_file):
            ecp = read_sas7bdat(ecp_file)
            if not ecp.empty:
                ecp = ecp.drop_duplicates(subset=['ACCTNO'], keep='first')
                df = df.merge(ecp, on='ACCTNO', how='left', suffixes=('', '_ECP'))
                print(f"  Merged ECP data")
        
        # Merge with SME
        sme_file = f"{PATHS['SME']}ibaselsme{rep_date['reptmon']}{rep_date['reptyear']}.sas7bdat"
        if os.path.exists(sme_file):
            sme = read_sas7bdat(sme_file)
            if not sme.empty:
                sme = sme.drop_duplicates(subset=['ACCTNO'], keep='first')
                df = df.merge(sme, on='ACCTNO', how='left', suffixes=('', '_SME'))
                print(f"  Merged SME data")
        
        # Process each record
        special_39 = [4391161, 2115999, 12579649, 13468207, 14300254,
                     14675929, 15327497, 17104931, 12677444, 3703533,
                     5978659, 16185090, 2558344, 10819745]
        special_49 = [4391161, 2115999, 12579649, 13468207, 14675929,
                     15327497, 17104931, 12677444, 3703533, 5978659,
                     16185090, 10819745, 2558344]
        special_59 = [9888664, 11565156, 170458, 17835250, 12078514, 12542063]
        special_reg = ['061904X', '186852H', '211510H', '685480K', '643815V', '734789U']
        
        for idx, row in df.iterrows():
            custno = row.get('CUSTNO', 0)
            custno_cis = row.get('CUSTNO_CIS', custno)
            
            # Apply special customer overrides
            cust = row.get('CUST', '29')
            cusx = row.get('CUSX', '29')
            
            if custno in special_39:
                cust = '39'
            if custno in special_49:
                cusx = '49'
            if custno in special_59 or str(row.get('BUSSREG', '')).strip() in special_reg:
                cust = '59'
                cusx = '59'
            
            # ECP handling
            ecp = str(row.get('ECP', '00')).strip()
            if ecp == '' or ecp == 'nan':
                ecp = '00'
            if ecp == '01':
                intrate = float(row.get('INTRATE', 0)) if pd.notna(row.get('INTRATE', 0)) else 0
                oprrate = float(row.get('OPRRATE', 0)) if pd.notna(row.get('OPRRATE', 0)) else 0
                if intrate < oprrate:
                    ecp = '01'
                else:
                    ecp = '00'
            
            billerind = str(row.get('BILLERIND', '')).strip()
            pbmerch = str(row.get('PBMERCH', '')).strip()
            if billerind == 'Y' or pbmerch == 'Y':
                ecp = '01'
            
            # SIGN indicator
            sign = ''
            product = row.get('PRODUCT', 0)
            intplan = row.get('INTPLAN', 0)
            source = str(row.get('SOURCE', '')).strip()
            dtsigned = row.get('DTSIGNED', 0)
            
            if (product in [106, 151, 158, 97, 164, 201, 215] or
                (pd.notna(intplan) and (
                    (intplan >= 400 and intplan <= 419) or
                    (intplan >= 600 and intplan <= 658) or
                    (intplan >= 720 and intplan <= 740) or
                    (intplan >= 864 and intplan <= 890) or
                    (intplan >= 941 and intplan <= 967)
                ))):
                sign = 'R '
            elif (source != 'PGD' and pd.notna(dtsigned) and dtsigned > 0):
                try:
                    dtsigned_dt = datetime(1960, 1, 1) + timedelta(days=int(dtsigned))
                    if (rep_date['reptdate_full'] - dtsigned_dt).days / 365.25 >= 1:
                        sign = 'R '
                except:
                    pass
            
            # BIC handling for MGIA
            bic = str(row['BNMCODE'])[:5]
            if bic == '95317' and product in MGIA_PRODUCTS:
                bic = '95315'
            
            # Build codes
            remmth = row.get('REMMTH', 1)
            rem30d = row.get('REM30D', 1)
            
            bnmcode = f"{bic}{cust}020000Y"
            cmmcode = f"{bic}{cust}{format_mth_bucket(remmth)}0000Y"
            nsfcode = f"{bic}{cusx}020000Y"
            
            # Get ICGRP from NEWIC or OLDIC
            newic = str(row.get('NEWIC', '')).strip()
            oldic = str(row.get('OLDIC', '')).strip()
            icgrp = newic.replace(' ', '') if newic != '' and newic != 'nan' else oldic.replace(' ', '')
            
            records.append({
                'SRC': 'BANKING',
                'BIC': bic,
                'BNMCODE': bnmcode,
                'CMMCODE': cmmcode,
                'NSFCODE': nsfcode,
                'BRANCH': str(row.get('BRANCH', '')).strip(),
                'ACCTNO': str(row.get('ACCTNO', '')).strip(),
                'CUSTCD': str(row.get('CUSTCD', '00')).strip(),
                'PRODUCT': product,
                'CURCODE': str(row.get('CURCODE', 'MYR')).strip(),
                'AMOUNT': float(row.get('AMOUNT', 0)),
                'CUSTNO': custno,
                'NEWIC': newic,
                'OLDIC': oldic,
                'CUSTNAME': str(row.get('CUSTNAME', '')).strip(),
                'REM30D': rem30d,
                'REMMTH': remmth,
                'ECP': ecp,
                'CDNO': str(row.get('CDNO', '')).strip(),
                'MATDT': str(row.get('MATDT', '')).strip(),
                'BILLERIND': billerind,
                'SME_TAG': str(row.get('SME_TAG', '')).strip(),
                'PBMERCH': pbmerch,
                'INTPLAN': intplan,
                'ICGRP': icgrp,
                'FDHOLD': str(row.get('FDHOLD', 'N')).strip(),
                'SIGN': sign,
                'SOURCE': source,
                'DTSIGNED': dtsigned,
                'TRX': row.get('TRX', 0),
                'DEALREF': '',
                'DEALTYPE': '',
                'CUSTFISS': 0,
                'ORI30D': 0,
                'CUSTID': '',
                'ICNO': '',
                'CISNO': '',
                'CISNAME': ''
            })
        
        print(f"  Processed {len(records)} banking records")
    except Exception as e:
        print(f"  Banking error: {e}")
        import traceback
        traceback.print_exc()
    
    return records

# =============================================================================
# SME RECLASSIFICATION AND INSURANCE SPLIT
# =============================================================================
def apply_sme_reclassification(records):
    """Apply SME reclassification logic"""
    print("Applying SME reclassification...")
    
    # Calculate total deposits by ICGRP
    icgrp_totals = {}
    for r in records:
        icgrp = r.get('ICGRP', '')
        if icgrp:
            icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + r.get('AMOUNT', 0)
    
    special_custnos = [14094942, 16557696, 3728510, 11335374, 16265490,
                      3523050, 11880426, 16771972, 15241330, 16500538]
    
    for r in records:
        icgrp = r.get('ICGRP', '')
        custno = r.get('CUSTNO', 0)
        custcd = str(r.get('CUSTCD', '00')).strip()
        sme_tag = str(r.get('SME_TAG', '')).strip()
        
        toticbal = icgrp_totals.get(icgrp, 0)
        toticeqbal = 0  # For banking records only
        
        bnmcode = r['BNMCODE']
        cmmcode = r.get('CMMCODE', '')
        nsfcode = r.get('NSFCODE', '')
        bic = r.get('BIC', '')
        
        # Calculate total deposit balance
        totdpbal = toticbal + toticeqbal
        
        # Reclassify retail to SME if total deposits < 5M
        if (custno not in special_custnos and bnmcode[5:7] == '29') or custcd in ['72', '73', '74']:
            if totdpbal < 5000000:
                r['BNMCODE'] = bic + '19' + bnmcode[7:]
                if cmmcode:
                    r['CMMCODE'] = bic + '19' + cmmcode[7:]
                if nsfcode:
                    r['NSFCODE'] = bic + '19' + nsfcode[7:]
        
        # Reclassify SME to retail if total deposits >= 5M and not SME tagged
        elif bnmcode[5:7] == '19' and sme_tag == 'N':
            if totdpbal >= 5000000:
                r['BNMCODE'] = bic + '29' + bnmcode[7:]
                if cmmcode:
                    r['CMMCODE'] = bic + '29' + cmmcode[7:]
                if nsfcode:
                    r['NSFCODE'] = bic + '29' + nsfcode[7:]
        
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
            if r.get('NSFCODE', ''):
                r['NSFCODE'] = r['NSFCODE'][:7] + tag + '0000Y'
        
        # Apply ECP for CA accounts
        ecp = r.get('ECP', '00')
        if bic in ['95313', '96313']:
            r['BNMCODE'] = r['BNMCODE'][:9] + ecp + '00Y'
            if r.get('CMMCODE', ''):
                r['CMMCODE'] = r['CMMCODE'][:9] + ecp + '00Y'
            if r.get('NSFCODE', ''):
                r['NSFCODE'] = r['NSFCODE'][:9] + ecp + '00Y'
    
    return records

def apply_insurance_split(records):
    """Split insured/uninsured portions"""
    print("Applying insurance split...")
    
    # Calculate total banking deposits by ICGRP
    icgrp_totals = {}
    for r in records:
        if r.get('SRC') == 'BANKING':
            icgrp = r.get('ICGRP', '')
            if icgrp:
                icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + r.get('AMOUNT', 0)
    
    result = []
    for r in records:
        icgrp = r.get('ICGRP', '')
        toticbal = icgrp_totals.get(icgrp, 0)
        
        if toticbal > 250000 and r.get('SRC') == 'BANKING':
            bnmcode = r['BNMCODE']
            nsfcode = r.get('NSFCODE', '')
            curbal = r['AMOUNT']
            
            # Check if fully covered
            if bnmcode[5:7] in ['29', '39'] and r.get('ECP', '00') != '01':
                # Not fully covered - all becomes uninsured
                r['BNMCODE'] = bnmcode[:7] + '10' + bnmcode[10:]
                if nsfcode:
                    r['NSFCODE'] = nsfcode[:7] + '10' + nsfcode[10:]
                result.append(r)
            else:
                # Split into insured and uninsured
                insured_amt = (curbal / toticbal) * 250000
                uninsured_amt = curbal - insured_amt
                
                # Insured portion
                r1 = r.copy()
                r1['AMOUNT'] = insured_amt
                if bnmcode[5:7] in ['49'] and r.get('ECP', '00') != '01':
                    if r1.get('NSFCODE', ''):
                        r1['NSFCODE'] = r1['NSFCODE'][:7] + '10' + r1['NSFCODE'][10:]
                result.append(r1)
                
                # Uninsured portion
                r2 = r.copy()
                r2['AMOUNT'] = uninsured_amt
                r2['BNMCODE'] = bnmcode[:7] + '10' + bnmcode[10:]
                if r2.get('NSFCODE', ''):
                    r2['NSFCODE'] = nsfcode[:7] + '10' + nsfcode[10:]
                result.append(r2)
        else:
            result.append(r)
    
    return result

def process_nsfr_and_fdhold(records):
    """Process NSFR codes and FD hold flags"""
    print("Processing NSFR and FD hold...")
    
    all_records = []
    fd_hold_records = []
    
    for r in records:
        bic = r.get('BIC', '')
        bnmcode = r.get('BNMCODE', '')
        nsfcode = r.get('NSFCODE', '')
        rem30d = r.get('REM30D', 1)
        remmth = r.get('REMMTH', 1)
        fdhold = str(r.get('FDHOLD', 'N')).strip().upper()
        
        # NSFR processing for FD products
        if bic in ['95315', '95317'] and r.get('SRC') == 'BANKING':
            if nsfcode:
                nsfcode = nsfcode[:9] + format_mth_bucket(remmth) + '00Y'
                if fdhold == 'Y':
                    nsfcode = nsfcode[:7] + '20' + nsfcode[10:]
                r['NSFCODE'] = nsfcode
            
            # BNMCODE for FD holdings
            if rem30d <= 1:
                r['BNMCODE'] = bnmcode[:9] + '0100Y'
            else:
                r['BNMCODE'] = bnmcode[:9] + '0200Y'
            
            if fdhold == 'Y':
                fd_hold_records.append({
                    'BNMCODE': r['BNMCODE'],
                    'CURCODE': r.get('CURCODE', 'MYR'),
                    'AMOUNT': r['AMOUNT'],
                    'BIC': bic
                })
                r['BNMCODE'] = bnmcode[:7] + '20' + bnmcode[10:]
        
        all_records.append(r)
    
    # Process FD hold records
    fd_hold_processed = []
    fd_hold_df = pd.DataFrame(fd_hold_records) if fd_hold_records else pd.DataFrame()
    
    if not fd_hold_df.empty:
        for _, row in fd_hold_df.iterrows():
            bnmcode = row['BNMCODE']
            item = LCRCDMNI.get(bnmcode[5:9], '')
            if item:
                bic = bnmcode[:5]
                amount = row['AMOUNT']
                
                if bnmcode[9:11] == '01':  # REM30D <= 1
                    if bic == '95315':
                        fd_hold_processed.append({
                            'BNMCODE': bnmcode,
                            'CURCODE': row['CURCODE'],
                            'FDPLEDGE1': amount,
                            'FDPLEDGE2': 0,
                            'TDPLEDGE1': 0,
                            'TDPLEDGE2': 0,
                            'ITEM': item,
                            'BIC': bic
                        })
                    else:
                        fd_hold_processed.append({
                            'BNMCODE': bnmcode,
                            'CURCODE': row['CURCODE'],
                            'FDPLEDGE1': 0,
                            'FDPLEDGE2': 0,
                            'TDPLEDGE1': amount,
                            'TDPLEDGE2': 0,
                            'ITEM': item,
                            'BIC': bic
                        })
                else:
                    if bic == '95315':
                        fd_hold_processed.append({
                            'BNMCODE': bnmcode,
                            'CURCODE': row['CURCODE'],
                            'FDPLEDGE1': 0,
                            'FDPLEDGE2': amount,
                            'TDPLEDGE1': 0,
                            'TDPLEDGE2': 0,
                            'ITEM': item,
                            'BIC': bic
                        })
                    else:
                        fd_hold_processed.append({
                            'BNMCODE': bnmcode,
                            'CURCODE': row['CURCODE'],
                            'FDPLEDGE1': 0,
                            'FDPLEDGE2': 0,
                            'TDPLEDGE1': 0,
                            'TDPLEDGE2': amount,
                            'ITEM': item,
                            'BIC': bic
                        })
    
    return all_records, fd_hold_processed

# =============================================================================
# REPORT GENERATION
# =============================================================================
def process_gl_data(rep_date):
    """Process WALK.TXT for GL data"""
    print("Processing GL data...")
    
    gl_file = f"{PATHS['TEMPLATE']}walk.txt"
    if not os.path.exists(gl_file):
        print(f"  WALK.TXT not found: {gl_file}")
        return pd.DataFrame()
    
    gl = read_walk_file(gl_file)
    if gl.empty:
        return gl
    
    # Apply item and currency mappings
    gl['ITEM'] = gl['SET_ID'].map(LCRCDIGL)
    gl['CURCODE'] = gl['SET_ID'].map(LCRCDIGLCCY)
    
    # For items without ITEM but with CURCODE, use other mapping
    mask = (gl['ITEM'].isna() | (gl['ITEM'] == '')) & (gl['CURCODE'].notna() & (gl['CURCODE'] != ''))
    gl.loc[mask, 'ITEM'] = gl.loc[mask, 'SET_ID'].map(LCRCDGLOTH)
    
    # Filter valid items
    gl = gl[gl['ITEM'].notna() & (gl['ITEM'] != '')]
    gl = gl.drop_duplicates(subset=['SET_ID'], keep='first')
    
    # Summarize by ITEM and CURCODE
    if not gl.empty:
        gl_summary = gl.groupby(['ITEM', 'CURCODE'])['AMOUNT'].sum().reset_index()
        gl_summary.columns = ['ITEM', 'CURCODE', 'OTHSOURCE']
        print(f"  Processed {len(gl_summary)} GL records")
        return gl_summary
    
    return pd.DataFrame()

def generate_sharex_report(records, source_type):
    """Generate SHAREX formatted records for reporting"""
    report_records = []
    
    for r in records:
        bnmcode = r.get('BNMCODE', '')
        if not bnmcode or len(bnmcode) < 13:
            continue
        
        bic = bnmcode[:5]
        curcode = r.get('CURCODE', 'MYR')
        amount = abs(round(r.get('AMOUNT', 0) / 1000, 2))
        
        if source_type == 'BANKING':
            colname = COLID.get(bic, '')
            ecp = bnmcode[9:11] if len(bnmcode) > 11 else '00'
            
            # Get item code
            if bic in ['95313', '96313'] and ecp == '01':
                item = LCRCDMNIOPR.get(bnmcode[5:9], '')
            if not item if 'item' in locals() else True:
                item = LCRCDMNI.get(bnmcode[5:9], '')
            
            remmth = bnmcode[9:11] if len(bnmcode) > 11 else '00'
            
        else:  # TREASURY
            dltype = bnmcode[11:13] if len(bnmcode) > 13 else '00'
            if dltype == '01':
                colname = 'STQ95830'
            else:
                colname = COLID.get(bic, '')
            
            item = LCRCDEQU.get(bnmcode[5:7], '')
            remmth = bnmcode[7:9] if len(bnmcode) > 9 else '00'
            orimth = bnmcode[9:11] if len(bnmcode) > 11 else '00'
            
            if item == 'B3.30' and orimth == '02':
                item = 'B6.30'
        
        if not colname or not item:
            continue
        
        # Apply maturity suffix
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
    
    return report_records

def generate_lcr_reports(all_data, fd_hold_data, gl_data, rep_date):
    """Generate final LCR reports by currency"""
    print("Generating LCR reports...")
    
    # Separate banking and treasury records
    banking = [r for r in all_data if r.get('SRC') == 'BANKING']
    treasury = [r for r in all_data if r.get('SRC') == 'TREASURY']
    
    # Generate SHAREX reports
    banking_report = generate_sharex_report(banking, 'BANKING')
    treasury_report = generate_sharex_report(treasury, 'TREASURY')
    
    # Combine all source records
    all_report = banking_report + treasury_report
    
    if not all_report:
        print("  No report records generated")
        return
    
    df_report = pd.DataFrame(all_report)
    
    # Summarize by ITEM, CURCODE, COLNAME
    deposit = df_report.groupby(['ITEM', 'CURCODE', 'COLNAME'])['AMOUNT'].sum().reset_index()
    
    # Pivot to wide format
    deposit_wide = deposit.pivot_table(
        index=['ITEM', 'CURCODE'],
        columns='COLNAME',
        values='AMOUNT',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    
    # Process FD hold data
    fd_hold_df = pd.DataFrame(fd_hold_data) if fd_hold_data else pd.DataFrame()
    if not fd_hold_df.empty:
        fd_hold_summary = fd_hold_df.groupby(['ITEM', 'CURCODE']).agg({
            'FDPLEDGE1': 'sum',
            'FDPLEDGE2': 'sum',
            'TDPLEDGE1': 'sum',
            'TDPLEDGE2': 'sum'
        }).reset_index()
        
        # Merge with deposit
        deposit_wide = deposit_wide.merge(fd_hold_summary, on=['ITEM', 'CURCODE'], how='left')
    
    # Fill missing FD hold columns
    for col in ['FDPLEDGE1', 'FDPLEDGE2', 'TDPLEDGE1', 'TDPLEDGE2']:
        if col not in deposit_wide.columns:
            deposit_wide[col] = 0
        deposit_wide[col] = deposit_wide[col].fillna(0)
    
    # Add GL data
    if not gl_data.empty:
        deposit_wide = deposit_wide.merge(gl_data, on=['ITEM', 'CURCODE'], how='outer')
        if 'OTHSOURCE' not in deposit_wide.columns:
            deposit_wide['OTHSOURCE'] = 0
        deposit_wide['OTHSOURCE'] = deposit_wide['OTHSOURCE'].fillna(0)
    else:
        deposit_wide['OTHSOURCE'] = 0
    
    # Generate reports by currency
    currency_configs = [
        ('MTH', 'LCRMTH', None),  # All currencies
        ('USD', 'LCRUSD', ['USD']),
        ('SGD', 'LCRSGD', ['SGD']),
        ('MYR', 'LCRMYR', ['MYR'])
    ]
    
    for suffix, prefix, currencies in currency_configs:
        if currencies:
            df_curr = deposit_wide[deposit_wide['CURCODE'].isin(currencies)]
        else:
            df_curr = deposit_wide
        
        # For main report, zero out USD/SGD OTHSOURCE
        if suffix == 'MTH':
            df_curr.loc[df_curr['CURCODE'].isin(['USD', 'SGD']), 'OTHSOURCE'] = 0
        
        # Generate report file
        generate_report_file(df_curr, suffix, rep_date, prefix)
    
    print("  Reports generated successfully")

def generate_report_file(df_data, suffix, rep_date, prefix):
    """Generate individual LCR report text file"""
    
    # Read template
    template_file = f"{PATHS['TEMPLATE']}templ.txt"
    template_items = []
    
    try:
        with open(template_file, 'r') as f:
            for line in f:
                if len(line) >= 8:
                    item = line[0:5].strip()
                    idesc = line[7:].strip() if len(line) > 7 else ''
                    template_items.append({'ITEM': item, 'IDESC': idesc})
    except:
        print(f"  Template file not found: {template_file}")
        return
    
    # Calculate totals
    if not df_data.empty:
        # Group by ITEM for sub-totals
        item_summary = df_data.groupby('ITEM').sum().reset_index()
        
        # Group by PART for section totals
        df_data['PART'] = df_data['ITEM'].str[0]
        part_summary = df_data.groupby('PART').sum().reset_index()
        
        # Create section total items
        section_totals = []
        for _, row in part_summary.iterrows():
            if row['PART'] == 'A':
                section_totals.append({'ITEM': 'A9.01', 'PART': 'A'})
            else:
                section_totals.append({'ITEM': 'B9.01', 'PART': 'B'})
    
    # Generate output file
    output_file = f"{PATHS['OUTPUT']}{prefix}{rep_date['mon']}.txt"
    delim = '\t'  # '05'x in SAS (tab delimiter)
    
    with open(output_file, 'w') as f:
        # Header
        f.write(f'PUBLIC ISLAMIC BANK BERHAD\n')
        f.write(f'LIQUIDITY COVERAGE RATIO (LCR) AS AT {rep_date["rdate"]}\n')
        
        # Column headers (simplified)
        headers = [
            '', 'MGIA(P)', '', '', 'TD-I(Q)', '', '', 'FX TD-I(R)', '', '',
            'SA(S)', 'CA(T)', '', 
            'SHORT TERM DEPOSIT(U)', '', '', '',
            'RM&FX NID ISSUED', '', '', '', '', '', '', '',
            'RM&FX IBB', '', '', '', '', '', '', '',
            'RM&FX REPOS', '', '', '', '', '', '', '',
            'RM&FX BAS PAYABLE', '', '', '', '', '', '',
            'OTHER SOURCE', 'TOTAL', 'TOTAL', 'FD PLEDGED', '', 'TD PLEDGED', ''
        ]
        f.write(delim.join(headers) + '\n')
        
        sub_headers = [
            '', '<=30(P1)', '>30(P2)', 'TOTAL(P)', '<=30(Q1)', '>30(Q2)', 'TOTAL(Q)',
            '<=30(R1)', '>30(R2)', 'TOTAL(R)', '', 'RM', 'FX',
            '<=30(U1)', '>30(U2)', 'TOTAL(U)', 
            '<=30(V1)', '>30(V2)', 'TOTAL(V)',
            '<=30(W1)', '>30-3M(W2)', '>3-6M(W3)', '>6-9M(W4)', '>9-12M(W5)', '>1Y(W6)', 'TOTAL(W)',
            '<=30(X1)', '>30-3M(X2)', '>3-6M(X3)', '>6-9M(X4)', '>9-12M(X5)', '>1Y(X6)', 'TOTAL(X)',
            '<=30(Y1)', '>30-3M(Y2)', '>3-6M(Y3)', '>6-9M(Y4)', '>9-12M(Y5)', '>1Y(Y6)', 'TOTAL(Y)',
            '<=30(Z1)', '>30-3M(Z2)', '>3-6M(Z3)', '>6-9M(Z4)', '>9-12M(Z5)', '>1Y(Z6)', 'TOTAL(Z)',
            '(GL)', 'TOTAL', 'TOTAL', '<=30', '>30', '<=30', '>30'
        ]
        f.write(delim.join(sub_headers) + '\n')
        
        # Data rows
        for template_row in template_items:
            item = template_row['ITEM']
            idesc = template_row['IDESC']
            
            # Add blank line before B sections
            if idesc and idesc.upper().startswith('B)'):
                f.write('\n')
            
            # Get data for this item
            row_data = []
            if not df_data.empty and item in df_data['ITEM'].values:
                item_data = df_data[df_data['ITEM'] == item].iloc[0]
                
                # Extract values for each column
                cols_to_extract = [
                    'FD95315RM1', 'FD95315RM2', 'FD95315RM', 'FD95317RM1', 'FD95317RM2', 'FD95317RM',
                    '', '', '', 'SA95312RM', 'CA95313RM', 'CA96313FX',
                    'STD95830V1', 'STD95830V2', 'STD95830',
                    'STQ95830V1', 'STQ95830V2', 'STQ95830',
                    'NID95840V1', 'NID95840V2', 'NID95840V3', 'NID95840V4', 'NID95840V5', 'NID95840V6', 'NID95840',
                    'IBB9X810V1', 'IBB9X810V2', 'IBB9X810V3', 'IBB9X810V4', 'IBB9X810V5', 'IBB9X810V6', 'IBB9X810',
                    '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                    'OTHSOURCE', 'TOTALV1', 'TOTALDP',
                    'FDPLEDGE1', 'FDPLEDGE2', 'TDPLEDGE1', 'TDPLEDGE2'
                ]
                
                values = []
                for col in cols_to_extract:
                    if col == '':
                        values.append('')
                    elif col in item_data.index:
                        val = item_data[col]
                        if pd.notna(val) and val != 0:
                            values.append(f"{val:,.2f}")
                        else:
                            values.append('')
                    else:
                        values.append('')
                
                # Calculate totals
                fd95315rm1 = item_data.get('FD95315RM1', 0)
                fd95315rm2 = item_data.get('FD95315RM2', 0)
                fd95317rm1 = item_data.get('FD95317RM1', 0)
                fd95317rm2 = item_data.get('FD95317RM2', 0)
                
                fd95315rm = fd95315rm1 + fd95315rm2
                fd95317rm = fd95317rm1 + fd95317rm2
                
                std95830 = sum([item_data.get(f'STD95830V{i}', 0) for i in range(1, 7)])
                stq95830 = sum([item_data.get(f'STQ95830V{i}', 0) for i in range(1, 7)])
                nid95840 = sum([item_data.get(f'NID95840V{i}', 0) for i in range(1, 7)])
                ibb9x810 = sum([item_data.get(f'IBB9X810V{i}', 0) for i in range(1, 7)])
                
                othsource = item_data.get('OTHSOURCE', 0)
                totalv1 = (fd95315rm + fd95317rm1 + item_data.get('SA95312RM', 0) + 
                          item_data.get('CA95313RM', 0) + item_data.get('CA96313FX', 0) +
                          std95830 + stq95830 + nid95840 + 
                          item_data.get('IBB9X810V1', 0) + othsource)
                totaldp = (fd95315rm + fd95317rm + item_data.get('SA95312RM', 0) + 
                          item_data.get('CA95313RM', 0) + item_data.get('CA96313FX', 0) +
                          std95830 + stq95830 + nid95840 + ibb9x810 + othsource)
                
                # Format output line
                output_line = f"{idesc}\t"
                
                # Add all numeric columns
                numeric_cols = [
                    fd95315rm1, fd95315rm2, fd95315rm,
                    fd95317rm1, fd95317rm2, fd95317rm,
                    '', '', '',
                    item_data.get('SA95312RM', 0),
                    item_data.get('CA95313RM', 0),
                    item_data.get('CA96313FX', 0),
                    item_data.get('STD95830V1', 0),
                    item_data.get('STD95830V2', 0),
                    std95830,
                    item_data.get('STQ95830V1', 0),
                    item_data.get('STQ95830V2', 0),
                    stq95830
                ]
                
                # NID values
                for i in range(1, 7):
                    numeric_cols.append(item_data.get(f'NID95840V{i}', 0))
                numeric_cols.append(nid95840)
                
                # IBB values
                for i in range(1, 7):
                    numeric_cols.append(item_data.get(f'IBB9X810V{i}', 0))
                numeric_cols.append(ibb9x810)
                
                # Empty columns for REPOS and BAS
                numeric_cols.extend([''] * 14)
                
                # Other source and totals
                numeric_cols.extend([
                    othsource, totalv1, totaldp,
                    item_data.get('FDPLEDGE1', 0),
                    item_data.get('FDPLEDGE2', 0),
                    item_data.get('TDPLEDGE1', 0),
                    item_data.get('TDPLEDGE2', 0)
                ])
                
                formatted_values = []
                for val in numeric_cols:
                    if val == '':
                        formatted_values.append('')
                    elif isinstance(val, (int, float)) and val != 0:
                        formatted_values.append(f"{val:,.2f}")
                    else:
                        formatted_values.append('')
                
                output_line += '\t'.join(formatted_values)
            else:
                # Empty row with just description
                output_line = f"{idesc}\t" + '\t' * 50
            
            f.write(output_line + '\n')
    
    print(f"  Generated {output_file}")

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 70)
    print("EIIMLCRM - BNM LCR Reporting (Islamic Banking)")
    print("=" * 70)
    
    # Get report date
    rep_date = get_report_date()
    print(f"\nReport Date: {rep_date['date'].strftime('%d/%m/%Y')}")
    print(f"Week: {rep_date['nowk']}, Month: {rep_date['mon']}")
    
    # Process Treasury
    treasury_records = process_treasury(rep_date)
    print(f"  Total treasury records: {len(treasury_records):,}")
    
    # Process Core Banking
    banking_records = process_banking(rep_date)
    print(f"  Total banking records: {len(banking_records):,}")
    
    # Combine all records
    all_data = treasury_records + banking_records
    if not all_data:
        print("\n⚠️ No data found!")
        return
    
    print(f"\nTotal combined records: {len(all_data):,}")
    
    # Apply SME reclassification
    all_data = apply_sme_reclassification(all_data)
    
    # Apply insurance split
    all_data = apply_insurance_split(all_data)
    print(f"  Records after insurance split: {len(all_data):,}")
    
    # Process NSFR and FD hold
    all_data, fd_hold_data = process_nsfr_and_fdhold(all_data)
    
    # Process GL data
    gl_data = process_gl_data(rep_date)
    
    # Generate reports
    generate_lcr_reports(all_data, fd_hold_data, gl_data, rep_date)
    
    # Summary statistics
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    df_summary = pd.DataFrame(all_data)
    total_amount = df_summary['AMOUNT'].sum()
    
    print(f"\nTotal Amount: RM {total_amount:,.2f}")
    print(f"\nBy Source:")
    src_summary = df_summary.groupby('SRC')['AMOUNT'].sum()
    for src, amt in src_summary.items():
        print(f"  {src}: RM {amt:,.2f}")
    
    if 'CURCODE' in df_summary.columns:
        print(f"\nBy Currency:")
        curr_summary = df_summary.groupby('CURCODE')['AMOUNT'].sum()
        for curr, amt in curr_summary.items():
            print(f"  {curr}: RM {amt:,.2f}")
    
    print("\n" + "=" * 70)
    print("✓ EIIMLCRM Complete")
    print("=" * 70)

if __name__ == "__main__":
    main()
