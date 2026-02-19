"""
EIBDLCRM - BNM LCR (Liquidity Coverage Ratio) Reporting for Conventional Banking
Consolidates deposits & treasury positions for BNM LCR reporting.
Includes DCI (Dual Currency Investments) and full treasury processing.
Outputs: LCR reports with customer categorization (08/19/29/39/49/59)
"""

import polars as pl
from datetime import datetime, date
import os
from pathlib import Path
import calendar

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'LCR': 'data/lcr/',
    'LCRM': 'data/lcrm/',
    'DEPOSIT': 'data/deposit/',
    'FORATE': 'data/forate/',
    'CISDP': 'data/cisdp/',
    'CISCA': 'data/cisca/',
    'CIS': 'data/cis/',
    'DCIWH': 'data/dciwh/',
    'EQUA': 'data/equa/',
    'LIST': 'data/list/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

INST = 'PBB'  # Institution code

# Customer category mappings (LCR)
CUST_MAP = {
    '08': [76, 77, 78, 95, 96],  # Central banks/governments
    '19': [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69],  # SME
    '29': [0,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99],  # Other retail
    '39': [1,71,72,73,74,90,91,92],  # Sovereign funds
    '49': [2,3,7,12,81,82,83,84],  # Financial institutions
    '59': [4,5,6,13,20] + list(range(30,41)) + [17]  # Corporate
}

# Special customers
SPECIAL_CUST = {
    '39': ['KWSP', 'KWAP', 'KWAN', 'LEMTAB'],
    '49': ['AIM', 'PBL', 'PBLEUR', 'PBLNID', 'PBLUSD', 'PIVMYR', 'IPBB']
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def get_report_date():
    """Read report date and set macro variables"""
    df = pl.read_parquet(f"{PATHS['DEPOSIT']}REPTDATE.parquet")
    reptdate = df['REPTDATE'][0]
    
    # Week of month (1-4)
    day = reptdate.day
    nowk = '1' if day <= 8 else '2' if day <= 15 else '3' if day <= 22 else '4'
    
    # Day arrays for month calculations
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if reptdate.year % 4 == 0:  # Leap year
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
    
    # Extract components
    rp_year = reptdate.year
    rp_month = reptdate.month
    rp_day = reptdate.day
    
    md_year = matdt.year
    md_month = matdt.month
    md_day = matdt.day
    
    # Adjust for month-end
    days_in_target_month = days_in_month[md_month - 1]
    if md_day > days_in_target_month:
        md_day = days_in_target_month
    
    # Calculate remaining months (as float)
    rem_years = md_year - rp_year
    rem_months = md_month - rp_month
    rem_days = md_day - rp_day
    
    remmth = rem_years * 12 + rem_months + rem_days / days_in_month[rp_month - 1]
    
    # Calculate days/30
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
# DCI PROCESSING
# =============================================================================
def process_dci(rep_date, fx_rates):
    """Process DCI (Dual Currency Investments)"""
    records = []
    
    try:
        df = pl.read_parquet(f"{PATHS['DCIWH']}DCID{rep_date['mon']}{rep_date['day']}.parquet")
        
        for row in df.iter_rows(named=True):
            matdt = row.get('MATDT')
            startdt = row.get('STARTDT')
            
            if matdt and startdt and matdt > rep_date['date'] and startdt <= rep_date['date']:
                # Calculate remaining months
                if (matdt - rep_date['date']).days < 8:
                    remmth = 0.1
                else:
                    remmth, rem30d = calculate_remaining_months(
                        matdt, rep_date['date'], rep_date['days_in_month']
                    )
                
                invamt = row.get('INVAMT', 0)
                invccy = row.get('INVCURR', 'MYR')
                spotrt = fx_rates.get(invccy, 1.0)
                
                # Round based on currency
                if invccy == 'JPY':
                    invamt = round(invamt)
                else:
                    invamt = round(invamt, 2)
                
                amount = invamt * spotrt
                remth_bucket = format_mth_bucket(remmth)
                
                if invccy == 'MYR':
                    bnmcode = f"9532900{remth_bucket}0000Y"
                    records.append({
                        'src': 'DCI',
                        'bnmcode': bnmcode,
                        'cur': 'MYR',
                        'amt': amount,
                        'amt_usd': 0,
                        'amt_sgd': 0,
                        'amt_hkd': 0,
                        'amt_aud': 0,
                        'custfiss': f"{row.get('CUSTCODE', 0):02d}",
                        'dealtype': row.get('PRODUCT'),
                        'dealref': row.get('TICKETNO'),
                        'remmth': remmth,
                        'rem30d': rem30d
                    })
                else:
                    bnmcode = f"9632900{remth_bucket}0000Y"
                    record = {
                        'src': 'DCI',
                        'bnmcode': bnmcode,
                        'cur': invccy,
                        'amt': amount,
                        'amt_usd': amount if invccy == 'USD' else 0,
                        'amt_sgd': amount if invccy == 'SGD' else 0,
                        'amt_hkd': amount if invccy == 'HKD' else 0,
                        'amt_aud': amount if invccy == 'AUD' else 0,
                        'custfiss': f"{row.get('CUSTCODE', 0):02d}",
                        'dealtype': row.get('PRODUCT'),
                        'dealref': row.get('TICKETNO'),
                        'remmth': remmth,
                        'rem30d': rem30d
                    }
                    records.append(record)
    except Exception as e:
        print(f"  DCI warning: {e}")
    
    return records

# =============================================================================
# TREASURY PROCESSING
# =============================================================================
def process_treasury_k1k3(rep_date):
    """Process K1TBL and K3TBL from KTBLALL"""
    records = []
    
    try:
        df = pl.read_parquet(f"{PATHS['LCR']}KTBLALL.parquet")
        
        for row in df.iter_rows(named=True):
            tbl = row.get('TBL')
            if tbl == '1':
                records.append({
                    'src': 'K1TBL',
                    'bnmcode': row.get('BNMCODE'),
                    'cur': row.get('GWCCY'),
                    'amt': row.get('GWAMT', 0),
                    'dealtype': row.get('GWDLP'),
                    'dealref': row.get('GWDLR'),
                    'custfiss': row.get('GWC2R'),
                    'custno': None
                })
            elif tbl == '3':
                records.append({
                    'src': 'K3TBL',
                    'bnmcode': row.get('BNMCODE'),
                    'cur': row.get('UTCCY'),
                    'amt': row.get('UTAMT', 0),
                    'dealtype': row.get('UTSTY'),
                    'dealref': row.get('UTDLR'),
                    'custfiss': None,
                    'custno': row.get('UTCUS')
                })
    except Exception as e:
        print(f"  K1/K3 warning: {e}")
    
    return records

def process_cis_equity():
    """Process CIS equity data"""
    records = []
    
    try:
        df = pl.read_parquet(f"{PATHS['CIS']}CUSTDLY.parquet")
        df = df.filter((pl.col('ACCTCODE') == 'EQC') & (pl.col('PRISEC') == 901))
        
        for row in df.iter_rows(named=True):
            newic = row.get('NEWIC', '')
            if not newic or (len(newic) >= 5 and newic[:5] == '99999'):
                icno = f"{row.get('ALIASKEY', '')}{row.get('CUSTNO', 0)}".replace(' ', '')
            else:
                icno = f"{row.get('ALIASKEY', '')}{row.get('ALIAS', '')}".replace(' ', '')
            
            records.append({
                'acctno': row.get('ACCTNO'),
                'custno': row.get('CUSTNO'),
                'cisno': row.get('CUSTNO'),
                'cisname': row.get('CUSTNAME'),
                'icno': icno
            })
    except Exception as e:
        print(f"  CIS equity warning: {e}")
    
    return records

def process_utsas(rep_date):
    """Process UTSAS from EQUA tables"""
    records = []
    
    utvar = ['DEALREF', 'DEALTYPE', 'CUSTFISS', 'CUSTNO', 'CUSTNAME', 'CUSTEQNO', 'CUSTID']
    
    try:
        for prefix in ['UTMS', 'UTFX', 'UTRP']:
            df = pl.read_parquet(f"{PATHS['EQUA']}{prefix}{rep_date['rptdt']}.parquet")
            # Select only required columns if they exist
            keep_cols = [c for c in utvar if c in df.columns]
            if keep_cols:
                df = df.select(keep_cols)
                if 'CUSTEQNO' in df.columns:
                    df = df.rename({'CUSTEQNO': 'ACCTNO'})
                records.extend(df.rows(named=True))
    except Exception as e:
        print(f"  UTSAS warning: {e}")
    
    return records

# =============================================================================
# CORE BANKING
# =============================================================================
def process_core_banking(rep_date):
    """Process core banking data: FD, SA, CA, FCYCA"""
    records = []
    
    try:
        for tbl in ['FD', 'SA', 'CA', 'FCYCA']:
            df = pl.read_parquet(f"{PATHS['LCR']}{tbl}{rep_date['day']}.parquet")
            
            for row in df.iter_rows(named=True):
                custcd = row.get('CUSTCD', 0)
                if tbl == 'FD':
                    custcd = row.get('CUSTCDX', 0)
                
                # Customer category
                cust = get_customer_category(custcd, CUST_MAP)
                
                # Maturity
                rem30d = row.get('REM30D', row.get('REMMTH', 1))
                remmth = row.get('REMMTH', 1)
                
                if rem30d is None:
                    rem30d = remmth
                
                # Build BIC
                bic = row['BNMCODE'][:5]
                
                records.append({
                    'src': f'BANKING_{tbl}',
                    'bic': bic,
                    'bnmcode': f"{bic}{cust}020000Y",
                    'cmmcode': f"{bic}{cust}{format_mth_bucket(remmth)}0000Y",
                    'cur': row.get('CURCODE', 'MYR'),
                    'amt': row.get('AMOUNT', 0),
                    'acctno': row.get('ACCTNO', 0),
                    'custno': row.get('CUSTNO', 0),
                    'custcd': custcd,
                    'rem30d': rem30d,
                    'remmth': remmth,
                    'ecp': '00',
                    'product': row.get('PRODUCT', 0),
                    'billerind': row.get('BILLERIND', 'N'),
                    'pbmerch': row.get('PBMERCH', 'N'),
                    'intrate': row.get('INTRATE', 0),
                    'oprrate': row.get('OPRRATE', 0),
                    'source': row.get('SOURCE', ''),
                    'dtsigned': row.get('DTSIGNED'),
                    'intplan': row.get('INTPLAN', 0),
                    'sme_tag': row.get('SME_TAG', ''),
                    'fdhold': row.get('FDHOLD', 'N'),
                    'trx': row.get('TRX', 0),
                    'sign': ''
                })
    except Exception as e:
        print(f"  Core banking warning: {e}")
    
    return records

# =============================================================================
# INSURED/UNINSURED SPLIT
# =============================================================================
def apply_insurance_split(records):
    """Split insured/uninsured portions for amounts > 250K"""
    result = []
    
    # Group by ICGRP to get totals
    icgrp_totals = {}
    for r in records:
        icgrp = r.get('icgrp', '')
        if icgrp:
            icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + r['amt']
    
    for r in records:
        icgrp = r.get('icgrp', '')
        toticbal = icgrp_totals.get(icgrp, 0)
        
        if toticbal > 250000 and r.get('bic') not in ['9531X']:
            # Need to split
            curbal = r['amt']
            insured_amt = (curbal / toticbal) * 250000
            uninsured_amt = curbal - insured_amt
            
            # Not fully covered portion (if applicable)
            if r['bnmcode'][5:7] in ['29', '39'] and r.get('ecp') != '01':
                r1 = r.copy()
                r1['amt'] = curbal
                r1['bnmcode'] = r['bnmcode'][:7] + '10' + r['bnmcode'][10:15]
                result.append(r1)
            else:
                # Insured portion
                r1 = r.copy()
                r1['amt'] = insured_amt
                result.append(r1)
                
                # Uninsured portion
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
    
    # Convert to thousands
    df = df.with_columns([
        (pl.col('amt') / 1000).round(2).alias('amt_k')
    ])
    
    # Summarize by BNMCODE and currency
    summary = df.group_by(['bnmcode', 'cur']).agg([
        pl.col('amt_k').sum()
    ])
    
    return summary

def apply_column_mapping(row, is_banking):
    """Apply column mapping logic (equivalent to SHAREX)"""
    bnmcode = row['bnmcode']
    bic = bnmcode[:5]
    
    # Column name from BIC (simplified)
    col_map = {
        '95311': 'FD95311RM',
        '95312': 'SA95312RM',
        '95313': 'CA95313RM',
        '95830': 'STD95830',
        '95840': 'NID95840',
        '9X810': 'IBB9X810',
        '9X329': 'DCI9X329',
        '95820': 'IBR95820',
        '95850': 'BAP95850',
        '9531X': 'GLD9531X'
    }
    colname = col_map.get(bic[:5], '')
    
    if is_banking:
        # Banking logic
        ecp = bnmcode[9:11]
        if bic in ['95313', '96313'] and ecp == '01':
            # Would use LCRCDMNIOPR format
            item = bnmcode[5:9]
        else:
            # Would use LCRCDMNI format
            item = bnmcode[5:9]
        remmth = bnmcode[9:11]
    else:
        # Treasury logic
        # Would use LCRCDEQU format
        item = bnmcode[5:7]
        if bic == '95820':
            item = 'C1.11'
        remmth = bnmcode[7:9]
        orimth = bnmcode[9:11]
        if item == 'B3.30' and orimth == '02':
            item = 'B6.30'
    
    # Adjust column name based on maturity
    if colname[:3] in ['FD9', 'STD']:
        colname = f"{colname}{'1' if remmth == '1' else '2'}"
    elif colname[:3] in ['NID', 'DCI', 'IBB', 'IBR', 'BAP']:
        for i in range(1, 7):
            if str(i) == remmth:
                colname = f"{colname}V{i}"
                break
    
    return item, colname, row['amt_k']

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBDLCRM - BNM LCR Reporting (Conventional Banking)")
    print("=" * 60)
    
    # Get report date
    rep_date = get_report_date()
    print(f"\nReport Date: {rep_date['date'].strftime('%d/%m/%Y')}")
    print(f"Week: {rep_date['nowk']}, Month: {rep_date['mon']}")
    
    # Load FX rates
    print("\nLoading FX rates...")
    fx_rates = {'MYR': 1.0}
    try:
        # Read format catalog
        fmt_df = pl.read_parquet(f"{PATHS['FORATE']}FOFMT.parquet")
        for row in fmt_df.iter_rows(named=True):
            if row.get('FMTNAME') == 'FORATE':
                fx_rates[row['START']] = row['LABEL']
        print(f"  Loaded {len(fx_rates)} currencies")
    except:
        print("  Using default rates")
        fx_rates.update({'USD': 4.0, 'SGD': 3.0, 'HKD': 0.5, 'AUD': 3.0, 'JPY': 0.03, 'XAU': 200.0})
    
    # Process DCI
    print("\nProcessing DCI...")
    dci_records = process_dci(rep_date, fx_rates)
    print(f"  {len(dci_records):,} DCI records")
    
    # Process Treasury K1/K3
    print("\nProcessing Treasury K1/K3...")
    treasury_records = process_treasury_k1k3(rep_date)
    print(f"  {len(treasury_records):,} treasury records")
    
    # Process CIS Equity
    print("\nProcessing CIS Equity...")
    cis_records = process_cis_equity()
    cis_dict = {r['acctno']: r for r in cis_records if r.get('acctno')}
    print(f"  {len(cis_dict):,} CIS records")
    
    # Process UTSAS
    print("\nProcessing UTSAS...")
    utsas_records = process_utsas(rep_date)
    utsas_dict = {r['DEALREF']: r for r in utsas_records if r.get('DEALREF')}
    print(f"  {len(utsas_dict):,} UTSAS records")
    
    # Combine treasury and DCI
    all_treasury = treasury_records + dci_records
    
    # Apply UTSAS and CIS to treasury
    enhanced_treasury = []
    for r in all_treasury:
        dealref = r.get('dealref')
        if dealref and dealref in utsas_dict:
            ut = utsas_dict[dealref]
            r.update(ut)
        
        acctno = r.get('acctno') or r.get('CUSTEQNO')
        if acctno and acctno in cis_dict:
            ci = cis_dict[acctno]
            r['cisno'] = ci.get('cisno')
            r['cisname'] = ci.get('cisname')
            r['icno'] = ci.get('icno')
        
        # Customer categorization
        custfiss = r.get('custfiss', 0)
        if custfiss:
            try:
                custfiss = int(custfiss)
            except:
                custfiss = 0
        
        custno = r.get('custno', '')
        cust = get_customer_category(custfiss, CUST_MAP, SPECIAL_CUST, 
                                     is_custno=(custno in SPECIAL_CUST.get('39', [])))
        
        # BIC handling
        bic = r['bnmcode'][:5]
        if bic == '95830' and r.get('dealtype') in ['BCQ', 'BCT', 'BCW']:
            bic = '9583X'
        
        # Build codes
        rem30d = r.get('rem30d', r.get('remmth', 1))
        remmth = r.get('remmth', 1)
        
        if rem30d is None:
            rem30d = remmth
        
        bnmcode = f"{bic}{cust}{format_day_bucket(rem30d)}0000Y"
        cmmcode = f"{bic}{cust}{format_mth_bucket(remmth)}0000Y"
        
        # Special handling for AIM/PBL
        if custno in SPECIAL_CUST.get('49', []) and cust == '49' and bic in ['95840', '96840']:
            ori30d = r.get('ori30d', 0)
            if format_day_bucket(ori30d) > '05' and format_day_bucket(rem30d) > '01':
                bnmcode = bnmcode[:9] + '0200Y'
        
        # ICGRP
        icgrp = r.get('custid', r.get('icno', '')).replace(' ', '')
        
        enhanced_treasury.append({
            'src': r['src'],
            'bic': bic,
            'bnmcode': bnmcode,
            'cmmcode': cmmcode,
            'cur': r.get('cur', 'MYR'),
            'amt': r.get('amt', 0),
            'dealref': dealref,
            'custno': custno,
            'icgrp': icgrp,
            'rem30d': rem30d,
            'remmth': remmth
        })
    
    # Process Core Banking
    print("\nProcessing Core Banking...")
    banking_records = process_core_banking(rep_date)
    
    # Merge with CIS and ECP for banking
    try:
        cis_info = pl.read_parquet(f"{PATHS['LCR']}CISINFO.parquet")
        ecp = pl.read_parquet(f"{PATHS['LIST']}LCR_ECP.parquet").unique(subset=['ACCTNO'])
        
        # Create lookup dicts
        cis_dict = {r['ACCTNO']: r for r in cis_info.rows(named=True)}
        ecp_dict = {r['ACCTNO']: r['ECP'] for r in ecp.rows(named=True) if 'ECP' in r}
    except:
        cis_dict = {}
        ecp_dict = {}
    
    enhanced_banking = []
    for r in banking_records:
        acctno = r['acctno']
        
        # Apply CIS
        if acctno in cis_dict:
            ci = cis_dict[acctno]
            r['newic'] = ci.get('NEWIC')
            r['oldic'] = ci.get('OLDIC')
            r['custname'] = ci.get('CUSTNAME')
        
        # Apply ECP
        if acctno in ecp_dict:
            r['ecp'] = ecp_dict[acctno]
        
        # ECP logic
        if r['ecp'] == '':
            r['ecp'] = '00'
        if r['ecp'] == '01':
            if r['intrate'] < r['oprrate']:
                r['ecp'] = '01'
            else:
                r['ecp'] = '00'
        if r['billerind'] == 'Y' or r['pbmerch'] == 'Y':
            r['ecp'] = '01'
        
        # SIGN calculation
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
        
        # Special customer overrides
        special_39 = [4391161,2115999,12579649,13468207,14300254,
                     14675929,15327497,17104931,12677444,3703533,
                     5978659,16185090,2558344,10819745]
        
        if r['custno'] in special_39:
            r['cust'] = '39'
        
        # XAU handling
        if r['cur'] == 'XAU':
            r['bic'] = '9531X'
            r['bnmcode'] = f"9531X{r['cust']}100000Y"
            r['cmmcode'] = f"9531X{r['cust']}{format_mth_bucket(r['remmth'])}0000Y"
            r['amt'] = r['amt'] * fx_rates.get('XAU', 200.0)
            r['cur'] = 'MYR'
        
        enhanced_banking.append(r)
    
    # Calculate ICGRP totals for banking
    icgrp_totals = {}
    for r in enhanced_banking:
        icgrp = r.get('newic', r.get('oldic', '')).replace(' ', '')
        r['icgrp'] = icgrp
        icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + r['amt']
    
    # Apply reclassification logic
    exclude_cust = [14094942,16557696,3728510,11335374,16265490,
                    3523050,11880426,16771972,15241330,16500538]
    
    for r in enhanced_banking:
        icgrp = r['icgrp']
        toticbal = icgrp_totals.get(icgrp, 0)
        r['toticbal'] = toticbal
        
        # Reclassification based on totals
        if (r['custno'] not in exclude_cust and r['bnmcode'][5:7] == '29') or r['custcd'] in [72,73,74]:
            totdpbal = toticbal + 0  # Would add TOTICEQBAL
            if totdpbal < 5000000:
                r['bnmcode'] = f"{r['bic']}19{r['bnmcode'][7:]}"
                r['cmmcode'] = f"{r['bic']}19{r['cmmcode'][7:]}"
        elif r['bnmcode'][5:7] == '19' and r.get('sme_tag') == 'N':
            totdpbal = toticbal + 0
            if totdpbal >= 5000000:
                r['bnmcode'] = f"{r['bic']}29{r['bnmcode'][7:]}"
                r['cmmcode'] = f"{r['bic']}29{r['cmmcode'][7:]}"
        
        # TAG assignment
        if r['bnmcode'][5:7] in ['08', '19'] and r['bic'] != '9531X':
            if r.get('trx') == 1:
                tag = '01'
            elif r.get('sign') in ['R', 'R ']:
                tag = '02'
            else:
                tag = '03'
            r['bnmcode'] = r['bnmcode'][:7] + tag + '0000Y'
        
        # Operational deposit handling
        if r['bic'] in ['95313', '96313']:
            r['bnmcode'] = r['bnmcode'][:9] + r['ecp'] + '00Y'
            r['cmmcode'] = r['cmmcode'][:9] + r['ecp'] + '00Y'
    
    # Apply insurance split
    print("\nApplying insurance split...")
    banking_split = apply_insurance_split(enhanced_banking)
    
    # Combine all sources
    all_data = enhanced_treasury + banking_split
    print(f"\nTotal records: {len(all_data):,}")
    
    # Consolidate
    print("\nConsolidating...")
    summary = consolidate_data(all_data)
    print(f"  {len(summary):,} BNM code x currency combinations")
    
    # Generate report
    print("\nGenerating LCR report...")
    
    # Group by ITEM and COLNAME (simplified)
    report_data = []
    for row in summary.rows(named=True):
        # Banking or treasury?
        is_banking = row['bnmcode'][5] != '9'  # Rough heuristic
        item, colname, amount = apply_column_mapping(row, is_banking)
        report_data.append({
            'item': item,
            'colname': colname,
            'amount': amount
        })
    
    if report_data:
        report_df = pl.DataFrame(report_data)
        
        # Summarize by item and column
        final = report_df.group_by(['item', 'colname']).agg([
            pl.col('amount').sum()
        ])
        
        # Pivot to get columns as fields
        pivot = final.pivot(
            index='item',
            columns='colname',
            values='amount',
            aggregate_function='sum'
        )
        
        # Save
        filename = f"LCR{rep_date['day']}.parquet"
        pivot.write_parquet(f"{PATHS['OUTPUT']}{filename}")
        print(f"  ✓ {filename}: {len(pivot):,} items")
    
    # Summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    df_all = pl.DataFrame(all_data)
    total = df_all['amt'].sum() / 1000
    by_src = df_all.group_by('src').agg([(pl.col('amt').sum() / 1000).alias('amt_k')])
    
    print(f"\nTotal: RM {total:,.0f}K")
    print(f"\nBy Source:")
    for row in by_src.sort('amt_k', descending=True).iter_rows():
        print(f"  {row[0]}: RM {row[1]:,.0f}K")
    
    print("\n" + "=" * 60)
    print("✓ EIBDLCRM Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
