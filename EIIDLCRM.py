"""
EIIDLCRM - BNM LCR Reporting for Islamic Banking (Simplified)
Consolidates Islamic deposits & treasury positions for BNM LCR reporting.
Includes MGIA, TD-I, and Islamic treasury products.
"""

import polars as pl
from datetime import datetime
from pathlib import Path
import calendar

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'LCR': 'data/lcr/',
    'LCRM': 'data/lcrm/',
    'DEPOSIT': 'data/deposit/',
    'CISDP': 'data/cisdp/',
    'CISCA': 'data/cisca/',
    'CIS': 'data/cis/',
    'EQUA': 'data/equa/',
    'LIST': 'data/list/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

INST = 'PBB'  # Institution code

# Customer category mappings (LCR)
CUST_MAP = {
    '08': [76, 77, 78, 95, 96],      # Central banks
    '19': [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69],  # SME
    '29': [0,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99],  # Retail
    '39': [1,71,72,73,74,90,91,92],  # Sovereign funds
    '49': [2,3,7,12,81,82,83,84],    # Financial inst
    '59': [4,5,6,13,20] + list(range(30,41)) + [17]  # Corporate
}

SPECIAL_CUST = {
    '39': ['KWSP', 'KWAP', 'KWAN', 'LEMTAB'],
    '49': ['AIM', 'PBL', 'PBLEUR', 'PBLNID', 'PBLUSD', 'PIVMYR', 'PBB', 'PBBMYR', 'PBBUSD']
}

MGIA_PRODUCTS = [302, 315, 394, 396]  # Products that map to MGIA

# =============================================================================
# DATE UTILITIES
# =============================================================================
def get_report_date():
    """Read report date and set macro variables"""
    df = pl.read_parquet(f"{PATHS['DEPOSIT']}REPTDATE.parquet")
    reptdate = df['REPTDATE'][0]
    
    day = reptdate.day
    nowk = '1' if day <= 8 else '2' if day <= 15 else '3' if day <= 22 else '4'
    
    # Days in month arrays for REMMTH calculation
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
        'days_in_month': days_in_month
    }

def calculate_remmonths(matdt, reptdate, days_in_month):
    """Calculate REMMTH and REM30D (equivalent to %REMMTH macro)"""
    if matdt <= reptdate:
        return 0.1, 0
    
    rp_year, rp_month, rp_day = reptdate.year, reptdate.month, reptdate.day
    md_year, md_month, md_day = matdt.year, matdt.month, matdt.day
    
    # Adjust for month-end
    days_in_target = days_in_month[md_month - 1]
    if md_day > days_in_target:
        md_day = days_in_target
    
    rem_years = md_year - rp_year
    rem_months = md_month - rp_month
    rem_days = md_day - rp_day
    
    remmth = rem_years * 12 + rem_months + rem_days / days_in_month[rp_month - 1]
    rem30d = (matdt - reptdate).days / 30
    
    return remmth, rem30d

def fmt_mth(months): return '01' if months <= 1 else '02' if months <= 3 else '03' if months <= 6 else '04' if months <= 9 else '05' if months <= 12 else '10'
def fmt_day(days): return '01' if days <= 1 else '02'

def get_cust(code, mapping, special=None, is_custno=False):
    if is_custno and special and code in special:
        return next((c for c, v in special.items() if code in v), '29')
    for cat, codes in mapping.items():
        if code in codes:
            return cat
    return '29'

# =============================================================================
# TREASURY PROCESSING (KAPITI)
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
                    'src': 'K1TBL', 'bnmcode': row['BNMCODE'], 'cur': row['GWCCY'],
                    'amt': row['GWAMT'], 'dealtype': row['GWDLP'], 'dealref': row['GWDLR'],
                    'custfiss': row['GWC2R'], 'custno': None
                })
            elif tbl == '3':
                records.append({
                    'src': 'K3TBL', 'bnmcode': row['BNMCODE'], 'cur': row['UTCCY'],
                    'amt': row['UTAMT'], 'dealtype': row['UTSTY'], 'dealref': row['UTDLR'],
                    'custfiss': None, 'custno': row['UTCUS']
                })
    except Exception as e:
        print(f"  K1/K3 warning: {e}")
    return records

def process_cis_equity():
    """Process CIS equity data for customer mapping"""
    records = {}
    try:
        df = pl.read_parquet(f"{PATHS['CIS']}CUSTDLY.parquet")
        df = df.filter((pl.col('ACCTCODE') == 'EQC') & (pl.col('PRISEC') == 901))
        
        for row in df.iter_rows(named=True):
            newic = row.get('NEWIC', '')
            if not newic or (len(newic) >= 5 and newic[:5] == '99999'):
                icno = f"{row.get('ALIASKEY', '')}{row.get('CUSTNO', 0)}".replace(' ', '')
            else:
                icno = f"{row.get('ALIASKEY', '')}{row.get('ALIAS', '')}".replace(' ', '')
            
            records[row['ACCTNO']] = {
                'cisno': row['CUSTNO'], 'cisname': row['CUSTNAME'], 'icno': icno
            }
    except Exception as e:
        print(f"  CIS equity warning: {e}")
    return records

def process_utsas(rep_date):
    """Process UTSAS from EQUA Islamic tables"""
    records = {}
    utvar = ['DEALREF', 'DEALTYPE', 'CUSTFISS', 'CUSTNO', 'CUSTNAME', 'CUSTEQNO', 'CUSTID']
    
    try:
        for prefix in ['IUTMS', 'IUTFX', 'IUTRP']:
            df = pl.read_parquet(f"{PATHS['EQUA']}{prefix}{rep_date['rptdt']}.parquet")
            keep = [c for c in utvar if c in df.columns]
            if keep:
                df = df.select(keep)
                if 'CUSTEQNO' in df.columns:
                    df = df.rename({'CUSTEQNO': 'ACCTNO'})
                for row in df.rows(named=True):
                    records[row['DEALREF']] = row
    except Exception as e:
        print(f"  UTSAS warning: {e}")
    return records

# =============================================================================
# CORE BANKING
# =============================================================================
def process_core_banking(rep_date):
    """Process Islamic core banking: FD, SA, CA, FCYCA"""
    records = []
    
    for tbl in ['FD', 'SA', 'CA', 'FCYCA']:
        try:
            df = pl.read_parquet(f"{PATHS['LCR']}{tbl}{rep_date['day']}.parquet")
            
            for row in df.iter_rows(named=True):
                custcd = row.get('CUSTCDX' if tbl == 'FD' else 'CUSTCD', 0)
                cust = get_cust(custcd, CUST_MAP)
                
                rem30d = row.get('REM30D', row.get('REMMTH', 1)) or row.get('REMMTH', 1)
                remmth = row.get('REMMTH', 1)
                
                bic = row['BNMCODE'][:5]
                if bic == '95317' and row.get('PRODUCT') in MGIA_PRODUCTS:
                    bic = '95315'  # MGIA mapping
                
                records.append({
                    'src': tbl, 'bic': bic, 'bnmcode': f"{bic}{cust}020000Y",
                    'cmmcode': f"{bic}{cust}{fmt_mth(remmth)}0000Y",
                    'cur': row.get('CURCODE', 'MYR'), 'amt': row.get('AMOUNT', 0),
                    'acctno': row.get('ACCTNO'), 'custno': row.get('CUSTNO'),
                    'rem30d': rem30d, 'remmth': remmth, 'ecp': '00',
                    'product': row.get('PRODUCT'), 'billerind': row.get('BILLERIND', 'N'),
                    'pbmerch': row.get('PBMERCH', 'N'), 'intrate': row.get('INTRATE', 0),
                    'oprrate': row.get('OPRRATE', 0), 'source': row.get('SOURCE', ''),
                    'dtsigned': row.get('DTSIGNED'), 'intplan': row.get('INTPLAN', 0),
                    'sme_tag': row.get('SME_TAG', ''), 'fdhold': row.get('FDHOLD', 'N'),
                    'trx': row.get('TRX', 0), 'sign': '', 'custcd': custcd
                })
        except Exception as e:
            print(f"  {tbl} warning: {e}")
    
    return records

# =============================================================================
# INSURED/UNINSURED SPLIT
# =============================================================================
def split_insurance(records):
    """Split insured/uninsured for amounts > 250K"""
    result = []
    
    # Group by ICGRP for totals
    icgrp_totals = {}
    for r in records:
        icgrp = r.get('icgrp', '')
        if icgrp:
            icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + r['amt']
    
    for r in records:
        icgrp = r.get('icgrp', '')
        toticbal = icgrp_totals.get(icgrp, 0)
        
        if toticbal > 250000:
            curbal = r['amt']
            insured = (curbal / toticbal) * 250000
            
            if r['bnmcode'][5:7] in ['29','39'] and r.get('ecp') != '01':
                # Not fully covered
                r1 = r.copy()
                r1['bnmcode'] = r['bnmcode'][:7] + '10' + r['bnmcode'][10:15]
                result.append(r1)
            else:
                # Insured portion
                r1 = r.copy()
                r1['amt'] = insured
                result.append(r1)
                
                # Uninsured portion
                r2 = r.copy()
                r2['amt'] = curbal - insured
                r2['bnmcode'] = r['bnmcode'][:7] + '10' + r['bnmcode'][10:15]
                result.append(r2)
        else:
            result.append(r)
    
    return result

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIIDLCRM - BNM LCR Reporting (Islamic Banking)")
    print("=" * 60)
    
    # Report date
    rep_date = get_report_date()
    print(f"\nDate: {rep_date['date'].strftime('%d/%m/%Y')} Week:{rep_date['nowk']} Mon:{rep_date['mon']}")
    
    # CIS data
    cis_dict = process_cis_equity()
    print(f"CIS: {len(cis_dict)} records")
    
    # Treasury
    print("\nTreasury...")
    k_records = process_treasury_k1k3(rep_date)
    utsas_dict = process_utsas(rep_date)
    
    treasury = []
    for r in k_records:
        # Merge UTSAS
        if r['dealref'] in utsas_dict:
            ut = utsas_dict[r['dealref']]
            r.update(ut)
        
        # Customer category
        custfiss = r.get('custfiss', 0)
        if custfiss and isinstance(custfiss, str) and custfiss.isdigit():
            custfiss = int(custfiss)
        custno = r.get('custno', '')
        cust = get_cust(custfiss, CUST_MAP, SPECIAL_CUST, is_custno=(custno in SPECIAL_CUST.get('39', [])))
        
        # Deal type for BQD
        dtype = '01' if r.get('dealtype') == 'BQD' else '00'
        
        # Build codes
        bic = r['bnmcode'][:5]
        rem30d = r.get('rem30d', r.get('remmth', 1)) or r.get('remmth', 1)
        remmth = r.get('remmth', 1)
        
        bnmcode = f"{bic}{cust}{fmt_day(rem30d)}00{dtype}Y"
        cmmcode = f"{bic}{cust}{fmt_mth(remmth)}00{dtype}Y"
        
        # AIM/PBL special
        if custno in SPECIAL_CUST.get('49', []) and cust == '49' and bic in ['95840','96840']:
            ori30d = r.get('ori30d', 0)
            if fmt_day(ori30d) > '05' and fmt_day(rem30d) > '01':
                bnmcode = bnmcode[:9] + '0200Y'
        
        # ICGRP
        icgrp = str(r.get('custid', r.get('icno', ''))).replace(' ', '')
        
        treasury.append({
            'src': 'TREASURY', 'bic': bic, 'bnmcode': bnmcode, 'cmmcode': cmmcode,
            'cur': r.get('cur', 'MYR'), 'amt': r.get('amt', 0), 'icgrp': icgrp,
            'rem30d': rem30d, 'remmth': remmth, 'custno': custno
        })
    
    print(f"  Treasury: {len(treasury)} records")
    
    # Core Banking
    print("\nBanking...")
    banking = process_core_banking(rep_date)
    
    # Merge CIS and ECP
    try:
        cis_info = pl.read_parquet(f"{PATHS['LCR']}CISINFO.parquet")
        cis_dict2 = {r['ACCTNO']: r for r in cis_info.rows(named=True)}
        ecp_df = pl.read_parquet(f"{PATHS['LIST']}LCR_ECP.parquet").unique(subset=['ACCTNO'])
        ecp_dict = {r['ACCTNO']: r['ECP'] for r in ecp_df.rows(named=True)}
    except:
        cis_dict2, ecp_dict = {}, {}
    
    enhanced = []
    special_39 = [4391161,2115999,12579649,13468207,14300254,14675929,
                  15327497,17104931,12677444,3703533,5978659,16185090,
                  2558344,10819745]
    
    for r in banking:
        # CIS
        if r['acctno'] in cis_dict2:
            ci = cis_dict2[r['acctno']]
            r['newic'] = ci.get('NEWIC')
            r['oldic'] = ci.get('OLDIC')
        
        # ECP
        if r['acctno'] in ecp_dict:
            r['ecp'] = ecp_dict[r['acctno']]
        if r['ecp'] == '':
            r['ecp'] = '00'
        if r['ecp'] == '01':
            if r['intrate'] < r['oprrate']:
                r['ecp'] = '01'
            else:
                r['ecp'] = '00'
        if r['billerind'] == 'Y' or r['pbmerch'] == 'Y':
            r['ecp'] = '01'
        
        # SIGN
        prod_list = [106,151,158,97,164,201,215]
        intplan_list = list(range(400,420)) + list(range(600,659)) + \
                       list(range(720,741)) + list(range(864,891)) + list(range(941,968))
        
        if (r['product'] in prod_list or r['intplan'] in intplan_list or
            (r['source'] != 'PGD' and r['dtsigned'] and 
             (rep_date['date'] - r['dtsigned']).days >= 365)):
            r['sign'] = 'R '
        
        # Special customer
        if r['custno'] in special_39:
            r['cust'] = '39'
        
        # ICGRP
        r['icgrp'] = str(r.get('newic', r.get('oldic', ''))).replace(' ', '')
        enhanced.append(r)
    
    # ICGRP totals
    icgrp_totals = {}
    for r in enhanced:
        icgrp_totals[r['icgrp']] = icgrp_totals.get(r['icgrp'], 0) + r['amt']
    
    # Reclassification
    exclude = [14094942,16557696,3728510,11335374,16265490,
               3523050,11880426,16771972,15241330,16500538]
    
    for r in enhanced:
        r['toticbal'] = icgrp_totals.get(r['icgrp'], 0)
        
        # Reclass
        if (r['custno'] not in exclude and r['bnmcode'][5:7] == '29') or r['custcd'] in [72,73,74]:
            totdp = r['toticbal']  # + TOTICEQBAL (simplified)
            if totdp < 5000000:
                r['bnmcode'] = f"{r['bic']}19{r['bnmcode'][7:]}"
                r['cmmcode'] = f"{r['bic']}19{r['cmmcode'][7:]}"
        elif r['bnmcode'][5:7] == '19' and r.get('sme_tag') == 'N':
            totdp = r['toticbal']
            if totdp >= 5000000:
                r['bnmcode'] = f"{r['bic']}29{r['bnmcode'][7:]}"
                r['cmmcode'] = f"{r['bic']}29{r['cmmcode'][7:]}"
        
        # TAG
        if r['bnmcode'][5:7] in ['08','19']:
            if r.get('trx') == 1:
                tag = '01'
            elif r.get('sign') in ['R','R ']:
                tag = '02'
            else:
                tag = '03'
            r['bnmcode'] = r['bnmcode'][:7] + tag + '0000Y'
        
        # Operational deposit
        if r['bic'] in ['95313','96313']:
            r['bnmcode'] = r['bnmcode'][:9] + r['ecp'] + '00Y'
            r['cmmcode'] = r['cmmcode'][:9] + r['ecp'] + '00Y'
    
    print(f"  Banking: {len(enhanced)} records")
    
    # Insurance split
    print("\nInsurance split...")
    banking_split = split_insurance(enhanced)
    
    # Combine all
    all_data = treasury + banking_split
    print(f"Total: {len(all_data)} records")
    
    # Consolidate
    df = pl.DataFrame(all_data)
    df = df.with_columns([(pl.col('amt') / 1000).round(2).alias('amt_k')])
    summary = df.group_by(['bnmcode', 'cur']).agg([pl.col('amt_k').sum()])
    print(f"Summary: {len(summary)} codes")
    
    # Simple column mapping (SHAREX equivalent)
    col_map = {
        '95315': 'FD95315RM', '95317': 'FD95317RM', '95312': 'SA95312RM',
        '95313': 'CA95313RM', '9531X': 'GLD9531X', '95830': 'STD95830',
        '95840': 'NID95840', '9X810': 'IBB9X810'
    }
    
    report_data = []
    for row in summary.rows(named=True):
        bic = row['bnmcode'][:5]
        col = col_map.get(bic[:5], '')
        if col:
            rem = row['bnmcode'][9:11]
            if col[:3] in ['FD9','STD']:
                col = f"{col}{'1' if rem == '01' else '2'}"
            elif col[:3] in ['NID','IBB']:
                for i in range(1,7):
                    if fmt_mth(i) == rem:
                        col = f"{col}V{i}"
                        break
            report_data.append({'item': row['bnmcode'][5:9], 'col': col, 'amt': row['amt_k']})
    
    if report_data:
        rep_df = pl.DataFrame(report_data)
        final = rep_df.group_by(['item', 'col']).agg([pl.col('amt').sum()])
        pivot = final.pivot(index='item', columns='col', values='amt', aggregate_function='sum')
        pivot.write_parquet(f"{PATHS['OUTPUT']}LCR{rep_date['day']}.parquet")
        print(f"Report: LCR{rep_date['day']}.parquet")
    
    # Summary
    total = df['amt'].sum() / 1000
    print(f"\nTotal: RM {total:,.0f}K")
    print("=" * 60)
    print("✓ EIIDLCRM Complete")

if __name__ == "__main__":
    main()
