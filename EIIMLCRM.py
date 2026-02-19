"""
EIIMLCRM - BNM LCR (Liquidity Coverage Ratio) Reporting for Islamic Banking
Consolidates Islamic deposits & treasury positions for BNM LCR reporting.
Outputs: LCR reports by currency with customer categorization (08/19/29/39/49/59)
"""

import polars as pl
from datetime import datetime
import os
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'LCR': 'data/lcr/',
    'DEPOSIT': 'data/deposit/',
    'FORATE': 'data/forate/',
    'CISDP': 'data/cisdp/',
    'CISCA': 'data/cisca/',
    'LIST': 'data/list/',
    'SME': 'data/sme/',
    'OUTPUT': 'output/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# Customer category mappings (LCR)
CUST_MAP = {
    '08': [76, 77, 78, 95, 96],  # Central banks/governments
    '19': [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69],  # SME
    '29': [0,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99],  # Other retail
    '39': [1,71,72,73,74,90,91,92],  # Sovereign funds
    '49': [2,3,7,12,81,82,83,84],  # Financial institutions
    '59': [4,5,6,13,20] + list(range(30,41)) + [17]  # Corporate
}

# NSFR customer mappings
CUSX_MAP = {
    '08': [76,77,78,95,96],
    '19': [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69],
    '29': [0,17,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99],
    '39': [1,91],
    '49': [71,72,73,74,90,92],
    '59': [2,3,4,5,6,7,12,13,20] + list(range(30,41)) + [81,82,83,84]
}

# Special customers (override)
SPECIAL_CUST = {
    '39': ['KWSP', 'KWAP', 'KWAN', 'LEMTAB'],
    '49': ['KWSPKL', 'KWAPKL', 'KWANKL', 'LEMTABKL']
}

# Product to BIC mapping for MGIA
MGIA_PRODUCTS = [302, 315, 394, 396]

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
    
    return {
        'date': reptdate,
        'nowk': nowk,
        'mon': f"{reptdate.month:02d}",
        'day': f"{reptdate.day:02d}",
        'year': f"{reptdate.year % 100:02d}",
        'rdate': reptdate.strftime('%d%m%y')
    }

def get_customer_category(code, mapping, special=None, is_custno=False):
    """Get customer category from code"""
    if is_custno and special and code in special:
        return next(cat for cat, vals in special.items() if code in vals)
    
    for cat, codes in mapping.items():
        if code in codes:
            return cat
    return '29'  # Default

def format_mth_bucket(months):
    """Format months into bucket (01-10)"""
    if months <= 1: return '01'
    if months <= 3: return '02'
    if months <= 6: return '03'
    if months <= 9: return '04'
    if months <= 12: return '05'
    return '10'

def format_day_bucket(days):
    """Format days into bucket (01=<=30, 02=>30)"""
    return '01' if days <= 1 else '02'

# =============================================================================
# DATA PROCESSING
# =============================================================================
def process_treasury(rep_date):
    """Process Treasury (Kapiti) data: K1TBL, K3TBL"""
    records = []
    
    try:
        # Read treasury tables
        dfs = []
        for tbl in ['K1TBL', 'K3TBL']:
            df = pl.read_parquet(f"{PATHS['LCR']}{tbl}.parquet")
            dfs.append(df)
        
        df = pl.concat(dfs).unique(subset=['DEALREF'], keep='first')
        
        # Merge with UTSAS if available
        try:
            utsas = pl.read_parquet(f"{PATHS['LCR']}UTSAS{rep_date['mon']}.parquet")
            df = df.join(utsas, on='DEALREF', how='left')
        except:
            pass
        
        for row in df.iter_rows(named=True):
            custno = row.get('CUSTNO', '')
            custfiss = row.get('CUSTFISS', 0)
            
            # Customer categories
            cust = get_customer_category(custfiss, CUST_MAP, SPECIAL_CUST, 
                                         is_custno=(custno in SPECIAL_CUST.get('39', [])))
            cusx = get_customer_category(custfiss, CUSX_MAP, SPECIAL_CUST,
                                         is_custno=(custno in SPECIAL_CUST.get('49', [])))
            
            # Maturity
            rem30d = row.get('REM30D', row.get('REMMTH', 1))
            remmth = row.get('REMMTH', 1)
            
            if rem30d is None:
                rem30d = remmth
            
            # Deal type for BQD
            dtype = '01' if row.get('DEALTYPE') == 'BQD' else '00'
            
            # Build codes
            bic = row['BNMCODE'][:5]
            bnmcode = f"{bic}{cust}{format_day_bucket(rem30d)}00{dtype}Y"
            cmmcode = f"{bic}{cust}{format_mth_bucket(remmth)}00{dtype}Y"
            nsfcode = f"{bic}{cusx}{format_day_bucket(rem30d)}00{dtype}Y"
            
            records.append({
                'src': 'TREASURY',
                'bic': bic,
                'bnmcode': bnmcode,
                'cmmcode': cmmcode,
                'nsfcode': nsfcode,
                'cur': row.get('CURCODE', 'MYR'),
                'amt': row.get('AMOUNT', 0),
                'ref': row.get('DEALREF', ''),
                'icgrp': row.get('CUSTID', row.get('ICNO', '')).replace(' ', '')
            })
    except Exception as e:
        print(f"  Treasury warning: {e}")
    
    return records

def process_banking(rep_date):
    """Process Core Banking data: FD, SA, CA, FCYCA"""
    records = []
    
    try:
        # Read banking tables
        dfs = []
        for tbl in ['FD', 'SA', 'CA', 'FCYCA']:
            df = pl.read_parquet(f"{PATHS['LCR']}{tbl}.parquet")
            dfs.append(df)
        
        df = pl.concat(dfs)
        
        # Merge with CIS info
        try:
            cis_dp = pl.read_parquet(f"{PATHS['CISDP']}DEPOSIT.parquet")
            cis_ca = pl.read_parquet(f"{PATHS['CISCA']}DEPOSIT.parquet")
            cis = pl.concat([cis_dp, cis_ca]).filter(pl.col('SECCUST') == '901')
            cis = cis.unique(subset=['ACCTNO'], keep='first')
            df = df.join(cis, on='ACCTNO', how='left')
        except:
            pass
        
        # Merge with ECP
        try:
            ecp = pl.read_parquet(f"{PATHS['LIST']}LCR_ECP_{rep_date['mon']}.parquet")
            ecp = ecp.unique(subset=['ACCTNO'], keep='first')
            df = df.join(ecp, on='ACCTNO', how='left')
        except:
            pass
        
        # Merge with SME
        try:
            sme = pl.read_parquet(f"{PATHS['SME']}IBASELSME{rep_date['mon']}{rep_date['year']}.parquet")
            sme = sme.unique(subset=['ACCTNO'], keep='first')
            df = df.join(sme, on='ACCTNO', how='left')
        except:
            pass
        
        for row in df.iter_rows(named=True):
            custcd = row.get('CUSTCD', 0)
            custno = row.get('CUSTNO', 0)
            
            # Customer categories
            cust = get_customer_category(custcd, CUST_MAP)
            cusx = get_customer_category(custcd, CUSX_MAP)
            
            # Special customer overrides
            special_39 = [4391161,2115999,12579649,13468207,14300254,
                         14675929,15327497,17104931,12677444,3703533,
                         5978659,16185090,2558344,10819745]
            special_49 = [4391161,2115999,12579649,13468207,14675929,
                         15327497,17104931,12677444,3703533,5978659,
                         16185090,10819745,2558344]
            special_59 = [9888664,11565156,170458,17835250,12078514,12542063]
            special_reg = ['061904X','186852H','211510H','685480K','643815V','734789U']
            
            if custno in special_39:
                cust = '39'
            if custno in special_49:
                cusx = '49'
            if custno in special_59 or row.get('BUSSREG') in special_reg:
                cust = '59'
                cusx = '59'
            
            # Maturity
            rem30d = row.get('REM30D', row.get('REMMTH', 1))
            remmth = row.get('REMMTH', 1)
            
            if rem30d is None:
                rem30d = remmth
            
            # BIC handling for MGIA
            bic = row['BNMCODE'][:5]
            if bic == '95317' and row.get('PRODUCT') in MGIA_PRODUCTS:
                bic = '95315'
            
            # ECP handling
            ecp_val = row.get('ECP', '00')
            if ecp_val == '':
                ecp_val = '00'
            if ecp_val == '01':
                if row.get('INTRATE', 0) < row.get('OPRRATE', 0):
                    ecp_val = '01'
                else:
                    ecp_val = '00'
            if row.get('BILLERIND') == 'Y' or row.get('PBMERCH') == 'Y':
                ecp_val = '01'
            
            # Build codes
            bnmcode = f"{bic}{cust}020000Y"
            cmmcode = f"{bic}{cust}{format_mth_bucket(remmth)}0000Y"
            nsfcode = f"{bic}{cusx}020000Y"
            
            records.append({
                'src': 'BANKING',
                'bic': bic,
                'bnmcode': bnmcode,
                'cmmcode': cmmcode,
                'nsfcode': nsfcode,
                'cur': row.get('CURCODE', 'MYR'),
                'amt': row.get('AMOUNT', 0),
                'acctno': row.get('ACCTNO', 0),
                'custno': custno,
                'icgrp': row.get('NEWIC', row.get('OLDIC', '')).replace(' ', ''),
                'ecp': ecp_val,
                'rem30d': rem30d,
                'remmth': remmth,
                'product': row.get('PRODUCT', 0),
                'fdhold': row.get('FDHOLD', 'N'),
                'toticbal': 0,  # Will be populated later
                'toticeqbal': 0
            })
    except Exception as e:
        print(f"  Banking warning: {e}")
    
    return records

# =============================================================================
# INSURED/UNINSURED SPLIT LOGIC
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
        
        if toticbal > 250000:
            # Need to split
            curbal = r['amt']
            insured_amt = (curbal / toticbal) * 250000
            uninsured_amt = curbal - insured_amt
            
            # Insured portion
            r1 = r.copy()
            r1['amt'] = insured_amt
            if r['bnmcode'][5:7] in ['49'] and r.get('ecp') != '01':
                r1['nsfcode'] = r['nsfcode'][:7] + '10' + r['nsfcode'][10:15]
            result.append(r1)
            
            # Uninsured portion
            r2 = r.copy()
            r2['amt'] = uninsured_amt
            r2['bnmcode'] = r['bnmcode'][:7] + '10' + r['bnmcode'][10:15]
            r2['nsfcode'] = r['nsfcode'][:7] + '10' + r['nsfcode'][10:15]
            result.append(r2)
        else:
            result.append(r)
    
    return result

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIIMLCRM - BNM LCR Reporting (Islamic Banking)")
    print("=" * 60)
    
    # Get report date
    rep_date = get_report_date()
    print(f"\nReport Date: {rep_date['date'].strftime('%d/%m/%Y')}")
    print(f"Week: {rep_date['nowk']}, Month: {rep_date['mon']}")
    
    # Process data sources
    print("\nProcessing Treasury...")
    treasury = process_treasury(rep_date)
    print(f"  {len(treasury):,} treasury records")
    
    print("\nProcessing Core Banking...")
    banking = process_banking(rep_date)
    print(f"  {len(banking):,} banking records")
    
    # Combine all sources
    all_data = treasury + banking
    if not all_data:
        print("\n⚠ No data found!")
        return
    
    df = pl.DataFrame(all_data)
    
    # Apply insurance split
    print("\nApplying insurance split...")
    split_data = apply_insurance_split(all_data)
    df = pl.DataFrame(split_data)
    
    # Convert to thousands and summarize
    print("\nConsolidating...")
    df = df.with_columns([
        (pl.col('amt') / 1000).round(2).alias('amt_k')
    ])
    
    summary = df.group_by(['bnmcode', 'cur']).agg([
        pl.col('amt_k').sum()
    ])
    print(f"  {len(summary):,} BNM code x currency combinations")
    
    # Generate reports by currency
    print("\nGenerating reports...")
    report_configs = [
        ('MTH', None),  # All currencies
        ('USD', ['USD']),
        ('SGD', ['SGD']),
        ('MYR', ['MYR'])
    ]
    
    for suffix, currencies in report_configs:
        if currencies:
            df_rep = summary.filter(pl.col('cur').is_in(currencies))
        else:
            df_rep = summary
        
        if len(df_rep) > 0:
            # Aggregate by currency
            result = df_rep.group_by('cur').agg([
                pl.col('amt_k').sum().alias('total_rm_k')
            ])
            
            # Save
            filename = f"LCR{suffix}{rep_date['mon']}.parquet"
            result.write_parquet(f"{PATHS['OUTPUT']}{filename}")
            print(f"  ✓ {filename}: {len(result):,} currencies")
    
    # Summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    total = df['amt_k'].sum()
    by_src = df.group_by('src').agg([pl.col('amt_k').sum()])
    by_cur = df.group_by('cur').agg([pl.col('amt_k').sum()])
    
    print(f"\nTotal: RM {total:,.0f}K")
    print(f"\nBy Source:")
    for row in by_src.iter_rows():
        print(f"  {row[0]}: RM {row[1]:,.0f}K")
    print(f"\nBy Currency:")
    for row in by_cur.sort('cur').iter_rows():
        print(f"  {row[0]}: RM {row[1]:,.0f}K")
    
    print("\n" + "=" * 60)
    print("✓ EIIMLCRM Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
