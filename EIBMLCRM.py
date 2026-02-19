"""
EIBMLCRM - BNM LCR (Liquidity Coverage Ratio) Reporting
Consolidates deposits & treasury positions for BNM LCR reporting.
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
    '59': [4,5,6,13,17,20] + list(range(30,41))  # Corporate
}

# Special customers (override)
SPECIAL_CUST = {
    '39': ['KWSP', 'KWAP', 'KWAN', 'LEMTAB'],
    '49': ['KWSPKL', 'KWAPKL', 'KWANKL', 'LEMTABKL']
}

# NSFR customer mappings (different from LCR)
CUSX_MAP = {
    '08': [76,77,78,95,96],
    '19': [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69],
    '29': [0,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99],
    '39': [1,91],
    '49': [71,72,73,74,90,92],
    '59': [2,3,4,5,6,7,12,13,17,20] + list(range(30,41)) + [81,82,83,84]
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
    
    return {
        'date': reptdate,
        'nowk': nowk,
        'mon': f"{reptdate.month:02d}",
        'day': f"{reptdate.day:02d}",
        'year': f"{reptdate.year % 100:02d}",
        'rdate': reptdate.strftime('%d%m%y')
    }

def get_customer_category(code, mapping, special=None):
    """Get customer category from code"""
    if special and code in special:
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
    """Process Treasury (Kapiti) data: K1TBL, K3TBL, DCI"""
    records = []
    
    try:
        # Read and combine treasury tables
        dfs = []
        for tbl in ['K1TBL', 'K3TBL', 'DCI']:
            df = pl.read_parquet(f"{PATHS['LCR']}{tbl}.parquet")
            df = df.filter(pl.col('BNMCODE').str.slice(0, 2).is_in(['95', '96']))
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
            cust = get_customer_category(custfiss, CUST_MAP, SPECIAL_CUST)
            cusx = get_customer_category(custfiss, CUSX_MAP, SPECIAL_CUST)
            
            # Maturity
            rem30d = row.get('REM30D', row.get('REMMTH', 1))
            remmth = row.get('REMMTH', 1)
            
            # Build codes
            bic = row['BNMCODE'][:5]
            records.append({
                'src': 'TREASURY',
                'bic': bic,
                'bnmcode': f"{bic}{cust}{format_day_bucket(rem30d)}0000Y",
                'cmmcode': f"{bic}{cust}{format_mth_bucket(remmth)}0000Y",
                'nsfcode': f"{bic}{cusx}{format_day_bucket(rem30d)}0000Y",
                'cur': row.get('CURCODE', 'MYR'),
                'amt': row.get('AMOUNT', 0),
                'ref': row.get('DEALREF', '')
            })
    except Exception as e:
        print(f"  Treasury warning: {e}")
    
    return records

def process_banking(rep_date, fx_rates):
    """Process Core Banking data: FD, SA, CA, FCYCA, NID"""
    records = []
    
    try:
        # Read all banking tables
        dfs = []
        for tbl in ['FD', 'SA', 'CA', 'FCYCA', 'NID']:
            df = pl.read_parquet(f"{PATHS['LCR']}{tbl}.parquet")
            df = df.filter(pl.col('BNMCODE').str.slice(0, 2).is_in(['95', '96']))
            dfs.append(df)
        
        df = pl.concat(dfs)
        
        for row in df.iter_rows(named=True):
            custcd = row.get('CUSTCD', 0)
            
            # Customer categories
            cust = get_customer_category(custcd, CUST_MAP)
            cusx = get_customer_category(custcd, CUSX_MAP)
            
            # Maturity
            rem30d = row.get('REM30D', row.get('REMMTH', 1))
            remmth = row.get('REMMTH', 1)
            
            # Currency & amount
            cur = row.get('CURCODE', 'MYR')
            amt = row.get('AMOUNT', 0)
            bic = row['BNMCODE'][:5]
            
            # XAU (Gold) special handling
            if cur == 'XAU':
                bic = '9531X'
                amt = amt * fx_rates.get('XAU', 200.0)  # Convert to MYR
                cur = 'MYR'
            
            records.append({
                'src': 'BANKING',
                'bic': bic,
                'bnmcode': f"{bic}{cust}020000Y",
                'cmmcode': f"{bic}{cust}{format_mth_bucket(remmth)}0000Y",
                'nsfcode': f"{bic}{cusx}020000Y",
                'cur': cur,
                'amt': amt,
                'acctno': row.get('ACCTNO', 0)
            })
    except Exception as e:
        print(f"  Banking warning: {e}")
    
    return records

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
        df_fx = pl.read_parquet(f"{PATHS['FORATE']}FORATEBKP.parquet")
        df_fx = df_fx.filter(pl.col('REPTDATE') <= rep_date['date'])
        df_fx = df_fx.sort('REPTDATE', descending=True)
        df_fx = df_fx.unique(subset=['CURCODE'], keep='first')
        fx_rates.update({r['CURCODE']: r['SPOTRATE'] for r in df_fx.iter_rows(named=True)})
        print(f"  Loaded {len(fx_rates)} currencies")
    except:
        print("  Using default rates")
        fx_rates.update({'USD': 4.0, 'SGD': 3.0, 'HKD': 0.5, 'XAU': 200.0})
    
    # Process data sources
    print("\nProcessing Treasury...")
    treasury = process_treasury(rep_date)
    print(f"  {len(treasury):,} treasury records")
    
    print("\nProcessing Core Banking...")
    banking = process_banking(rep_date, fx_rates)
    print(f"  {len(banking):,} banking records")
    
    # Combine all sources
    all_data = treasury + banking
    if not all_data:
        print("\n⚠ No data found!")
        return
    
    df = pl.DataFrame(all_data)
    
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
        ('HKD', ['HKD']),
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
    print("✓ EIBMLCRM Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
