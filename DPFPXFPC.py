"""
EIBMLCRM - BNM LCR (Liquidity Coverage Ratio) Reporting
Consolidates deposits & treasury positions for BNM LCR reporting.
Outputs: LCR reports by currency with customer categorization (08/19/29/39/49/59)
"""

import pyreadstat
import pandas as pd
from datetime import datetime, timedelta
import os
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'lcr': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMLCRM/lcr/',
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
    '19': [41,42,43,44,46,47,48,49,51,52,53,54,65,66,67,68,69],  # SME
    '29': [0,45,57,59,60,61,62,63,64,75,79,85,86,87,88,89,98,99],  # Other retail
    '39': [1,71,72,73,74,90,91,92],  # Sovereign funds
    '49': [2,3,7,12,81,82,83,84],  # Financial institutions
    '59': [4,5,6,13,17,20] + list(range(30,41))  # Corporate
}

# Special customers (override)
SPECIAL_CUST = {
    '39': ['kwsp', 'kwap', 'kwan', 'lemtab'],
    '49': ['kwspkl', 'kwapkl', 'kwankl', 'lemtabkl']
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
def read_sas_dataset(filepath):
    """Read SAS dataset using pyreadstat"""
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        # Convert column names to lowercase
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def get_report_date():
    """Calculate report date as yesterday"""
    reptdate = datetime.now() - timedelta(days=1)
    
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
    """Process Treasury (Kapiti) data: k1tbl, k3tbl, dci"""
    records = []
    
    try:
        # Read and combine treasury tables
        dfs = []
        for tbl in ['k1tbl', 'k3tbl', 'dci']:
            df = read_sas_dataset(f"{PATHS['lcr']}{tbl}.sas7bdat")
            if df is not None:
                # Filter for BNM codes starting with 95 or 96
                if 'bnmcode' in df.columns:
                    df = df[df['bnmcode'].str[:2].isin(['95', '96'])]
                dfs.append(df)
        
        if not dfs:
            return records
            
        df = pd.concat(dfs, ignore_index=True)
        df = df.drop_duplicates(subset=['dealref'], keep='first')
        
        # Merge with utsas if available
        try:
            utsas = read_sas_dataset(f"{PATHS['lcr']}utsas{rep_date['mon']}.sas7bdat")
            if utsas is not None:
                df = df.merge(utsas, on='dealref', how='left')
        except:
            pass
        
        for _, row in df.iterrows():
            custno = row.get('custno', '')
            custfiss = row.get('custfiss', 0)
            
            # Customer categories
            cust = get_customer_category(custfiss, CUST_MAP, SPECIAL_CUST)
            cusx = get_customer_category(custfiss, CUSX_MAP, SPECIAL_CUST)
            
            # Maturity
            rem30d = row.get('rem30d', row.get('remmth', 1))
            remmth = row.get('remmth', 1)
            
            # Build codes
            bic = str(row['bnmcode'])[:5]
            records.append({
                'src': 'TREASURY',
                'bic': bic,
                'bnmcode': f"{bic}{cust}{format_day_bucket(rem30d)}0000Y",
                'cmmcode': f"{bic}{cust}{format_mth_bucket(remmth)}0000Y",
                'nsfcode': f"{bic}{cusx}{format_day_bucket(rem30d)}0000Y",
                'cur': row.get('curcode', 'MYR'),
                'amt': row.get('amount', 0),
                'ref': row.get('dealref', '')
            })
    except Exception as e:
        print(f"  Treasury warning: {e}")
    
    return records

def process_banking(rep_date, fx_rates):
    """Process Core Banking data: fd, sa, ca, fcyca, nid"""
    records = []
    
    try:
        # Read all banking tables
        dfs = []
        for tbl in ['fd', 'sa', 'ca', 'fcyca', 'nid']:
            df = read_sas_dataset(f"{PATHS['lcr']}{tbl}.sas7bdat")
            if df is not None:
                if 'bnmcode' in df.columns:
                    df = df[df['bnmcode'].str[:2].isin(['95', '96'])]
                dfs.append(df)
        
        if not dfs:
            return records
            
        df = pd.concat(dfs, ignore_index=True)
        
        for _, row in df.iterrows():
            custcd = row.get('custcd', 0)
            
            # Customer categories
            cust = get_customer_category(custcd, CUST_MAP)
            cusx = get_customer_category(custcd, CUSX_MAP)
            
            # Maturity
            rem30d = row.get('rem30d', row.get('remmth', 1))
            remmth = row.get('remmth', 1)
            
            # Currency & amount
            cur = row.get('curcode', 'MYR')
            amt = row.get('amount', 0)
            bic = str(row['bnmcode'])[:5]
            
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
                'acctno': row.get('acctno', 0)
            })
    except Exception as e:
        print(f"  Banking warning: {e}")
    
    return records

def write_text_report(df, filename, rep_date):
    """Write DataFrame to formatted text file"""
    filepath = f"{PATHS['output']}{filename}"
    with open(filepath, 'w') as f:
        # Header
        f.write(f"BNM LCR REPORT - {rep_date['date'].strftime('%d/%m/%Y')}\n")
        f.write("=" * 80 + "\n")
        f.write(f"{'Currency':<10}{'Total (RM K)':<20}\n")
        f.write("-" * 80 + "\n")
        
        # Data rows
        for _, row in df.iterrows():
            f.write(f"{row['cur']:<10}{row['total_rm_k']:>15,.2f}\n")
        
        # Footer
        f.write("-" * 80 + "\n")
        f.write(f"Report Date: {rep_date['date'].strftime('%d/%m/%Y %H:%M:%S')}\n")
        f.write(f"Total Records: {len(df)}\n")

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
            df_fx = df_fx[df_fx['reptdate'] <= rep_date['date']]
            df_fx = df_fx.sort_values('reptdate', ascending=False)
            df_fx = df_fx.drop_duplicates(subset=['curcode'], keep='first')
            fx_rates.update({r['curcode']: r['spotrate'] for _, r in df_fx.iterrows()})
        print(f"  Loaded {len(fx_rates)} currencies")
    except Exception as e:
        print(f"  Using default rates: {e}")
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
        print("\n⚠️ No data found!")
        return
    
    df = pd.DataFrame(all_data)
    
    # Convert to thousands and summarize
    print("\nConsolidating...")
    df['amt_k'] = (df['amt'] / 1000).round(2)
    
    summary = df.groupby(['bnmcode', 'cur'])['amt_k'].sum().reset_index()
    print(f"  {len(summary):,} BNM code x currency combinations")
    
    # Generate reports by currency
    print("\nGenerating reports...")
    report_configs = [
        ('mth', None),  # All currencies
        ('usd', ['USD']),
        ('sgd', ['SGD']),
        ('hkd', ['HKD']),
        ('myr', ['MYR'])
    ]
    
    for suffix, currencies in report_configs:
        if currencies:
            df_rep = summary[summary['cur'].isin(currencies)]
        else:
            df_rep = summary
        
        if len(df_rep) > 0:
            # Aggregate by currency
            result = df_rep.groupby('cur')['amt_k'].sum().reset_index()
            result.columns = ['cur', 'total_rm_k']
            
            # Save as text file
            filename = f"lcr{suffix}{rep_date['mon']}.txt"
            write_text_report(result, filename, rep_date)
            print(f"  ✓ {filename}: {len(result):,} currencies")
    
    # Summary statistics
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    total = df['amt_k'].sum()
    by_src = df.groupby('src')['amt_k'].sum()
    by_cur = df.groupby('cur')['amt_k'].sum()
    
    print(f"\nTotal: RM {total:,.0f}K")
    print(f"\nBy Source:")
    for src, amt in by_src.items():
        print(f"  {src}: RM {amt:,.0f}K")
    print(f"\nBy Currency:")
    for cur, amt in by_cur.sort_index().items():
        print(f"  {cur}: RM {amt:,.0f}K")
    
    # Save detailed report
    detail_file = f"lcr_detail_{rep_date['rdate']}.txt"
    with open(f"{PATHS['output']}{detail_file}", 'w') as f:
        f.write(f"BNM LCR DETAILED REPORT - {rep_date['date'].strftime('%d/%m/%Y')}\n")
        f.write("=" * 100 + "\n")
        f.write(f"{'Source':<10}{'BNM Code':<20}{'Currency':<10}{'Amount (K)':<15}{'Reference':<20}\n")
        f.write("-" * 100 + "\n")
        for _, row in df.iterrows():
            ref = row.get('ref', row.get('acctno', ''))
            f.write(f"{row['src']:<10}{row['bnmcode']:<20}{row['cur']:<10}{row['amt_k']:>12,.2f}   {str(ref):<20}\n")
    print(f"\n✓ Detailed report saved: {detail_file}")
    
    print("\n" + "=" * 60)
    print("✓ EIBMLCRM Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
