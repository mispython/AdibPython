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

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def get_report_date():
    """Get report date as yesterday's date"""
    reptdate = date.today() - timedelta(days=1)
    
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

def read_sas_file(filepath, columns=None):
    """Read SAS dataset using pyreadstat and return polars DataFrame"""
    try:
        if columns:
            df, meta = pyreadstat.read_sas7bdat(filepath, usecols=columns)
        else:
            df, meta = pyreadstat.read_sas7bdat(filepath)
        return pl.from_pandas(df)
    except Exception as e:
        print(f"  Warning: Could not read {filepath}: {e}")
        return None

def read_parquet_file(filepath):
    """Read parquet file and return polars DataFrame"""
    try:
        return pl.read_parquet(filepath)
    except Exception as e:
        print(f"  Warning: Could not read {filepath}: {e}")
        return None

def read_walk_file(filepath):
    """Read WALK.TXT file (fixed width format)"""
    records = []
    try:
        with open(filepath, 'r') as f:
            for line in f:
                if len(line) >= 18:
                    records.append({
                        'acctno': int(line[0:11].strip()) if line[0:11].strip() else None,
                        'custno': int(line[11:18].strip()) if line[11:18].strip() else None
                    })
    except Exception as e:
        print(f"  Warning: Could not read {filepath}: {e}")
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
    except Exception as e:
        print(f"  Warning: Could not read {filepath}: {e}")
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
        # Find the latest DCI file
        dci_pattern = f"{PATHS['dciwh']}dcid*.sas7bdat"
        dci_files = glob.glob(dci_pattern)
        if not dci_files:
            print(f"  No DCI files found")
            return records
        
        # Use the most recent file
        dci_file = max(dci_files)
        df = read_sas_file(dci_file)
        
        if df is None:
            return records
        
        for row in df.iter_rows(named=True):
            matdt = row.get('matdt')
            startdt = row.get('startdt')
            
            if matdt and startdt and matdt > rep_date['date'] and startdt <= rep_date['date']:
                # Calculate remaining months
                if (matdt - rep_date['date']).days < 8:
                    remmth = 0.1
                else:
                    remmth, rem30d = calculate_remaining_months(
                        matdt, rep_date['date'], rep_date['days_in_month']
                    )
                
                invamt = row.get('invamt', 0)
                invccy = row.get('invcurr', 'MYR')
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
                        'src': 'dci',
                        'bnmcode': bnmcode,
                        'cur': 'MYR',
                        'amt': amount,
                        'amt_usd': 0,
                        'amt_sgd': 0,
                        'amt_hkd': 0,
                        'amt_aud': 0,
                        'custfiss': f"{row.get('custcode', 0):02d}",
                        'dealtype': row.get('product'),
                        'dealref': row.get('ticketno'),
                        'remmth': remmth,
                        'rem30d': rem30d
                    })
                else:
                    bnmcode = f"9632900{remth_bucket}0000Y"
                    record = {
                        'src': 'dci',
                        'bnmcode': bnmcode,
                        'cur': invccy,
                        'amt': amount,
                        'amt_usd': amount if invccy == 'USD' else 0,
                        'amt_sgd': amount if invccy == 'SGD' else 0,
                        'amt_hkd': amount if invccy == 'HKD' else 0,
                        'amt_aud': amount if invccy == 'AUD' else 0,
                        'custfiss': f"{row.get('custcode', 0):02d}",
                        'dealtype': row.get('product'),
                        'dealref': row.get('ticketno'),
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
        df = read_sas_file(f"{PATHS['lcr']}ktblall.sas7bdat")
        
        if df is None:
            return records
        
        for row in df.iter_rows(named=True):
            tbl = row.get('tbl')
            if tbl == '1':
                records.append({
                    'src': 'k1tbl',
                    'bnmcode': row.get('bnmcode'),
                    'cur': row.get('gwccy'),
                    'amt': row.get('gwamt', 0),
                    'dealtype': row.get('gwdlp'),
                    'dealref': row.get('gwdlr'),
                    'custfiss': row.get('gwc2r'),
                    'custno': None
                })
            elif tbl == '3':
                records.append({
                    'src': 'k3tbl',
                    'bnmcode': row.get('bnmcode'),
                    'cur': row.get('utccy'),
                    'amt': row.get('utamt', 0),
                    'dealtype': row.get('utsty'),
                    'dealref': row.get('utdlr'),
                    'custfiss': None,
                    'custno': row.get('utcus')
                })
    except Exception as e:
        print(f"  K1/K3 warning: {e}")
    
    return records

def process_cis_equity():
    """Process CIS equity data from parquet file"""
    records = []
    
    try:
        # Find the CIS parquet file
        cis_pattern = f"{PATHS['cis']}custdly*.parquet"
        cis_files = glob.glob(cis_pattern)
        if not cis_files:
            print(f"  No CIS parquet files found")
            return records
        
        # Use the most recent file
        cis_file = max(cis_files)
        df = read_parquet_file(cis_file)
        
        if df is None:
            return records
        
        # Filter for equity accounts
        df = df.filter((pl.col('acctcode') == 'EQC') & (pl.col('prisec') == 901))
        
        for row in df.iter_rows(named=True):
            newic = row.get('newic', '')
            if not newic or (len(str(newic)) >= 5 and str(newic)[:5] == '99999'):
                icno = f"{row.get('aliaskey', '')}{row.get('custno', 0)}".replace(' ', '')
            else:
                icno = f"{row.get('aliaskey', '')}{row.get('alias', '')}".replace(' ', '')
            
            records.append({
                'acctno': row.get('acctno'),
                'custno': row.get('custno'),
                'cisno': row.get('custno'),
                'cisname': row.get('custname'),
                'icno': icno
            })
    except Exception as e:
        print(f"  CIS equity warning: {e}")
    
    return records

def process_utsas(rep_date):
    """Process UTSAS from EQUA tables"""
    records = []
    
    utvar = ['dealref', 'dealtype', 'custfiss', 'custno', 'custname', 'custeqno', 'custid']
    
    try:
        for prefix in ['utms', 'utfx', 'utrp']:
            file_pattern = f"{PATHS['equa']}{prefix}*.sas7bdat"
            files = glob.glob(file_pattern)
            for filepath in files:
                df = read_sas_file(filepath)
                if df is not None:
                    # Select only required columns if they exist
                    keep_cols = [c for c in utvar if c in df.columns]
                    if keep_cols:
                        df = df.select(keep_cols)
                        if 'custeqno' in df.columns:
                            df = df.rename({'custeqno': 'acctno'})
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
        for tbl in ['fd', 'sa', 'ca', 'fcyca']:
            file_pattern = f"{PATHS['lcr']}{tbl}*.sas7bdat"
            files = glob.glob(file_pattern)
            
            for filepath in files:
                df = read_sas_file(filepath)
                if df is None:
                    continue
                
                for row in df.iter_rows(named=True):
                    custcd = row.get('custcd', 0)
                    if tbl == 'fd':
                        custcd = row.get('custcdx', 0)
                    
                    # Customer category
                    cust = get_customer_category(custcd, cust_map)
                    
                    # Maturity
                    rem30d = row.get('rem30d', row.get('remmth', 1))
                    remmth = row.get('remmth', 1)
                    
                    if rem30d is None:
                        rem30d = remmth
                    
                    # Build BIC
                    bic = row['bnmcode'][:5] if row.get('bnmcode') else '95311'
                    
                    records.append({
                        'src': f'banking_{tbl}',
                        'bic': bic,
                        'bnmcode': f"{bic}{cust}020000Y",
                        'cmmcode': f"{bic}{cust}{format_mth_bucket(remmth)}0000Y",
                        'cur': row.get('curcode', 'MYR'),
                        'amt': row.get('amount', 0),
                        'acctno': row.get('acctno', 0),
                        'custno': row.get('custno', 0),
                        'custcd': custcd,
                        'rem30d': rem30d,
                        'remmth': remmth,
                        'ecp': '00',
                        'product': row.get('product', 0),
                        'billerind': row.get('billerind', 'N'),
                        'pbmerch': row.get('pbmerch', 'N'),
                        'intrate': row.get('intrate', 0),
                        'oprrate': row.get('oprrate', 0),
                        'source': row.get('source', ''),
                        'dtsigned': row.get('dtsigned'),
                        'intplan': row.get('intplan', 0),
                        'sme_tag': row.get('sme_tag', ''),
                        'fdhold': row.get('fdhold', 'N'),
                        'trx': row.get('trx', 0),
                        'sign': ''
                    })
    except Exception as e:
        print(f"  Core banking warning: {e}")
    
    return records

def read_walk_and_templ():
    """Read WALK.TXT and TEMPL.TXT files"""
    walk_records = []
    templ_records = []
    
    # Read WALK.TXT
    walk_files = glob.glob(f"{PATHS['list']}walk*.txt")
    if walk_files:
        walk_records = read_walk_file(walk_files[0])
        print(f"  WALK: {len(walk_records)} records")
    
    # Read TEMPL.TXT
    templ_files = glob.glob(f"{PATHS['list']}templ*.txt")
    if templ_files:
        templ_records = read_templ_file(templ_files[0])
        print(f"  TEMPL: {len(templ_records)} records")
    
    return walk_records, templ_records

# =============================================================================
# INSURED/UNINSURED SPLIT
# =============================================================================
def apply_insurance_split(records, walk_records, templ_records):
    """Split insured/uninsured portions for amounts > 250K"""
    result = []
    
    # Build lookup dicts
    walk_dict = {r['acctno']: r for r in walk_records if r.get('acctno')}
    templ_tags = {r['tag']: r['desc'] for r in templ_records if r.get('tag')}
    
    # Group by ICGRP to get totals
    icgrp_totals = {}
    for r in records:
        icgrp = r.get('icgrp', '')
        if icgrp:
            icgrp_totals[icgrp] = icgrp_totals.get(icgrp, 0) + r['amt']
    
    for r in records:
        icgrp = r.get('icgrp', '')
        toticbal = icgrp_totals.get(icgrp, 0)
        
        # Check if account is in WALK
        acctno = r.get('acctno')
        if acctno in walk_dict:
            # Add WALK attributes
            r['walk_custno'] = walk_dict[acctno].get('custno')
        
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
        # Banking logic
        ecp = bnmcode[9:11]
        if bic.lower() in ['95313', '96313'] and ecp == '01':
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
    
    # Create output directory
    output_dir = PATHS['output']
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Convert to DataFrame and pivot
    report_df = pl.DataFrame(report_data)
    
    # Summarize by item and column
    final = report_df.group_by(['item', 'colname']).agg([
        pl.col('amount').sum()
    ])
    
    # Get unique items and columns
    items = sorted(final['item'].unique().to_list())
    columns = sorted(final['colname'].unique().to_list())
    
    # Write to text file (tab-delimited)
    filename = f"lcr{rep_date['day']}.txt"
    filepath = f"{output_dir}{filename}"
    
    with open(filepath, 'w') as f:
        # Write header
        f.write("item\t" + "\t".join(columns) + "\n")
        
        # Write data
        for item in items:
            row_data = [item]
            for col in columns:
                # Find the amount for this item and column
                mask = (final['item'] == item) & (final['colname'] == col)
                if mask.any():
                    amount = final.filter(mask)['amount'].sum()
                    row_data.append(f"{amount:.2f}")
                else:
                    row_data.append("0.00")
            f.write("\t".join(row_data) + "\n")
    
    print(f"  ✓ {filename}: {len(items)} items x {len(columns)} columns")
    
    # Also write a detailed report
    detail_filename = f"lcr_detail{rep_date['day']}.txt"
    detail_filepath = f"{output_dir}{detail_filename}"
    
    with open(detail_filepath, 'w') as f:
        f.write("item\tcolname\tamount\tcurrency\n")
        for row in report_data:
            f.write(f"{row['item']}\t{row['colname']}\t{row['amount']:.2f}\t{row.get('cur', 'MYR')}\n")
    
    print(f"  ✓ {detail_filename}")

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBDLCRM - BNM LCR Reporting (Conventional Banking)")
    print("=" * 60)
    
    # Get report date (yesterday)
    rep_date = get_report_date()
    print(f"\nReport Date: {rep_date['date'].strftime('%d/%m/%Y')}")
    print(f"Week: {rep_date['nowk']}, Month: {rep_date['mon']}")
    
    # Load FX rates
    print("\nLoading FX rates...")
    fx_rates = {'MYR': 1.0}
    try:
        df = read_sas_file(f"{PATHS['forate']}fofmt.sas7bdat")
        if df is not None:
            for row in df.iter_rows(named=True):
                if row.get('fmtname') == 'FORATE':
                    fx_rates[row['start']] = row['label']
            print(f"  Loaded {len(fx_rates)} currencies")
    except Exception as e:
        print(f"  Warning: Could not load FX rates: {e}")
        print("  Using default rates")
        fx_rates.update({'USD': 4.0, 'SGD': 3.0, 'HKD': 0.5, 'AUD': 3.0, 'JPY': 0.03, 'XAU': 200.0})
    
    # Read WALK.TXT and TEMPL.TXT
    print("\nLoading WALK and TEMPL files...")
    walk_records, templ_records = read_walk_and_templ()
    
    # Process DCI
    print("\nProcessing DCI...")
    dci_records = process_dci(rep_date, fx_rates)
    print(f"  {len(dci_records):,} DCI records")
    
    # Process Treasury K1/K3
    print("\nProcessing Treasury K1/K3...")
    treasury_records = process_treasury_k1k3(rep_date)
    print(f"  {len(treasury_records):,} treasury records")
    
    # Process CIS Equity from parquet
    print("\nProcessing CIS Equity...")
    cis_records = process_cis_equity()
    cis_dict = {r['acctno']: r for r in cis_records if r.get('acctno')}
    print(f"  {len(cis_dict):,} CIS records")
    
    # Process UTSAS
    print("\nProcessing UTSAS...")
    utsas_records = process_utsas(rep_date)
    utsas_dict = {r['dealref']: r for r in utsas_records if r.get('dealref')}
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
        
        acctno = r.get('acctno') or r.get('custeqno')
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
        cust = get_customer_category(custfiss, cust_map, special_cust, 
                                     is_custno=(custno in special_cust.get('39', [])))
        
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
        if custno in special_cust.get('49', []) and cust == '49' and bic in ['95840', '96840']:
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
            'remmth': remmth,
            'acctno': acctno
        })
    
    # Process Core Banking
    print("\nProcessing Core Banking...")
    banking_records = process_core_banking(rep_date)
    
    # Merge with CIS and ECP for banking
    try:
        cis_info = read_sas_file(f"{PATHS['lcr']}cisinfo.sas7bdat")
        ecp = read_sas_file(f"{PATHS['list']}lcr_ecp.sas7bdat")
        
        # Create lookup dicts
        cis_dict = {r['acctno']: r for r in cis_info.rows(named=True)} if cis_info is not None else {}
        ecp_dict = {r['acctno']: r['ecp'] for r in ecp.rows(named=True) if 'ecp' in r} if ecp is not None else {}
    except:
        cis_dict = {}
        ecp_dict = {}
    
    enhanced_banking = []
    for r in banking_records:
        acctno = r['acctno']
        
        # Apply CIS
        if acctno in cis_dict:
            ci = cis_dict[acctno]
            r['newic'] = ci.get('newic')
            r['oldic'] = ci.get('oldic')
            r['custname'] = ci.get('custname')
        
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
    
    # Apply insurance split with WALK and TEMPL data
    print("\nApplying insurance split...")
    banking_split = apply_insurance_split(enhanced_banking, walk_records, templ_records)
    
    # Combine all sources
    all_data = enhanced_treasury + banking_split
    print(f"\nTotal records: {len(all_data):,}")
    
    # Consolidate
    print("\nConsolidating...")
    summary = consolidate_data(all_data)
    print(f"  {len(summary):,} BNM code x currency combinations")
    
    # Generate report as text
    print("\nGenerating LCR report (text format)...")
    
    # Group by ITEM and COLNAME
    report_data = []
    for row in summary.rows(named=True):
        # Banking or treasury?
        is_banking = row['bnmcode'][5] != '9'  # Rough heuristic
        item, colname, amount = apply_column_mapping(row, is_banking)
        report_data.append({
            'item': item,
            'colname': colname,
            'amount': amount,
            'cur': row['cur']
        })
    
    if report_data:
        write_text_report(report_data, rep_date)
    
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
