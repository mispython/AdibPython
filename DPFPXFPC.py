"""
EIIDLCRM - BNM LCR Reporting for Islamic Banking (Simplified)
Consolidates Islamic deposits & treasury positions for BNM LCR reporting.
Includes MGIA, TD-I, and Islamic treasury products.

Converted from SAS program (PBBELF / PBLCRFMT).
All inputs are sas7bdat files (except walk.txt and templ.txt).
Output in sas7bdat and text files via saspy.
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import calendar
import pyreadstat
import saspy
import pandas as pd

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'LCR': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/lcr',
    'LCRM': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/lcr',
    'CISDP': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/cisdp',
    'CISCA': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/cisca',
    'CIS': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cis',
    'EQUA': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/equa',
    'LIST': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/list',
    'DEPOSIT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/DEPOSIT',
    'WALK': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/walk.txt',
    'TEMPL': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMLCRM/templ.txt',
    'OUTPUT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLCRM'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

INST = 'PBB'  # Institution code

# =============================================================================
# CUSTOMER CATEGORY MAPPINGS (LCR)
# =============================================================================
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
    '49': ['AIM', 'PBL', 'PBLEUR', 'PBLNID', 'PBLUSD', 'PIVMYR', 'PBB', 'PBBMYR', 'PBBUSD', 'CUST']
}

MGIA_PRODUCTS = [302, 315, 394, 396]  # Products that map to MGIA

# Special customer numbers mapped to CUST='39'
SPECIAL_39_NUMBERS = [
    4391161, 2115999, 12579649, 13468207, 14300254,
    14675929, 15327497, 17104931, 12677444, 3703533,
    5978659, 16185090, 2558344, 10819745
]

# Exclusion list for reclassification
EXCLUDE_CUSTNO = [
    14094942, 16557696, 3728510, 11335374, 16265490,
    3523050, 11880426, 16771972, 15241330, 16500538
]

# =============================================================================
# FORMAT MAPPINGS (from PBLCRFMT)
# =============================================================================
# BIC -> COLID mapping
COLID_MAP = {
    '95315': 'FD95315RM',
    '95317': 'FD95317RM',
    '95312': 'SA95312RM',
    '95313': 'CA95313RM',
    '96313': 'CA96313FX',
    '95830': 'STD95830',
    '95840': 'NID95840',
    '96840': 'NID95840',
    '9X810': 'IBB9X810',
}

# LCRCDMNI - Item mapping for deposits (BIC + CUST -> ITEM)
# These are the item codes used in the report template
LCRCDMNI_MAP = {
    # MGIA (P) - 95315
    '95315' + '08': 'B1.01', '95315' + '19': 'B1.02', '95315' + '29': 'B1.03',
    '95315' + '39': 'B1.04', '95315' + '49': 'B1.05', '95315' + '59': 'B1.06',
    # TD-I (Q) - 95317
    '95317' + '08': 'B2.01', '95317' + '19': 'B2.02', '95317' + '29': 'B2.03',
    '95317' + '39': 'B2.04', '95317' + '49': 'B2.05', '95317' + '59': 'B2.06',
    # SA (S) - 95312
    '95312' + '08': 'B3.01', '95312' + '19': 'B3.02', '95312' + '29': 'B3.03',
    '95312' + '39': 'B3.04', '95312' + '49': 'B3.05', '95312' + '59': 'B3.06',
    # CA (T) - 95313
    '95313' + '08': 'B4.01', '95313' + '19': 'B4.02', '95313' + '29': 'B4.03',
    '95313' + '39': 'B4.04', '95313' + '49': 'B4.05', '95313' + '59': 'B4.06',
    # FX CA (T) - 96313
    '96313' + '08': 'B4.07', '96313' + '19': 'B4.08', '96313' + '29': 'B4.09',
    '96313' + '39': 'B4.10', '96313' + '49': 'B4.11', '96313' + '59': 'B4.12',
    # STD (U) - 95830
    '95830' + '08': 'B5.01', '95830' + '19': 'B5.02', '95830' + '29': 'B5.03',
    '95830' + '39': 'B5.04', '95830' + '49': 'B5.05', '95830' + '59': 'B5.06',
    # NID (W) - 95840/96840
    '95840' + '08': 'B6.01', '95840' + '19': 'B6.02', '95840' + '29': 'B6.03',
    '95840' + '39': 'B6.04', '95840' + '49': 'B6.05', '95840' + '59': 'B6.06',
    '96840' + '08': 'B6.07', '96840' + '19': 'B6.08', '96840' + '29': 'B6.09',
    '96840' + '39': 'B6.10', '96840' + '49': 'B6.11', '96840' + '59': 'B6.12',
    # IBB (X) - 9X810
    '9X810' + '08': 'B7.01', '9X810' + '19': 'B7.02', '9X810' + '29': 'B7.03',
    '9X810' + '39': 'B7.04', '9X810' + '49': 'B7.05', '9X810' + '59': 'B7.06',
}

# LCRCDMNIOPR - Operational deposit item mapping
LCRCDMNIOPR_MAP = {
    '95313' + '08': 'B4.13', '95313' + '19': 'B4.14', '95313' + '29': 'B4.15',
    '95313' + '39': 'B4.16', '95313' + '49': 'B4.17', '95313' + '59': 'B4.18',
    '96313' + '08': 'B4.19', '96313' + '19': 'B4.20', '96313' + '29': 'B4.21',
    '96313' + '39': 'B4.22', '96313' + '49': 'B4.23', '96313' + '59': 'B4.24',
}

# LCRCDEQU - Equity item mapping (Part A)
LCRCDEQU_MAP = {
    '08': 'A1.01', '19': 'A1.02', '29': 'A1.03',
    '39': 'A1.04', '49': 'A1.05', '59': 'A1.06',
}

# LCRCDIGL - GL item mapping
LCRCDIGL_MAP = {
    # This would need the actual SET_ID -> ITEM mapping
    # Placeholder - actual mapping depends on GL structure
}

# =============================================================================
# SAS HELPER
# =============================================================================
def get_sas_session():
    """Create a SAS session via saspy"""
    return saspy.SASsession(cfgname='default')

# =============================================================================
# DATE UTILITIES
# =============================================================================
def get_report_date():
    """Set report date as yesterday (datetime timedelta - 1)"""
    reptdate = datetime.now() - timedelta(days=1)
    reptdate = datetime(reptdate.year, reptdate.month, reptdate.day)  # strip time
    
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
# SAS7BDAT READER (all lowercase)
# =============================================================================
def read_sas(path):
    """Read a sas7bdat file into a polars DataFrame with all lowercase column names."""
    df_pd, meta = pyreadstat.read_sas7bdat(path)
    df_pd.columns = [c.lower() for c in df_pd.columns]
    return pl.from_pandas(df_pd)

def read_sas_pd(path):
    """Read a sas7bdat file into a pandas DataFrame with all lowercase column names."""
    df_pd, meta = pyreadstat.read_sas7bdat(path)
    df_pd.columns = [c.lower() for c in df_pd.columns]
    return df_pd

# =============================================================================
# TREASURY PROCESSING (KAPITI)
# =============================================================================
def process_treasury_k1k3(rep_date):
    """Process K1TBL and K3TBL from KTBLALL"""
    records = []
    try:
        df = read_sas(f"{PATHS['LCR']}ktblall.sas7bdat")
        
        for row in df.iter_rows(named=True):
            tbl = row.get('tbl')
            if tbl == '1':
                records.append({
                    'src': 'K1TBL', 'bnmcode': row['bnmcode'], 'cur': row['gwccy'],
                    'amt': row['gwamt'], 'dealtype': row['gwdlp'], 'dealref': row['gwdlr'],
                    'custfiss': row['gwc2r'], 'custno': None, 'utctp': row.get('utctp', ''),
                    'gwshn': row.get('gwshn', '')
                })
            elif tbl == '3':
                records.append({
                    'src': 'K3TBL', 'bnmcode': row['bnmcode'], 'cur': row['utccy'],
                    'amt': row['utamt'], 'dealtype': row['utsty'], 'dealref': row['utdlr'],
                    'custfiss': None, 'custno': row['utcus'], 'utctp': row.get('utctp', ''),
                    'gwshn': row.get('gwshn', '')
                })
    except Exception as e:
        print(f"  K1/K3 warning: {e}")
    return records

def process_cis_equity():
    """Process CIS equity data for customer mapping"""
    records = {}
    try:
        df = read_sas(f"{PATHS['CIS']}custdly.sas7bdat")
        df = df.filter((pl.col('acctcode') == 'EQC') & (pl.col('prisec') == 901))
        
        for row in df.iter_rows(named=True):
            newic = row.get('newic', '')
            if not newic or (len(newic) >= 5 and newic[:5] == '99999'):
                icno = f"{row.get('aliaskei', '')}{row.get('custno', 0)}".replace(' ', '')
            else:
                icno = f"{row.get('aliaskei', '')}{row.get('alias', '')}".replace(' ', '')
            
            records[row['acctno']] = {
                'cisno': row['custno'], 'cisname': row['custname'], 'icno': icno
            }
    except Exception as e:
        print(f"  CIS equity warning: {e}")
    return records

def process_utsas(rep_date):
    """Process UTSAS from EQUA Islamic tables"""
    records = {}
    utvar = ['dealref', 'dealtype', 'custfiss', 'custno', 'custname', 'custeqno', 'custid']
    
    try:
        for prefix in ['iutms', 'iutfx', 'iutrp']:
            df = read_sas(f"{PATHS['EQUA']}{prefix}{rep_date['rptdt']}.sas7bdat")
            keep = [c for c in utvar if c in df.columns]
            if keep:
                df = df.select(keep)
                if 'custeqno' in df.columns:
                    df = df.rename({'custeqno': 'acctno'})
                for row in df.rows(named=True):
                    records[row['dealref']] = row
    except Exception as e:
        print(f"  UTSAS warning: {e}")
    return records

# =============================================================================
# CORE BANKING
# =============================================================================
def process_core_banking(rep_date):
    """Process Islamic core banking: FD, SA, CA, FCYCA"""
    records = []
    
    for tbl in ['fd', 'sa', 'ca', 'fcyca']:
        try:
            df = read_sas(f"{PATHS['LCR']}{tbl}{rep_date['day']}.sas7bdat")
            
            for row in df.iter_rows(named=True):
                custcd = row.get('custcdx' if tbl == 'fd' else 'custcd', 0)
                if tbl == 'fd' and custcd is not None:
                    custcd = f"{int(custcd):02d}"
                cust = get_cust(custcd, CUST_MAP)
                
                rem30d = row.get('rem30d', row.get('remmth', 1)) or row.get('remmth', 1)
                remmth = row.get('remmth', 1)
                
                bic = row['bnmcode'][:5]
                if bic == '95317' and row.get('product') in MGIA_PRODUCTS:
                    bic = '95315'  # MGIA mapping
                
                records.append({
                    'src': tbl.upper(), 'bic': bic, 'bnmcode': f"{bic}{cust}020000Y",
                    'cmmcode': f"{bic}{cust}{fmt_mth(remmth)}0000Y",
                    'cur': row.get('curcode', 'MYR'), 'amt': row.get('amount', 0),
                    'acctno': row.get('acctno'), 'custno': row.get('custno'),
                    'rem30d': rem30d, 'remmth': remmth, 'ecp': '00',
                    'product': row.get('product'), 'billerind': row.get('billerind', 'N'),
                    'pbmerch': row.get('pbmerch', 'N'), 'intrate': row.get('intrate', 0),
                    'oprrate': row.get('oprrate', 0), 'source': row.get('source', ''),
                    'dtsigned': row.get('dtsigned'), 'intplan': row.get('intplan', 0),
                    'sme_tag': row.get('sme_tag', ''), 'fdhold': row.get('fdhold', 'N'),
                    'trx': row.get('trx', 0), 'sign': '', 'custcd': custcd,
                    'branch': row.get('branch', ''), 'cdno': row.get('cdno', ''),
                    'matdt': row.get('matdt')
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
# SAS OUTPUT WRITERS
# =============================================================================
def write_sas7bdat_via_proc(df_pl, out_path, sas):
    """Write a polars DataFrame to sas7bdat using saspy + PROC EXPORT."""
    df_pd = df_pl.to_pandas()
    # Convert any problematic types
    for col in df_pd.columns:
        if df_pd[col].dtype == object:
            df_pd[col] = df_pd[col].astype(str)
        # Convert date columns
        if pd.api.types.is_datetime64_any_dtype(df_pd[col]):
            df_pd[col] = df_pd[col].dt.strftime('%Y-%m-%d')
    
    sas.df2sd(df_pd, table='_tmp_out', libref='WORK')
    sas.submit(f"""
    proc export data=WORK._tmp_out
        outfile="{out_path}"
        dbms=sas7bdat
        replace;
    run;
    """)

def write_text_file(df_pl, out_path, sas, header_lines=None):
    """Write a polars DataFrame to a text file via saspy."""
    df_pd = df_pl.to_pandas()
    for col in df_pd.columns:
        if df_pd[col].dtype == object:
            df_pd[col] = df_pd[col].astype(str)
    
    sas.df2sd(df_pd, table='_tmp_txt', libref='WORK')
    
    header_sas = ""
    if header_lines:
        header_sas = "\n".join([f'    put "{line}";' for line in header_lines])
    
    sas.submit(f"""
    data _null_;
        file "{out_path}";
        {header_sas}
        set WORK._tmp_txt;
        put _all_;
    run;
    """)

def write_lcr_report_sas(rep_df_pl, template_df_pl, out_sas_path, out_txt_path, rep_date, sas):
    """Write the LCR report in SAS format matching the original program."""
    df_pd = rep_df_pl.to_pandas()
    tmpl_pd = template_df_pl.to_pandas()
    
    # Merge report with template
    merged = tmpl_pd.merge(df_pd, on='item', how='left')
    
    # Write sas7bdat
    for col in merged.columns:
        if merged[col].dtype == object:
            merged[col] = merged[col].astype(str)
    sas.df2sd(merged, table='_lcr_report', libref='WORK')
    sas.submit(f"""
    proc export data=WORK._lcr_report
        outfile="{out_sas_path}"
        dbms=sas7bdat
        replace;
    run;
    """)
    
    # Write text file (simple tab-delimited version)
    sas.submit(f"""
    data _null_;
        file "{out_txt_path}";
        set WORK._lcr_report;
        put _all_;
    run;
    """)

# =============================================================================
# TEMPLATE READER
# =============================================================================
def read_template():
    """Read the template file (templ.txt)"""
    items = []
    try:
        with open(PATHS['TEMPL'], 'r') as f:
            for line in f:
                if len(line) >= 7:
                    item = line[0:5].strip()
                    idesc = line[7:127].strip() if len(line) > 7 else ''
                    if item:
                        items.append({'item': item, 'idesc': idesc})
    except Exception as e:
        print(f"  Template warning: {e}")
    return pl.DataFrame(items) if items else pl.DataFrame({'item': [], 'idesc': []})

def read_walker_gl():
    """Read walker GL file (walk.txt)"""
    records = []
    try:
        with open(PATHS['WALK'], 'r') as f:
            for line in f:
                if len(line) >= 63:
                    set_id = line[1:20].strip()
                    amount_str = line[41:61].strip().replace(',', '')
                    sign = line[61:62].strip()
                    try:
                        amount = float(amount_str) if amount_str else 0.0
                    except ValueError:
                        amount = 0.0
                    if sign == '':
                        amount = -1 * amount
                    item = LCRCDIGL_MAP.get(set_id, '')
                    if item:
                        records.append({'set_id': set_id, 'item': item, 'amount': amount})
    except Exception as e:
        print(f"  Walker GL warning: {e}")
    return records

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIIDLCRM - BNM LCR Reporting (Islamic Banking)")
    print("=" * 60)
    
    # SAS session
    sas = get_sas_session()
    
    # Report date
    rep_date = get_report_date()
    print(f"\nDate: {rep_date['date'].strftime('%d/%m/%Y')} Week:{rep_date['nowk']} Mon:{rep_date['mon']}")
    
    # Template
    template = read_template()
    print(f"Template: {len(template)} items")
    
    # CIS data
    cis_dict = process_cis_equity()
    print(f"CIS: {len(cis_dict)} records")
    
    # =========================================================================
    # TREASURY
    # =========================================================================
    print("\nTreasury...")
    k_records = process_treasury_k1k3(rep_date)
    utsas_dict = process_utsas(rep_date)
    
    # Merge ALLEQU with UTSAS
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
        
        # Special customer
        if custno in SPECIAL_CUST.get('39', []):
            cust = '39'
        
        # Deal type for BQD
        dtype = '01' if r.get('dealtype') == 'BQD' else '00'
        
        # Build codes
        bic = r['bnmcode'][:5]
        rem30d = r.get('rem30d', r.get('remmth', 1)) or r.get('remmth', 1)
        remmth = r.get('remmth', 1)
        
        if rem30d is None:
            rem30d = remmth
        if rem30d > 1 and remmth > 1:
            rem30d = remmth
        
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
            'rem30d': rem30d, 'remmth': remmth, 'custno': custno,
            'dealtype': r.get('dealtype', ''), 'matdt': r.get('matdt')
        })
    
    print(f"  Treasury: {len(treasury)} records")
    
    # Treasury totals (EQUTOT)
    equtot = {}
    for r in treasury:
        key = (r['bnmcode'], r['cur'])
        equtot[key] = equtot.get(key, 0) + r['amt']
    
    # Treasury equity totals (TOTEQU)
    totequ = {}
    for r in treasury:
        if r['bic'][2:5] in ['810','820','830','83X','840','850'] or r['bic'][2:5].startswith('8'):
            icgrp = r.get('icgrp', '')
            if icgrp:
                totequ[icgrp] = totequ.get(icgrp, 0) + r['amt']
    
    # =========================================================================
    # CORE BANKING
    # =========================================================================
    print("\nBanking...")
    banking = process_core_banking(rep_date)
    
    # Merge CIS and ECP
    try:
        cis_info = read_sas(f"{PATHS['LCR']}cisinfo.sas7bdat")
        cis_dict2 = {r['acctno']: r for r in cis_info.rows(named=True)}
    except:
        cis_dict2 = {}
    
    try:
        ecp_df = read_sas(f"{PATHS['LIST']}lcr_ecp.sas7bdat").unique(subset=['acctno'])
        ecp_dict = {r['acctno']: r['ecp'] for r in ecp_df.rows(named=True)}
    except:
        ecp_dict = {}
    
    # SME data
    try:
        sme_df = read_sas(f"{PATHS['LCRM']}sme.sas7bdat")
        sme_dict = {r['acctno']: r.get('sme_tag', '') for r in sme_df.rows(named=True)}
    except:
        sme_dict = {}
    
    enhanced = []
    
    for r in banking:
        # CIS
        if r['acctno'] in cis_dict2:
            ci = cis_dict2[r['acctno']]
            r['newic'] = ci.get('newic')
            r['oldic'] = ci.get('oldic')
            r['custname'] = ci.get('custname', '')
        
        # ECP
        if r['acctno'] in ecp_dict:
            r['ecp'] = ecp_dict[r['acctno']]
        if r['ecp'] == '' or r['ecp'] is None:
            r['ecp'] = '00'
        if r['ecp'] == '01':
            if r['intrate'] < r['oprrate']:
                r['ecp'] = '01'
            else:
                r['ecp'] = '00'
        if r['billerind'] == 'Y' or r['pbmerch'] == 'Y':
            r['ecp'] = '01'
        
        # SME tag
        if r['acctno'] in sme_dict:
            r['sme_tag'] = sme_dict[r['acctno']]
        
        # SIGN
        prod_list = [106,151,158,97,164,201,215]
        intplan_list = list(range(400,420)) + list(range(600,659)) + \
                       list(range(720,741)) + list(range(864,891)) + list(range(941,968))
        
        if (r['product'] in prod_list or r['intplan'] in intplan_list or
            (r['source'] != 'PGD' and r['dtsigned'] and 
             (rep_date['date'] - r['dtsigned']).days >= 365)):
            r['sign'] = 'R '
        
        # Special customer
        if r['custno'] in SPECIAL_39_NUMBERS:
            r['cust'] = '39'
        
        # ICGRP
        r['icgrp'] = str(r.get('newic', r.get('oldic', ''))).replace(' ', '')
        enhanced.append(r)
    
    # ICGRP totals
    icgrp_totals = {}
    for r in enhanced:
        icgrp_totals[r['icgrp']] = icgrp_totals.get(r['icgrp'], 0) + r['amt']
    
    # Reclassification
    for r in enhanced:
        r['toticbal'] = icgrp_totals.get(r['icgrp'], 0)
        
        # Reclass
        if (r['custno'] not in EXCLUDE_CUSTNO and r['bnmcode'][5:7] == '29') or r['custcd'] in ['72','73','74']:
            totdp = r['toticbal'] + totequ.get(r['icgrp'], 0)
            if totdp < 5000000:
                r['bnmcode'] = f"{r['bic']}19{r['bnmcode'][7:]}"
                r['cmmcode'] = f"{r['bic']}19{r['cmmcode'][7:]}"
        elif r['bnmcode'][5:7] == '19' and r.get('sme_tag') == 'N':
            totdp = r['toticbal'] + totequ.get(r['icgrp'], 0)
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
    
    # =========================================================================
    # COMBINE ALL
    # =========================================================================
    all_data = treasury + banking_split
    print(f"Total: {len(all_data)} records")
    
    # Consolidate
    df = pl.DataFrame(all_data)
    df = df.with_columns([(pl.col('amt') / 1000).round(2).alias('amt_k')])
    summary = df.group_by(['bnmcode', 'cur']).agg([pl.col('amt_k').sum()])
    print(f"Summary: {len(summary)} codes")
    
    # =========================================================================
    # BUILD REPORT
    # =========================================================================
    # Map BNMCODE to ITEM and COLNAME
    report_data = []
    for row in summary.rows(named=True):
        bic = row['bnmcode'][:5]
        cust = row['bnmcode'][5:7]
        rem = row['bnmcode'][9:11]
        ecp = row['bnmcode'][9:11]
        dltype = row['bnmcode'][11:13]
        
        # Determine COLNAME
        colname = COLID_MAP.get(bic, '')
        
        # Determine ITEM
        item = ''
        if dltype == '01':
            # Treasury / Equity
            colname = 'STQ95830'
            item = LCRCDEQU_MAP.get(cust, '')
            if item == 'B3.30' and rem == '02':
                item = 'B6.30'
        else:
            # Banking
            if bic in ['95313','96313'] and ecp == '01':
                item = LCRCDMNIOPR_MAP.get(f"{bic}{cust}", '')
            if not item:
                item = LCRCDMNI_MAP.get(f"{bic}{cust}", '')
        
        if colname and item:
            amt = abs(round(row['amt_k'], 2))
            
            # Adjust COLNAME for remmth
            if colname[:2] == 'FD' or colname[:3] in ['STD','STQ']:
                if rem == '01':
                    colname = f"{colname}1"
                else:
                    colname = f"{colname}2"
            elif colname[:3] in ['NID','IBB']:
                for i in range(1,7):
                    if fmt_mth(i) == rem:
                        colname = f"{colname}V{i}"
                        break
            
            report_data.append({'item': item, 'col': colname, 'amt': amt})
    
    # Pivot
    if report_data:
        rep_df = pl.DataFrame(report_data)
        final = rep_df.group_by(['item', 'col']).agg([pl.col('amt').sum()])
        pivot = final.pivot(index='item', columns='col', values='amt', aggregate_function='sum')
        
        # Fill nulls with 0
        pivot = pivot.fill_null(0)
        
        # =====================================================================
        # ADD TOTALS AND DERIVED COLUMNS
        # =====================================================================
        # FD95315RM = FD95315RM1 + FD95315RM2
        if 'FD95315RM1' in pivot.columns and 'FD95315RM2' in pivot.columns:
            pivot = pivot.with_columns(
                (pl.col('FD95315RM1').fill_null(0) + pl.col('FD95315RM2').fill_null(0)).alias('FD95315RM')
            )
        if 'FD95317RM1' in pivot.columns and 'FD95317RM2' in pivot.columns:
            pivot = pivot.with_columns(
                (pl.col('FD95317RM1').fill_null(0) + pl.col('FD95317RM2').fill_null(0)).alias('FD95317RM')
            )
        
        # STD95830 = SUM(STD95830V1, STD95830V2)
        std_cols = [c for c in pivot.columns if c.startswith('STD95830V')]
        if std_cols:
            pivot = pivot.with_columns(pl.sum_horizontal(std_cols).alias('STD95830'))
        
        # STQ95830 = SUM(STQ95830V1, STQ95830V2)
        stq_cols = [c for c in pivot.columns if c.startswith('STQ95830V')]
        if stq_cols:
            pivot = pivot.with_columns(pl.sum_horizontal(stq_cols).alias('STQ95830'))
        
        # NID95840 = SUM(NID95840V1..V6)
        nid_cols = [c for c in pivot.columns if c.startswith('NID95840V')]
        if nid_cols:
            pivot = pivot.with_columns(pl.sum_horizontal(nid_cols).alias('NID95840'))
        
        # IBB9X810 = SUM(IBB9X810V1..V6)
        ibb_cols = [c for c in pivot.columns if c.startswith('IBB9X810V')]
        if ibb_cols:
            pivot = pivot.with_columns(pl.sum_horizontal(ibb_cols).alias('IBB9X810'))
        
        # TOTALV1 = FD95315RM + FD95317RM1 + SA95312RM + CA95313RM + CA96313FX + STD95830 + STQ95830 + NID95840 + IBB9X810V1 + OTHSOURCE
        totalv1_cols = []
        for c in ['FD95315RM', 'FD95317RM1', 'SA95312RM', 'CA95313RM', 'CA96313FX',
                  'STD95830', 'STQ95830', 'NID95840', 'IBB9X810V1', 'OTHSOURCE']:
            if c in pivot.columns:
                totalv1_cols.append(c)
        if totalv1_cols:
            pivot = pivot.with_columns(pl.sum_horizontal(totalv1_cols).alias('TOTALV1'))
        
        # TOTALDP = FD95315RM + FD95317RM + SA95312RM + CA95313RM + CA96313FX + STD95830 + STQ95830 + NID95840 + IBB9X810 + OTHSOURCE
        totaldp_cols = []
        for c in ['FD95315RM', 'FD95317RM', 'SA95312RM', 'CA95313RM', 'CA96313FX',
                  'STD95830', 'STQ95830', 'NID95840', 'IBB9X810', 'OTHSOURCE']:
            if c in pivot.columns:
                totaldp_cols.append(c)
        if totaldp_cols:
            pivot = pivot.with_columns(pl.sum_horizontal(totaldp_cols).alias('TOTALDP'))
        
        # =====================================================================
        # WALKER GL
        # =====================================================================
        gl_records = read_walker_gl()
        if gl_records:
            gl_df = pl.DataFrame(gl_records)
            gl_summary = gl_df.group_by('item').agg(pl.col('amount').sum().alias('othsource'))
            # Round
            gl_summary = gl_summary.with_columns((pl.col('othsource') / 1000).round(2).alias('othsource'))
            # Merge into pivot
            pivot = pivot.join(gl_summary, on='item', how='left', suffix='_gl')
            if 'othsource' in pivot.columns:
                pivot = pivot.rename({'othsource': 'OTHSOURCE'})
        
        # =====================================================================
        # MERGE WITH TEMPLATE
        # =====================================================================
        # Convert pivot to long format for merging with template
        pivot_long = pivot.unpivot(index='item', variable_name='col', value_name='amt')
        
        # Merge with template
        if len(template) > 0:
            template_pd = template.to_pandas()
            pivot_pd = pivot.to_pandas()
            merged = template_pd.merge(pivot_pd, on='item', how='left')
        else:
            merged = pivot.to_pandas()
        
        # =====================================================================
        # OUTPUT
        # =====================================================================
        out_df = pl.from_pandas(merged)
        
        # sas7bdat output
        sas_out = f"{PATHS['OUTPUT']}lcr{rep_date['day']}.sas7bdat"
        write_sas7bdat_via_proc(out_df, sas_out, sas)
        print(f"Report (sas7bdat): lcr{rep_date['day']}.sas7bdat")
        
        # Text output
        txt_out = f"{PATHS['OUTPUT']}lcr{rep_date['day']}.txt"
        header_lines = [
            'PUBLIC ISLAMIC BANK BERHAD',
            f"LIQUIDITY COVERAGE RATIO (LCR) AS AT {rep_date['rdate']}",
            ''
        ]
        write_text_file(out_df, txt_out, sas, header_lines)
        print(f"Report (text): lcr{rep_date['day']}.txt")
    
    # Summary
    total = df['amt'].sum() / 1000
    print(f"\nTotal: RM {total:,.0f}K")
    print("=" * 60)
    print("EIIDLCRM Complete")
    
    sas.endsas()

if __name__ == "__main__":
    main()
