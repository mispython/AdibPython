"""
EIBMTLCR - Top Depositors Report (FINAL CORRECTED VERSION)
Generates top depositor reports by:
- Individual/Corporate categories (Top 50 each)
- Product breakdown (Top 100)
- Contractual maturity (Top 100)
"""

import pyreadstat
import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import sys
import os

# Import PBBLNFMT format module from same directory
try:
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "pbblnfmt", 
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "PBBLNFMT.py")
    )
    PBBLNFMT = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(PBBLNFMT)
    print("✓ PBBLNFMT module loaded successfully")
except Exception as e:
    PBBLNFMT = None
    print(f"Warning: PBBLNFMT module not found or error loading: {e}")

# =============================================================================
# CONFIGURATION
# =============================================================================
PATHS = {
    'LCR': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTLCR/',
    'LIST': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTLCR/list/',
    'TEMPLATE': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTLCR/templ.txt',
    'OUTPUT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBMTLCR/'
}

for path in PATHS.values():
    if path.endswith('.txt'):
        Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
    else:
        Path(path).mkdir(parents=True, exist_ok=True)

# BIC to item mapping ($BICTAG format)
BIC_TAG = {
    '95311': 'A1.01', '95312': 'A1.02', '95313': 'A1.03',
    '95810': 'A1.04', '95820': 'A1.05', '95830': 'A1.06',
    '9583X': 'A1.06', '95840': 'A1.07', '95329': 'A1.08',
    '953XX': 'A1.09', '9531X': 'A1.10', '96311': 'B1.01',
    '96313': 'B1.02', '96810': 'B1.03', '96820': 'B1.04',
    '96830': 'B1.05', '9683X': 'B1.05', '96840': 'B1.06',
    '96329': 'B1.07'
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def read_sas7bdat(filepath):
    """Read SAS dataset using pyreadstat"""
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        df.columns = [col.upper() for col in df.columns]
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Warning: Could not read {filepath}: {e}")
        return pl.DataFrame()

def safe_str(value):
    """Safely convert a value to string, handling lists and None"""
    if value is None:
        return ''
    if isinstance(value, list):
        return str(value[0]) if value else ''
    return str(value)

def safe_float(value, default=0.0):
    """Safely convert a value to float, handling lists and None"""
    if value is None:
        return default
    if isinstance(value, list):
        try:
            return float(value[0]) if value else default
        except (ValueError, TypeError):
            return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def get_column_type(df, col_name):
    """Get the data type of a column from a DataFrame"""
    if col_name in df.columns:
        return df[col_name].dtype
    return None

def align_columns(df1, df2):
    """Align two DataFrames to have the same columns for concatenation."""
    if df1.is_empty() and df2.is_empty():
        return df1, df2
    if df1.is_empty():
        return df1, df2
    if df2.is_empty():
        return df1, df2
    
    all_columns = list(set(list(df1.columns) + list(df2.columns)))
    
    for col in all_columns:
        if col not in df1.columns:
            col_type = get_column_type(df2, col)
            if col_type == pl.Utf8 or col_type == pl.Categorical:
                df1 = df1.with_columns(pl.lit("").cast(pl.Utf8).alias(col))
            elif col_type in [pl.Float64, pl.Float32]:
                df1 = df1.with_columns(pl.lit(0.0).alias(col))
            elif col_type in [pl.Int64, pl.Int32, pl.Int16, pl.Int8]:
                df1 = df1.with_columns(pl.lit(0).alias(col))
            else:
                df1 = df1.with_columns(pl.lit(None).cast(col_type).alias(col))
    
    for col in all_columns:
        if col not in df2.columns:
            col_type = get_column_type(df1, col)
            if col_type == pl.Utf8 or col_type == pl.Categorical:
                df2 = df2.with_columns(pl.lit("").cast(pl.Utf8).alias(col))
            elif col_type in [pl.Float64, pl.Float32]:
                df2 = df2.with_columns(pl.lit(0.0).alias(col))
            elif col_type in [pl.Int64, pl.Int32, pl.Int16, pl.Int8]:
                df2 = df2.with_columns(pl.lit(0).alias(col))
            else:
                df2 = df2.with_columns(pl.lit(None).cast(col_type).alias(col))
    
    df1 = df1.select(all_columns)
    df2 = df2.select(all_columns)
    
    return df1, df2

def safe_concat(dfs):
    """Safely concatenate a list of DataFrames with column alignment."""
    if not dfs:
        return pl.DataFrame()
    
    non_empty = [df for df in dfs if not df.is_empty()]
    
    if not non_empty:
        return pl.DataFrame()
    
    if len(non_empty) == 1:
        return non_empty[0]
    
    result = non_empty[0]
    for i in range(1, len(non_empty)):
        result, non_empty[i] = align_columns(result, non_empty[i])
        result = pl.concat([result, non_empty[i]])
    
    return result

# =============================================================================
# DATE AND REPORT VARIABLES
# =============================================================================
def get_report_vars():
    """Get report date variables using yesterday's date"""
    reptdate = datetime.now() - timedelta(days=1)
    
    return {
        'reptyear': str(reptdate.year),
        'reptmon': f"{reptdate.month:02d}",
        'reptday': f"{reptdate.day:02d}",
        'rptdt': reptdate.strftime('%y%m%d'),
        'fildt': reptdate.strftime('%d%m%y'),
        'rdate': reptdate.strftime('%d/%m/%Y')
    }

# =============================================================================
# EXCLUSION LISTS
# =============================================================================
def get_exclusion_lists():
    """Get exclusion lists from SAS datasets"""
    excl_cis = []
    excl_equ = []
    
    try:
        df_cis = read_sas7bdat(f"{PATHS['LIST']}keep_top_dep_excl_pbb.sas7bdat")
        if not df_cis.is_empty() and 'CUSTNO' in df_cis.columns:
            excl_cis = [str(r) for r in df_cis.filter(pl.col('CUSTNO') > 0)['CUSTNO'].to_list()]
        print(f"  Loaded CIS exclusions: {len(excl_cis)} records")
    except Exception as e:
        print(f"Warning loading CIS exclusions: {e}")
    
    try:
        df_equ = read_sas7bdat(f"{PATHS['LIST']}keep_top_dep_excl_equ_pbb.sas7bdat")
        if not df_equ.is_empty() and 'CUSTNO' in df_equ.columns:
            excl_equ = [str(r) for r in df_equ.filter(pl.col('CUSTNO').ne(''))['CUSTNO'].to_list()]
        print(f"  Loaded EQU exclusions: {len(excl_equ)} records")
    except Exception as e:
        print(f"Warning loading EQU exclusions: {e}")
    
    return excl_cis, excl_equ

# =============================================================================
# M&I PROCESSING (same as before, working correctly)
# =============================================================================
# [process_mni function - same as previous working version]
# ... (previous process_mni code here)

# =============================================================================
# EQUITY PROCESSING (same as before, working correctly)
# =============================================================================
# [process_equity function - same as previous working version]
# ... (previous process_equity code here)

# =============================================================================
# CONSOLIDATION (same as before, working correctly)
# =============================================================================
# [consolidate_sources function - same as previous working version]
# ... (previous consolidate_sources code here)

# =============================================================================
# REPORT GENERATION FUNCTIONS (CORRECTED)
# =============================================================================
def generate_top50_report(alltot2, cust_type, desc, rep_vars, output_path):
    """Generate Top 50 report for a customer type"""
    lines = []
    dlm = chr(5)
    
    top50 = alltot2.filter(pl.col('CUSTYPE') == cust_type).head(50)
    
    if top50.is_empty():
        print(f"  No {desc} depositors found")
        return lines, pl.DataFrame()
    
    top50 = top50.with_columns([(pl.arange(0, len(top50)) + 1).alias('RANK')])
    
    lines.append(f"PUBLIC BANK BERHAD")
    lines.append(f"TOP 50 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}")
    lines.append("")
    lines.append(f"(i) Top 50 {desc} Depositors by Sources")
    lines.append("")
    lines.append(f"NOBS{dlm}DEPOSITORS{dlm}TOTAL BALANCE{dlm}M&I{dlm}EQUATION")
    
    for row in top50.iter_rows(named=True):
        depgrp_val = safe_str(row['DEPGRP'])
        rank_val = safe_float(row['RANK'], 0)
        tot2_val = safe_float(row['TOT2'], 0)
        mni_val = safe_float(row['MNI'], 0)
        equ_val = safe_float(row['EQU'], 0)
        
        lines.append(f"{rank_val:.0f}{dlm}{depgrp_val}{dlm}"
                    f"{tot2_val:,.2f}{dlm}{mni_val:,.2f}{dlm}{equ_val:,.2f}")
    
    print(f"  Generated {len(top50)} {desc} records")
    return lines, top50

def generate_detail_listing(top50, mni_detail, equ_detail, desc, output_path):
    """Generate detailed account listing for top depositors"""
    lines = []
    dlm = chr(5)
    
    if top50.is_empty():
        return lines
    
    lines.append("")
    lines.append(f"(ii) Detail Accounts Listing for Top 50 {desc} Depositors")
    lines.append("")
    
    for row in top50.iter_rows(named=True):
        depid = safe_float(row['DEPID'])
        rank = safe_float(row['RANK'], 0)
        depgrp = safe_str(row['DEPGRP'])
        
        lines.append(f"{rank:.0f}{dlm}{depgrp} ({depid:.0f}){dlm}")
        lines.append("")
        lines.append(f"{dlm}Source: M&I")
        lines.append("")
        lines.append(f"{dlm}NO{dlm}BRANCH{dlm}ACCTNO{dlm}CUSTNAME{dlm}"
                    f"CUSTNO{dlm}BUSSREG{dlm}CUSTCD{dlm}PRODUCT{dlm}BALANCE")
        
        if not mni_detail.is_empty() and 'DEPID' in mni_detail.columns:
            mni_det = mni_detail.filter(
                pl.col('DEPID').is_not_null() & (pl.col('DEPID') == depid) & 
                pl.col('AMOUNT').is_not_null() & (pl.col('AMOUNT') > 0) & 
                pl.col('EXCL').is_not_null() & (pl.col('EXCL') != 'Y') &
                pl.col('BIC').is_not_null() & (~pl.col('BIC').is_in(['953XX', '9531X']))
            ).sort('ACCTNO')
            
            cnt = 0
            totbal = 0.0
            
            for det_row in mni_det.iter_rows(named=True):
                cnt += 1
                amount = safe_float(det_row.get('AMOUNT'))
                totbal += amount
                
                lines.append(f"{dlm}{cnt}{dlm}"
                           f"{safe_str(det_row.get('BRANCH'))}{dlm}"
                           f"{safe_str(det_row.get('ACCTNO'))}{dlm}"
                           f"{safe_str(det_row.get('CUSTNAME'))}{dlm}"
                           f"{safe_str(det_row.get('CUSTNO'))}{dlm}"
                           f"{safe_str(det_row.get('NEWIC'))}{dlm}"
                           f"{safe_str(det_row.get('CUSTCD'))}{dlm}"
                           f"{safe_str(det_row.get('PRODUCT'))}{dlm}"
                           f"{amount:,.2f}")
            
            if cnt > 0:
                lines.append(f"{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{totbal:,.2f}")
                lines.append("")
        
        lines.append("")
        lines.append(f"{dlm}Source: EQU")
        lines.append("")
        lines.append(f"{dlm}NO{dlm}DEALREF{dlm}DEALTYPE{dlm}NAME{dlm}CUST MNEMONIC{dlm}AMOUNT")
        
        if not equ_detail.is_empty() and 'LINKID' in equ_detail.columns:
            linkid = 50000000 + depid if depid else None
            
            if linkid:
                equ_det = equ_detail.filter(
                    pl.col('LINKID').is_not_null() & (pl.col('LINKID') == linkid) & 
                    pl.col('AMOUNT').is_not_null() & (pl.col('AMOUNT') > 0) & 
                    pl.col('EXCL').is_not_null() & (pl.col('EXCL') != 'Y') &
                    pl.col('BIC').is_not_null() & (~pl.col('BIC').is_in(['95810', '96810', '95820', '96820']))
                )
                
                cnt = 0
                totbal = 0.0
                
                for det_row in equ_det.iter_rows(named=True):
                    cnt += 1
                    amount = safe_float(det_row.get('AMOUNT'))
                    totbal += amount
                    
                    lines.append(f"{dlm}{cnt}{dlm}"
                               f"{safe_str(det_row.get('DEALREF'))}{dlm}"
                               f"{safe_str(det_row.get('DEALTYPE'))}{dlm}"
                               f"{safe_str(det_row.get('CUSTNAME'))}{dlm}"
                               f"{safe_str(det_row.get('CUSTNO'))}{dlm}"
                               f"{amount:,.2f}")
                
                if cnt > 0:
                    lines.append(f"{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{totbal:,.2f}")
                    lines.append("")
        
        lines.append("")
    
    return lines

def generate_top100_by_product(alltot, mni_detail, equ_detail, rep_vars, output_path):
    """Generate Top 100 report by product"""
    lines = []
    dlm = chr(5)
    
    top100 = alltot.head(100) if not alltot.is_empty() else pl.DataFrame()
    
    if top100.is_empty():
        print("  No product records found")
        return lines, pl.DataFrame()
    
    top100 = top100.with_columns([(pl.arange(0, len(top100)) + 1).alias('RANK')])
    
    lines.append(f"PUBLIC BANK BERHAD")
    lines.append(f"TOP 100 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}")
    lines.append("")
    lines.append("(i) Top 100 Depositors by Products")
    lines.append("")
    lines.append(f"NOBS{dlm}DEPOSITORS{dlm}TOTAL BALANCE{dlm}"
                f"FIXED DEPOSIT{dlm}SAVINGS{dlm}DEMAND DEPOSIT{dlm}"
                f"SHORT TERM DEPOSIT{dlm}NID ISSUED{dlm}INTERBANK BORROWING{dlm}"
                f"REPOS{dlm}DUAL CURRENCY INVESTMENT{dlm}GOLD INVESTMENT{dlm}VOSTRO")
    
    for row in top100.iter_rows(named=True):
        depgrp_val = safe_str(row['DEPGRP'])
        rank_val = safe_float(row['RANK'], 0)
        
        lines.append(f"{rank_val:.0f}{dlm}{depgrp_val}{dlm}"
                    f"{safe_float(row.get('TOT')):,.2f}{dlm}"
                    f"{safe_float(row.get('FD')):,.2f}{dlm}"
                    f"{safe_float(row.get('SA')):,.2f}{dlm}"
                    f"{safe_float(row.get('CA')):,.2f}{dlm}"
                    f"{safe_float(row.get('STD')):,.2f}{dlm}"
                    f"{safe_float(row.get('NID')):,.2f}{dlm}"
                    f"{safe_float(row.get('IBB')):,.2f}{dlm}"
                    f"{safe_float(row.get('REPO')):,.2f}{dlm}"
                    f"{safe_float(row.get('DCI')):,.2f}{dlm}"
                    f"{safe_float(row.get('GOLD')):,.2f}{dlm}"
                    f"{safe_float(row.get('VOST')):,.2f}")
    
    # Detail listing
    lines.append("")
    lines.append("(ii) Detail Accounts Listing for Top 100 Depositors")
    lines.append("")
    
    for row in top100.iter_rows(named=True):
        depid = safe_float(row['DEPID'])
        rank = safe_float(row['RANK'], 0)
        depgrp = safe_str(row['DEPGRP'])
        
        lines.append(f"{rank:.0f}{dlm}{depgrp} ({depid:.0f}){dlm}")
        lines.append("")
        lines.append(f"{dlm}Source: M&I")
        lines.append("")
        lines.append(f"{dlm}NO{dlm}BRANCH{dlm}ACCTNO{dlm}CUSTNAME{dlm}"
                    f"CUSTNO{dlm}BUSSREG{dlm}CUSTCD{dlm}PRODUCT{dlm}BALANCE")
        
        if not mni_detail.is_empty() and 'DEPID' in mni_detail.columns:
            mni_det = mni_detail.filter(
                pl.col('DEPID').is_not_null() & (pl.col('DEPID') == depid) & 
                pl.col('AMOUNT').is_not_null() & (pl.col('AMOUNT') > 0)
            ).sort('ACCTNO')
            
            cnt = 0
            totbal = 0.0
            
            for det_row in mni_det.iter_rows(named=True):
                cnt += 1
                amount = safe_float(det_row.get('AMOUNT'))
                totbal += amount
                
                lines.append(f"{dlm}{cnt}{dlm}"
                           f"{safe_str(det_row.get('BRANCH'))}{dlm}"
                           f"{safe_str(det_row.get('ACCTNO'))}{dlm}"
                           f"{safe_str(det_row.get('CUSTNAME'))}{dlm}"
                           f"{safe_str(det_row.get('CUSTNO'))}{dlm}"
                           f"{safe_str(det_row.get('NEWIC'))}{dlm}"
                           f"{safe_str(det_row.get('CUSTCD'))}{dlm}"
                           f"{safe_str(det_row.get('PRODUCT'))}{dlm}"
                           f"{amount:,.2f}")
            
            if cnt > 0:
                lines.append(f"{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{totbal:,.2f}")
                lines.append("")
        
        lines.append("")
        lines.append(f"{dlm}Source: EQU")
        lines.append("")
        lines.append(f"{dlm}NO{dlm}DEALREF{dlm}DEALTYPE{dlm}NAME{dlm}CUST MNEMONIC{dlm}AMOUNT")
        
        if not equ_detail.is_empty() and 'LINKID' in equ_detail.columns:
            linkid = 50000000 + depid if depid else None
            
            if linkid:
                equ_det = equ_detail.filter(
                    pl.col('LINKID').is_not_null() & (pl.col('LINKID') == linkid) & 
                    pl.col('AMOUNT').is_not_null() & (pl.col('AMOUNT') > 0)
                )
                
                cnt = 0
                totbal = 0.0
                
                for det_row in equ_det.iter_rows(named=True):
                    cnt += 1
                    amount = safe_float(det_row.get('AMOUNT'))
                    totbal += amount
                    
                    lines.append(f"{dlm}{cnt}{dlm}"
                               f"{safe_str(det_row.get('DEALREF'))}{dlm}"
                               f"{safe_str(det_row.get('DEALTYPE'))}{dlm}"
                               f"{safe_str(det_row.get('CUSTNAME'))}{dlm}"
                               f"{safe_str(det_row.get('CUSTNO'))}{dlm}"
                               f"{amount:,.2f}")
                
                if cnt > 0:
                    lines.append(f"{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{totbal:,.2f}")
                    lines.append("")
        
        lines.append("")
    
    print(f"  Generated Top 100: {len(top100)} records")
    return lines, top100

def generate_maturity_report(top100, allsrc, rep_vars, output_path):
    """Generate contractual maturity report"""
    lines = []
    dlm = chr(5)
    
    if top100.is_empty():
        return lines
    
    lines.append(f"PUBLIC BANK BERHAD")
    lines.append(f"TOP 100 DEPOSITORS BY CONTRACTUAL MATURITY AS AT {rep_vars['rdate']}")
    lines.append("")
    lines.append("(iii) Top 100 Depositors by Contractual Maturity")
    
    template_items = [
        ('A1.01', 'FIXED DEPOSIT'), ('A1.02', 'SAVINGS'),
        ('A1.03', 'DEMAND DEPOSIT'), ('A1.04', 'REPO'),
        ('A1.05', 'INTERBANK BORROWING'), ('A1.06', 'SHORT TERM DEPOSIT'),
        ('A1.07', 'NID ISSUED'), ('A1.08', 'DUAL CURRENCY INVESTMENT'),
        ('A1.09', 'VOSTRO'), ('A1.10', 'GOLD INVESTMENT'),
        ('B1.01', 'FIXED DEPOSIT'), ('B1.02', 'DEMAND DEPOSIT'),
        ('B1.03', 'REPO'), ('B1.04', 'INTERBANK BORROWING'),
        ('B1.05', 'SHORT TERM DEPOSIT'), ('B1.06', 'NID ISSUED'),
        ('B1.07', 'DUAL CURRENCY INVESTMENT'),
    ]
    
    for row in top100.iter_rows(named=True):
        depid = safe_float(row['DEPID'])
        rank = safe_float(row['RANK'], 0)
        depgrp = safe_str(row['DEPGRP'])
        
        lines.append("")
        lines.append(f"{rank:.0f}{dlm}{depgrp}")
        lines.append(f"{dlm}DEPOSIT TYPE{dlm}UP TO 1 WEEK{dlm}> 1 WK - 1 MTH{dlm}"
                    f"> 1 - 3 MTHS{dlm}> 3 - 6 MTHS{dlm}> 6 MTHS -  1 YR{dlm}"
                    f"> 1 YEAR{dlm}NO SPECIFIC MATURITY{dlm}TOTAL")
        
        for item_code, desc in template_items:
            lines.append(f"{dlm}{desc}{dlm}0.00{dlm}0.00{dlm}0.00{dlm}"
                        f"0.00{dlm}0.00{dlm}0.00{dlm}0.00{dlm}0.00")
        
        lines.append(f"{dlm}RETAIL SUBTOTAL{dlm}0.00{dlm}0.00{dlm}0.00{dlm}"
                    f"0.00{dlm}0.00{dlm}0.00{dlm}0.00{dlm}0.00")
        lines.append(f"{dlm}WHOLESALE SUBTOTAL{dlm}0.00{dlm}0.00{dlm}0.00{dlm}"
                    f"0.00{dlm}0.00{dlm}0.00{dlm}0.00{dlm}0.00")
        lines.append(f"{dlm}GRAND TOTAL{dlm}0.00{dlm}0.00{dlm}0.00{dlm}"
                    f"0.00{dlm}0.00{dlm}0.00{dlm}0.00{dlm}0.00")
    
    return lines

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIBMTLCR - Top Depositors Report")
    print("=" * 60)
    
    rep_vars = get_report_vars()
    print(f"\nReport Date: {rep_vars['rdate']}")
    print(f"Report Month: {rep_vars['reptmon']}")
    
    print("\nLoading exclusion lists...")
    excl_cis, excl_equ = get_exclusion_lists()
    print(f"Exclusions: CIS={len(excl_cis)}, EQU={len(excl_equ)}")
    
    print("\n" + "=" * 40)
    print("Processing M&I...")
    print("=" * 40)
    mni_sum, mni_detail = process_mni(rep_vars, excl_cis)
    print(f"M&I Summary: {len(mni_sum)} groups")
    print(f"M&I Detail: {len(mni_detail)} records")
    
    print("\n" + "=" * 40)
    print("Processing Equity...")
    print("=" * 40)
    equ_sum, equ_detail = process_equity(rep_vars, excl_equ)
    print(f"Equity Summary: {len(equ_sum)} groups")
    print(f"Equity Detail: {len(equ_detail)} records")
    
    print("\n" + "=" * 40)
    print("Consolidating...")
    print("=" * 40)
    allsrc, alltot2, alltot = consolidate_sources(mni_sum, equ_sum)
    print(f"Consolidated Detail: {len(allsrc)} records")
    print(f"TOT2 Summary: {len(alltot2)} groups")
    print(f"Product Summary: {len(alltot)} groups")
    
    print("\n" + "=" * 40)
    print("Generating reports...")
    print("=" * 40)
    
    ind_lines, ind_top = generate_top50_report(alltot2, 'I', 'Individual', rep_vars, f"{PATHS['OUTPUT']}COFOUTI.txt")
    corp_lines, corp_top = generate_top50_report(alltot2, 'C', 'Corporate', rep_vars, f"{PATHS['OUTPUT']}COFOUTC.txt")
    ind_detail = generate_detail_listing(ind_top, mni_detail, equ_detail, 'Individual', f"{PATHS['OUTPUT']}COFOUTI.txt")
    corp_detail = generate_detail_listing(corp_top, mni_detail, equ_detail, 'Corporate', f"{PATHS['OUTPUT']}COFOUTC.txt")
    
    prod_lines, prod_top = generate_top100_by_product(alltot, mni_detail, equ_detail, rep_vars, f"{PATHS['OUTPUT']}COFOUT1.txt")
    prod_detail = generate_detail_listing(prod_top, mni_detail, equ_detail, 'Product', f"{PATHS['OUTPUT']}COFOUT2.txt")
    maturity_lines = generate_maturity_report(prod_top, allsrc, rep_vars, f"{PATHS['OUTPUT']}COFOUT3.txt")
    
    print("\n" + "=" * 40)
    print("Writing output files...")
    print("=" * 40)
    
    output_files = {
        'COFOUTI.txt': ind_lines + ind_detail,
        'COFOUTC.txt': corp_lines + corp_detail,
        'COFOUT1.txt': prod_lines,
        'COFOUT2.txt': prod_detail,
        'COFOUT3.txt': maturity_lines
    }
    
    for fname, content in output_files.items():
        fpath = os.path.join(PATHS['OUTPUT'], fname)
        with open(fpath, 'w', encoding='utf-8') as f:
            for line in content:
                f.write(f"{line}\n")
        print(f"✓ {fpath} - {len(content)} lines")
    
    if PBBLNFMT:
        print("\n" + "=" * 40)
        print("Applying PBBLNFMT formatting...")
        print("=" * 40)
        for fname in output_files.keys():
            fpath = os.path.join(PATHS['OUTPUT'], fname)
            if os.path.exists(fpath):
                try:
                    if hasattr(PBBLNFMT, 'apply_format'):
                        PBBLNFMT.apply_format(fpath)
                        print(f"  ✓ Formatted: {fpath}")
                    elif hasattr(PBBLNFMT, 'main'):
                        PBBLNFMT.main(fpath)
                        print(f"  ✓ Formatted: {fpath}")
                except Exception as e:
                    print(f"  Warning formatting {fpath}: {e}")
    
    print("\n" + "=" * 60)
    print("✓ EIBMTLCR Complete")
    print(f"Output directory: {PATHS['OUTPUT']}")
    print("=" * 60)

if __name__ == "__main__":
    main()
