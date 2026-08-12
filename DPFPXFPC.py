"""
EIIMTLCR - Top Depositors Report (Islamic Banking)
Generates top depositor reports for Islamic Banking by:
- Individual/Corporate categories (Top 50 each)
- Product breakdown (Top 100)
- Contractual maturity (Top 100)
"""

import pyreadstat
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import gc

# =============================================================================
# CONFIGURATION
# =============================================================================
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/'
OUTPUT_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTLCR/'
LIST_PATH = os.path.join(BASE_PATH, 'list/')
LCR_PATH = BASE_PATH

os.makedirs(OUTPUT_PATH, exist_ok=True)

DLM = '\x05'  # SAS hex 05 delimiter

# BIC to item mapping for Islamic products
BIC_TAG = {
    '95315': 'A1.01', '95317': 'A1.02', '95312': 'A1.03',
    '95313': 'A1.04', '95810': 'A1.05', '95820': 'A1.06',
    '95830': 'A1.07', '95840': 'A1.08', '96317': 'B1.01',
    '96313': 'B1.02', '96810': 'B1.03', '96820': 'B1.04',
    '96830': 'B1.05', '96840': 'B1.06'
}

# Product classification sets
FD_BICS = {'95315', '96315', '95317', '96317'}
CA_BICS = {'95313', '96313'}
RNID_BICS = {'95840', '96840'}
STD_BICS = {'95830', '96830'}
IBB_BICS = {'95810', '96810'}
REPO_BICS = {'95820', '96820'}
EXCLUDE_BICS = {'95810', '96810', '95820', '96820'}
NO_MATURITY_BICS = {'95312', '95313', '96313'}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def read_sas(filepath):
    """Read SAS dataset with lowercase column names"""
    if not os.path.exists(filepath):
        print(f"WARNING: File not found: {filepath}")
        return pd.DataFrame()
    try:
        df, _ = pyreadstat.read_sas7bdat(filepath)
        df.columns = [c.lower() for c in df.columns]
        return df
    except Exception as e:
        print(f"ERROR reading {filepath}: {e}")
        return pd.DataFrame()

def get_report_vars():
    """Calculate report date variables (yesterday)"""
    d = datetime.now() - timedelta(days=1)
    return {
        'reptmon': d.strftime('%m'),
        'rdate': d.strftime('%d/%m/%Y')
    }

def fmt(value):
    """Format number as COMMA20.2"""
    if value is None or pd.isna(value):
        return '0.00'
    try:
        return f"{float(value):,.2f}"
    except (ValueError, TypeError):
        return '0.00'

def s(value, max_len=None):
    """Safe string conversion"""
    if value is None or pd.isna(value):
        return ''
    val = str(value).strip()
    return val[:max_len] if max_len else val

# =============================================================================
# EXCLUSION LISTS
# =============================================================================
def get_exclusions():
    """Load exclusion lists from SAS datasets"""
    excl_cis, excl_equ = set(), set()
    
    # CIS exclusions (CUSTNO > 0)
    df = read_sas(os.path.join(LIST_PATH, 'keep_top_dep_excl_pibb.sas7bdat'))
    if not df.empty and 'custno' in df.columns:
        excl_cis = {str(int(r)) for r in df[df['custno'] > 0]['custno']}
    
    # EQU exclusions (non-empty CUSTNO)
    df = read_sas(os.path.join(LIST_PATH, 'keep_top_dep_excl_equ_pibb.sas7bdat'))
    if not df.empty and 'custno' in df.columns:
        excl_equ = {s(r) for r in df[df['custno'].notna()]['custno'] if s(r)}
    
    return excl_cis, excl_equ

# =============================================================================
# DEPOSITOR MATCHING
# =============================================================================
def match_depositors(data, cof, merge_col, id_col='depid', grp_col='depgrp'):
    """
    Match depositors using COF list with fallback to new DEPID assignment.
    merge_col: column to merge on ('newic' for M&I, 'custno' for EQU)
    """
    if cof.empty or merge_col not in cof.columns:
        # Assign new DEPIDs sequentially
        unique = data[[merge_col]].drop_duplicates().sort_values(merge_col)
        start_id = 5001 if merge_col == 'newic' else 50005001
        unique['depid'] = range(start_id, start_id + len(unique))
        result = data.merge(unique, on=merge_col, how='left')
        result['depgrp'] = result.get('custname', '')
        return result
    
    # Prepare COF lookup (deduplicated)
    cof_lookup = cof[[id_col, grp_col, merge_col]].drop_duplicates(subset=merge_col)
    
    # Merge
    result = data.merge(cof_lookup, on=merge_col, how='left')
    
    # Assign new DEPID for unmatched
    unmatched = result[id_col].isna()
    if unmatched.any():
        unique = result.loc[unmatched, merge_col].drop_duplicates().sort_values()
        start_id = 5001 if merge_col == 'newic' else 50005001
        new_ids = {v: start_id + i for i, v in enumerate(unique)}
        result.loc[unmatched, id_col] = result.loc[unmatched, merge_col].map(new_ids)
        result.loc[unmatched, grp_col] = result.loc[unmatched, 'custname'].fillna('')
    
    return result

# =============================================================================
# M&I PROCESSING
# =============================================================================
def process_mni(rep_vars, excl_cis):
    """Process Money & Islamic deposits"""
    print("\n--- Processing M&I ---")
    
    cmm = read_sas(os.path.join(LCR_PATH, f"cmm{rep_vars['reptmon']}.sas7bdat"))
    if cmm.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"  Records: {len(cmm):,}")
    
    # Apply exclusions
    cmm['excl'] = 'N'
    if excl_cis and 'custno' in cmm.columns:
        cmm.loc[cmm['custno'].apply(lambda x: s(int(x)) if pd.notna(x) else '').isin(excl_cis), 'excl'] = 'Y'
        print(f"  Excluded: {(cmm['excl'] == 'Y').sum():,}")
    
    # Match depositors
    cof = read_sas(os.path.join(LIST_PATH, 'icof_mni_depositor_list.sas7bdat'))
    cmm = match_depositors(cmm, cof, 'newic')
    
    # Classify products
    cmm['bic'] = cmm['cmmcode'].astype(str).str[:5]
    cmm['amount'] = cmm['amount'].fillna(0)
    
    cmm['fd']   = np.where(cmm['bic'].isin(FD_BICS), cmm['amount'], 0)
    cmm['sa']   = np.where(cmm['bic'] == '95312', cmm['amount'], 0)
    cmm['ca']   = np.where(cmm['bic'].isin(CA_BICS), cmm['amount'], 0)
    cmm['rnid'] = np.where(cmm['bic'].isin(RNID_BICS), cmm['amount'], 0)
    
    not_excl = cmm['excl'] != 'Y'
    cmm['fd2']   = np.where(cmm['bic'].isin(FD_BICS) & not_excl, cmm['amount'], 0)
    cmm['sa2']   = np.where((cmm['bic'] == '95312') & not_excl, cmm['amount'], 0)
    cmm['ca2']   = np.where(cmm['bic'].isin(CA_BICS) & not_excl, cmm['amount'], 0)
    cmm['rnid2'] = np.where(cmm['bic'].isin(RNID_BICS) & not_excl, cmm['amount'], 0)
    
    cmm['custype'] = np.where(
        cmm['custcd'].astype(str).str.strip().isin(['77', '78', '95', '96']), 'I', 'C'
    )
    
    # Filter and summarize
    cmm = cmm[~cmm['bic'].isin(EXCLUDE_BICS)]
    
    agg_cols = ['fd', 'sa', 'ca', 'rnid', 'fd2', 'sa2', 'ca2', 'rnid2']
    summary = cmm.groupby(['depid', 'depgrp', 'custype'], observed=True)[agg_cols].sum().reset_index()
    
    print(f"  Groups: {len(summary):,}")
    return summary, cmm

# =============================================================================
# EQUITY PROCESSING
# =============================================================================
def process_equity(rep_vars, excl_equ):
    """Process Equity deposits"""
    print("\n--- Processing Equity ---")
    
    equ = read_sas(os.path.join(LCR_PATH, f"equ{rep_vars['reptmon']}.sas7bdat"))
    if equ.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # Filter valid CUSTNO
    equ = equ[equ['custno'].notna() & (equ['custno'].astype(str).str.strip() != '')].copy()
    print(f"  Records: {len(equ):,}")
    
    # Apply exclusions
    equ['excl'] = 'N'
    if excl_equ:
        equ.loc[equ['custno'].astype(str).str.strip().isin(excl_equ), 'excl'] = 'Y'
        print(f"  Excluded: {(equ['excl'] == 'Y').sum():,}")
    
    # Match depositors
    cof = read_sas(os.path.join(LIST_PATH, 'icof_equ_depositor_list.sas7bdat'))
    equ = match_depositors(equ, cof, 'custno')
    
    # Handle LINKID
    if 'linkid' not in equ.columns:
        equ['linkid'] = np.nan
    equ['linkid'] = equ['linkid'].fillna(50000000 + equ['depid'])
    
    # Classify products
    equ['bic'] = equ['cmmcode'].astype(str).str[:5]
    equ['amount'] = equ['amount'].fillna(0)
    
    equ['std']  = np.where(equ['bic'].isin(STD_BICS), equ['amount'], 0)
    equ['nid']  = np.where(equ['bic'].isin(RNID_BICS), equ['amount'], 0)
    equ['ibb']  = np.where(equ['bic'].isin(IBB_BICS), equ['amount'], 0)
    equ['repo'] = np.where(equ['bic'].isin(REPO_BICS), equ['amount'], 0)
    
    not_excl = equ['excl'] != 'Y'
    equ['std2'] = np.where(equ['bic'].isin(STD_BICS) & not_excl, equ['amount'], 0)
    equ['nid2'] = np.where(equ['bic'].isin(RNID_BICS) & not_excl, equ['amount'], 0)
    
    equ['custype'] = np.where(
        equ['custfiss'].astype(str).str.strip().isin(['77', '78', '95', '96']), 'I', 'C'
    )
    
    # Summarize by LINKID
    agg_cols = ['std', 'nid', 'ibb', 'repo', 'std2', 'nid2']
    summary = equ.groupby(['linkid', 'depgrp', 'custype'], observed=True)[agg_cols].sum().reset_index()
    summary.rename(columns={'linkid': 'depid'}, inplace=True)
    
    print(f"  Groups: {len(summary):,}")
    return summary, equ

# =============================================================================
# CONSOLIDATION
# =============================================================================
def consolidate(mni_sum, equ_sum):
    """Merge M&I and Equity summaries"""
    print("\n--- Consolidating ---")
    
    if mni_sum.empty and equ_sum.empty:
        return pd.DataFrame()
    
    # Merge
    if not mni_sum.empty and not equ_sum.empty:
        allsrc = mni_sum.merge(equ_sum, on='depid', how='outer', suffixes=('_mni', '_equ'))
    else:
        allsrc = mni_sum.copy() if not mni_sum.empty else equ_sum.copy()
    
    # Combine DEPGRP and CUSTYPE
    allsrc['depgrp'] = allsrc.get('depgrp_mni', allsrc.get('depgrp', '')).fillna(
        allsrc.get('depgrp_equ', '')
    )
    allsrc['custype'] = allsrc.get('custype_mni', allsrc.get('custype', '')).fillna(
        allsrc.get('custype_equ', '')
    )
    
    # Fill missing numeric columns
    num_cols = ['fd', 'sa', 'ca', 'rnid', 'fd2', 'sa2', 'ca2', 'rnid2',
                'std', 'nid', 'ibb', 'repo', 'std2', 'nid2']
    for col in num_cols:
        if col not in allsrc.columns:
            allsrc[col] = 0
        allsrc[col] = allsrc[col].fillna(0)
    
    # Calculate totals
    allsrc['tot'] = (allsrc['fd'] + allsrc['sa'] + allsrc['ca'] + 
                     allsrc['std'] + allsrc['nid'] + allsrc['rnid'] + 
                     allsrc['ibb'] + allsrc['repo'])
    allsrc['mni'] = allsrc['fd2'] + allsrc['sa2'] + allsrc['ca2'] + allsrc['rnid2']
    allsrc['equ'] = allsrc['std2'] + allsrc['nid2']
    allsrc['tot2'] = allsrc['mni'] + allsrc['equ']
    
    print(f"  Total groups: {len(allsrc):,}")
    return allsrc

# =============================================================================
# REPORT WRITERS
# =============================================================================
def write_top50(allsrc, cust_type, desc, rep_vars, filepath):
    """Write Top 50 depositors report"""
    filtered = allsrc[allsrc['custype'] == cust_type]
    if filtered.empty:
        print(f"  No {desc} data")
        return pd.DataFrame()
    
    top50 = filtered.nlargest(50, 'tot2').reset_index(drop=True)
    top50['rank'] = range(1, len(top50) + 1)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write("PUBLIC ISLAMIC BANK BERHAD\n")
        f.write(f"TOP 50 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}\n\n")
        f.write(f"(i) Top 50 {desc} Depositors by Sources\n\n")
        f.write(f"NOBS{DLM}DEPOSITORS{DLM}TOTAL BALANCE{DLM}M&I{DLM}EQUATION{DLM}\n")
        
        for _, row in top50.iterrows():
            f.write(f"{row['rank']}{DLM}{s(row['depgrp'], 50)}{DLM}"
                   f"{fmt(row['tot2'])}{DLM}{fmt(row['mni'])}{DLM}{fmt(row['equ'])}{DLM}\n")
    
    print(f"  Written: {os.path.basename(filepath)} ({os.path.getsize(filepath):,} bytes)")
    return top50

def write_detail(top50, mni_detail, equ_detail, filepath):
    """Append detail listing to report"""
    if top50.empty:
        return
    
    with open(filepath, 'a', encoding='utf-8') as f:
        for idx, (_, row) in enumerate(top50.iterrows()):
            if idx % 25 == 0:
                print(f"  Detail {idx+1}/{len(top50)}")
            
            depid = row['depid']
            
            f.write(f"\n\n(ii) Detail Accounts Listing for Top 50 Depositors\n\n")
            f.write(f"{row['rank']}{DLM}{s(row['depgrp'], 50)} ({depid}){DLM}\n")
            
            # M&I Section
            f.write(f"{DLM}Source: M&I\n")
            f.write(f"{DLM}NO{DLM}BRANCH{DLM}ACCTNO{DLM}CUSTNAME{DLM}"
                   f"CUSTNO{DLM}BUSSREG{DLM}CUSTCD{DLM}PRODUCT{DLM}BALANCE{DLM}\n")
            
            if not mni_detail.empty:
                det = mni_detail[(mni_detail['depid'] == depid) & (mni_detail['amount'] > 0)]
                cnt, total = 0, 0
                for _, r in det.iterrows():
                    cnt += 1
                    total += r['amount']
                    f.write(f"{DLM}{cnt}{DLM}{s(r.get('branch'), 10)}{DLM}"
                           f"{s(r.get('acctno'), 15)}{DLM}{s(r.get('custname'), 25)}{DLM}"
                           f"{s(r.get('custno'), 10)}{DLM}{s(r.get('newic'), 10)}{DLM}"
                           f"{s(r.get('custcd'), 6)}{DLM}{s(r.get('product'), 8)}{DLM}"
                           f"{fmt(r['amount'])}{DLM}\n")
                if cnt > 0:
                    f.write(f"{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{fmt(total)}{DLM}\n")
            
            # Equity Section
            f.write(f"\n{DLM}Source: EQU\n")
            f.write(f"{DLM}NO{DLM}DEALREF{DLM}DEALTYPE{DLM}NAME{DLM}"
                   f"CUST MNEMONIC{DLM}AMOUNT{DLM}\n")
            
            if not equ_detail.empty:
                det = equ_detail[(equ_detail['linkid'] == depid) & (equ_detail['amount'] > 0)]
                cnt, total = 0, 0
                for _, r in det.iterrows():
                    cnt += 1
                    total += r['amount']
                    dealref = r.get('dealref') or r.get('gwdlr') or r.get('utdlr') or ''
                    dealtype = r.get('dealtype') or r.get('gwdlp') or r.get('utsty') or ''
                    f.write(f"{DLM}{cnt}{DLM}{s(dealref, 15)}{DLM}{s(dealtype, 10)}{DLM}"
                           f"{s(r.get('custname'), 25)}{DLM}{s(r.get('custno'), 15)}{DLM}"
                           f"{fmt(r['amount'])}{DLM}\n")
                if cnt > 0:
                    f.write(f"{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{fmt(total)}{DLM}\n")

def write_top100(allsrc, rep_vars, filepath):
    """Write Top 100 by product report"""
    top100 = allsrc.nlargest(100, 'tot').reset_index(drop=True)
    top100['rank'] = range(1, len(top100) + 1)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write("PUBLIC ISLAMIC BANK BERHAD\n")
        f.write(f"TOP 100 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}\n\n")
        f.write("(i) Top 100 Depositors by Products\n\n")
        f.write(f"NOBS{DLM}DEPOSITORS{DLM}TOTAL BALANCE{DLM}"
               f"MGIA/IA/TERM DEPOSIT-I{DLM}SAVINGS{DLM}DEMAND DEPOSIT{DLM}"
               f"SHORT TERM DEPOSIT{DLM}NID ISSUED{DLM}"
               f"INTERBANK BORROWING{DLM}REPOS{DLM}\n")
        
        for _, row in top100.iterrows():
            nid_total = row.get('nid', 0) + row.get('rnid', 0)
            f.write(f"{row['rank']}{DLM}{s(row['depgrp'], 50)}{DLM}"
                   f"{fmt(row['tot'])}{DLM}{fmt(row.get('fd', 0))}{DLM}"
                   f"{fmt(row.get('sa', 0))}{DLM}{fmt(row.get('ca', 0))}{DLM}"
                   f"{fmt(row.get('std', 0))}{DLM}{fmt(nid_total)}{DLM}"
                   f"{fmt(row.get('ibb', 0))}{DLM}{fmt(row.get('repo', 0))}{DLM}\n")
    
    print(f"  Written: {os.path.basename(filepath)} ({os.path.getsize(filepath):,} bytes)")
    return top100

def write_maturity(top100, mni_detail, rep_vars, filepath):
    """Write contractual maturity report"""
    if top100.empty:
        return
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write("PUBLIC ISLAMIC BANK BERHAD\n")
        f.write(f"TOP 100 DEPOSITORS BY CONTRACTUAL MATURITY AS AT {rep_vars['rdate']}\n\n")
        f.write("(iii) Top 100 Depositors by Contractual Maturity\n")
        
        for idx, (_, row) in enumerate(top100.iterrows()):
            if idx % 25 == 0:
                print(f"  Maturity {idx+1}/{len(top100)}")
            
            depid = row['depid']
            
            f.write(f"\n{row['rank']}{DLM}{s(row['depgrp'], 50)}\n")
            f.write(f"{DLM}DEPOSIT TYPE{DLM}UP TO 1 WEEK{DLM}"
                   f"> 1 WK - 1 MTH{DLM}> 1 - 3 MTHS{DLM}"
                   f"> 3 - 6 MTHS{DLM}> 6 MTHS - 1 YR{DLM}"
                   f"> 1 YEAR{DLM}NO SPECIFIC MATURITY{DLM}TOTAL{DLM}\n")
            
            if not mni_detail.empty:
                det = mni_detail[(mni_detail['depid'] == depid) & (mni_detail['amount'] > 0)].copy()
                
                if not det.empty:
                    det['bic'] = det['cmmcode'].astype(str).str[:5]
                    det['rem'] = det['cmmcode'].astype(str).str[7:9]
                    det.loc[det['bic'].isin(NO_MATURITY_BICS), 'rem'] = '07'
                    
                    for i in range(1, 8):
                        det[f'buc{i}'] = np.where(det['rem'] == f'0{i}', det['amount'], 0)
                    
                    det['item'] = det['bic'].map(BIC_TAG)
                    det = det[det['item'].notna()]
                    
                    if not det.empty:
                        buckets = ['buc1', 'buc2', 'buc3', 'buc4', 'buc5', 'buc6', 'buc7']
                        agg = det.groupby('item')[['amount'] + buckets].sum().reset_index()
                        
                        for _, r in agg.iterrows():
                            f.write(f"{DLM}{s(r['item'], 50)}{DLM}"
                                   f"{fmt(r['buc1'])}{DLM}{fmt(r['buc2'])}{DLM}"
                                   f"{fmt(r['buc3'])}{DLM}{fmt(r['buc4'])}{DLM}"
                                   f"{fmt(r['buc5'])}{DLM}{fmt(r['buc6'])}{DLM}"
                                   f"{fmt(r['buc7'])}{DLM}{fmt(r['amount'])}{DLM}\n")
    
    print(f"  Written: {os.path.basename(filepath)} ({os.path.getsize(filepath):,} bytes)")

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIIMTLCR - Top Depositors Report (Islamic Banking)")
    print("=" * 60)
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    rep_vars = get_report_vars()
    print(f"\nReport Date: {rep_vars['rdate']}")
    print(f"Output: {OUTPUT_PATH}")
    
    # Load exclusions
    excl_cis, excl_equ = get_exclusions()
    print(f"Exclusions - CIS: {len(excl_cis)}, EQU: {len(excl_equ)}")
    
    # Process sources
    mni_sum, mni_detail = process_mni(rep_vars, excl_cis)
    equ_sum, equ_detail = process_equity(rep_vars, excl_equ)
    
    # Consolidate
    allsrc = consolidate(mni_sum, equ_sum)
    
    if allsrc.empty:
        print("ERROR: No data to report")
        return
    
    # Generate reports
    print("\n" + "=" * 60)
    print("GENERATING REPORTS")
    print("=" * 60)
    
    # Individual Top 50
    ind_file = os.path.join(OUTPUT_PATH, 'COFOUTI.txt')
    ind_top = write_top50(allsrc, 'I', 'Individual', rep_vars, ind_file)
    if not ind_top.empty:
        write_detail(ind_top, mni_detail, equ_detail, ind_file)
    
    # Corporate Top 50
    corp_file = os.path.join(OUTPUT_PATH, 'COFOUTC.txt')
    corp_top = write_top50(allsrc, 'C', 'Corporate', rep_vars, corp_file)
    if not corp_top.empty:
        write_detail(corp_top, mni_detail, equ_detail, corp_file)
    
    # Top 100 by Product
    prod_file = os.path.join(OUTPUT_PATH, 'COFOUT1.txt')
    top100 = write_top100(allsrc, rep_vars, prod_file)
    
    # Top 100 Detail
    detail100_file = os.path.join(OUTPUT_PATH, 'COFOUT2.txt')
    if not top100.empty:
        write_detail(top100, mni_detail, equ_detail, detail100_file)
    
    # Maturity Report
    mat_file = os.path.join(OUTPUT_PATH, 'COFOUT3.txt')
    write_maturity(top100, mni_detail, rep_vars, mat_file)
    
    # Cleanup
    del allsrc, mni_sum, mni_detail, equ_sum, equ_detail
    gc.collect()
    
    # Summary
    print("\nOutput files:")
    for f in ['COFOUTI.txt', 'COFOUTC.txt', 'COFOUT1.txt', 'COFOUT2.txt', 'COFOUT3.txt']:
        path = os.path.join(OUTPUT_PATH, f)
        if os.path.exists(path):
            print(f"  {f}: {os.path.getsize(path):,} bytes")
    
    print(f"\nEnd: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print("✓ Complete")

if __name__ == "__main__":
    main()
