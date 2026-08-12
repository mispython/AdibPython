"""
EIIMTLCR - Top Depositors Report (Islamic Banking)
Generates top depositor reports by:
- Individual/Corporate categories (Top 50 each)
- Product breakdown (Top 100)
- Contractual maturity (Top 100)
"""

import pyreadstat
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import os
import gc

# =============================================================================
# CONFIGURATION
# =============================================================================
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/'
PATHS = {
    'LCR': os.path.join(BASE_PATH, ''),
    'LIST': os.path.join(BASE_PATH, 'list/'),
    'OUTPUT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTCOF/'
}

# Check and create output directory
os.makedirs(PATHS['OUTPUT'], exist_ok=True)

# Check available space
def check_disk_space(path, required_mb=100):
    """Check if there's enough disk space"""
    try:
        stat = os.statvfs(path)
        available_mb = (stat.f_bavail * stat.f_frsize) / (1024 * 1024)
        print(f"Available space: {available_mb:.2f} MB")
        if available_mb < required_mb:
            print(f"WARNING: Low disk space! Required: {required_mb}MB, Available: {available_mb:.2f}MB")
            return False
        return True
    except Exception as e:
        print(f"Could not check disk space: {e}")
        return True  # Continue anyway

# Delimiter (hex 05)
DLM = '\x05'

# BIC to item mapping
BIC_TAG = {
    '95315': 'A1.01', '95317': 'A1.02', '95312': 'A1.03',
    '95313': 'A1.04', '95810': 'A1.05', '95820': 'A1.06',
    '95830': 'A1.07', '95840': 'A1.08', '96317': 'B1.01',
    '96313': 'B1.02', '96810': 'B1.03', '96820': 'B1.04',
    '96830': 'B1.05', '96840': 'B1.06'
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def read_sas7bdat(filepath, lowercase_cols=True):
    """Read SAS dataset with error handling"""
    if not os.path.exists(filepath):
        print(f"WARNING: File not found: {filepath}")
        return pd.DataFrame()
    
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        if lowercase_cols:
            df.columns = [col.lower() for col in df.columns]
        print(f"  Read {filepath}: {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        print(f"ERROR reading {filepath}: {e}")
        return pd.DataFrame()

def get_report_date():
    """Get report date (yesterday)"""
    return datetime.now() - timedelta(days=1)

def format_comma20_2(value):
    """Format number with comma and 2 decimal places"""
    if pd.isna(value):
        return '0.00'
    return f"{float(value):,.2f}"

def safe_str(value, max_len=None):
    """Safely convert to string"""
    if pd.isna(value):
        return ''
    s = str(value).strip()
    if max_len:
        s = s[:max_len]
    return s

# =============================================================================
# PROCESSING FUNCTIONS
# =============================================================================
def process_mni(rep_vars, excl_cis):
    """Process M&I source data"""
    print("\n--- M&I Processing ---")
    
    # Read CMM
    cmm_file = PATHS['LCR'] + f"cmm{rep_vars['reptmon']}.sas7bdat"
    print(f"Reading CMM: {cmm_file}")
    cmm = read_sas7bdat(cmm_file)
    
    if cmm.empty:
        print("ERROR: CMM file is empty")
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"CMM columns: {cmm.columns.tolist()}")
    print(f"CMM sample newic values: {cmm['newic'].dropna().head(3).tolist()}")
    
    # Read COF_MNI_DEPOSITOR_LIST
    cof_file = PATHS['LIST'] + 'icof_mni_depositor_list.sas7bdat'
    print(f"Reading COF: {cof_file}")
    cof = read_sas7bdat(cof_file)
    
    if not cof.empty:
        print(f"COF columns: {cof.columns.tolist()}")
        print(f"COF unique bussreg: {cof['bussreg'].nunique()}")
        print(f"COF sample bussreg: {cof['bussreg'].dropna().head(3).tolist()}")
    
    # Add EXCL flag
    cmm['excl'] = 'N'
    if excl_cis:
        cmm['custno_str'] = cmm['custno'].astype(str).str.strip()
        cmm.loc[cmm['custno_str'].isin(excl_cis), 'excl'] = 'Y'
        excluded_count = (cmm['excl'] == 'Y').sum()
        print(f"Excluded CMM records: {excluded_count}")
        cmm.drop(columns=['custno_str'], inplace=True)
    
    # First merge by NEWIC
    if not cof.empty:
        # Ensure unique by bussreg
        cof_idno = cof[['depid', 'depgrp', 'bussreg']].drop_duplicates(subset='bussreg').copy()
        cof_idno.rename(columns={'bussreg': 'newic'}, inplace=True)
        
        # Check for duplicates
        dup_newic = cof_idno['newic'].duplicated().sum()
        print(f"Duplicate NEWIC in COF: {dup_newic}")
        
        # Merge
        print(f"Merging CMM ({len(cmm)}) with COF by NEWIC ({len(cof_idno)})...")
        mni1 = cmm.merge(cof_idno, on='newic', how='left', indicator=True)
        
        matched_count = (mni1['_merge'] == 'both').sum()
        print(f"Matched by NEWIC: {matched_count}/{len(cmm)}")
        
        # Split
        mni1_matched = mni1[mni1['_merge'] == 'both'].drop(columns=['_merge']).copy()
        mni1_unmatched = mni1[mni1['_merge'] != 'both'].drop(columns=['depid', 'depgrp', '_merge']).copy()
        print(f"Matched: {len(mni1_matched)}, Unmatched: {len(mni1_unmatched)}")
    else:
        print("No COF data, all CMM records unmatched")
        mni1_matched = pd.DataFrame()
        mni1_unmatched = cmm.copy()
    
    # Second merge by CUSTNO
    mni2_matched = pd.DataFrame()
    mni2_unmatched = pd.DataFrame()
    
    if not mni1_unmatched.empty and not cof.empty:
        cof_cust = cof[['depid', 'depgrp', 'custno']].drop_duplicates(subset='custno').copy()
        
        print(f"Merging unmatched ({len(mni1_unmatched)}) with COF by CUSTNO ({len(cof_cust)})...")
        mni2 = mni1_unmatched.merge(cof_cust, on='custno', how='left', indicator=True)
        
        matched2 = (mni2['_merge'] == 'both').sum()
        print(f"Matched by CUSTNO: {matched2}/{len(mni1_unmatched)}")
        
        mni2_matched = mni2[mni2['_merge'] == 'both'].drop(columns=['_merge']).copy()
        mni2_unmatched = mni2[mni2['_merge'] != 'both'].drop(columns=['depid', 'depgrp', '_merge']).copy()
    elif not mni1_unmatched.empty:
        mni2_unmatched = mni1_unmatched.copy()
    
    # Assign new DEPID for remaining unmatched
    mni3 = pd.DataFrame()
    if not mni2_unmatched.empty:
        print(f"Assigning new DEPID for {mni2_unmatched['custno'].nunique()} unique customers...")
        unique_cust = mni2_unmatched[['custno']].drop_duplicates().sort_values('custno').copy()
        unique_cust['depid_new'] = range(5001, 5001 + len(unique_cust))
        
        mni3 = mni2_unmatched.merge(unique_cust, on='custno', how='left')
        mni3['depid'] = mni3['depid_new']
        mni3['depgrp'] = mni3['custname'].fillna('')
        mni3.drop(columns=['depid_new'], inplace=True)
        print(f"New DEPID range: 5001 - {5000 + len(unique_cust)}")
    
    # Combine all
    dfs = []
    if not mni1_matched.empty:
        dfs.append(mni1_matched)
    if not mni2_matched.empty:
        dfs.append(mni2_matched)
    if not mni3.empty:
        dfs.append(mni3)
    
    if not dfs:
        print("ERROR: No M&I data after processing")
        return pd.DataFrame(), pd.DataFrame()
    
    mni_all = pd.concat(dfs, ignore_index=True)
    print(f"Total M&I records: {len(mni_all)}")
    
    # Free memory
    del cmm, cof, mni1, mni2, mni3, mni1_matched, mni1_unmatched, mni2_matched, mni2_unmatched
    gc.collect()
    
    # Classify products
    print("Classifying products...")
    mni_all['bic'] = mni_all['cmmcode'].astype(str).str[:5]
    
    mni_all['fd'] = np.where(mni_all['bic'].isin(['95315', '96315', '95317', '96317']), mni_all['amount'].fillna(0), 0)
    mni_all['sa'] = np.where(mni_all['bic'] == '95312', mni_all['amount'].fillna(0), 0)
    mni_all['ca'] = np.where(mni_all['bic'].isin(['95313', '96313']), mni_all['amount'].fillna(0), 0)
    mni_all['rnid'] = np.where(mni_all['bic'].isin(['95840', '96840']), mni_all['amount'].fillna(0), 0)
    
    # Excluded amounts
    mni_all['fd2'] = np.where((mni_all['bic'].isin(['95315', '96315', '95317', '96317'])) & (mni_all['excl'] != 'Y'), mni_all['amount'].fillna(0), 0)
    mni_all['sa2'] = np.where((mni_all['bic'] == '95312') & (mni_all['excl'] != 'Y'), mni_all['amount'].fillna(0), 0)
    mni_all['ca2'] = np.where((mni_all['bic'].isin(['95313', '96313'])) & (mni_all['excl'] != 'Y'), mni_all['amount'].fillna(0), 0)
    mni_all['rnid2'] = np.where((mni_all['bic'].isin(['95840', '96840'])) & (mni_all['excl'] != 'Y'), mni_all['amount'].fillna(0), 0)
    
    # Customer type
    mni_all['custype'] = np.where(mni_all['custcd'].astype(str).str.strip().isin(['77', '78', '95', '96']), 'I', 'C')
    
    # Filter out unwanted products (keep only relevant for M&I)
    exclude_bics = ['95810', '96810', '95820', '96820']
    before_filter = len(mni_all)
    mni_all = mni_all[~mni_all['bic'].isin(exclude_bics)].copy()
    print(f"After filtering excluded BICs: {len(mni_all)} (removed {before_filter - len(mni_all)})")
    
    # Summarize
    print("Summarizing M&I...")
    group_cols = ['depid', 'depgrp', 'custype']
    mni_sum = mni_all.groupby(group_cols, as_index=False).agg({
        'fd': 'sum', 'sa': 'sum', 'ca': 'sum', 'rnid': 'sum',
        'fd2': 'sum', 'sa2': 'sum', 'ca2': 'sum', 'rnid2': 'sum'
    })
    
    print(f"M&I Summary: {len(mni_sum)} groups")
    print(f"M&I Summary sample: \n{mni_sum.head(3)}")
    
    return mni_sum, mni_all

def process_equity(rep_vars, excl_equ):
    """Process Equity source data"""
    print("\n--- Equity Processing ---")
    
    equ_file = PATHS['LCR'] + f"equ{rep_vars['reptmon']}.sas7bdat"
    print(f"Reading EQU: {equ_file}")
    equ = read_sas7bdat(equ_file)
    
    if equ.empty:
        print("WARNING: EQU file is empty")
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"EQU columns: {equ.columns.tolist()}")
    
    # Filter and add EXCL flag
    equ = equ[equ['custno'].notna() & (equ['custno'] != '')].copy()
    equ['excl'] = 'N'
    if excl_equ:
        equ['custno_str'] = equ['custno'].astype(str).str.strip()
        equ.loc[equ['custno_str'].isin(excl_equ), 'excl'] = 'Y'
        print(f"Excluded EQU records: {(equ['excl'] == 'Y').sum()}")
        equ.drop(columns=['custno_str'], inplace=True)
    
    # Read ICOF_EQU_DEPOSITOR_LIST
    cof_file = PATHS['LIST'] + 'icof_equ_depositor_list.sas7bdat'
    print(f"Reading EQU COF: {cof_file}")
    cof = read_sas7bdat(cof_file)
    
    if not cof.empty:
        print(f"EQU COF columns: {cof.columns.tolist()}")
        cof_equ = cof[['depid', 'depgrp', 'custno', 'linkid']].drop_duplicates(subset='custno').copy()
        print(f"Merging EQU ({len(equ)}) with COF by CUSTNO ({len(cof_equ)})...")
        equ1 = equ.merge(cof_equ, on='custno', how='left', indicator=True)
        
        matched = (equ1['_merge'] == 'both').sum()
        print(f"Matched by CUSTNO: {matched}/{len(equ)}")
        
        equ_matched = equ1[equ1['_merge'] == 'both'].drop(columns=['_merge']).copy()
        equ_unmatched = equ1[equ1['_merge'] != 'both'].drop(columns=['depid', 'depgrp', 'linkid', '_merge']).copy()
    else:
        print("No EQU COF data")
        equ_matched = pd.DataFrame()
        equ_unmatched = equ.copy()
    
    # Assign new DEPID for unmatched
    equ2 = pd.DataFrame()
    if not equ_unmatched.empty:
        print(f"Assigning new DEPID for {equ_unmatched['custno'].nunique()} unique EQU customers...")
        unique_cust = equ_unmatched[['custno']].drop_duplicates().sort_values('custno').copy()
        unique_cust['depid_new'] = range(50005001, 50005001 + len(unique_cust))
        
        equ2 = equ_unmatched.merge(unique_cust, on='custno', how='left')
        equ2['depid'] = equ2['depid_new']
        equ2['depgrp'] = equ2['custname'].fillna('')
        equ2['linkid'] = np.nan
        equ2.drop(columns=['depid_new'], inplace=True)
    
    # Combine
    dfs = []
    if not equ_matched.empty:
        dfs.append(equ_matched)
    if not equ2.empty:
        dfs.append(equ2)
    
    if not dfs:
        print("ERROR: No Equity data after processing")
        return pd.DataFrame(), pd.DataFrame()
    
    equ_all = pd.concat(dfs, ignore_index=True)
    print(f"Total EQU records: {len(equ_all)}")
    
    # Free memory
    del equ, cof, equ1, equ_matched, equ_unmatched, equ2
    gc.collect()
    
    # Handle LINKID
    equ_all['linkid'] = equ_all['linkid'].fillna(50000000 + equ_all['depid'])
    equ_all['linkid'] = equ_all['linkid'].fillna(equ_all['depid'])
    
    # Classify products
    equ_all['bic'] = equ_all['cmmcode'].astype(str).str[:5]
    
    equ_all['std'] = np.where(equ_all['bic'].isin(['95830', '96830']), equ_all['amount'].fillna(0), 0)
    equ_all['nid'] = np.where(equ_all['bic'].isin(['95840', '96840']), equ_all['amount'].fillna(0), 0)
    equ_all['ibb'] = np.where(equ_all['bic'].isin(['95810', '96810']), equ_all['amount'].fillna(0), 0)
    equ_all['repo'] = np.where(equ_all['bic'].isin(['95820', '96820']), equ_all['amount'].fillna(0), 0)
    
    equ_all['std2'] = np.where((equ_all['bic'].isin(['95830', '96830'])) & (equ_all['excl'] != 'Y'), equ_all['amount'].fillna(0), 0)
    equ_all['nid2'] = np.where((equ_all['bic'].isin(['95840', '96840'])) & (equ_all['excl'] != 'Y'), equ_all['amount'].fillna(0), 0)
    
    # Customer type
    equ_all['custype'] = np.where(equ_all['custfiss'].astype(str).str.strip().isin(['77', '78', '95', '96']), 'I', 'C')
    
    # Summarize by LINKID
    print("Summarizing Equity...")
    equ_sum = equ_all.groupby(['linkid', 'depgrp', 'custype'], as_index=False).agg({
        'std': 'sum', 'nid': 'sum', 'ibb': 'sum', 'repo': 'sum',
        'std2': 'sum', 'nid2': 'sum'
    })
    equ_sum.rename(columns={'linkid': 'depid'}, inplace=True)
    
    print(f"Equity Summary: {len(equ_sum)} groups")
    print(f"Equity Summary sample: \n{equ_sum.head(3)}")
    
    return equ_sum, equ_all

def consolidate_sources(mni_sum, equ_sum):
    """Consolidate M&I and Equity sources"""
    print("\n--- Consolidation ---")
    
    if mni_sum.empty and equ_sum.empty:
        print("ERROR: Both sources empty")
        return pd.DataFrame()
    
    # Ensure DEPID columns are same type
    if not mni_sum.empty:
        mni_sum['depid'] = mni_sum['depid'].astype(float)
    if not equ_sum.empty:
        equ_sum['depid'] = equ_sum['depid'].astype(float)
    
    # Merge
    if not mni_sum.empty and not equ_sum.empty:
        print(f"Merging M&I ({len(mni_sum)}) with Equity ({len(equ_sum)}) by DEPID...")
        allsrc = mni_sum.merge(equ_sum, on='depid', how='outer', suffixes=('_mni', '_equ'))
        print(f"Merged: {len(allsrc)} rows")
    elif not mni_sum.empty:
        allsrc = mni_sum.copy()
    else:
        allsrc = equ_sum.copy()
    
    # Combine fields
    if 'depgrp_mni' in allsrc.columns and 'depgrp_equ' in allsrc.columns:
        allsrc['depgrp'] = allsrc['depgrp_mni'].fillna('').combine_first(allsrc['depgrp_equ'].fillna(''))
    elif 'depgrp_mni' in allsrc.columns:
        allsrc['depgrp'] = allsrc['depgrp_mni'].fillna('')
    elif 'depgrp_equ' in allsrc.columns:
        allsrc['depgrp'] = allsrc['depgrp_equ'].fillna('')
    else:
        allsrc['depgrp'] = allsrc.get('depgrp', '')
    
    if 'custype_mni' in allsrc.columns and 'custype_equ' in allsrc.columns:
        allsrc['custype'] = allsrc['custype_mni'].fillna('').combine_first(allsrc['custype_equ'].fillna(''))
    elif 'custype_mni' in allsrc.columns:
        allsrc['custype'] = allsrc['custype_mni'].fillna('')
    elif 'custype_equ' in allsrc.columns:
        allsrc['custype'] = allsrc['custype_equ'].fillna('')
    else:
        allsrc['custype'] = allsrc.get('custype', '')
    
    # Fill NaN with 0 for numeric columns
    num_cols = ['fd', 'sa', 'ca', 'rnid', 'fd2', 'sa2', 'ca2', 'rnid2',
                'std', 'nid', 'ibb', 'repo', 'std2', 'nid2']
    for col in num_cols:
        if col not in allsrc.columns:
            allsrc[col] = 0
        allsrc[col] = allsrc[col].fillna(0)
    
    # Calculate totals
    allsrc['nid_total'] = allsrc['nid'] + allsrc['rnid']
    allsrc['tot'] = (allsrc['fd'] + allsrc['sa'] + allsrc['ca'] + 
                     allsrc['std'] + allsrc['nid_total'] + allsrc['ibb'] + allsrc['repo'])
    allsrc['mni'] = allsrc['fd2'] + allsrc['sa2'] + allsrc['ca2'] + allsrc['rnid2']
    allsrc['equ'] = allsrc['std2'] + allsrc['nid2']
    allsrc['tot2'] = allsrc['mni'] + allsrc['equ']
    
    print(f"Consolidated: {len(allsrc)} groups")
    print(f"Total TOT2: {allsrc['tot2'].sum():,.2f}")
    print(f"CUSTYPE distribution: \n{allsrc['custype'].value_counts()}")
    
    return allsrc

# =============================================================================
# REPORT GENERATION
# =============================================================================
def generate_top50_report(allsrc, cust_type, desc, rep_vars, output_file):
    """Generate Top 50 report"""
    print(f"\nGenerating Top 50 {desc} report...")
    
    # Filter and get top 50
    filtered = allsrc[allsrc['custype'] == cust_type].copy()
    print(f"  {desc} groups: {len(filtered)}")
    
    if filtered.empty:
        print(f"  WARNING: No {desc} data found")
        return pd.DataFrame()
    
    top50 = filtered.nlargest(50, 'tot2').copy()
    top50['rank'] = range(1, len(top50) + 1)
    print(f"  Top 50: {len(top50)}")
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"PUBLIC ISLAMIC BANK BERHAD\n")
            f.write(f"TOP 50 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}\n")
            f.write(f"\n")
            f.write(f"(i) Top 50 {desc} Depositors by Sources\n")
            f.write(f"\n")
            f.write(f"NOBS{DLM}DEPOSITORS{DLM}TOTAL BALANCE{DLM}M&I{DLM}EQUATION{DLM}\n")
            
            for _, row in top50.iterrows():
                depgrp = safe_str(row['depgrp'], 50)
                f.write(f"{row['rank']}{DLM}{depgrp}{DLM}"
                       f"{format_comma20_2(row['tot2'])}{DLM}"
                       f"{format_comma20_2(row['mni'])}{DLM}"
                       f"{format_comma20_2(row['equ'])}{DLM}\n")
            
            f.write(f"\n\n")
            f.write(f"(ii) Detail Accounts Listing for Top 50 {desc} Depositors\n")
            f.write(f"\n")
        
        print(f"  Report written: {output_file}")
    except Exception as e:
        print(f"  ERROR writing report: {e}")
    
    return top50

def generate_detail_listing(top50, mni_detail, equ_detail, output_file):
    """Generate detailed account listing"""
    if top50.empty:
        return
    
    print(f"  Generating detail listing...")
    
    try:
        with open(output_file, 'a', encoding='utf-8') as f:
            for idx, (_, top) in enumerate(top50.iterrows()):
                if idx % 10 == 0:
                    print(f"    Processing {idx+1}/{len(top50)}...")
                
                depid = top['depid']
                rank = top['rank']
                depgrp = safe_str(top['depgrp'], 50)
                
                f.write(f"{rank}{DLM}{depgrp} ({depid}){DLM}\n")
                f.write(f"\n")
                
                # M&I section
                f.write(f"{DLM}Source: M&I\n")
                f.write(f"\n")
                f.write(f"{DLM}NO{DLM}BRANCH{DLM}ACCTNO{DLM}CUSTNAME{DLM}"
                       f"CUSTNO{DLM}BUSSREG{DLM}CUSTCD{DLM}PRODUCT{DLM}BALANCE{DLM}\n")
                
                if not mni_detail.empty:
                    mni_det = mni_detail[
                        (mni_detail['depid'] == depid) & 
                        (mni_detail['amount'] > 0) & 
                        (mni_detail['excl'] != 'Y')
                    ].sort_values('acctno')
                    
                    cnt = 0
                    totbal = 0
                    for _, row in mni_det.iterrows():
                        cnt += 1
                        totbal += float(row['amount'])
                        
                        f.write(f"{DLM}{cnt}{DLM}"
                               f"{safe_str(row.get('branch'), 10)}{DLM}"
                               f"{safe_str(row.get('acctno'), 15)}{DLM}"
                               f"{safe_str(row.get('custname'), 25)}{DLM}"
                               f"{safe_str(row.get('custno'), 10)}{DLM}"
                               f"{safe_str(row.get('newic'), 10)}{DLM}"
                               f"{safe_str(row.get('custcd'), 6)}{DLM}"
                               f"{safe_str(row.get('product'), 8)}{DLM}"
                               f"{format_comma20_2(row['amount'])}{DLM}\n")
                    
                    if cnt > 0:
                        f.write(f"{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}"
                               f"{format_comma20_2(totbal)}{DLM}\n")
                        f.write(f"\n")
                
                # Equity section
                f.write(f"{DLM}Source: EQU\n")
                f.write(f"\n")
                f.write(f"{DLM}NO{DLM}DEALREF{DLM}DEALTYPE{DLM}NAME{DLM}"
                       f"CUST MNEMONIC{DLM}AMOUNT{DLM}\n")
                
                if not equ_detail.empty:
                    equ_det = equ_detail[
                        (equ_detail['linkid'] == depid) & 
                        (equ_detail['amount'] > 0) & 
                        (equ_detail['excl'] != 'Y')
                    ]
                    
                    cnt = 0
                    totbal = 0
                    for _, row in equ_det.iterrows():
                        cnt += 1
                        totbal += float(row['amount'])
                        
                        # Handle DEALREF and DEALTYPE
                        dealref = safe_str(row.get('dealref') or row.get('gwdlr') or row.get('utdlr'), 15)
                        dealtype = safe_str(row.get('dealtype') or row.get('gwdlp') or row.get('utsty'), 10)
                        
                        f.write(f"{DLM}{cnt}{DLM}{dealref}{DLM}{dealtype}{DLM}"
                               f"{safe_str(row.get('custname'), 25)}{DLM}"
                               f"{safe_str(row.get('custno'), 15)}{DLM}"
                               f"{format_comma20_2(row['amount'])}{DLM}\n")
                    
                    if cnt > 0:
                        f.write(f"{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}"
                               f"{format_comma20_2(totbal)}{DLM}\n")
                        f.write(f"\n")
                
                f.write(f"\n")
        
        print(f"  Detail listing appended to: {output_file}")
    except Exception as e:
        print(f"  ERROR in detail listing: {e}")
        import traceback
        traceback.print_exc()

def generate_top100_by_product(allsrc, rep_vars, output_file):
    """Generate Top 100 by product report"""
    print(f"\nGenerating Top 100 by Product report...")
    
    # Get top 100 by TOT
    top100 = allsrc.nlargest(100, 'tot').copy()
    top100['rank'] = range(1, len(top100) + 1)
    print(f"  Top 100 records: {len(top100)}")
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"PUBLIC ISLAMIC BANK BERHAD\n")
            f.write(f"TOP 100 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}\n")
            f.write(f"\n")
            f.write(f"(i) Top 100 Depositors by Products\n")
            f.write(f"\n")
            f.write(f"NOBS{DLM}DEPOSITORS{DLM}TOTAL BALANCE{DLM}"
                   f"MGIA/IA/TERM DEPOSIT-I{DLM}SAVINGS{DLM}DEMAND DEPOSIT{DLM}"
                   f"SHORT TERM DEPOSIT{DLM}NID ISSUED{DLM}"
                   f"INTERBANK BORROWING{DLM}REPOS{DLM}\n")
            
            for _, row in top100.iterrows():
                nid_total = row.get('nid', 0) + row.get('rnid', 0)
                
                f.write(f"{row['rank']}{DLM}"
                       f"{safe_str(row['depgrp'], 50)}{DLM}"
                       f"{format_comma20_2(row['tot'])}{DLM}"
                       f"{format_comma20_2(row.get('fd', 0))}{DLM}"
                       f"{format_comma20_2(row.get('sa', 0))}{DLM}"
                       f"{format_comma20_2(row.get('ca', 0))}{DLM}"
                       f"{format_comma20_2(row.get('std', 0))}{DLM}"
                       f"{format_comma20_2(nid_total)}{DLM}"
                       f"{format_comma20_2(row.get('ibb', 0))}{DLM}"
                       f"{format_comma20_2(row.get('repo', 0))}{DLM}\n")
        
        print(f"  Report written: {output_file}")
    except Exception as e:
        print(f"  ERROR writing report: {e}")
    
    return top100

def generate_maturity_report(top100, mni_detail, rep_vars, output_file):
    """Generate contractual maturity report"""
    print(f"\nGenerating Maturity report...")
    
    if top100.empty:
        print("  No data for maturity report")
        return
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"PUBLIC ISLAMIC BANK BERHAD\n")
            f.write(f"TOP 100 DEPOSITORS BY CONTRACTUAL MATURITY AS AT {rep_vars['rdate']}\n")
            f.write(f"\n")
            f.write(f"(iii) Top 100 Depositors by Contractual Maturity\n")
            
            for idx, (_, top) in enumerate(top100.iterrows()):
                if idx % 10 == 0:
                    print(f"    Processing maturity {idx+1}/{len(top100)}...")
                
                rank = top['rank']
                depgrp = safe_str(top['depgrp'], 50)
                depid = top['depid']
                
                f.write(f"\n")
                f.write(f"{rank}{DLM}{depgrp}\n")
                f.write(f"{DLM}DEPOSIT TYPE{DLM}UP TO 1 WEEK{DLM}"
                       f"> 1 WK - 1 MTH{DLM}> 1 - 3 MTHS{DLM}"
                       f"> 3 - 6 MTHS{DLM}> 6 MTHS - 1 YR{DLM}"
                       f"> 1 YEAR{DLM}NO SPECIFIC MATURITY{DLM}TOTAL{DLM}\n")
                
                # Process M&I detail for maturity buckets
                if not mni_detail.empty:
                    mni_det = mni_detail[(mni_detail['depid'] == depid) & (mni_detail['amount'] > 0)]
                    
                    if not mni_det.empty:
                        mni_det['bic'] = mni_det['cmmcode'].astype(str).str[:5]
                        
                        # Extract maturity code from CMMCODE (positions 8-9)
                        mni_det['rem'] = mni_det['cmmcode'].astype(str).str[7:9]
                        
                        # No specific maturity for certain BICs
                        mni_det.loc[mni_det['bic'].isin(['95312', '95313', '96313']), 'rem'] = '07'
                        
                        # Create maturity buckets
                        mni_det['buc1'] = np.where(mni_det['rem'] == '01', mni_det['amount'], 0)
                        mni_det['buc2'] = np.where(mni_det['rem'] == '02', mni_det['amount'], 0)
                        mni_det['buc3'] = np.where(mni_det['rem'] == '03', mni_det['amount'], 0)
                        mni_det['buc4'] = np.where(mni_det['rem'] == '04', mni_det['amount'], 0)
                        mni_det['buc5'] = np.where(mni_det['rem'] == '05', mni_det['amount'], 0)
                        mni_det['buc6'] = np.where(mni_det['rem'] == '06', mni_det['amount'], 0)
                        mni_det['buc7'] = np.where(mni_det['rem'] == '07', mni_det['amount'], 0)
                        
                        # Map BIC to item
                        mni_det['item'] = mni_det['bic'].map(BIC_TAG)
                        mni_det = mni_det[mni_det['item'].notna()]
                        
                        if not mni_det.empty:
                            item_agg = mni_det.groupby('item', as_index=False).agg({
                                'amount': 'sum', 'buc1': 'sum', 'buc2': 'sum',
                                'buc3': 'sum', 'buc4': 'sum', 'buc5': 'sum',
                                'buc6': 'sum', 'buc7': 'sum'
                            })
                            
                            for _, item_row in item_agg.iterrows():
                                f.write(f"{DLM}{safe_str(item_row['item'], 50)}{DLM}"
                                       f"{format_comma20_2(item_row['buc1'])}{DLM}"
                                       f"{format_comma20_2(item_row['buc2'])}{DLM}"
                                       f"{format_comma20_2(item_row['buc3'])}{DLM}"
                                       f"{format_comma20_2(item_row['buc4'])}{DLM}"
                                       f"{format_comma20_2(item_row['buc5'])}{DLM}"
                                       f"{format_comma20_2(item_row['buc6'])}{DLM}"
                                       f"{format_comma20_2(item_row['buc7'])}{DLM}"
                                       f"{format_comma20_2(item_row['amount'])}{DLM}\n")
        
        print(f"  Report written: {output_file}")
    except Exception as e:
        print(f"  ERROR writing maturity report: {e}")
        import traceback
        traceback.print_exc()

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIIMTLCR - Top Depositors Report (Islamic Banking)")
    print("=" * 60)
    
    # Check disk space
    if not check_disk_space(PATHS['OUTPUT']):
        print("WARNING: Low disk space, but continuing...")
    
    # Get report date (yesterday)
    report_date = get_report_date()
    rep_vars = get_report_vars(report_date)
    print(f"\nReport Date: {rep_vars['rdate']}")
    print(f"Output directory: {PATHS['OUTPUT']}")
    
    # Get exclusion lists
    excl_cis, excl_equ = get_exclusion_lists()
    print(f"Exclusions: CIS={len(excl_cis)}, EQU={len(excl_equ)}")
    
    # Process M&I
    mni_sum, mni_detail = process_mni(rep_vars, excl_cis)
    
    # Process Equity
    equ_sum, equ_detail = process_equity(rep_vars, excl_equ)
    
    # Consolidate
    allsrc = consolidate_sources(mni_sum, equ_sum)
    
    if allsrc.empty:
        print("ERROR: No consolidated data to report")
        return
    
    # Generate reports
    print("\n" + "=" * 60)
    print("Generating reports...")
    
    # Individual Top 50
    ind_file = PATHS['OUTPUT'] + 'COFOUTI.txt'
    ind_top = generate_top50_report(allsrc, 'I', 'Individual', rep_vars, ind_file)
    if not ind_top.empty:
        generate_detail_listing(ind_top, mni_detail, equ_detail, ind_file)
    
    # Corporate Top 50
    corp_file = PATHS['OUTPUT'] + 'COFOUTC.txt'
    corp_top = generate_top50_report(allsrc, 'C', 'Corporate', rep_vars, corp_file)
    if not corp_top.empty:
        generate_detail_listing(corp_top, mni_detail, equ_detail, corp_file)
    
    # Top 100 by Product
    prod_file = PATHS['OUTPUT'] + 'COFOUT1.txt'
    top100 = generate_top100_by_product(allsrc, rep_vars, prod_file)
    
    # Maturity report
    mat_file = PATHS['OUTPUT'] + 'COFOUT3.txt'
    generate_maturity_report(top100, mni_detail, rep_vars, mat_file)
    
    # Clean up
    print("\nCleaning up...")
    del allsrc, mni_sum, mni_detail, equ_sum, equ_detail
    gc.collect()
    
    print(f"\nAll reports written to {PATHS['OUTPUT']}")
    print("=" * 60)
    print("✓ EIIMTLCR Complete")

if __name__ == "__main__":
    main()
