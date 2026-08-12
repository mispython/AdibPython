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
        print(f"  Read {os.path.basename(filepath)}: {len(df)} rows, {len(df.columns)} columns")
        return df
    except Exception as e:
        print(f"ERROR reading {filepath}: {e}")
        return pd.DataFrame()

def get_report_date():
    """Get report date (yesterday) - same as SAS REPTDATE"""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday

def get_report_vars(report_date=None):
    """Calculate all report date variables from a date"""
    if report_date is None:
        report_date = get_report_date()
    
    return {
        'reptyear': report_date.strftime('%Y'),           # e.g., 2026
        'reptmon': report_date.strftime('%m'),            # e.g., 08
        'reptday': report_date.strftime('%d'),            # e.g., 11
        'rptdt': report_date.strftime('%y%m%d'),          # e.g., 260811
        'fildt': report_date.strftime('%d%m%y'),          # e.g., 110826
        'rdate': report_date.strftime('%d/%m/%Y')         # e.g., 11/08/2026
    }

def format_comma20_2(value):
    """Format number with comma and 2 decimal places (SAS COMMA20.2)"""
    if value is None or pd.isna(value):
        return '0.00'
    try:
        return f"{float(value):,.2f}"
    except (ValueError, TypeError):
        return '0.00'

def safe_str(value, max_len=None):
    """Safely convert to string with optional max length"""
    if value is None or pd.isna(value):
        return ''
    s = str(value).strip()
    if max_len and len(s) > max_len:
        s = s[:max_len]
    return s

# =============================================================================
# EXCLUSION LISTS
# =============================================================================
def get_exclusion_lists():
    """Get exclusion lists from SAS datasets"""
    excl_cis = []
    excl_equ = []
    
    # CIS exclusion list
    cis_file = PATHS['LIST'] + 'keep_top_dep_excl_pibb.sas7bdat'
    print(f"\nReading CIS exclusion list: {cis_file}")
    
    if os.path.exists(cis_file):
        try:
            df_cis = read_sas7bdat(cis_file)
            if not df_cis.empty:
                # Check if 'custno' column exists
                if 'custno' in df_cis.columns:
                    # Filter CUSTNO > 0 and convert to string list
                    df_cis_filtered = df_cis[df_cis['custno'] > 0]
                    excl_cis = [str(int(r)) for r in df_cis_filtered['custno'].tolist()]
                    print(f"  CIS exclusions: {len(excl_cis)}")
                else:
                    print(f"  WARNING: 'custno' column not found in CIS exclusion file")
                    print(f"  Available columns: {df_cis.columns.tolist()}")
            else:
                print(f"  CIS exclusion file is empty")
        except Exception as e:
            print(f"  ERROR reading CIS exclusion list: {e}")
    else:
        print(f"  CIS exclusion file not found, skipping")
    
    # EQU exclusion list
    equ_file = PATHS['LIST'] + 'keep_top_dep_excl_equ_pibb.sas7bdat'
    print(f"Reading EQU exclusion list: {equ_file}")
    
    if os.path.exists(equ_file):
        try:
            df_equ = read_sas7bdat(equ_file)
            if not df_equ.empty:
                if 'custno' in df_equ.columns:
                    # Filter non-empty CUSTNO values
                    df_equ_filtered = df_equ[
                        df_equ['custno'].notna() & 
                        (df_equ['custno'].astype(str).str.strip() != '')
                    ]
                    excl_equ = [str(r).strip() for r in df_equ_filtered['custno'].tolist()]
                    print(f"  EQU exclusions: {len(excl_equ)}")
                else:
                    print(f"  WARNING: 'custno' column not found in EQU exclusion file")
                    print(f"  Available columns: {df_equ.columns.tolist()}")
            else:
                print(f"  EQU exclusion file is empty")
        except Exception as e:
            print(f"  ERROR reading EQU exclusion list: {e}")
    else:
        print(f"  EQU exclusion file not found, skipping")
    
    return excl_cis, excl_equ

# =============================================================================
# M&I (Monetary & Islamic) PROCESSING
# =============================================================================
def process_mni(rep_vars, excl_cis):
    """Process M&I source data for Islamic banking"""
    print("\n" + "=" * 60)
    print("M&I PROCESSING")
    print("=" * 60)
    
    # Read CMM data
    cmm_file = PATHS['LCR'] + f"cmm{rep_vars['reptmon']}.sas7bdat"
    print(f"\nReading CMM file: {cmm_file}")
    cmm = read_sas7bdat(cmm_file)
    
    if cmm.empty:
        print("ERROR: CMM file is empty or not found")
        return pd.DataFrame(), pd.DataFrame()
    
    # Print column info for debugging
    print(f"CMM columns: {cmm.columns.tolist()}")
    if 'newic' in cmm.columns:
        print(f"CMM NEWIC sample: {cmm['newic'].dropna().head(5).tolist()}")
    if 'custno' in cmm.columns:
        print(f"CMM CUSTNO sample: {cmm['custno'].dropna().head(5).tolist()}")
    
    # Add EXCL flag based on exclusion list
    if 'custno' in cmm.columns:
        cmm['excl'] = 'N'
        if excl_cis:
            # Convert custno to string for comparison
            cmm['_custno_str'] = cmm['custno'].apply(lambda x: str(int(x)) if pd.notna(x) else '')
            cmm.loc[cmm['_custno_str'].isin(excl_cis), 'excl'] = 'Y'
            excluded_count = (cmm['excl'] == 'Y').sum()
            print(f"Records excluded: {excluded_count}/{len(cmm)}")
            cmm.drop(columns=['_custno_str'], inplace=True)
    else:
        print("WARNING: 'custno' column not found, cannot apply exclusions")
        cmm['excl'] = 'N'
    
    # Read COF_MNI_DEPOSITOR_LIST
    cof_file = PATHS['LIST'] + 'icof_mni_depositor_list.sas7bdat'
    print(f"\nReading COF file: {cof_file}")
    cof = read_sas7bdat(cof_file)
    
    mni1_matched = pd.DataFrame()
    mni1_unmatched = cmm.copy()
    
    if not cof.empty:
        print(f"COF columns: {cof.columns.tolist()}")
        
        # Check required columns
        required_cols = ['depid', 'depgrp', 'bussreg']
        missing_cols = [c for c in required_cols if c not in cof.columns]
        if missing_cols:
            print(f"WARNING: Missing columns in COF: {missing_cols}")
        else:
            # First merge by BUSSREG/NEWIC
            cof_idno = cof[['depid', 'depgrp', 'bussreg']].copy()
            cof_idno = cof_idno.drop_duplicates(subset='bussreg')
            cof_idno.rename(columns={'bussreg': 'newic'}, inplace=True)
            
            print(f"COF unique BUSSREG: {len(cof_idno)}")
            print(f"COF BUSSREG sample: {cof_idno['newic'].head(5).tolist()}")
            
            # Perform merge
            print(f"\nMerging CMM ({len(cmm)}) with COF by NEWIC...")
            mni1 = cmm.merge(cof_idno, on='newic', how='left', indicator=True)
            
            matched_count = (mni1['_merge'] == 'both').sum()
            unmatched_count = (mni1['_merge'] == 'left_only').sum()
            print(f"Matched by NEWIC: {matched_count}")
            print(f"Unmatched by NEWIC: {unmatched_count}")
            
            # Split matched and unmatched
            mni1_matched = mni1[mni1['_merge'] == 'both'].drop(columns=['_merge']).copy()
            mni1_unmatched = mni1[mni1['_merge'] == 'left_only'].drop(columns=['depid', 'depgrp', '_merge']).copy()
            
            # Second merge by CUSTNO for unmatched records
            mni2_matched = pd.DataFrame()
            mni2_unmatched = mni1_unmatched.copy()
            
            if not mni1_unmatched.empty and 'custno' in mni1_unmatched.columns and 'custno' in cof.columns:
                print(f"\nSecond merge: Unmatched ({len(mni1_unmatched)}) with COF by CUSTNO...")
                cof_cust = cof[['depid', 'depgrp', 'custno']].copy()
                cof_cust = cof_cust.drop_duplicates(subset='custno')
                
                mni2 = mni1_unmatched.merge(cof_cust, on='custno', how='left', indicator=True)
                
                matched2_count = (mni2['_merge'] == 'both').sum()
                print(f"Matched by CUSTNO: {matched2_count}")
                
                mni2_matched = mni2[mni2['_merge'] == 'both'].drop(columns=['_merge']).copy()
                mni2_unmatched = mni2[mni2['_merge'] == 'left_only'].drop(columns=['depid', 'depgrp', '_merge']).copy()
            else:
                mni2_matched = pd.DataFrame()
                mni2_unmatched = mni1_unmatched.copy()
            
            # Assign new DEPID for remaining unmatched
            mni3 = pd.DataFrame()
            if not mni2_unmatched.empty and 'custno' in mni2_unmatched.columns:
                print(f"\nAssigning new DEPID for {mni2_unmatched['custno'].nunique()} remaining unique customers...")
                
                # Get unique customers sorted
                unique_cust = mni2_unmatched[['custno']].drop_duplicates().sort_values('custno')
                unique_cust['depid'] = range(5001, 5001 + len(unique_cust))
                
                mni3 = mni2_unmatched.merge(unique_cust, on='custno', how='left')
                mni3['depgrp'] = mni3['custname'].fillna('')
                print(f"New DEPID range: 5001 to {5000 + len(unique_cust)}")
            else:
                mni3 = pd.DataFrame()
            
            # Combine all records
            dfs = []
            if not mni1_matched.empty:
                dfs.append(mni1_matched)
                print(f"  Group 1 (matched by NEWIC): {len(mni1_matched)}")
            if not mni2_matched.empty:
                dfs.append(mni2_matched)
                print(f"  Group 2 (matched by CUSTNO): {len(mni2_matched)}")
            if not mni3.empty:
                dfs.append(mni3)
                print(f"  Group 3 (new DEPID): {len(mni3)}")
            
            if dfs:
                mni_all = pd.concat(dfs, ignore_index=True)
            else:
                print("ERROR: No M&I records after processing")
                return pd.DataFrame(), pd.DataFrame()
    else:
        print("ERROR: COF file not found or empty, cannot assign DEPID")
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"\nTotal M&I records: {len(mni_all)}")
    
    # Clean up memory
    del cmm, cof
    if 'mni1' in locals(): del mni1
    if 'mni2' in locals(): del mni2
    del mni1_matched, mni1_unmatched, mni2_matched, mni2_unmatched, mni3
    gc.collect()
    
    # Classify products by BIC code
    print("\nClassifying products...")
    
    if 'cmmcode' not in mni_all.columns:
        print("ERROR: 'cmmcode' column not found")
        return pd.DataFrame(), pd.DataFrame()
    
    mni_all['bic'] = mni_all['cmmcode'].astype(str).str[:5]
    
    # Initialize product columns
    mni_all['amount'] = mni_all['amount'].fillna(0).astype(float)
    
    # Product classification
    fd_bics = ['95315', '96315', '95317', '96317']
    mni_all['fd'] = np.where(mni_all['bic'].isin(fd_bics), mni_all['amount'], 0.0)
    mni_all['sa'] = np.where(mni_all['bic'] == '95312', mni_all['amount'], 0.0)
    mni_all['ca'] = np.where(mni_all['bic'].isin(['95313', '96313']), mni_all['amount'], 0.0)
    mni_all['rnid'] = np.where(mni_all['bic'].isin(['95840', '96840']), mni_all['amount'], 0.0)
    
    # Excluded amounts (for TOT2 calculation)
    is_not_excl = (mni_all['excl'] != 'Y')
    mni_all['fd2'] = np.where(mni_all['bic'].isin(fd_bics) & is_not_excl, mni_all['amount'], 0.0)
    mni_all['sa2'] = np.where((mni_all['bic'] == '95312') & is_not_excl, mni_all['amount'], 0.0)
    mni_all['ca2'] = np.where(mni_all['bic'].isin(['95313', '96313']) & is_not_excl, mni_all['amount'], 0.0)
    mni_all['rnid2'] = np.where(mni_all['bic'].isin(['95840', '96840']) & is_not_excl, mni_all['amount'], 0.0)
    
    # Customer type
    if 'custcd' in mni_all.columns:
        mni_all['custype'] = np.where(
            mni_all['custcd'].astype(str).str.strip().isin(['77', '78', '95', '96']), 
            'I', 'C'
        )
    else:
        print("WARNING: 'custcd' column not found, defaulting to 'C'")
        mni_all['custype'] = 'C'
    
    # Filter out excluded BICs (keep only M&I products)
    exclude_bics = ['95810', '96810', '95820', '96820']
    before = len(mni_all)
    mni_all = mni_all[~mni_all['bic'].isin(exclude_bics)].copy()
    print(f"After removing non-M&I BICs: {len(mni_all)} (removed {before - len(mni_all)})")
    
    # Summarize by DEPID, DEPGRP, CUSTYPE
    print("\nSummarizing by DEPID/DEPGRP/CUSTYPE...")
    group_cols = ['depid', 'depgrp', 'custype']
    
    # Ensure all required columns exist
    agg_cols = ['fd', 'sa', 'ca', 'rnid', 'fd2', 'sa2', 'ca2', 'rnid2']
    for col in agg_cols:
        if col not in mni_all.columns:
            mni_all[col] = 0.0
    
    mni_sum = mni_all.groupby(group_cols, as_index=False)[agg_cols].sum()
    
    print(f"M&I summary: {len(mni_sum)} groups")
    if len(mni_sum) > 0:
        print(f"Sample:\n{mni_sum.head(3)}")
        print(f"CUSTYPE distribution: {mni_sum['custype'].value_counts().to_dict()}")
    
    return mni_sum, mni_all

# =============================================================================
# EQUITY PROCESSING
# =============================================================================
def process_equity(rep_vars, excl_equ):
    """Process Equity source data for Islamic banking"""
    print("\n" + "=" * 60)
    print("EQUITY PROCESSING")
    print("=" * 60)
    
    # Read EQU data
    equ_file = PATHS['LCR'] + f"equ{rep_vars['reptmon']}.sas7bdat"
    print(f"\nReading EQU file: {equ_file}")
    equ = read_sas7bdat(equ_file)
    
    if equ.empty:
        print("WARNING: EQU file is empty or not found")
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"EQU columns: {equ.columns.tolist()}")
    if 'custno' in equ.columns:
        print(f"EQU CUSTNO sample: {equ['custno'].dropna().head(5).tolist()}")
    
    # Filter non-empty CUSTNO
    if 'custno' in equ.columns:
        before = len(equ)
        equ = equ[equ['custno'].notna() & (equ['custno'].astype(str).str.strip() != '')].copy()
        print(f"After filtering empty CUSTNO: {len(equ)} (removed {before - len(equ)})")
    
    # Add EXCL flag
    equ['excl'] = 'N'
    if excl_equ and 'custno' in equ.columns:
        equ['_custno_str'] = equ['custno'].astype(str).str.strip()
        equ.loc[equ['_custno_str'].isin(excl_equ), 'excl'] = 'Y'
        excluded = (equ['excl'] == 'Y').sum()
        print(f"Excluded EQU records: {excluded}/{len(equ)}")
        equ.drop(columns=['_custno_str'], inplace=True)
    
    # Read ICOF_EQU_DEPOSITOR_LIST
    cof_file = PATHS['LIST'] + 'icof_equ_depositor_list.sas7bdat'
    print(f"\nReading EQU COF file: {cof_file}")
    cof = read_sas7bdat(cof_file)
    
    equ_matched = pd.DataFrame()
    equ_unmatched = equ.copy()
    
    if not cof.empty:
        print(f"EQU COF columns: {cof.columns.tolist()}")
        
        if 'custno' in cof.columns:
            cof_equ = cof[['depid', 'depgrp', 'custno', 'linkid']].copy()
            cof_equ = cof_equ.drop_duplicates(subset='custno')
            
            print(f"EQU COF unique CUSTNO: {len(cof_equ)}")
            
            # Merge by CUSTNO
            print(f"\nMerging EQU ({len(equ)}) with COF by CUSTNO...")
            equ1 = equ.merge(cof_equ, on='custno', how='left', indicator=True)
            
            matched = (equ1['_merge'] == 'both').sum()
            unmatched = (equ1['_merge'] == 'left_only').sum()
            print(f"Matched by CUSTNO: {matched}")
            print(f"Unmatched by CUSTNO: {unmatched}")
            
            equ_matched = equ1[equ1['_merge'] == 'both'].drop(columns=['_merge']).copy()
            equ_unmatched = equ1[equ1['_merge'] == 'left_only'].drop(
                columns=['depid', 'depgrp', 'linkid', '_merge']
            ).copy()
        else:
            print("WARNING: 'custno' column not found in EQU COF")
    else:
        print("WARNING: EQU COF file not found")
    
    # Assign new DEPID for unmatched
    equ2 = pd.DataFrame()
    if not equ_unmatched.empty and 'custno' in equ_unmatched.columns:
        print(f"\nAssigning new DEPID for {equ_unmatched['custno'].nunique()} unique EQU customers...")
        
        unique_cust = equ_unmatched[['custno']].drop_duplicates().sort_values('custno')
        unique_cust['depid'] = range(50005001, 50005001 + len(unique_cust))
        
        equ2 = equ_unmatched.merge(unique_cust, on='custno', how='left')
        equ2['depgrp'] = equ2['custname'].fillna('')
        if 'linkid' not in equ2.columns:
            equ2['linkid'] = np.nan
        print(f"New DEPID range: 50005001 to {50005000 + len(unique_cust)}")
    
    # Combine
    dfs = []
    if not equ_matched.empty:
        dfs.append(equ_matched)
        print(f"  Matched records: {len(equ_matched)}")
    if not equ2.empty:
        dfs.append(equ2)
        print(f"  New DEPID records: {len(equ2)}")
    
    if not dfs:
        print("ERROR: No Equity records after processing")
        return pd.DataFrame(), pd.DataFrame()
    
    equ_all = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal EQU records: {len(equ_all)}")
    
    # Clean up
    del equ, cof
    if 'equ1' in locals(): del equ1
    del equ_matched, equ_unmatched, equ2
    gc.collect()
    
    # Handle LINKID
    if 'linkid' in equ_all.columns:
        equ_all['linkid'] = equ_all['linkid'].fillna(50000000 + equ_all['depid'])
    else:
        equ_all['linkid'] = 50000000 + equ_all['depid']
    
    # Classify products
    print("\nClassifying Equity products...")
    
    if 'cmmcode' not in equ_all.columns:
        print("ERROR: 'cmmcode' column not found")
        return pd.DataFrame(), pd.DataFrame()
    
    equ_all['bic'] = equ_all['cmmcode'].astype(str).str[:5]
    equ_all['amount'] = equ_all['amount'].fillna(0).astype(float)
    
    equ_all['std'] = np.where(equ_all['bic'].isin(['95830', '96830']), equ_all['amount'], 0.0)
    equ_all['nid'] = np.where(equ_all['bic'].isin(['95840', '96840']), equ_all['amount'], 0.0)
    equ_all['ibb'] = np.where(equ_all['bic'].isin(['95810', '96810']), equ_all['amount'], 0.0)
    equ_all['repo'] = np.where(equ_all['bic'].isin(['95820', '96820']), equ_all['amount'], 0.0)
    
    is_not_excl = (equ_all['excl'] != 'Y')
    equ_all['std2'] = np.where(equ_all['bic'].isin(['95830', '96830']) & is_not_excl, equ_all['amount'], 0.0)
    equ_all['nid2'] = np.where(equ_all['bic'].isin(['95840', '96840']) & is_not_excl, equ_all['amount'], 0.0)
    
    # Customer type
    if 'custfiss' in equ_all.columns:
        equ_all['custype'] = np.where(
            equ_all['custfiss'].astype(str).str.strip().isin(['77', '78', '95', '96']),
            'I', 'C'
        )
    else:
        print("WARNING: 'custfiss' column not found, defaulting to 'C'")
        equ_all['custype'] = 'C'
    
    # Summarize by LINKID
    print("\nSummarizing Equity by LINKID/DEPGRP/CUSTYPE...")
    equ_sum = equ_all.groupby(['linkid', 'depgrp', 'custype'], as_index=False).agg({
        'std': 'sum', 'nid': 'sum', 'ibb': 'sum', 'repo': 'sum',
        'std2': 'sum', 'nid2': 'sum'
    })
    equ_sum.rename(columns={'linkid': 'depid'}, inplace=True)
    
    print(f"Equity summary: {len(equ_sum)} groups")
    if len(equ_sum) > 0:
        print(f"Sample:\n{equ_sum.head(3)}")
        print(f"CUSTYPE distribution: {equ_sum['custype'].value_counts().to_dict()}")
    
    return equ_sum, equ_all

# =============================================================================
# CONSOLIDATION
# =============================================================================
def consolidate_sources(mni_sum, equ_sum):
    """Consolidate M&I and Equity sources"""
    print("\n" + "=" * 60)
    print("CONSOLIDATION")
    print("=" * 60)
    
    if mni_sum.empty and equ_sum.empty:
        print("ERROR: Both sources empty")
        return pd.DataFrame()
    
    # Ensure DEPID is same type
    if not mni_sum.empty:
        mni_sum['depid'] = mni_sum['depid'].astype(float)
    if not equ_sum.empty:
        equ_sum['depid'] = equ_sum['depid'].astype(float)
    
    print(f"M&I groups: {len(mni_sum)}")
    print(f"Equity groups: {len(equ_sum)}")
    
    # Merge by DEPID
    if not mni_sum.empty and not equ_sum.empty:
        print(f"\nMerging by DEPID...")
        allsrc = mni_sum.merge(equ_sum, on='depid', how='outer', suffixes=('_mni', '_equ'))
        print(f"After merge: {len(allsrc)} rows")
    elif not mni_sum.empty:
        allsrc = mni_sum.copy()
        allsrc.rename(columns={'custype': 'custype_mni', 'depgrp': 'depgrp_mni'}, inplace=True)
    else:
        allsrc = equ_sum.copy()
        allsrc.rename(columns={'custype': 'custype_equ', 'depgrp': 'depgrp_equ'}, inplace=True)
    
    # Combine DEPGRP
    if 'depgrp_mni' in allsrc.columns and 'depgrp_equ' in allsrc.columns:
        allsrc['depgrp'] = allsrc['depgrp_mni'].fillna('')
        allsrc.loc[allsrc['depgrp'] == '', 'depgrp'] = allsrc['depgrp_equ'].fillna('')
    elif 'depgrp_mni' in allsrc.columns:
        allsrc['depgrp'] = allsrc['depgrp_mni'].fillna('')
    elif 'depgrp_equ' in allsrc.columns:
        allsrc['depgrp'] = allsrc['depgrp_equ'].fillna('')
    else:
        allsrc['depgrp'] = ''
    
    # Combine CUSTYPE
    if 'custype_mni' in allsrc.columns and 'custype_equ' in allsrc.columns:
        allsrc['custype'] = allsrc['custype_mni'].fillna('')
        allsrc.loc[allsrc['custype'] == '', 'custype'] = allsrc['custype_equ'].fillna('')
    elif 'custype_mni' in allsrc.columns:
        allsrc['custype'] = allsrc['custype_mni'].fillna('')
    elif 'custype_equ' in allsrc.columns:
        allsrc['custype'] = allsrc['custype_equ'].fillna('')
    else:
        allsrc['custype'] = ''
    
    # Fill numeric columns
    num_cols = ['fd', 'sa', 'ca', 'rnid', 'fd2', 'sa2', 'ca2', 'rnid2',
                'std', 'nid', 'ibb', 'repo', 'std2', 'nid2']
    for col in num_cols:
        if col not in allsrc.columns:
            allsrc[col] = 0.0
        allsrc[col] = allsrc[col].fillna(0.0)
    
    # Calculate totals
    allsrc['nid_total'] = allsrc['nid'] + allsrc['rnid']
    allsrc['tot'] = (allsrc['fd'] + allsrc['sa'] + allsrc['ca'] + 
                     allsrc['std'] + allsrc['nid_total'] + allsrc['ibb'] + allsrc['repo'])
    allsrc['mni'] = allsrc['fd2'] + allsrc['sa2'] + allsrc['ca2'] + allsrc['rnid2']
    allsrc['equ'] = allsrc['std2'] + allsrc['nid2']
    allsrc['tot2'] = allsrc['mni'] + allsrc['equ']
    
    print(f"\nFinal consolidated groups: {len(allsrc)}")
    print(f"CUSTYPE distribution:")
    print(allsrc['custype'].value_counts())
    print(f"\nTop 5 by TOT2:")
    print(allsrc.nlargest(5, 'tot2')[['depid', 'depgrp', 'custype', 'tot2', 'mni', 'equ']])
    
    return allsrc

# =============================================================================
# REPORT GENERATION
# =============================================================================
def generate_top50_report(allsrc, cust_type, desc, rep_vars, output_file):
    """Generate Top 50 report for a customer type"""
    print(f"\n{'=' * 60}")
    print(f"TOP 50 {desc.upper()} REPORT")
    print(f"{'=' * 60}")
    
    filtered = allsrc[allsrc['custype'] == cust_type].copy()
    print(f"Total {desc} groups: {len(filtered)}")
    
    if filtered.empty:
        print(f"WARNING: No {desc} data")
        return pd.DataFrame()
    
    top50 = filtered.nlargest(50, 'tot2').copy()
    top50['rank'] = range(1, len(top50) + 1)
    print(f"Top 50 TOT2 range: {top50['tot2'].min():,.2f} to {top50['tot2'].max():,.2f}")
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Header
            f.write(f"PUBLIC ISLAMIC BANK BERHAD\n")
            f.write(f"TOP 50 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}\n")
            f.write(f"\n")
            f.write(f"(i) Top 50 {desc} Depositors by Sources\n")
            f.write(f"\n")
            f.write(f"NOBS{DLM}DEPOSITORS{DLM}TOTAL BALANCE{DLM}M&I{DLM}EQUATION{DLM}\n")
            
            # Data rows
            for _, row in top50.iterrows():
                f.write(f"{row['rank']}{DLM}"
                       f"{safe_str(row['depgrp'], 50)}{DLM}"
                       f"{format_comma20_2(row['tot2'])}{DLM}"
                       f"{format_comma20_2(row['mni'])}{DLM}"
                       f"{format_comma20_2(row['equ'])}{DLM}\n")
            
            # Prepare for detail section
            f.write(f"\n\n")
            f.write(f"(ii) Detail Accounts Listing for Top 50 {desc} Depositors\n")
            f.write(f"\n")
        
        print(f"Report written: {output_file}")
        print(f"File size: {os.path.getsize(output_file):,} bytes")
    except Exception as e:
        print(f"ERROR writing report: {e}")
        import traceback
        traceback.print_exc()
    
    return top50

def generate_detail_listing(top50, mni_detail, equ_detail, output_file):
    """Generate detailed account listing"""
    if top50.empty:
        return
    
    print(f"\nGenerating detail listing for {len(top50)} depositors...")
    
    try:
        with open(output_file, 'a', encoding='utf-8') as f:
            for idx, (_, top) in enumerate(top50.iterrows()):
                if idx % 10 == 0:
                    print(f"  Processing {idx+1}/{len(top50)}: {safe_str(top['depgrp'], 30)}")
                
                depid = top['depid']
                rank = top['rank']
                depgrp = safe_str(top['depgrp'], 50)
                
                f.write(f"{rank}{DLM}{depgrp} ({depid}){DLM}\n")
                f.write(f"\n")
                
                # M&I Detail Section
                f.write(f"{DLM}Source: M&I\n")
                f.write(f"\n")
                f.write(f"{DLM}NO{DLM}BRANCH{DLM}ACCTNO{DLM}CUSTNAME{DLM}"
                       f"CUSTNO{DLM}BUSSREG{DLM}CUSTCD{DLM}PRODUCT{DLM}BALANCE{DLM}\n")
                
                mni_total = 0
                cnt = 0
                
                if not mni_detail.empty and 'depid' in mni_detail.columns:
                    mni_det = mni_detail[
                        (mni_detail['depid'] == depid) & 
                        (mni_detail['amount'] > 0) & 
                        (mni_detail['excl'] != 'Y')
                    ]
                    
                    if 'acctno' in mni_det.columns:
                        mni_det = mni_det.sort_values('acctno')
                    
                    for _, row in mni_det.iterrows():
                        cnt += 1
                        mni_total += float(row['amount'])
                        
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
                               f"{format_comma20_2(mni_total)}{DLM}\n")
                        f.write(f"\n")
                
                # Equity Detail Section
                f.write(f"{DLM}Source: EQU\n")
                f.write(f"\n")
                f.write(f"{DLM}NO{DLM}DEALREF{DLM}DEALTYPE{DLM}NAME{DLM}"
                       f"CUST MNEMONIC{DLM}AMOUNT{DLM}\n")
                
                equ_total = 0
                cnt = 0
                
                if not equ_detail.empty and 'linkid' in equ_detail.columns:
                    equ_det = equ_detail[
                        (equ_detail['linkid'] == depid) & 
                        (equ_detail['amount'] > 0) & 
                        (equ_detail['excl'] != 'Y')
                    ]
                    
                    for _, row in equ_det.iterrows():
                        cnt += 1
                        equ_total += float(row['amount'])
                        
                        dealref = safe_str(row.get('dealref') or row.get('gwdlr') or row.get('utdlr'), 15)
                        dealtype = safe_str(row.get('dealtype') or row.get('gwdlp') or row.get('utsty'), 10)
                        
                        f.write(f"{DLM}{cnt}{DLM}{dealref}{DLM}{dealtype}{DLM}"
                               f"{safe_str(row.get('custname'), 25)}{DLM}"
                               f"{safe_str(row.get('custno'), 15)}{DLM}"
                               f"{format_comma20_2(row['amount'])}{DLM}\n")
                    
                    if cnt > 0:
                        f.write(f"{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}"
                               f"{format_comma20_2(equ_total)}{DLM}\n")
                        f.write(f"\n")
                
                f.write(f"\n")
        
        print(f"Detail listing completed: {output_file}")
        print(f"File size: {os.path.getsize(output_file):,} bytes")
    except Exception as e:
        print(f"ERROR in detail listing: {e}")
        import traceback
        traceback.print_exc()

def generate_top100_by_product(allsrc, rep_vars, output_file):
    """Generate Top 100 by product report"""
    print(f"\n{'=' * 60}")
    print(f"TOP 100 BY PRODUCT REPORT")
    print(f"{'=' * 60}")
    
    top100 = allsrc.nlargest(100, 'tot').copy()
    top100['rank'] = range(1, len(top100) + 1)
    print(f"Top 100 TOT range: {top100['tot'].min():,.2f} to {top100['tot'].max():,.2f}")
    
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
        
        print(f"Report written: {output_file}")
        print(f"File size: {os.path.getsize(output_file):,} bytes")
    except Exception as e:
        print(f"ERROR writing report: {e}")
        import traceback
        traceback.print_exc()
    
    return top100

def generate_maturity_report(top100, mni_detail, rep_vars, output_file):
    """Generate contractual maturity report"""
    print(f"\n{'=' * 60}")
    print(f"MATURITY REPORT")
    print(f"{'=' * 60}")
    
    if top100.empty:
        print("No data for maturity report")
        return
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"PUBLIC ISLAMIC BANK BERHAD\n")
            f.write(f"TOP 100 DEPOSITORS BY CONTRACTUAL MATURITY AS AT {rep_vars['rdate']}\n")
            f.write(f"\n")
            f.write(f"(iii) Top 100 Depositors by Contractual Maturity\n")
            
            for idx, (_, top) in enumerate(top100.iterrows()):
                if idx % 10 == 0:
                    print(f"  Processing maturity {idx+1}/{len(top100)}: {safe_str(top['depgrp'], 30)}")
                
                rank = top['rank']
                depgrp = safe_str(top['depgrp'], 50)
                depid = top['depid']
                
                f.write(f"\n")
                f.write(f"{rank}{DLM}{depgrp}\n")
                f.write(f"{DLM}DEPOSIT TYPE{DLM}UP TO 1 WEEK{DLM}"
                       f"> 1 WK - 1 MTH{DLM}> 1 - 3 MTHS{DLM}"
                       f"> 3 - 6 MTHS{DLM}> 6 MTHS - 1 YR{DLM}"
                       f"> 1 YEAR{DLM}NO SPECIFIC MATURITY{DLM}TOTAL{DLM}\n")
                
                # Process M&I detail for this depositor
                if not mni_detail.empty and 'depid' in mni_detail.columns and 'cmmcode' in mni_detail.columns:
                    mni_det = mni_detail[
                        (mni_detail['depid'] == depid) & 
                        (mni_detail['amount'] > 0)
                    ].copy()
                    
                    if not mni_det.empty:
                        mni_det['bic'] = mni_det['cmmcode'].astype(str).str[:5]
                        mni_det['rem'] = mni_det['cmmcode'].astype(str).str[7:9]
                        
                        # No specific maturity for certain BICs
                        mni_det.loc[mni_det['bic'].isin(['95312', '95313', '96313']), 'rem'] = '07'
                        
                        # Maturity buckets
                        mni_det['buc1'] = np.where(mni_det['rem'] == '01', mni_det['amount'], 0.0)
                        mni_det['buc2'] = np.where(mni_det['rem'] == '02', mni_det['amount'], 0.0)
                        mni_det['buc3'] = np.where(mni_det['rem'] == '03', mni_det['amount'], 0.0)
                        mni_det['buc4'] = np.where(mni_det['rem'] == '04', mni_det['amount'], 0.0)
                        mni_det['buc5'] = np.where(mni_det['rem'] == '05', mni_det['amount'], 0.0)
                        mni_det['buc6'] = np.where(mni_det['rem'] == '06', mni_det['amount'], 0.0)
                        mni_det['buc7'] = np.where(mni_det['rem'] == '07', mni_det['amount'], 0.0)
                        
                        # Map to item
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
        
        print(f"Report written: {output_file}")
        print(f"File size: {os.path.getsize(output_file):,} bytes")
    except Exception as e:
        print(f"ERROR writing maturity report: {e}")
        import traceback
        traceback.print_exc()

# =============================================================================
# MAIN
# =============================================================================
def main():
    """Main execution"""
    print("=" * 60)
    print("EIIMTLCR - Top Depositors Report (Islamic Banking)")
    print("=" * 60)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Check disk space
    try:
        stat = os.statvfs(PATHS['OUTPUT'])
        available_mb = (stat.f_bavail * stat.f_frsize) / (1024 * 1024)
        print(f"\nAvailable disk space: {available_mb:.2f} MB")
        if available_mb < 100:
            print("WARNING: Low disk space!")
    except:
        print("Could not check disk space")
    
    # Get report date and variables
    report_date = get_report_date()
    rep_vars = get_report_vars(report_date)
    print(f"\nReport Date: {rep_vars['rdate']}")
    print(f"Report Month: {rep_vars['reptmon']}")
    print(f"Output directory: {PATHS['OUTPUT']}")
    
    # Get exclusion lists
    excl_cis, excl_equ = get_exclusion_lists()
    
    # Process data
    mni_sum, mni_detail = process_mni(rep_vars, excl_cis)
    equ_sum, equ_detail = process_equity(rep_vars, excl_equ)
    
    # Consolidate
    allsrc = consolidate_sources(mni_sum, equ_sum)
    
    if allsrc.empty:
        print("\nERROR: No data to report")
        return
    
    # Generate reports
    print("\n" + "=" * 60)
    print("GENERATING REPORTS")
    print("=" * 60)
    
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
    
    # Generate detail listing for Top 100
    if not top100.empty:
        # Top 100 detail file
        detail100_file = PATHS['OUTPUT'] + 'COFOUT2.txt'
        generate_detail_listing(top100, mni_detail, equ_detail, detail100_file)
    
    # Maturity report
    mat_file = PATHS['OUTPUT'] + 'COFOUT3.txt'
    generate_maturity_report(top100, mni_detail, rep_vars, mat_file)
    
    # Cleanup
    print("\n" + "=" * 60)
    print("CLEANUP")
    print("=" * 60)
    del allsrc, mni_sum, mni_detail, equ_sum, equ_detail
    gc.collect()
    
    # List output files
    print("\nOutput files:")
    for f in ['COFOUTI.txt', 'COFOUTC.txt', 'COFOUT1.txt', 'COFOUT2.txt', 'COFOUT3.txt']:
        path = PATHS['OUTPUT'] + f
        if os.path.exists(path):
            size_kb = os.path.getsize(path) / 1024
            print(f"  {f}: {size_kb:.1f} KB")
    
    print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    print("✓ EIIMTLCR Complete")

if __name__ == "__main__":
    main()
