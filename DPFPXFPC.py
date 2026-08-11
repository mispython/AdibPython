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

# =============================================================================
# CONFIGURATION
# =============================================================================
# Input paths (adjust based on your environment)
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/'
PATHS = {
    'LCR': os.path.join(BASE_PATH, ''),
    'LIST': os.path.join(BASE_PATH, 'list/'),
    'OUTPUT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTCOF/'
}

for path in PATHS.values():
    Path(path).mkdir(parents=True, exist_ok=True)

# BIC to item mapping ($BICTAG format) - Islamic version
BIC_TAG = {
    '95315': 'A1.01', '95317': 'A1.02', '95312': 'A1.03',
    '95313': 'A1.04', '95810': 'A1.05', '95820': 'A1.06',
    '95830': 'A1.07', '95840': 'A1.08', '96317': 'B1.01',
    '96313': 'B1.02', '96810': 'B1.03', '96820': 'B1.04',
    '96830': 'B1.05', '96840': 'B1.06'
}

# Delimiter (hex 05)
DLM = '\x05'

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================
def read_sas7bdat(filepath, lowercase_cols=True):
    """Read SAS dataset and optionally convert column names to lowercase"""
    if not os.path.exists(filepath):
        print(f"WARNING: File not found: {filepath}")
        return pd.DataFrame()
    
    df, meta = pyreadstat.read_sas7bdat(filepath)
    if lowercase_cols:
        df.columns = [col.lower() for col in df.columns]
    return df

def get_report_date():
    """Get report date (yesterday)"""
    return datetime.now() - timedelta(days=1)

def get_report_vars(report_date):
    """Calculate all report date variables"""
    return {
        'reptyear': str(report_date.year),
        'reptmon': f"{report_date.month:02d}",
        'reptday': f"{report_date.day:02d}",
        'rptdt': report_date.strftime('%y%m%d'),
        'fildt': report_date.strftime('%d%m%y'),
        'rdate': report_date.strftime('%d/%m/%Y')
    }

def format_comma20_2(value):
    """Format number with comma and 2 decimal places (SAS COMMA20.2)"""
    if pd.isna(value) or value == 0:
        return '0.00'
    return f"{value:,.2f}"

# =============================================================================
# EXCLUSION LISTS
# =============================================================================
def get_exclusion_lists():
    """Get exclusion lists from SAS datasets"""
    excl_cis = []
    excl_equ = []
    
    try:
        df_cis = read_sas7bdat(PATHS['LIST'] + 'keep_top_dep_excl_pibb.sas7bdat')
        if not df_cis.empty and 'custno' in df_cis.columns:
            excl_cis = [str(int(r)) for r in df_cis[df_cis['custno'] > 0]['custno'].tolist()]
    except Exception as e:
        print(f"Error reading CIS exclusion list: {e}")
    
    try:
        df_equ = read_sas7bdat(PATHS['LIST'] + 'keep_top_dep_excl_equ_pibb.sas7bdat')
        if not df_equ.empty and 'custno' in df_equ.columns:
            excl_equ = [str(r).strip() for r in df_equ[df_equ['custno'].notna() & (df_equ['custno'] != '')]['custno'].tolist()]
    except Exception as e:
        print(f"Error reading EQU exclusion list: {e}")
    
    return excl_cis, excl_equ

# =============================================================================
# M&I (Monetary & Islamic) PROCESSING
# =============================================================================
def process_mni(rep_vars, excl_cis):
    """Process M&I source data for Islamic banking"""
    # Read CMM
    cmm = read_sas7bdat(PATHS['LCR'] + f"cmm{rep_vars['reptmon']}.sas7bdat")
    
    if cmm.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # Sort by newic
    cmm.sort_values('newic', inplace=True)
    
    # Read COF_MNI_DEPOSITOR_LIST
    cof = read_sas7bdat(PATHS['LIST'] + 'icof_mni_depositor_list.sas7bdat')
    
    # First merge by BUSSREG (renamed to NEWIC)
    if not cof.empty:
        cof_idno = cof[['depid', 'depgrp', 'bussreg']].drop_duplicates(subset='bussreg').copy()
        cof_idno.rename(columns={'bussreg': 'newic'}, inplace=True)
        mni1 = cmm.merge(cof_idno, on='newic', how='left')
    else:
        mni1 = cmm.copy()
        mni1['depid'] = np.nan
        mni1['depgrp'] = ''
    
    # Add EXCL flag
    mni1['excl'] = 'N'
    if excl_cis:
        mni1.loc[mni1['custno'].astype(str).isin(excl_cis), 'excl'] = 'Y'
    
    # Split matched/unmatched
    mni1_matched = mni1[mni1['depid'].notna() & (mni1['depid'] > 0)].copy()
    mni1_unmatched = mni1[~(mni1['depid'].notna() & (mni1['depid'] > 0))].copy()
    mni1_unmatched.drop(columns=['depid', 'depgrp'], inplace=True, errors='ignore')
    
    # Second merge by CUSTNO
    mni2_matched = pd.DataFrame()
    mni2_unmatched = pd.DataFrame()
    
    if not mni1_unmatched.empty:
        if not cof.empty:
            cof_cust = cof[['depid', 'depgrp', 'custno']].drop_duplicates(subset='custno').copy()
            mni2 = mni1_unmatched.merge(cof_cust, on='custno', how='left')
            mni2_matched = mni2[mni2['depid'].notna() & (mni2['depid'] > 0)].copy()
            mni2_unmatched = mni2[~(mni2['depid'].notna() & (mni2['depid'] > 0))].copy()
            mni2_unmatched.drop(columns=['depid', 'depgrp'], inplace=True, errors='ignore')
        else:
            mni2_unmatched = mni1_unmatched.copy()
    
    # Assign new DEPID for unmatched
    mni3 = pd.DataFrame()
    if not mni2_unmatched.empty:
        # Get unique customers in order
        unique_cust = mni2_unmatched[['custno']].drop_duplicates().sort_values('custno').copy()
        unique_cust['depid'] = range(5001, 5001 + len(unique_cust))
        
        mni3 = mni2_unmatched.merge(unique_cust, on='custno', how='left')
        mni3['depgrp'] = mni3['custname']
    
    # Combine all M&I records
    dfs = [df for df in [mni1_matched, mni2_matched, mni3] if not df.empty]
    if dfs:
        mni_all = pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame(), pd.DataFrame()
    
    # Extract BIC code
    mni_all['bic'] = mni_all['cmmcode'].astype(str).str[:5]
    
    # Classify by product type
    fd_bics = ['95315', '96315', '95317', '96317']
    ca_bics = ['95313', '96313']
    rnid_bics = ['95840', '96840']
    exclude_bics = ['95810', '96810', '95820', '96820']
    
    mni_all['fd'] = np.where(mni_all['bic'].isin(fd_bics), mni_all['amount'], 0)
    mni_all['sa'] = np.where(mni_all['bic'] == '95312', mni_all['amount'], 0)
    mni_all['ca'] = np.where(mni_all['bic'].isin(ca_bics), mni_all['amount'], 0)
    mni_all['rnid'] = np.where(mni_all['bic'].isin(rnid_bics), mni_all['amount'], 0)
    
    # Excluded amounts (for TOT2 calculation)
    mni_all['fd2'] = np.where((mni_all['bic'].isin(fd_bics)) & (mni_all['excl'] != 'Y'), mni_all['amount'], 0)
    mni_all['sa2'] = np.where((mni_all['bic'] == '95312') & (mni_all['excl'] != 'Y'), mni_all['amount'], 0)
    mni_all['ca2'] = np.where((mni_all['bic'].isin(ca_bics)) & (mni_all['excl'] != 'Y'), mni_all['amount'], 0)
    mni_all['rnid2'] = np.where((mni_all['bic'].isin(rnid_bics)) & (mni_all['excl'] != 'Y'), mni_all['amount'], 0)
    
    # Customer type
    mni_all['custype'] = np.where(mni_all['custcd'].astype(str).isin(['77', '78', '95', '96']), 'I', 'C')
    
    # Filter out excluded products
    mni_all = mni_all[~mni_all['bic'].isin(exclude_bics)].copy()
    
    # Summarize by DEPID, DEPGRP, CUSTYPE
    mni_sum = mni_all.groupby(['depid', 'depgrp', 'custype']).agg({
        'fd': 'sum', 'sa': 'sum', 'ca': 'sum', 'rnid': 'sum',
        'fd2': 'sum', 'sa2': 'sum', 'ca2': 'sum', 'rnid2': 'sum'
    }).reset_index()
    
    return mni_sum, mni_all

# =============================================================================
# EQUITY PROCESSING
# =============================================================================
def process_equity(rep_vars, excl_equ):
    """Process Equity source data for Islamic banking"""
    # Read EQU
    equ = read_sas7bdat(PATHS['LCR'] + f"equ{rep_vars['reptmon']}.sas7bdat")
    
    if equ.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # Filter and add EXCL flag
    equ = equ[equ['custno'].notna() & (equ['custno'] != '')].copy()
    equ['excl'] = 'N'
    if excl_equ:
        equ.loc[equ['custno'].astype(str).isin(excl_equ), 'excl'] = 'Y'
    
    # Read ICOF_EQU_DEPOSITOR_LIST
    cof = read_sas7bdat(PATHS['LIST'] + 'icof_equ_depositor_list.sas7bdat')
    
    # Merge by CUSTNO
    if not cof.empty:
        cof_equ = cof[['depid', 'depgrp', 'custno', 'linkid']].drop_duplicates(subset='custno').copy()
        equ1 = equ.merge(cof_equ, on='custno', how='left')
        equ_matched = equ1[equ1['depid'].notna() & (equ1['depid'] > 0)].copy()
        equ_unmatched = equ1[~(equ1['depid'].notna() & (equ1['depid'] > 0))].copy()
        equ_unmatched.drop(columns=['depid', 'depgrp', 'linkid'], inplace=True, errors='ignore')
    else:
        equ_matched = pd.DataFrame()
        equ_unmatched = equ.copy()
    
    # Assign new DEPID for unmatched
    equ2 = pd.DataFrame()
    if not equ_unmatched.empty:
        unique_cust = equ_unmatched[['custno']].drop_duplicates().sort_values('custno').copy()
        unique_cust['depid'] = range(50005001, 50005001 + len(unique_cust))
        
        equ2 = equ_unmatched.merge(unique_cust, on='custno', how='left')
        equ2['depgrp'] = equ2['custname'].fillna(equ2['custno'])
        equ2['linkid'] = np.nan
    
    # Combine all equity records
    dfs = [df for df in [equ_matched, equ2] if not df.empty]
    if dfs:
        equ_all = pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame(), pd.DataFrame()
    
    # Assign LINKID (handle missing)
    equ_all['linkid'] = np.where(
        equ_all['linkid'].isna(),
        np.where(equ_all['depid'].notna(), 50000000 + equ_all['depid'], equ_all['depid']),
        equ_all['linkid']
    )
    
    # Extract BIC code
    equ_all['bic'] = equ_all['cmmcode'].astype(str).str[:5]
    
    # Classify by product type
    std_bics = ['95830', '96830']
    nid_bics = ['95840', '96840']
    ibb_bics = ['95810', '96810']
    repo_bics = ['95820', '96820']
    
    equ_all['std'] = np.where(equ_all['bic'].isin(std_bics), equ_all['amount'], 0)
    equ_all['nid'] = np.where(equ_all['bic'].isin(nid_bics), equ_all['amount'], 0)
    equ_all['ibb'] = np.where(equ_all['bic'].isin(ibb_bics), equ_all['amount'], 0)
    equ_all['repo'] = np.where(equ_all['bic'].isin(repo_bics), equ_all['amount'], 0)
    
    # Excluded amounts
    equ_all['std2'] = np.where((equ_all['bic'].isin(std_bics)) & (equ_all['excl'] != 'Y'), equ_all['amount'], 0)
    equ_all['nid2'] = np.where((equ_all['bic'].isin(nid_bics)) & (equ_all['excl'] != 'Y'), equ_all['amount'], 0)
    
    # Customer type
    equ_all['custype'] = np.where(equ_all['custfiss'].astype(str).isin(['77', '78', '95', '96']), 'I', 'C')
    
    # Summarize by LINKID, DEPGRP, CUSTYPE
    equ_sum = equ_all.groupby(['linkid', 'depgrp', 'custype']).agg({
        'std': 'sum', 'nid': 'sum', 'ibb': 'sum', 'repo': 'sum',
        'std2': 'sum', 'nid2': 'sum'
    }).reset_index()
    equ_sum.rename(columns={'linkid': 'depid'}, inplace=True)
    
    return equ_sum, equ_all

# =============================================================================
# CONSOLIDATION
# =============================================================================
def consolidate_sources(mni_sum, equ_sum):
    """Consolidate M&I and Equity sources"""
    if mni_sum.empty and equ_sum.empty:
        return pd.DataFrame()
    
    # Merge by DEPID
    if not mni_sum.empty and not equ_sum.empty:
        allsrc = mni_sum.merge(equ_sum, on='depid', how='outer', suffixes=('_mni', '_equ'))
    elif not mni_sum.empty:
        allsrc = mni_sum.copy()
        for col in ['std', 'nid', 'ibb', 'repo', 'std2', 'nid2', 'custype_equ', 'depgrp_equ']:
            if col not in allsrc.columns:
                allsrc[col] = 0 if col not in ['custype_equ', 'depgrp_equ'] else ''
    else:
        allsrc = equ_sum.copy()
        for col in ['fd', 'sa', 'ca', 'rnid', 'fd2', 'sa2', 'ca2', 'rnid2', 'custype_mni', 'depgrp_mni']:
            if col not in allsrc.columns:
                allsrc[col] = 0 if col not in ['custype_mni', 'depgrp_mni'] else ''
    
    # Combine fields
    allsrc['depgrp'] = allsrc.get('depgrp_mni', allsrc.get('depgrp', '')).fillna(allsrc.get('depgrp_equ', ''))
    allsrc['custype'] = allsrc.get('custype_mni', allsrc.get('custype', '')).fillna(allsrc.get('custype_equ', ''))
    
    # Fill NaN with 0
    num_cols = ['fd', 'sa', 'ca', 'rnid', 'fd2', 'sa2', 'ca2', 'rnid2',
                'std', 'nid', 'ibb', 'repo', 'std2', 'nid2']
    for col in num_cols:
        if col in allsrc.columns:
            allsrc[col] = allsrc[col].fillna(0)
    
    # Calculate totals
    allsrc['nid_comb'] = allsrc['nid'] + allsrc['rnid']
    allsrc['tot'] = (allsrc['fd'] + allsrc['sa'] + allsrc['ca'] + 
                     allsrc['std'] + allsrc['nid_comb'] + allsrc['ibb'] + allsrc['repo'])
    allsrc['mni'] = allsrc['fd2'] + allsrc['sa2'] + allsrc['ca2'] + allsrc['rnid2']
    allsrc['equ'] = allsrc['std2'] + allsrc['nid2']
    allsrc['tot2'] = allsrc['mni'] + allsrc['equ']
    
    return allsrc

# =============================================================================
# REPORT GENERATION
# =============================================================================
def write_report_header(f, report_date):
    """Write report header"""
    f.write(f"PUBLIC ISLAMIC BANK BERHAD\n")
    f.write(f"TOP 50 LARGEST DEPOSITORS AS AT {report_date.strftime('%d/%m/%Y')}\n")
    f.write("\n")

def generate_top50_report(allsrc, cust_type, desc, rep_vars, output_file):
    """Generate Top 50 report for a customer type"""
    # Filter and take top 50
    top50 = allsrc[allsrc['custype'] == cust_type].nlargest(50, 'tot2').copy()
    top50['rank'] = range(1, len(top50) + 1)
    
    with open(output_file, 'w') as f:
        f.write(f"PUBLIC ISLAMIC BANK BERHAD\n")
        f.write(f"TOP 50 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}\n")
        f.write(f"\n")
        f.write(f"(i) Top 50 {desc} Depositors by Sources\n")
        f.write(f"\n")
        f.write(f"{'NOBS'}{DLM}{'DEPOSITORS'}{DLM}{'TOTAL BALANCE'}{DLM}{'M&I'}{DLM}{'EQUATION'}{DLM}\n")
        
        for _, row in top50.iterrows():
            depgrp = str(row['depgrp'])[:50] if pd.notna(row['depgrp']) else ''
            f.write(f"{row['rank']}{DLM}{depgrp}{DLM}"
                   f"{format_comma20_2(row['tot2'])}{DLM}"
                   f"{format_comma20_2(row['mni'])}{DLM}"
                   f"{format_comma20_2(row['equ'])}{DLM}\n")
        
        # Detail section
        f.write(f"\n\n")
        f.write(f"(ii) Detail Accounts Listing for Top 50 {desc} Depositors\n")
        f.write(f"\n")
    
    return top50

def generate_detail_listing(top50, mni_detail, equ_detail, output_file):
    """Generate detailed account listing"""
    with open(output_file, 'a') as f:
        for _, top in top50.iterrows():
            depid = top['depid']
            f.write(f"{top['rank']}{DLM}{top['depgrp']} ({depid}){DLM}\n")
            f.write(f"\n")
            
            # M&I section
            f.write(f"{DLM}Source: M&I\n")
            f.write(f"\n")
            f.write(f"{DLM}{'NO'}{DLM}{'BRANCH'}{DLM}{'ACCTNO'}{DLM}{'CUSTNAME'}{DLM}"
                   f"{'CUSTNO'}{DLM}{'BUSSREG'}{DLM}{'CUSTCD'}{DLM}{'PRODUCT'}{DLM}{'BALANCE'}{DLM}\n")
            
            if not mni_detail.empty:
                mni_det = mni_detail[(mni_detail['depid'] == depid) & 
                                     (mni_detail['amount'] > 0) & 
                                     (mni_detail['excl'] != 'Y')].sort_values('acctno')
                
                cnt = 0
                totbal = 0
                for _, row in mni_det.iterrows():
                    cnt += 1
                    totbal += row['amount']
                    branch = str(row.get('branch', ''))[:10]
                    acctno = str(row.get('acctno', ''))[:15]
                    custname = str(row.get('custname', ''))[:25]
                    custno = str(row.get('custno', ''))[:10]
                    newic = str(row.get('newic', ''))[:10]
                    custcd = str(row.get('custcd', ''))[:6]
                    product = str(row.get('product', ''))[:8]
                    
                    f.write(f"{DLM}{cnt}{DLM}{branch}{DLM}{acctno}{DLM}{custname}{DLM}"
                           f"{custno}{DLM}{newic}{DLM}{custcd}{DLM}{product}{DLM}"
                           f"{format_comma20_2(row['amount'])}{DLM}\n")
                
                if cnt > 0:
                    f.write(f"{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}"
                           f"{format_comma20_2(totbal)}{DLM}\n")
            
            f.write(f"\n")
            
            # Equity section
            f.write(f"{DLM}Source: EQU\n")
            f.write(f"\n")
            f.write(f"{DLM}{'NO'}{DLM}{'DEALREF'}{DLM}{'DEALTYPE'}{DLM}{'NAME'}{DLM}"
                   f"{'CUST MNEMONIC'}{DLM}{'AMOUNT'}{DLM}\n")
            
            if not equ_detail.empty:
                equ_det = equ_detail[(equ_detail['linkid'] == depid) & 
                                     (equ_detail['amount'] > 0) & 
                                     (equ_detail['excl'] != 'Y')]
                
                cnt = 0
                totbal = 0
                for _, row in equ_det.iterrows():
                    cnt += 1
                    totbal += row['amount']
                    
                    # Handle DEALREF and DEALTYPE fallbacks
                    dealref = str(row.get('dealref', row.get('gwdlr', row.get('utdlr', ''))))[:15]
                    dealtype = str(row.get('dealtype', row.get('gwdlp', row.get('utsty', ''))))[:10]
                    custname = str(row.get('custname', ''))[:25]
                    eqcustno = str(row.get('custno', ''))[:15]
                    
                    f.write(f"{DLM}{cnt}{DLM}{dealref}{DLM}{dealtype}{DLM}{custname}{DLM}"
                           f"{eqcustno}{DLM}{format_comma20_2(row['amount'])}{DLM}\n")
                
                if cnt > 0:
                    f.write(f"{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}"
                           f"{format_comma20_2(totbal)}{DLM}\n")
            
            f.write(f"\n")

# =============================================================================
# TOP 100 BY PRODUCT
# =============================================================================
def generate_top100_by_product(allsrc, rep_vars, output_file):
    """Generate Top 100 report by product"""
    top100 = allsrc.nlargest(100, 'tot').copy()
    top100['rank'] = range(1, len(top100) + 1)
    
    with open(output_file, 'w') as f:
        f.write(f"PUBLIC ISLAMIC BANK BERHAD\n")
        f.write(f"TOP 100 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}\n")
        f.write(f"\n")
        f.write(f"(i) Top 100 Depositors by Products\n")
        f.write(f"\n")
        f.write(f"{'NOBS'}{DLM}{'DEPOSITORS'}{DLM}{'TOTAL BALANCE'}{DLM}"
               f"{'MGIA/IA/TERM DEPOSIT-I'}{DLM}{'SAVINGS'}{DLM}{'DEMAND DEPOSIT'}{DLM}"
               f"{'SHORT TERM DEPOSIT'}{DLM}{'NID ISSUED'}{DLM}"
               f"{'INTERBANK BORROWING'}{DLM}{'REPOS'}{DLM}\n")
        
        for _, row in top100.iterrows():
            depgrp = str(row['depgrp'])[:50] if pd.notna(row['depgrp']) else ''
            nid_total = row.get('nid', 0) + row.get('rnid', 0)
            
            f.write(f"{row['rank']}{DLM}{depgrp}{DLM}"
                   f"{format_comma20_2(row['tot'])}{DLM}"
                   f"{format_comma20_2(row.get('fd', 0))}{DLM}"
                   f"{format_comma20_2(row.get('sa', 0))}{DLM}"
                   f"{format_comma20_2(row.get('ca', 0))}{DLM}"
                   f"{format_comma20_2(row.get('std', 0))}{DLM}"
                   f"{format_comma20_2(nid_total)}{DLM}"
                   f"{format_comma20_2(row.get('ibb', 0))}{DLM}"
                   f"{format_comma20_2(row.get('repo', 0))}{DLM}\n")
    
    # Save for maturity report
    top100.to_parquet(PATHS['OUTPUT'] + 'top100_temp.parquet')
    
    return top100

# =============================================================================
# MATURITY REPORT
# =============================================================================
def generate_maturity_report(top100, mni_detail, equ_detail, rep_vars, output_file):
    """Generate contractual maturity report"""
    with open(output_file, 'w') as f:
        f.write(f"PUBLIC ISLAMIC BANK BERHAD\n")
        f.write(f"TOP 100 DEPOSITORS BY CONTRACTUAL MATURITY AS AT {rep_vars['rdate']}\n")
        f.write(f"\n")
        f.write(f"(iii) Top 100 Depositors by Contractual Maturity\n")
        
        for _, top in top100.iterrows():
            rank = top['rank']
            depgrp = top['depgrp']
            depid = top['depid']
            
            f.write(f"\n")
            f.write(f"{rank}{DLM}{depgrp}\n")
            f.write(f"{DLM}{'DEPOSIT TYPE'}{DLM}{'UP TO 1 WEEK'}{DLM}"
                   f"{'> 1 WK - 1 MTH'}{DLM}{'> 1 - 3 MTHS'}{DLM}"
                   f"{'> 3 - 6 MTHS'}{DLM}{'> 6 MTHS - 1 YR'}{DLM}"
                   f"{'> 1 YEAR'}{DLM}{'NO SPECIFIC MATURITY'}{DLM}{'TOTAL'}{DLM}\n")
            
            # Aggregate M&I detail by BIC and maturity bucket
            if not mni_detail.empty:
                mni_det = mni_detail[(mni_detail['depid'] == depid) & (mni_detail['amount'] > 0)]
                
                if not mni_det.empty:
                    mni_det['bic'] = mni_det['cmmcode'].astype(str).str[:5]
                    mni_det['rem'] = mni_det['cmmcode'].astype(str).str[7:9] if 'cmmcode' in mni_det.columns else '07'
                    
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
                    
                    # Aggregate by item
                    item_agg = mni_det.groupby('item').agg({
                        'amount': 'sum', 'buc1': 'sum', 'buc2': 'sum',
                        'buc3': 'sum', 'buc4': 'sum', 'buc5': 'sum',
                        'buc6': 'sum', 'buc7': 'sum'
                    }).reset_index()
                    
                    # Write item lines
                    for _, item_row in item_agg.iterrows():
                        item_code = item_row['item']
                        desc = BIC_TAG.get(item_code.split('.')[0], 'UNKNOWN')
                        f.write(f"{DLM}{desc}{DLM}"
                               f"{format_comma20_2(item_row['buc1'])}{DLM}"
                               f"{format_comma20_2(item_row['buc2'])}{DLM}"
                               f"{format_comma20_2(item_row['buc3'])}{DLM}"
                               f"{format_comma20_2(item_row['buc4'])}{DLM}"
                               f"{format_comma20_2(item_row['buc5'])}{DLM}"
                               f"{format_comma20_2(item_row['buc6'])}{DLM}"
                               f"{format_comma20_2(item_row['buc7'])}{DLM}"
                               f"{format_comma20_2(item_row['amount'])}{DLM}\n")
    
    print(f"Maturity report written to {output_file}")

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIIMTLCR - Top Depositors Report (Islamic Banking)")
    print("=" * 60)
    
    # Get report date (yesterday)
    report_date = get_report_date()
    rep_vars = get_report_vars(report_date)
    print(f"\nReport Date: {rep_vars['rdate']}")
    
    # Get exclusion lists
    excl_cis, excl_equ = get_exclusion_lists()
    print(f"Exclusions: CIS={len(excl_cis)}, EQU={len(excl_equ)}")
    
    # Process M&I
    print("\nProcessing M&I...")
    mni_sum, mni_detail = process_mni(rep_vars, excl_cis)
    print(f"  M&I Summary: {len(mni_sum)} groups, Detail: {len(mni_detail)} records")
    
    # Process Equity
    print("Processing Equity...")
    equ_sum, equ_detail = process_equity(rep_vars, excl_equ)
    print(f"  Equity Summary: {len(equ_sum)} groups, Detail: {len(equ_detail)} records")
    
    # Consolidate
    print("\nConsolidating...")
    allsrc = consolidate_sources(mni_sum, equ_sum)
    print(f"  Consolidated: {len(allsrc)} groups")
    
    # Generate reports
    print("\nGenerating reports...")
    
    # Individual Top 50
    ind_file = PATHS['OUTPUT'] + 'COFOUTI.txt'
    ind_top = generate_top50_report(allsrc, 'I', 'Individual', rep_vars, ind_file)
    generate_detail_listing(ind_top, mni_detail, equ_detail, ind_file)
    print(f"  Individual report: {ind_file}")
    
    # Corporate Top 50
    corp_file = PATHS['OUTPUT'] + 'COFOUTC.txt'
    corp_top = generate_top50_report(allsrc, 'C', 'Corporate', rep_vars, corp_file)
    generate_detail_listing(corp_top, mni_detail, equ_detail, corp_file)
    print(f"  Corporate report: {corp_file}")
    
    # Top 100 by Product
    prod_file = PATHS['OUTPUT'] + 'COFOUT1.txt'
    top100 = generate_top100_by_product(allsrc, rep_vars, prod_file)
    print(f"  Top 100 by Product: {prod_file}")
    
    # Maturity report
    mat_file = PATHS['OUTPUT'] + 'COFOUT3.txt'
    generate_maturity_report(top100, mni_detail, equ_detail, rep_vars, mat_file)
    print(f"  Maturity report: {mat_file}")
    
    # Save consolidated data
    allsrc.to_parquet(PATHS['OUTPUT'] + 'alltot.parquet')
    
    print(f"\nAll reports written to {PATHS['OUTPUT']}")
    print("=" * 60)
    print("✓ EIIMTLCR Complete")

if __name__ == "__main__":
    main()
