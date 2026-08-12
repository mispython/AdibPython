"""
EIIMTLCR - Top Depositors Report (Islamic Banking)
Generates top depositor reports for Islamic Banking.
Matches SAS original logic exactly.
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

DLM = '\x05'

BIC_TAG = {
    '95315': 'A1.01', '95317': 'A1.02', '95312': 'A1.03',
    '95313': 'A1.04', '95810': 'A1.05', '95820': 'A1.06',
    '95830': 'A1.07', '95840': 'A1.08', '96317': 'B1.01',
    '96313': 'B1.02', '96810': 'B1.03', '96820': 'B1.04',
    '96830': 'B1.05', '96840': 'B1.06'
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def read_sas(filepath):
    """Read SAS dataset"""
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
    """Report date = yesterday"""
    d = datetime.now() - timedelta(days=1)
    return {
        'reptyear': d.strftime('%Y'),
        'reptmon': d.strftime('%m'),
        'reptday': d.strftime('%d'),
        'rptdt': d.strftime('%y%m%d'),
        'fildt': d.strftime('%d%m%y'),
        'rdate': d.strftime('%d/%m/%Y')
    }

def fmt(value):
    """COMMA20.2 format"""
    if value is None or pd.isna(value):
        return '0.00'
    try:
        return f"{float(value):,.2f}"
    except (ValueError, TypeError):
        return '0.00'

def s(value, max_len=None):
    """Safe string"""
    if value is None or pd.isna(value):
        return ''
    val = str(value).strip()
    return val[:max_len] if max_len else val

# =============================================================================
# EXCLUSIONS
# =============================================================================
def get_exclusions():
    """Get exclusion lists matching SAS PROC SQL"""
    excl_cis = []
    excl_equ = []
    
    # SAS: SELECT DISTINCT CUSTNO INTO :EXCLCIS FROM LIST.KEEP_TOP_DEP_EXCL_PIBB WHERE CUSTNO > 0
    df = read_sas(os.path.join(LIST_PATH, 'keep_top_dep_excl_pibb.sas7bdat'))
    if not df.empty and 'custno' in df.columns:
        excl_cis = [str(int(r)) for r in df[df['custno'] > 0]['custno'].drop_duplicates()]
    
    # SAS: SELECT COMPRESS(CAT("'",CUSTNO,"'")) INTO :EXCLEQU FROM LIST.KEEP_TOP_DEP_EXCL_EQU_PIBB WHERE CUSTNO NE ''
    df = read_sas(os.path.join(LIST_PATH, 'keep_top_dep_excl_equ_pibb.sas7bdat'))
    if not df.empty and 'custno' in df.columns:
        excl_equ = [s(r) for r in df[df['custno'].notna() & (df['custno'] != '')]['custno']]
    
    return excl_cis, excl_equ

# =============================================================================
# M&I PROCESSING - Matches SAS exactly
# =============================================================================
def process_mni(rep_vars, excl_cis):
    """
    SAS Logic:
    1. PROC SORT CMM by NEWIC
    2. PROC SORT COF by BUSSREG (keep DEPID DEPGRP BUSSREG, NODUPKEY)
    3. MERGE CMM(IN=A) COF(RENAME BUSSREG=NEWIC) by NEWIC; IF A;
    4. IF CUSTNO IN (&EXCLCIS) THEN EXCL='Y';
    5. IF DEPID > 0 THEN OUTPUT MNISRC1; ELSE OUTPUT XMNISRC;
    6. PROC SORT XMNISRC by CUSTNO
    7. PROC SORT COF by CUSTNO (keep DEPID DEPGRP CUSTNO, NODUPKEY)
    8. MERGE XMNISRC(IN=A) COF by CUSTNO; IF A;
    9. IF DEPID > 0 THEN OUTPUT MNISRC2; ELSE OUTPUT XMNISRC;
    10. For remaining XMNISRC: RETAIN DEPID; IF _N_=1 THEN DEPID=5000; 
        IF FIRST.CUSTNO THEN DEPID+1; DEPGRP=CUSTNAME;
    """
    print("\n--- M&I Processing ---")
    
    # Read CMM and sort by NEWIC (SAS step 1)
    cmm = read_sas(os.path.join(LCR_PATH, f"cmm{rep_vars['reptmon']}.sas7bdat"))
    if cmm.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    cmm = cmm.sort_values('newic').reset_index(drop=True)
    print(f"  CMM records: {len(cmm):,}")
    
    # Apply SAS exclusion: IF CUSTNO IN (&EXCLCIS) THEN EXCL='Y'
    cmm['excl'] = 'N'
    if excl_cis:
        # SAS does string comparison - convert CUSTNO to match
        mask = cmm['custno'].apply(lambda x: s(int(x)) if pd.notna(x) else '').isin(excl_cis)
        cmm.loc[mask, 'excl'] = 'Y'
    
    # Read COF and prepare for first merge (SAS step 2)
    # SAS: PROC SORT DATA=LIST.ICOF_MNI_DEPOSITOR_LIST OUT=COF_MNI_IDNO(KEEP=DEPID DEPGRP BUSSREG) NODUPKEY; BY BUSSREG;
    cof = read_sas(os.path.join(LIST_PATH, 'icof_mni_depositor_list.sas7bdat'))
    
    if cof.empty:
        print("  ERROR: COF file required")
        return pd.DataFrame(), pd.DataFrame()
    
    cof_idno = cof[['depid', 'depgrp', 'bussreg']].drop_duplicates(subset='bussreg').rename(columns={'bussreg': 'newic'})
    
    # SAS step 3: MERGE CMM(IN=A) COF(RENAME BUSSREG=NEWIC); BY NEWIC; IF A;
    mni1 = cmm.merge(cof_idno, on='newic', how='left')
    
    # SAS step 5: IF DEPID > 0 THEN OUTPUT MNISRC1; ELSE OUTPUT XMNISRC;
    mni1_matched = mni1[mni1['depid'].notna() & (mni1['depid'] > 0)].copy()
    mni1_unmatched = mni1[~(mni1['depid'].notna() & (mni1['depid'] > 0))].copy()
    mni1_unmatched.drop(columns=['depid', 'depgrp'], inplace=True)
    
    print(f"  First merge (by NEWIC): matched={len(mni1_matched):,}, unmatched={len(mni1_unmatched):,}")
    
    # SAS step 6-7: Sort unmatched by CUSTNO, prepare COF by CUSTNO
    # SAS: PROC SORT DATA=LIST.ICOF_MNI_DEPOSITOR_LIST OUT=COF_MNI_CUST(KEEP=DEPID DEPGRP CUSTNO) NODUPKEY; BY CUSTNO;
    cof_cust = cof[['depid', 'depgrp', 'custno']].drop_duplicates(subset='custno')
    
    # SAS step 8: MERGE XMNISRC(IN=A) COF_MNI_CUST; BY CUSTNO; IF A;
    mni2 = mni1_unmatched.merge(cof_cust, on='custno', how='left')
    
    # SAS step 9: IF DEPID > 0 THEN OUTPUT MNISRC2; ELSE OUTPUT XMNISRC;
    mni2_matched = mni2[mni2['depid'].notna() & (mni2['depid'] > 0)].copy()
    mni2_unmatched = mni2[~(mni2['depid'].notna() & (mni2['depid'] > 0))].copy()
    mni2_unmatched.drop(columns=['depid', 'depgrp'], inplace=True)
    
    print(f"  Second merge (by CUSTNO): matched={len(mni2_matched):,}, unmatched={len(mni2_unmatched):,}")
    
    # SAS step 10: Assign sequential DEPID for remaining unmatched
    # SAS: RETAIN DEPID; IF _N_=1 THEN DEPID=5000; IF FIRST.CUSTNO THEN DEPID+1; DEPGRP=CUSTNAME;
    mni3 = pd.DataFrame()
    if not mni2_unmatched.empty:
        mni3 = mni2_unmatched.copy()
        # Sort by CUSTNO to match SAS BY CUSTNO behavior
        mni3 = mni3.sort_values('custno')
        # Assign sequential DEPID starting from 5001
        unique_cust = mni3['custno'].drop_duplicates()
        depid_map = {cust: 5001 + i for i, cust in enumerate(unique_cust)}
        mni3['depid'] = mni3['custno'].map(depid_map)
        mni3['depgrp'] = mni3['custname'].fillna('').astype(str)
        print(f"  New DEPIDs assigned: {len(depid_map):,}")
    
    # SAS: DATA MNISRC; SET MNISRC1 MNISRC2 XMNISRC;
    dfs = [df for df in [mni1_matched, mni2_matched, mni3] if not df.empty]
    mni_all = pd.concat(dfs, ignore_index=True)
    print(f"  Total M&I records: {len(mni_all):,}")
    
    # Clean up
    del cmm, cof, cof_idno, cof_cust, mni1, mni2, mni3
    del mni1_matched, mni1_unmatched, mni2_matched, mni2_unmatched
    gc.collect()
    
    # SAS: BIC = SUBSTR(CMMCODE,1,5);
    mni_all['bic'] = mni_all['cmmcode'].astype(str).str[:5]
    mni_all['amount'] = mni_all['amount'].fillna(0).astype(float)
    
    # SAS SELECT block for product classification
    # SAS: WHEN('95315','96315','95317','96317') FD = AMOUNT;
    mni_all['fd'] = np.where(mni_all['bic'].isin(['95315', '96315', '95317', '96317']), mni_all['amount'], 0.0)
    # SAS: WHEN('95312') SA = AMOUNT;
    mni_all['sa'] = np.where(mni_all['bic'] == '95312', mni_all['amount'], 0.0)
    # SAS: WHEN('95313','96313') CA = AMOUNT;
    mni_all['ca'] = np.where(mni_all['bic'].isin(['95313', '96313']), mni_all['amount'], 0.0)
    # SAS: WHEN('95840','96840') RNID = AMOUNT;
    mni_all['rnid'] = np.where(mni_all['bic'].isin(['95840', '96840']), mni_all['amount'], 0.0)
    
    # SAS: IF EXCL NE 'Y' THEN DO; FD2=FD; SA2=SA; CA2=CA; RNID2=RNID; END;
    not_excl = mni_all['excl'] != 'Y'
    mni_all['fd2'] = np.where(not_excl, mni_all['fd'], 0.0)
    mni_all['sa2'] = np.where(not_excl, mni_all['sa'], 0.0)
    mni_all['ca2'] = np.where(not_excl, mni_all['ca'], 0.0)
    mni_all['rnid2'] = np.where(not_excl, mni_all['rnid'], 0.0)
    
    # SAS: IF CUSTCD IN ('77','78','95','96') THEN CUSTYPE='I'; ELSE CUSTYPE='C';
    mni_all['custype'] = np.where(
        mni_all['custcd'].astype(str).str.strip().isin(['77', '78', '95', '96']), 'I', 'C'
    )
    
    # SAS: OTHERWISE DELETE; (implicit filter in DATA step)
    # Note: SAS keeps records that match WHEN conditions, others are deleted
    mni_all = mni_all[
        mni_all['bic'].isin(['95315', '96315', '95317', '96317', '95312', '95313', '96313', '95840', '96840'])
    ].copy()
    
    # SAS: PROC SUMMARY by DEPID DEPGRP CUSTYPE; VAR FD SA CA RNID FD2 SA2 CA2 RNID2; OUTPUT SUM=;
    agg_cols = ['fd', 'sa', 'ca', 'rnid', 'fd2', 'sa2', 'ca2', 'rnid2']
    mni_sum = mni_all.groupby(['depid', 'depgrp', 'custype'], as_index=False)[agg_cols].sum()
    
    print(f"  M&I groups: {len(mni_sum):,}")
    return mni_sum, mni_all

# =============================================================================
# EQUITY PROCESSING - Matches SAS exactly
# =============================================================================
def process_equity(rep_vars, excl_equ):
    """
    SAS Logic:
    1. DATA CMM; SET LCR.EQU; WHERE CUSTNO NE ''; 
    2. IF CUSTNO IN (&EXCLEQU) THEN EXCL='Y';
    3. PROC SORT by CUSTNO
    4. PROC SORT COF by CUSTNO (keep DEPID DEPGRP CUSTNO LINKID, NODUPKEY)
    5. MERGE CMM(IN=A) COF_EQU_CUST; BY CUSTNO; IF A;
    6. IF DEPID > 0 THEN OUTPUT EQUSRC; ELSE OUTPUT XEQUSRC;
    7. For XEQUSRC: RETAIN DEPID; IF _N_=1 THEN DEPID=50005000;
       IF FIRST.CUSTNO THEN DEPID+1; DEPGRP=CUSTNAME (or CUSTNO);
    8. SET EQUSRC(IN=A) XEQUSRC;
    9. BIC classification; IF A & LINKID=. THEN LINKID=50000000+DEPID;
       IF LINKID=. THEN LINKID=DEPID;
    """
    print("\n--- Equity Processing ---")
    
    # SAS step 1: DATA CMM; SET LCR.EQU; WHERE CUSTNO NE '';
    equ = read_sas(os.path.join(LCR_PATH, f"equ{rep_vars['reptmon']}.sas7bdat"))
    if equ.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    equ = equ[equ['custno'].notna() & (equ['custno'].astype(str).str.strip() != '')].copy()
    
    # SAS step 2: IF CUSTNO IN (&EXCLEQU) THEN EXCL='Y';
    equ['excl'] = 'N'
    if excl_equ:
        equ.loc[equ['custno'].astype(str).str.strip().isin(excl_equ), 'excl'] = 'Y'
    
    # SAS step 3: PROC SORT; BY CUSTNO;
    equ = equ.sort_values('custno').reset_index(drop=True)
    print(f"  EQU records: {len(equ):,}")
    
    # SAS step 4: PROC SORT COF by CUSTNO (keep DEPID DEPGRP CUSTNO LINKID, NODUPKEY)
    cof = read_sas(os.path.join(LIST_PATH, 'icof_equ_depositor_list.sas7bdat'))
    
    if cof.empty:
        print("  WARNING: No EQU COF, assigning new DEPIDs")
        # All unmatched - assign sequential DEPIDs
        unique_cust = equ['custno'].drop_duplicates().sort_values()
        depid_map = {cust: 50005001 + i for i, cust in enumerate(unique_cust)}
        equ['depid'] = equ['custno'].map(depid_map)
        equ['depgrp'] = equ['custname'].fillna(equ['custno']).astype(str)
        equ['linkid'] = np.nan
    else:
        cof_cust = cof[['depid', 'depgrp', 'custno', 'linkid']].drop_duplicates(subset='custno')
        
        # SAS step 5: MERGE CMM(IN=A) COF_EQU_CUST; BY CUSTNO; IF A;
        equ1 = equ.merge(cof_cust, on='custno', how='left')
        
        # SAS step 6: IF DEPID > 0 THEN OUTPUT EQUSRC; ELSE OUTPUT XEQUSRC;
        equ_matched = equ1[equ1['depid'].notna() & (equ1['depid'] > 0)].copy()
        equ_unmatched = equ1[~(equ1['depid'].notna() & (equ1['depid'] > 0))].copy()
        equ_unmatched.drop(columns=['depid', 'depgrp', 'linkid'], inplace=True)
        
        print(f"  Merge by CUSTNO: matched={len(equ_matched):,}, unmatched={len(equ_unmatched):,}")
        
        # SAS step 7: Assign DEPID for unmatched
        # SAS: RETAIN DEPID; IF _N_=1 THEN DEPID=50005000; IF FIRST.CUSTNO THEN DEPID+1;
        equ2 = pd.DataFrame()
        if not equ_unmatched.empty:
            equ2 = equ_unmatched.copy()
            equ2 = equ2.sort_values('custno')
            unique_cust = equ2['custno'].drop_duplicates()
            depid_map = {cust: 50005001 + i for i, cust in enumerate(unique_cust)}
            equ2['depid'] = equ2['custno'].map(depid_map)
            # SAS: IF DEPGRP = '' THEN DEPGRP = CUSTNAME; IF DEPGRP = '' THEN DEPGRP = CUSTNO;
            equ2['depgrp'] = equ2['custname'].fillna('')
            equ2.loc[equ2['depgrp'] == '', 'depgrp'] = equ2['custno'].astype(str)
            equ2['linkid'] = np.nan
            print(f"  New DEPIDs: {len(depid_map):,}")
        
        # SAS step 8: DATA EQUSRC; SET EQUSRC(IN=A) XEQUSRC;
        dfs = [df for df in [equ_matched, equ2] if not df.empty]
        equ = pd.concat(dfs, ignore_index=True)
        
        del equ1, equ_matched, equ_unmatched, equ2, cof_cust
    
    del cof
    gc.collect()
    
    # SAS: BIC = SUBSTR(CMMCODE,1,5);
    equ['bic'] = equ['cmmcode'].astype(str).str[:5]
    equ['amount'] = equ['amount'].fillna(0).astype(float)
    
    # SAS SELECT block
    # SAS: WHEN('95830','96830') STD = AMOUNT;
    equ['std'] = np.where(equ['bic'].isin(['95830', '96830']), equ['amount'], 0.0)
    # SAS: WHEN('95840','96840') NID = AMOUNT;
    equ['nid'] = np.where(equ['bic'].isin(['95840', '96840']), equ['amount'], 0.0)
    # SAS: WHEN('95810','96810') IBB = AMOUNT;
    equ['ibb'] = np.where(equ['bic'].isin(['95810', '96810']), equ['amount'], 0.0)
    # SAS: WHEN('95820','96820') REPO = AMOUNT;
    equ['repo'] = np.where(equ['bic'].isin(['95820', '96820']), equ['amount'], 0.0)
    
    # SAS: IF EXCL NE 'Y' THEN DO; STD2=STD; NID2=NID; END;
    not_excl = equ['excl'] != 'Y'
    equ['std2'] = np.where(not_excl, equ['std'], 0.0)
    equ['nid2'] = np.where(not_excl, equ['nid'], 0.0)
    
    # SAS: IF A & LINKID = . THEN LINKID = 50000000+DEPID;
    # SAS: IF LINKID = . THEN LINKID = DEPID;
    # (A flag is True for matched records, but we apply to all)
    equ['linkid'] = equ['linkid'].fillna(0)
    equ.loc[equ['linkid'] == 0, 'linkid'] = 50000000 + equ['depid']
    equ.loc[equ['linkid'].isna(), 'linkid'] = equ['depid']
    
    # SAS: IF CUSTFISS IN ('77','78','95','96') THEN CUSTYPE='I'; ELSE CUSTYPE='C';
    equ['custype'] = np.where(
        equ['custfiss'].astype(str).str.strip().isin(['77', '78', '95', '96']), 'I', 'C'
    )
    
    # SAS: OTHERWISE DELETE;
    equ = equ[equ['bic'].isin(['95830', '96830', '95840', '96840', '95810', '96810', '95820', '96820'])].copy()
    
    # SAS: PROC SUMMARY by LINKID DEPGRP CUSTYPE
    agg_cols = ['std', 'nid', 'ibb', 'repo', 'std2', 'nid2']
    equ_sum = equ.groupby(['linkid', 'depgrp', 'custype'], as_index=False)[agg_cols].sum()
    equ_sum.rename(columns={'linkid': 'depid'}, inplace=True)
    
    print(f"  Equity groups: {len(equ_sum):,}")
    return equ_sum, equ

# =============================================================================
# CONSOLIDATION - Matches SAS DATA ALLSRC; MERGE LIST.MNITOT LIST.EQUTOT
# =============================================================================
def consolidate(mni_sum, equ_sum):
    """
    SAS: DATA ALLSRC; MERGE LIST.MNITOT LIST.EQUTOT(RENAME=(LINKID=DEPID CUSTYPE=CUSTYPEQ DEPGRP=DEPGRPEQ)); BY DEPID;
    """
    print("\n--- Consolidating ---")
    
    if mni_sum.empty and equ_sum.empty:
        return pd.DataFrame()
    
    # Rename equity columns to match SAS RENAME
    if not equ_sum.empty:
        equ_renamed = equ_sum.rename(columns={'custype': 'custypeq', 'depgrp': 'depgrpeq'})
    else:
        equ_renamed = pd.DataFrame()
    
    # SAS: MERGE by DEPID
    if not mni_sum.empty and not equ_renamed.empty:
        allsrc = mni_sum.merge(equ_renamed, on='depid', how='outer')
    elif not mni_sum.empty:
        allsrc = mni_sum.copy()
    else:
        allsrc = equ_renamed.copy()
    
    # SAS: IF DEPGRP = '' THEN DEPGRP = DEPGRPEQ;
    if 'depgrp' in allsrc.columns and 'depgrpeq' in allsrc.columns:
        allsrc['depgrp'] = allsrc['depgrp'].fillna('')
        mask = (allsrc['depgrp'] == '') & allsrc['depgrpeq'].notna()
        allsrc.loc[mask, 'depgrp'] = allsrc.loc[mask, 'depgrpeq']
    elif 'depgrpeq' in allsrc.columns:
        allsrc['depgrp'] = allsrc['depgrpeq'].fillna('')
    elif 'depgrp' not in allsrc.columns:
        allsrc['depgrp'] = ''
    
    # SAS: IF CUSTYPE= '' THEN CUSTYPE= CUSTYPEQ;
    if 'custype' in allsrc.columns and 'custypeq' in allsrc.columns:
        allsrc['custype'] = allsrc['custype'].fillna('')
        mask = (allsrc['custype'] == '') & allsrc['custypeq'].notna()
        allsrc.loc[mask, 'custype'] = allsrc.loc[mask, 'custypeq']
    elif 'custypeq' in allsrc.columns:
        allsrc['custype'] = allsrc['custypeq'].fillna('')
    elif 'custype' not in allsrc.columns:
        allsrc['custype'] = ''
    
    # Fill missing numeric columns
    for col in ['fd', 'sa', 'ca', 'rnid', 'fd2', 'sa2', 'ca2', 'rnid2',
                'std', 'nid', 'ibb', 'repo', 'std2', 'nid2']:
        if col not in allsrc.columns:
            allsrc[col] = 0.0
        allsrc[col] = allsrc[col].fillna(0.0)
    
    # SAS: NID = SUM(NID,RNID);  *18-3852;
    allsrc['nid'] = allsrc['nid'] + allsrc['rnid']
    
    # SAS: TOT = SUM(FD,SA,CA,STD,NID,IBB,REPO);
    allsrc['tot'] = (allsrc['fd'] + allsrc['sa'] + allsrc['ca'] + 
                     allsrc['std'] + allsrc['nid'] + allsrc['ibb'] + allsrc['repo'])
    
    # SAS: *19-3762; MNI = SUM(FD2,SA2,CA2,RNID2); EQU = SUM(STD2,NID2);
    allsrc['mni'] = allsrc['fd2'] + allsrc['sa2'] + allsrc['ca2'] + allsrc['rnid2']
    allsrc['equ'] = allsrc['std2'] + allsrc['nid2']
    
    # SAS: TOT2= SUM(MNI,EQU);
    allsrc['tot2'] = allsrc['mni'] + allsrc['equ']
    
    print(f"  Consolidated groups: {len(allsrc):,}")
    return allsrc

# =============================================================================
# REPORT WRITERS
# =============================================================================
def write_top50(allsrc, cust_type, desc, rep_vars, filepath):
    """SAS: %MACRO REPORT(CTYPE,DESC); DATA ALLTOT; SET ALLTOT2(WHERE=(CUSTYPE="&CTYPE") OBS=50);"""
    filtered = allsrc[allsrc['custype'] == cust_type]
    if filtered.empty:
        print(f"  No {desc} data")
        return pd.DataFrame()
    
    # SAS: PROC SORT DATA=ALLTOT2; BY CUSTYPE DESCENDING TOT2; (top 50)
    top50 = filtered.nlargest(50, 'tot2').reset_index(drop=True)
    top50['rank'] = range(1, len(top50) + 1)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        # SAS header
        f.write("PUBLIC ISLAMIC BANK BERHAD\n")
        f.write(f"TOP 50 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}\n\n")
        f.write(f"(i) Top 50 {desc} Depositors by Sources\n\n")
        f.write(f"NOBS{DLM}DEPOSITORS{DLM}TOTAL BALANCE{DLM}M&I{DLM}EQUATION{DLM}\n")
        
        for _, row in top50.iterrows():
            f.write(f"{row['rank']}{DLM}{s(row['depgrp'], 50)}{DLM}"
                   f"{fmt(row['tot2'])}{DLM}{fmt(row['mni'])}{DLM}{fmt(row['equ'])}{DLM}\n")
    
    print(f"  {os.path.basename(filepath)}: {os.path.getsize(filepath):,} bytes")
    return top50

def write_detail(top50, mni_detail, equ_detail, rep_vars, filepath):
    """SAS: (ii) Detail Accounts Listing for Top 50 Depositors"""
    if top50.empty:
        return
    
    with open(filepath, 'a', encoding='utf-8') as f:
        # SAS header for detail section
        f.write(f"\n\n(ii) Detail Accounts Listing for Top 50 Depositors\n\n")
        
        for idx, (_, row) in enumerate(top50.iterrows()):
            if idx % 25 == 0:
                print(f"  Detail {idx+1}/{len(top50)}")
            
            depid = row['depid']
            
            # SAS: PUT RANK DEPGRP '(' DEPID ')'
            f.write(f"{row['rank']}{DLM}{s(row['depgrp'], 50)} ({depid}){DLM}\n")
            
            # SAS: Source: M&I
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
            
            # SAS: Source: EQU
            f.write(f"\n{DLM}Source: EQU\n")
            f.write(f"{DLM}NO{DLM}DEALREF{DLM}DEALTYPE{DLM}NAME{DLM}"
                   f"CUST MNEMONIC{DLM}AMOUNT{DLM}\n")
            
            if not equ_detail.empty:
                det = equ_detail[(equ_detail['linkid'] == depid) & (equ_detail['amount'] > 0)]
                cnt, total = 0, 0
                for _, r in det.iterrows():
                    cnt += 1
                    total += r['amount']
                    # SAS: IF DEALREF='' THEN DEALREF=GWDLR; IF DEALREF='' THEN DEALREF=UTDLR;
                    dealref = r.get('dealref') or r.get('gwdlr') or r.get('utdlr') or ''
                    dealtype = r.get('dealtype') or r.get('gwdlp') or r.get('utsty') or ''
                    f.write(f"{DLM}{cnt}{DLM}{s(dealref, 15)}{DLM}{s(dealtype, 10)}{DLM}"
                           f"{s(r.get('custname'), 25)}{DLM}{s(r.get('custno'), 15)}{DLM}"
                           f"{fmt(r['amount'])}{DLM}\n")
                if cnt > 0:
                    f.write(f"{DLM}{DLM}{DLM}{DLM}{DLM}{DLM}{fmt(total)}{DLM}\n")
            
            f.write(f"\n")

def write_top100(allsrc, rep_vars, filepath):
    """SAS: PROC SUMMARY DATA=ALLSRC NWAY; BY DEPID DEPGRP; VAR TOT FD SA CA STD NID IBB REPO;"""
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
            f.write(f"{row['rank']}{DLM}{s(row['depgrp'], 50)}{DLM}"
                   f"{fmt(row['tot'])}{DLM}{fmt(row.get('fd', 0))}{DLM}"
                   f"{fmt(row.get('sa', 0))}{DLM}{fmt(row.get('ca', 0))}{DLM}"
                   f"{fmt(row.get('std', 0))}{DLM}{fmt(row.get('nid', 0))}{DLM}"
                   f"{fmt(row.get('ibb', 0))}{DLM}{fmt(row.get('repo', 0))}{DLM}\n")
    
    print(f"  {os.path.basename(filepath)}: {os.path.getsize(filepath):,} bytes")
    return top100

def write_maturity(top100, mni_detail, rep_vars, filepath):
    """SAS: (iii) Top 100 Depositors by Contractual Maturity"""
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
                    # SAS: BIC = SUBSTR(CMMCODE,1,5); REM = SUBSTR(CMMCODE,8,2);
                    det['bic'] = det['cmmcode'].astype(str).str[:5]
                    det['rem'] = det['cmmcode'].astype(str).str[7:9]
                    
                    # SAS: IF BIC IN ('95312','95313','96313') THEN REM = '07';
                    det.loc[det['bic'].isin(['95312', '95313', '96313']), 'rem'] = '07'
                    
                    # SAS: SELECT(REM); WHEN(01) BUC1=AMOUNT; etc.
                    for i in range(1, 8):
                        det[f'buc{i}'] = np.where(det['rem'] == f'0{i}', det['amount'], 0.0)
                    
                    # SAS: ITEM = PUT(BIC,$BICTAG.); IF ITEM NE '';
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
    
    print(f"  {os.path.basename(filepath)}: {os.path.getsize(filepath):,} bytes")

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("=" * 60)
    print("EIIMTLCR - Top Depositors Report (Islamic Banking)")
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    rep_vars = get_report_vars()
    print(f"Report Date: {rep_vars['rdate']}")
    
    excl_cis, excl_equ = get_exclusions()
    print(f"Exclusions: CIS={len(excl_cis)}, EQU={len(excl_equ)}")
    
    mni_sum, mni_detail = process_mni(rep_vars, excl_cis)
    equ_sum, equ_detail = process_equity(rep_vars, excl_equ)
    allsrc = consolidate(mni_sum, equ_sum)
    
    if allsrc.empty:
        print("ERROR: No data")
        return
    
    print("\n--- Generating Reports ---")
    
    # SAS: %REPORT(I,Individual);
    ind_file = os.path.join(OUTPUT_PATH, 'COFOUTI.txt')
    ind_top = write_top50(allsrc, 'I', 'Individual', rep_vars, ind_file)
    if not ind_top.empty:
        write_detail(ind_top, mni_detail, equ_detail, rep_vars, ind_file)
    
    # SAS: %REPORT(C,Corporate);
    corp_file = os.path.join(OUTPUT_PATH, 'COFOUTC.txt')
    corp_top = write_top50(allsrc, 'C', 'Corporate', rep_vars, corp_file)
    if not corp_top.empty:
        write_detail(corp_top, mni_detail, equ_detail, rep_vars, corp_file)
    
    # SAS: Top 100 by Products
    prod_file = os.path.join(OUTPUT_PATH, 'COFOUT1.txt')
    top100 = write_top100(allsrc, rep_vars, prod_file)
    
    detail100_file = os.path.join(OUTPUT_PATH, 'COFOUT2.txt')
    if not top100.empty:
        write_detail(top100, mni_detail, equ_detail, rep_vars, detail100_file)
    
    # SAS: Top 100 by Maturity
    mat_file = os.path.join(OUTPUT_PATH, 'COFOUT3.txt')
    write_maturity(top100, mni_detail, rep_vars, mat_file)
    
    del allsrc, mni_sum, mni_detail, equ_sum, equ_detail
    gc.collect()
    
    print("\nOutput:")
    for f in ['COFOUTI.txt', 'COFOUTC.txt', 'COFOUT1.txt', 'COFOUT2.txt', 'COFOUT3.txt']:
        path = os.path.join(OUTPUT_PATH, f)
        if os.path.exists(path):
            print(f"  {f}: {os.path.getsize(path):,} bytes")
    
    print(f"\nEnd: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("✓ Complete")

if __name__ == "__main__":
    main()
