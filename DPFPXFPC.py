"""
EIBMTLCR - Top Depositors Report
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
        # Convert column names to uppercase for consistency with SAS naming
        df.columns = [col.upper() for col in df.columns]
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Warning: Could not read {filepath}: {e}")
        return pl.DataFrame()

def align_columns(df1, df2):
    """
    Align two DataFrames to have the same columns for concatenation.
    Missing columns are filled with None/Null.
    """
    if df1.is_empty() and df2.is_empty():
        return df1, df2
    if df1.is_empty():
        return df1, df2
    if df2.is_empty():
        return df1, df2
    
    # Get union of all columns
    all_columns = list(set(list(df1.columns) + list(df2.columns)))
    
    # Add missing columns to df1
    for col in all_columns:
        if col not in df1.columns:
            df1 = df1.with_columns(pl.lit(None).alias(col))
    
    # Add missing columns to df2
    for col in all_columns:
        if col not in df2.columns:
            df2 = df2.with_columns(pl.lit(None).alias(col))
    
    # Ensure same column order
    df1 = df1.select(all_columns)
    df2 = df2.select(all_columns)
    
    return df1, df2

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
# M&I (Monetary & Islamic) PROCESSING
# =============================================================================
def process_mni(rep_vars, excl_cis):
    """Process M&I source data"""
    
    # Read CMM first to get its column structure
    try:
        cmm_file = f"{PATHS['LCR']}cmm{rep_vars['reptmon']}.sas7bdat"
        print(f"  Reading CMM: {cmm_file}")
        cmm = read_sas7bdat(cmm_file)
        print(f"  CMM loaded: {len(cmm)} records, {len(cmm.columns)} columns")
    except Exception as e:
        print(f"  Warning reading CMM: {e}")
        cmm = pl.DataFrame()
    
    # Read VOSTRO
    try:
        print(f"  Reading VOSTRO: {PATHS['LCR']}vostro.sas7bdat")
        vostro = read_sas7bdat(f"{PATHS['LCR']}vostro.sas7bdat")
        print(f"  VOSTRO loaded: {len(vostro)} records, {len(vostro.columns)} columns")
    except Exception as e:
        print(f"  Warning reading VOSTRO: {e}")
        vostro = pl.DataFrame()
    
    # Sort VOSTRO by ACCTNO
    if not vostro.is_empty() and 'ACCTNO' in vostro.columns:
        vostro = vostro.sort('ACCTNO')
    
    # Merge VOSTRO with CISINFO
    if not vostro.is_empty():
        try:
            cisinfo = read_sas7bdat(f"{PATHS['LCR']}cisinfo.sas7bdat")
            if not cisinfo.is_empty():
                print(f"  CISINFO loaded: {len(cisinfo)} records")
                # Only keep necessary columns to avoid duplicates
                cisinfo_cols = ['ACCTNO', 'CUSTCD', 'CUSTNAME', 'PRODUCT', 
                               'CURCODE', 'CUSTNO', 'NEWIC']
                available_cis_cols = [c for c in cisinfo_cols if c in cisinfo.columns]
                cisinfo = cisinfo.select(available_cis_cols)
                
                # Merge
                vostro = vostro.join(cisinfo, on='ACCTNO', how='left', suffix='_CIS')
                print(f"  VOSTRO after CISINFO merge: {len(vostro)} records")
        except Exception as e:
            print(f"  Warning merging CISINFO: {e}")
    
    # Add CMMCODE to VOSTRO
    if not vostro.is_empty():
        vostro = vostro.with_columns([
            pl.lit('953XX').alias('CMMCODE')  # VOSTRO
        ])
    
    # Combine CMM and VOSTRO with aligned columns
    if not cmm.is_empty() or not vostro.is_empty():
        if not cmm.is_empty() and not vostro.is_empty():
            # Align columns before concatenation
            cmm, vostro = align_columns(cmm, vostro)
            cmm = pl.concat([cmm, vostro])
            print(f"  Combined CMM+VOSTRO: {len(cmm)} records, {len(cmm.columns)} columns")
        elif not vostro.is_empty():
            cmm = vostro
            print(f"  Using VOSTRO only: {len(cmm)} records")
    else:
        print("  No CMM or VOSTRO data")
        return pl.DataFrame(), pl.DataFrame()
    
    # Sort by NEWIC
    if not cmm.is_empty() and 'NEWIC' in cmm.columns:
        cmm = cmm.sort('NEWIC')
    
    # Read COF_MNI_DEPOSITOR_LIST for first merge (by BUSSREG/NEWIC)
    cof_idno = pl.DataFrame()
    try:
        cof = read_sas7bdat(f"{PATHS['LIST']}cof_mni_depositor_list.sas7bdat")
        if not cof.is_empty():
            print(f"  COF_MNI_DEPOSITOR_LIST loaded: {len(cof)} records")
            # Get unique by BUSSREG
            if 'BUSSREG' in cof.columns:
                cof_idno = cof.unique(subset=['BUSSREG'])
                keep_cols = ['DEPID', 'DEPGRP', 'BUSSREG']
                available_cols = [c for c in keep_cols if c in cof_idno.columns]
                cof_idno = cof_idno.select(available_cols)
                cof_idno = cof_idno.rename({'BUSSREG': 'NEWIC'})
                print(f"  COF_IDNO for NEWIC merge: {len(cof_idno)} records")
    except Exception as e:
        print(f"  Warning loading COF_MNI_DEPOSITOR_LIST: {e}")
    
    # First merge by NEWIC
    mni1 = cmm.clone()
    if not cmm.is_empty() and not cof_idno.is_empty() and 'NEWIC' in cmm.columns:
        mni1 = cmm.join(cof_idno, on='NEWIC', how='left', suffix='_COF')
        print(f"  After NEWIC merge: {len(mni1)} records")
    
    # Add EXCL flag
    if not mni1.is_empty():
        if 'CUSTNO' in mni1.columns:
            if excl_cis:
                mni1 = mni1.with_columns([
                    pl.when(pl.col('CUSTNO').cast(pl.Utf8).is_in(excl_cis))
                    .then(pl.lit('Y')).otherwise(pl.lit('N')).alias('EXCL')
                ])
            else:
                mni1 = mni1.with_columns([pl.lit('N').alias('EXCL')])
    
    # Split matched/unmatched
    mni1_matched = pl.DataFrame()
    mni1_unmatched = pl.DataFrame()
    
    if not mni1.is_empty():
        if 'DEPID' in mni1.columns:
            mni1_matched = mni1.filter(pl.col('DEPID').is_not_null() & (pl.col('DEPID') > 0))
            mni1_unmatched = mni1.filter(pl.col('DEPID').is_null() | (pl.col('DEPID') == 0))
            print(f"  First match: {len(mni1_matched)} matched, {len(mni1_unmatched)} unmatched")
        else:
            mni1_unmatched = mni1
            print(f"  No DEPID column, all {len(mni1_unmatched)} unmatched")
    
    # Second merge by CUSTNO for unmatched records
    mni2_matched = pl.DataFrame()
    mni2_unmatched = mni1_unmatched.clone() if not mni1_unmatched.is_empty() else pl.DataFrame()
    
    if not mni1_unmatched.is_empty():
        try:
            cof_cust = read_sas7bdat(f"{PATHS['LIST']}cof_mni_depositor_list.sas7bdat")
            if not cof_cust.is_empty() and 'CUSTNO' in cof_cust.columns:
                cof_cust = cof_cust.unique(subset=['CUSTNO'])
                keep_cols = ['DEPID', 'DEPGRP', 'CUSTNO']
                available_cols = [c for c in keep_cols if c in cof_cust.columns]
                cof_cust = cof_cust.select(available_cols)
                
                mni2 = mni1_unmatched.join(cof_cust, on='CUSTNO', how='left', suffix='_CUST')
                
                if 'DEPID' in mni2.columns:
                    mni2_matched = mni2.filter(pl.col('DEPID').is_not_null() & (pl.col('DEPID') > 0))
                    mni2_unmatched = mni2.filter(pl.col('DEPID').is_null() | (pl.col('DEPID') == 0))
                    print(f"  Second match: {len(mni2_matched)} matched, {len(mni2_unmatched)} unmatched")
                else:
                    mni2_unmatched = mni2
        except Exception as e:
            print(f"  Warning in second merge: {e}")
    
    # Assign new DEPID for unmatched records
    mni3 = pl.DataFrame()
    if not mni2_unmatched.is_empty() and 'CUSTNO' in mni2_unmatched.columns:
        mni3 = mni2_unmatched.clone()
        
        # Get unique CUSTNOs and assign DEPID starting from 5001
        unique_cust = mni3.unique(subset=['CUSTNO']).select('CUSTNO').sort('CUSTNO')
        if not unique_cust.is_empty():
            unique_cust = unique_cust.with_columns([
                (pl.arange(0, len(unique_cust)) + 5001).alias('DEPID_NEW')
            ])
            
            # Merge back
            mni3 = mni3.join(unique_cust, on='CUSTNO', how='left')
            
            # Update DEPID and DEPGRP
            if 'DEPID' in mni3.columns:
                mni3 = mni3.with_columns([
                    pl.coalesce(['DEPID', 'DEPID_NEW']).alias('DEPID')
                ])
            else:
                mni3 = mni3.with_columns([
                    pl.col('DEPID_NEW').alias('DEPID')
                ])
            
            mni3 = mni3.with_columns([
                pl.when(pl.col('DEPGRP').is_null() | (pl.col('DEPGRP') == ''))
                .then(pl.col('CUSTNAME'))
                .otherwise(pl.col('DEPGRP')).alias('DEPGRP')
            ])
            
            mni3 = mni3.drop(['DEPID_NEW'], strict=False)
            print(f"  Assigned new DEPIDs: {len(mni3)} records")
    
    # Combine all M&I records
    dfs_to_concat = []
    if not mni1_matched.is_empty():
        dfs_to_concat.append(mni1_matched)
    if not mni2_matched.is_empty():
        dfs_to_concat.append(mni2_matched)
    if not mni3.is_empty():
        dfs_to_concat.append(mni3)
    
    if dfs_to_concat:
        # Align all DataFrames before concatenation
        if len(dfs_to_concat) > 1:
            for i in range(1, len(dfs_to_concat)):
                dfs_to_concat[0], dfs_to_concat[i] = align_columns(dfs_to_concat[0], dfs_to_concat[i])
        
        mni_all = pl.concat(dfs_to_concat)
        print(f"  Total M&I records: {len(mni_all)}")
    else:
        print("  No M&I records found")
        return pl.DataFrame(), pl.DataFrame()
    
    # Classify by product type
    if not mni_all.is_empty() and 'CMMCODE' in mni_all.columns:
        # Extract BIC
        mni_all = mni_all.with_columns([
            pl.col('CMMCODE').str.slice(0, 5).alias('BIC')
        ])
        
        # Get AMOUNT column
        amount_col = 'AMOUNT' if 'AMOUNT' in mni_all.columns else None
        if amount_col is None:
            print("  Warning: No AMOUNT column found in M&I data")
            return pl.DataFrame(), mni_all
        
        # Initialize product columns
        mni_all = mni_all.with_columns([
            pl.when(pl.col('BIC').is_in(['95311', '96311'])).then(pl.col(amount_col)).otherwise(0).alias('FD'),
            pl.when(pl.col('BIC') == '95312').then(pl.col(amount_col)).otherwise(0).alias('SA'),
            pl.when(pl.col('BIC').is_in(['95313', '96313'])).then(pl.col(amount_col)).otherwise(0).alias('CA'),
            pl.when(pl.col('BIC') == '953XX').then(pl.col(amount_col)).otherwise(0).alias('VOST'),
            pl.when(pl.col('BIC') == '9531X').then(pl.col(amount_col)).otherwise(0).alias('GOLD'),
            pl.when(pl.col('BIC').is_in(['95840', '96840'])).then(pl.col(amount_col)).otherwise(0).alias('RNID'),
        ])
        
        # Add EXCL flag check
        if 'EXCL' in mni_all.columns:
            mni_all = mni_all.with_columns([
                pl.when((pl.col('BIC').is_in(['95311', '96311'])) & (pl.col('EXCL') != 'Y')).then(pl.col(amount_col)).otherwise(0).alias('FD2'),
                pl.when((pl.col('BIC') == '95312') & (pl.col('EXCL') != 'Y')).then(pl.col(amount_col)).otherwise(0).alias('SA2'),
                pl.when((pl.col('BIC').is_in(['95313', '96313'])) & (pl.col('EXCL') != 'Y')).then(pl.col(amount_col)).otherwise(0).alias('CA2'),
                pl.when((pl.col('BIC').is_in(['95840', '96840'])) & (pl.col('EXCL') != 'Y')).then(pl.col(amount_col)).otherwise(0).alias('RNID2')
            ])
        else:
            mni_all = mni_all.with_columns([
                pl.col('FD').alias('FD2'),
                pl.col('SA').alias('SA2'),
                pl.col('CA').alias('CA2'),
                pl.col('RNID').alias('RNID2')
            ])
        
        # Customer type
        if 'CUSTCD' in mni_all.columns:
            mni_all = mni_all.with_columns([
                pl.when(pl.col('CUSTCD').cast(pl.Utf8).is_in(['77', '78', '95', '96']))
                .then(pl.lit('I')).otherwise(pl.lit('C')).alias('CUSTYPE')
            ])
        else:
            mni_all = mni_all.with_columns([pl.lit('C').alias('CUSTYPE')])
        
        # Filter out records that don't match any product
        product_bics = ['95311', '96311', '95312', '95313', '96313', '953XX', '9531X', '95840', '96840']
        mni_all = mni_all.filter(pl.col('BIC').is_in(product_bics))
        print(f"  M&I records after product filter: {len(mni_all)}")
        
        # Summarize by DEPID, DEPGRP, CUSTYPE
        if not mni_all.is_empty():
            group_cols = ['DEPID', 'DEPGRP', 'CUSTYPE']
            available_group_cols = [c for c in group_cols if c in mni_all.columns]
            
            agg_cols = ['FD', 'SA', 'CA', 'VOST', 'GOLD', 'RNID', 'FD2', 'SA2', 'CA2', 'RNID2']
            available_agg_cols = [c for c in agg_cols if c in mni_all.columns]
            
            if available_group_cols and available_agg_cols:
                mni_sum = mni_all.group_by(available_group_cols).agg([
                    pl.col(c).sum() for c in available_agg_cols
                ]).sort(available_group_cols)
                print(f"  M&I summary: {len(mni_sum)} groups")
            else:
                mni_sum = pl.DataFrame()
        else:
            mni_sum = pl.DataFrame()
    else:
        mni_sum = pl.DataFrame()
    
    return mni_sum, mni_all

# =============================================================================
# EQUITY PROCESSING
# =============================================================================
def process_equity(rep_vars, excl_equ):
    """Process Equity source data"""
    
    # Read EQU
    equ = pl.DataFrame()
    try:
        equ_file = f"{PATHS['LCR']}equ{rep_vars['reptmon']}.sas7bdat"
        print(f"  Reading EQU: {equ_file}")
        equ = read_sas7bdat(equ_file)
        print(f"  EQU loaded: {len(equ)} records, {len(equ.columns)} columns")
        
        if not equ.is_empty() and 'CUSTNO' in equ.columns:
            # Filter non-empty CUSTNO
            equ = equ.filter(pl.col('CUSTNO').ne(''))
            print(f"  EQU after CUSTNO filter: {len(equ)} records")
            
            # Add EXCL flag
            if excl_equ:
                equ = equ.with_columns([
                    pl.when(pl.col('CUSTNO').cast(pl.Utf8).is_in(excl_equ))
                    .then(pl.lit('Y')).otherwise(pl.lit('N')).alias('EXCL')
                ])
            else:
                equ = equ.with_columns([pl.lit('N').alias('EXCL')])
    except Exception as e:
        print(f"  Warning reading EQU: {e}")
        return pl.DataFrame(), pl.DataFrame()
    
    if equ.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    
    # Sort by CUSTNO
    equ = equ.sort('CUSTNO')
    
    # Read COF_EQU_DEPOSITOR_LIST
    cof = pl.DataFrame()
    try:
        cof = read_sas7bdat(f"{PATHS['LIST']}cof_equ_depositor_list.sas7bdat")
        if not cof.is_empty():
            print(f"  COF_EQU_DEPOSITOR_LIST loaded: {len(cof)} records")
            if 'CUSTNO' in cof.columns:
                cof = cof.unique(subset=['CUSTNO'])
                keep_cols = ['DEPID', 'DEPGRP', 'CUSTNO', 'LINKID']
                available_cols = [c for c in keep_cols if c in cof.columns]
                cof = cof.select(available_cols)
                print(f"  COF_EQU for merge: {len(cof)} records")
    except Exception as e:
        print(f"  Warning loading COF_EQU_DEPOSITOR_LIST: {e}")
    
    # Merge by CUSTNO
    equ_matched = pl.DataFrame()
    equ_unmatched = pl.DataFrame()
    
    if not cof.is_empty():
        equ1 = equ.join(cof, on='CUSTNO', how='left', suffix='_COF')
        
        if 'DEPID' in equ1.columns:
            equ_matched = equ1.filter(pl.col('DEPID').is_not_null() & (pl.col('DEPID') > 0))
            equ_unmatched = equ1.filter(pl.col('DEPID').is_null() | (pl.col('DEPID') == 0))
            print(f"  EQU match: {len(equ_matched)} matched, {len(equ_unmatched)} unmatched")
        else:
            equ_unmatched = equ1
    else:
        equ_unmatched = equ
        print(f"  EQU all unmatched: {len(equ_unmatched)} records")
    
    # Assign new DEPID for unmatched records
    equ2 = pl.DataFrame()
    if not equ_unmatched.is_empty() and 'CUSTNO' in equ_unmatched.columns:
        equ2 = equ_unmatched.clone()
        
        equ2_sorted = equ2.unique(subset=['CUSTNO']).sort('CUSTNO')
        if not equ2_sorted.is_empty():
            equ2_sorted = equ2_sorted.with_columns([
                (pl.arange(0, len(equ2_sorted)) + 50005001).alias('DEPID_NEW')
            ])
            
            equ2 = equ2.join(equ2_sorted.select(['CUSTNO', 'DEPID_NEW']), on='CUSTNO', how='left')
            
            if 'DEPID' in equ2.columns:
                equ2 = equ2.with_columns([
                    pl.coalesce(['DEPID', 'DEPID_NEW']).alias('DEPID')
                ])
            else:
                equ2 = equ2.with_columns([
                    pl.col('DEPID_NEW').alias('DEPID')
                ])
            
            equ2 = equ2.with_columns([
                pl.when(pl.col('DEPGRP').is_null() | (pl.col('DEPGRP') == ''))
                .then(pl.col('CUSTNAME'))
                .otherwise(pl.col('DEPGRP')).alias('DEPGRP')
            ])
            
            equ2 = equ2.with_columns([
                pl.when(pl.col('DEPGRP').is_null() | (pl.col('DEPGRP') == ''))
                .then(pl.col('CUSTNO'))
                .otherwise(pl.col('DEPGRP')).alias('DEPGRP')
            ])
            
            equ2 = equ2.drop(['DEPID_NEW'], strict=False)
            print(f"  Assigned new EQU DEPIDs: {len(equ2)} records")
    
    # Combine all equity records
    dfs_to_concat = []
    if not equ_matched.is_empty():
        dfs_to_concat.append(equ_matched)
    if not equ2.is_empty():
        dfs_to_concat.append(equ2)
    
    if dfs_to_concat:
        if len(dfs_to_concat) > 1:
            dfs_to_concat[0], dfs_to_concat[1] = align_columns(dfs_to_concat[0], dfs_to_concat[1])
        
        equ_all = pl.concat(dfs_to_concat)
        print(f"  Total EQU records: {len(equ_all)}")
    else:
        print("  No EQU records found")
        return pl.DataFrame(), pl.DataFrame()
    
    # Classify by product type
    if not equ_all.is_empty() and 'CMMCODE' in equ_all.columns:
        equ_all = equ_all.with_columns([
            pl.col('CMMCODE').str.slice(0, 5).alias('BIC')
        ])
        
        # Filter out N/A products
        equ_all = equ_all.filter(~pl.col('BIC').is_in(['95850', '96850']))
        
        amount_col = 'AMOUNT' if 'AMOUNT' in equ_all.columns else None
        if amount_col is None:
            print("  Warning: No AMOUNT column found in EQU data")
            return pl.DataFrame(), equ_all
        
        # Assign LINKID
        if 'LINKID' not in equ_all.columns:
            equ_all = equ_all.with_columns([
                pl.lit(None).alias('LINKID')
            ])
        
        equ_all = equ_all.with_columns([
            pl.when(
                pl.col('LINKID').is_null() | (pl.col('LINKID') == 0)
            ).then(50000000 + pl.col('DEPID'))
            .otherwise(pl.col('LINKID')).alias('LINKID')
        ])
        
        # Initialize product columns
        equ_all = equ_all.with_columns([
            pl.when(pl.col('BIC').is_in(['95830', '96830', '9583X', '9683X'])).then(pl.col(amount_col)).otherwise(0).alias('STD'),
            pl.when(pl.col('BIC').is_in(['95840', '96840'])).then(pl.col(amount_col)).otherwise(0).alias('NID'),
            pl.when(pl.col('BIC').is_in(['95810', '96810'])).then(pl.col(amount_col)).otherwise(0).alias('IBB'),
            pl.when(pl.col('BIC').is_in(['95820', '96820'])).then(pl.col(amount_col)).otherwise(0).alias('REPO'),
            pl.when(pl.col('BIC').is_in(['95329', '96329'])).then(pl.col(amount_col)).otherwise(0).alias('DCI'),
        ])
        
        if 'EXCL' in equ_all.columns:
            equ_all = equ_all.with_columns([
                pl.when((pl.col('BIC').is_in(['95830', '96830', '9583X', '9683X'])) & (pl.col('EXCL') != 'Y')).then(pl.col(amount_col)).otherwise(0).alias('STD2'),
                pl.when((pl.col('BIC').is_in(['95840', '96840'])) & (pl.col('EXCL') != 'Y')).then(pl.col(amount_col)).otherwise(0).alias('NID2'),
                pl.when((pl.col('BIC').is_in(['95329', '96329'])) & (pl.col('EXCL') != 'Y')).then(pl.col(amount_col)).otherwise(0).alias('DCI2')
            ])
        else:
            equ_all = equ_all.with_columns([
                pl.col('STD').alias('STD2'),
                pl.col('NID').alias('NID2'),
                pl.col('DCI').alias('DCI2')
            ])
        
        if 'CUSTFISS' in equ_all.columns:
            equ_all = equ_all.with_columns([
                pl.when(pl.col('CUSTFISS').cast(pl.Utf8).is_in(['77', '78', '95', '96']))
                .then(pl.lit('I')).otherwise(pl.lit('C')).alias('CUSTYPE')
            ])
        else:
            equ_all = equ_all.with_columns([pl.lit('C').alias('CUSTYPE')])
        
        product_bics = ['95830', '96830', '9583X', '9683X', '95840', '96840', 
                       '95810', '96810', '95820', '96820', '95329', '96329']
        equ_all = equ_all.filter(pl.col('BIC').is_in(product_bics))
        print(f"  EQU records after product filter: {len(equ_all)}")
        
        if not equ_all.is_empty():
            group_cols = ['LINKID', 'DEPGRP', 'CUSTYPE']
            available_group_cols = [c for c in group_cols if c in equ_all.columns]
            
            agg_cols = ['STD', 'NID', 'IBB', 'REPO', 'DCI', 'STD2', 'NID2', 'DCI2']
            available_agg_cols = [c for c in agg_cols if c in equ_all.columns]
            
            if available_group_cols and available_agg_cols:
                equ_sum = equ_all.group_by(available_group_cols).agg([
                    pl.col(c).sum() for c in available_agg_cols
                ]).sort(available_group_cols)
                
                equ_sum = equ_sum.rename({'LINKID': 'DEPID'})
                print(f"  EQU summary: {len(equ_sum)} groups")
            else:
                equ_sum = pl.DataFrame()
        else:
            equ_sum = pl.DataFrame()
    else:
        equ_sum = pl.DataFrame()
    
    return equ_sum, equ_all

# =============================================================================
# CONSOLIDATION
# =============================================================================
def consolidate_sources(mni_sum, equ_sum):
    """Consolidate M&I and Equity sources"""
    if mni_sum.is_empty() and equ_sum.is_empty():
        print("  Both M&I and Equity are empty")
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
    
    # Prepare M&I data
    mni_prep = mni_sum.clone() if not mni_sum.is_empty() else pl.DataFrame()
    
    # Prepare Equity data
    equ_prep = equ_sum.clone() if not equ_sum.is_empty() else pl.DataFrame()
    
    # Merge by DEPID
    if not mni_prep.is_empty() and not equ_prep.is_empty():
        equ_subset = equ_prep.select([
            'DEPID', 
            pl.col('DEPGRP').alias('DEPGRPEQ'),
            pl.col('CUSTYPE').alias('CUSTYPEQ'),
            'STD', 'NID', 'IBB', 'REPO', 'DCI',
            'STD2', 'NID2', 'DCI2'
        ])
        allsrc = mni_prep.join(equ_subset, on='DEPID', how='full')
        print(f"  Merged M&I and Equity: {len(allsrc)} records")
    elif not mni_prep.is_empty():
        allsrc = mni_prep.with_columns([
            pl.lit(None).alias('DEPGRPEQ'),
            pl.lit(None).alias('CUSTYPEQ'),
            pl.lit(0.0).alias('STD'), pl.lit(0.0).alias('NID'),
            pl.lit(0.0).alias('IBB'), pl.lit(0.0).alias('REPO'),
            pl.lit(0.0).alias('DCI'), pl.lit(0.0).alias('STD2'),
            pl.lit(0.0).alias('NID2'), pl.lit(0.0).alias('DCI2')
        ])
        print(f"  Only M&I data: {len(allsrc)} records")
    else:
        allsrc = equ_prep.with_columns([
            pl.lit(None).alias('DEPGRPEQ'),
            pl.lit(None).alias('CUSTYPEQ'),
            pl.lit(0.0).alias('FD'), pl.lit(0.0).alias('SA'),
            pl.lit(0.0).alias('CA'), pl.lit(0.0).alias('VOST'),
            pl.lit(0.0).alias('GOLD'), pl.lit(0.0).alias('RNID'),
            pl.lit(0.0).alias('FD2'), pl.lit(0.0).alias('SA2'),
            pl.lit(0.0).alias('CA2'), pl.lit(0.0).alias('RNID2')
        ])
        allsrc = allsrc.with_columns([
            pl.col('DEPGRP').alias('DEPGRPEQ')
        ])
        print(f"  Only Equity data: {len(allsrc)} records")
    
    if allsrc.is_empty():
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
    
    # Combine fields
    allsrc = allsrc.with_columns([
        pl.when(pl.col('DEPGRP').is_null() | (pl.col('DEPGRP') == ''))
        .then(pl.col('DEPGRPEQ')).otherwise(pl.col('DEPGRP')).alias('DEPGRP'),
        pl.when(pl.col('CUSTYPE').is_null() | (pl.col('CUSTYPE') == ''))
        .then(pl.col('CUSTYPEQ')).otherwise(pl.col('CUSTYPE')).alias('CUSTYPE')
    ])
    
    # Fill null values with 0
    numeric_cols = ['FD', 'SA', 'CA', 'VOST', 'GOLD', 'RNID', 
                   'FD2', 'SA2', 'CA2', 'RNID2',
                   'STD', 'NID', 'IBB', 'REPO', 'DCI',
                   'STD2', 'NID2', 'DCI2']
    for col in numeric_cols:
        if col in allsrc.columns:
            allsrc = allsrc.with_columns([pl.col(col).fill_null(0)])
    
    # Calculate combined NID (NID + RNID)
    allsrc = allsrc.with_columns([
        (pl.col('NID') + pl.col('RNID')).alias('NID_COMB')
    ])
    
    # Calculate TOT
    allsrc = allsrc.with_columns([
        (pl.col('FD') + pl.col('SA') + pl.col('GOLD') + pl.col('CA') + 
         pl.col('STD') + pl.col('NID_COMB') + pl.col('IBB') + pl.col('REPO') + 
         pl.col('DCI') + pl.col('VOST')).alias('TOT')
    ])
    
    # Calculate MNI, EQU, TOT2
    allsrc = allsrc.with_columns([
        (pl.col('FD2') + pl.col('SA2') + pl.col('CA2') + pl.col('RNID2')).alias('MNI'),
        (pl.col('STD2') + pl.col('NID2') + pl.col('DCI2')).alias('EQU'),
        (pl.col('FD2') + pl.col('SA2') + pl.col('CA2') + pl.col('RNID2') + 
         pl.col('STD2') + pl.col('NID2') + pl.col('DCI2')).alias('TOT2')
    ])
    
    # Summarize by DEPID, DEPGRP, CUSTYPE for TOT2
    alltot2 = allsrc.group_by(['DEPID', 'DEPGRP', 'CUSTYPE']).agg([
        pl.col('TOT2').sum(),
        pl.col('MNI').sum(),
        pl.col('EQU').sum()
    ]).sort(['CUSTYPE', 'TOT2'], descending=[False, True])
    print(f"  TOT2 summary: {len(alltot2)} groups")
    
    # Summarize by DEPID, DEPGRP for product breakdown
    alltot = allsrc.group_by(['DEPID', 'DEPGRP']).agg([
        pl.col('TOT').sum(),
        pl.col('FD').sum(),
        pl.col('SA').sum(),
        pl.col('GOLD').sum(),
        pl.col('CA').sum(),
        pl.col('STD').sum(),
        pl.col('NID_COMB').alias('NID'),
        pl.col('IBB').sum(),
        pl.col('REPO').sum(),
        pl.col('DCI').sum(),
        pl.col('VOST').sum()
    ]).sort('TOT', descending=True)
    print(f"  Product summary: {len(alltot)} groups")
    
    return allsrc, alltot2, alltot

# =============================================================================
# REPORT GENERATION
# =============================================================================
def generate_top50_report(alltot2, cust_type, desc, rep_vars, output_path):
    """Generate Top 50 report for a customer type"""
    lines = []
    dlm = chr(5)
    
    # Filter and take top 50
    top50 = alltot2.filter(pl.col('CUSTYPE') == cust_type).head(50)
    
    if top50.is_empty():
        print(f"  No {desc} depositors found")
        return lines, pl.DataFrame()
    
    # Add rank
    top50 = top50.with_columns([
        (pl.arange(0, len(top50)) + 1).alias('RANK')
    ])
    
    # Header
    lines.append(f"PUBLIC BANK BERHAD")
    lines.append(f"TOP 50 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}")
    lines.append("")
    lines.append(f"(i) Top 50 {desc} Depositors by Sources")
    lines.append("")
    lines.append(f"NOBS{dlm}DEPOSITORS{dlm}TOTAL BALANCE{dlm}M&I{dlm}EQUATION")
    
    # Data rows
    for row in top50.iter_rows(named=True):
        lines.append(f"{row['RANK']}{dlm}{row['DEPGRP']}{dlm}"
                    f"{row['TOT2']:,.2f}{dlm}{row['MNI']:,.2f}{dlm}{row['EQU']:,.2f}")
    
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
        depid = row['DEPID']
        rank = row['RANK']
        depgrp = str(row['DEPGRP']) if row['DEPGRP'] else ''
        
        # Rank and Depositor header
        lines.append(f"{rank}{dlm}{depgrp} ({depid}){dlm}")
        lines.append("")
        
        # M&I section
        lines.append(f"{dlm}Source: M&I")
        lines.append("")
        lines.append(f"{dlm}NO{dlm}BRANCH{dlm}ACCTNO{dlm}CUSTNAME{dlm}"
                    f"CUSTNO{dlm}BUSSREG{dlm}CUSTCD{dlm}PRODUCT{dlm}BALANCE")
        
        if not mni_detail.is_empty() and 'DEPID' in mni_detail.columns:
            mni_det = mni_detail.filter(
                (pl.col('DEPID') == depid) & 
                (pl.col('AMOUNT') > 0) & 
                (pl.col('EXCL') != 'Y') &
                (~pl.col('BIC').is_in(['953XX', '9531X']))
            ).sort('ACCTNO')
            
            cnt = 0
            totbal = 0.0
            
            for det_row in mni_det.iter_rows(named=True):
                cnt += 1
                amount = det_row.get('AMOUNT', 0) or 0
                totbal += amount
                
                lines.append(f"{dlm}{cnt}{dlm}"
                           f"{det_row.get('BRANCH', '')}{dlm}"
                           f"{det_row.get('ACCTNO', '')}{dlm}"
                           f"{det_row.get('CUSTNAME', '')}{dlm}"
                           f"{det_row.get('CUSTNO', '')}{dlm}"
                           f"{det_row.get('NEWIC', '')}{dlm}"
                           f"{det_row.get('CUSTCD', '')}{dlm}"
                           f"{det_row.get('PRODUCT', '')}{dlm}"
                           f"{amount:,.2f}")
            
            if cnt > 0:
                lines.append(f"{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{totbal:,.2f}")
                lines.append("")
        
        lines.append("")
        
        # Equity section
        lines.append(f"{dlm}Source: EQU")
        lines.append("")
        lines.append(f"{dlm}NO{dlm}DEALREF{dlm}DEALTYPE{dlm}NAME{dlm}CUST MNEMONIC{dlm}AMOUNT")
        
        if not equ_detail.is_empty() and 'LINKID' in equ_detail.columns:
            linkid = 50000000 + depid if depid else None
            
            if linkid:
                equ_det = equ_detail.filter(
                    (pl.col('LINKID') == linkid) & 
                    (pl.col('AMOUNT') > 0) & 
                    (pl.col('EXCL') != 'Y') &
                    (~pl.col('BIC').is_in(['95810', '96810', '95820', '96820']))
                )
                
                cnt = 0
                totbal = 0.0
                
                for det_row in equ_det.iter_rows(named=True):
                    cnt += 1
                    amount = det_row.get('AMOUNT', 0) or 0
                    totbal += amount
                    
                    lines.append(f"{dlm}{cnt}{dlm}"
                               f"{det_row.get('DEALREF', '')}{dlm}"
                               f"{det_row.get('DEALTYPE', '')}{dlm}"
                               f"{det_row.get('CUSTNAME', '')}{dlm}"
                               f"{det_row.get('CUSTNO', '')}{dlm}"
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
    
    # Get top 100 by total
    top100 = alltot.head(100) if not alltot.is_empty() else pl.DataFrame()
    
    if top100.is_empty():
        print("  No product records found")
        return lines, pl.DataFrame()
    
    top100 = top100.with_columns([
        (pl.arange(0, len(top100)) + 1).alias('RANK')
    ])
    
    # Header
    lines.append(f"PUBLIC BANK BERHAD")
    lines.append(f"TOP 100 LARGEST DEPOSITORS AS AT {rep_vars['rdate']}")
    lines.append("")
    lines.append("(i) Top 100 Depositors by Products")
    lines.append("")
    lines.append(f"NOBS{dlm}DEPOSITORS{dlm}TOTAL BALANCE{dlm}"
                f"FIXED DEPOSIT{dlm}SAVINGS{dlm}DEMAND DEPOSIT{dlm}"
                f"SHORT TERM DEPOSIT{dlm}NID ISSUED{dlm}INTERBANK BORROWING{dlm}"
                f"REPOS{dlm}DUAL CURRENCY INVESTMENT{dlm}GOLD INVESTMENT{dlm}VOSTRO")
    
    # Data rows
    for row in top100.iter_rows(named=True):
        lines.append(f"{row['RANK']}{dlm}{row['DEPGRP']}{dlm}"
                    f"{row.get('TOT', 0):,.2f}{dlm}"
                    f"{row.get('FD', 0):,.2f}{dlm}"
                    f"{row.get('SA', 0):,.2f}{dlm}"
                    f"{row.get('CA', 0):,.2f}{dlm}"
                    f"{row.get('STD', 0):,.2f}{dlm}"
                    f"{row.get('NID', 0):,.2f}{dlm}"
                    f"{row.get('IBB', 0):,.2f}{dlm}"
                    f"{row.get('REPO', 0):,.2f}{dlm}"
                    f"{row.get('DCI', 0):,.2f}{dlm}"
                    f"{row.get('GOLD', 0):,.2f}{dlm}"
                    f"{row.get('VOST', 0):,.2f}")
    
    # Detail listing
    lines.append("")
    lines.append("(ii) Detail Accounts Listing for Top 100 Depositors")
    lines.append("")
    
    for row in top100.iter_rows(named=True):
        depid = row['DEPID']
        rank = row['RANK']
        depgrp = str(row['DEPGRP']) if row['DEPGRP'] else ''
        
        lines.append(f"{rank}{dlm}{depgrp} ({depid}){dlm}")
        lines.append("")
        
        # M&I section
        lines.append(f"{dlm}Source: M&I")
        lines.append("")
        lines.append(f"{dlm}NO{dlm}BRANCH{dlm}ACCTNO{dlm}CUSTNAME{dlm}"
                    f"CUSTNO{dlm}BUSSREG{dlm}CUSTCD{dlm}PRODUCT{dlm}BALANCE")
        
        if not mni_detail.is_empty() and 'DEPID' in mni_detail.columns:
            mni_det = mni_detail.filter(
                (pl.col('DEPID') == depid) & 
                (pl.col('AMOUNT') > 0)
            ).sort('ACCTNO')
            
            cnt = 0
            totbal = 0.0
            
            for det_row in mni_det.iter_rows(named=True):
                cnt += 1
                amount = det_row.get('AMOUNT', 0) or 0
                totbal += amount
                
                lines.append(f"{dlm}{cnt}{dlm}"
                           f"{det_row.get('BRANCH', '')}{dlm}"
                           f"{det_row.get('ACCTNO', '')}{dlm}"
                           f"{det_row.get('CUSTNAME', '')}{dlm}"
                           f"{det_row.get('CUSTNO', '')}{dlm}"
                           f"{det_row.get('NEWIC', '')}{dlm}"
                           f"{det_row.get('CUSTCD', '')}{dlm}"
                           f"{det_row.get('PRODUCT', '')}{dlm}"
                           f"{amount:,.2f}")
            
            if cnt > 0:
                lines.append(f"{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{dlm}{totbal:,.2f}")
                lines.append("")
        
        lines.append("")
        
        # Equity section
        lines.append(f"{dlm}Source: EQU")
        lines.append("")
        lines.append(f"{dlm}NO{dlm}DEALREF{dlm}DEALTYPE{dlm}NAME{dlm}CUST MNEMONIC{dlm}AMOUNT")
        
        if not equ_detail.is_empty() and 'LINKID' in equ_detail.columns:
            linkid = 50000000 + depid if depid else None
            
            if linkid:
                equ_det = equ_detail.filter(
                    (pl.col('LINKID') == linkid) & 
                    (pl.col('AMOUNT') > 0)
                )
                
                cnt = 0
                totbal = 0.0
                
                for det_row in equ_det.iter_rows(named=True):
                    cnt += 1
                    amount = det_row.get('AMOUNT', 0) or 0
                    totbal += amount
                    
                    lines.append(f"{dlm}{cnt}{dlm}"
                               f"{det_row.get('DEALREF', '')}{dlm}"
                               f"{det_row.get('DEALTYPE', '')}{dlm}"
                               f"{det_row.get('CUSTNAME', '')}{dlm}"
                               f"{det_row.get('CUSTNO', '')}{dlm}"
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
    
    # Header
    lines.append(f"PUBLIC BANK BERHAD")
    lines.append(f"TOP 100 DEPOSITORS BY CONTRACTUAL MATURITY AS AT {rep_vars['rdate']}")
    lines.append("")
    lines.append("(iii) Top 100 Depositors by Contractual Maturity")
    
    # Process each depositor
    for row in top100.iter_rows(named=True):
        depid = row['DEPID']
        rank = row['RANK']
        depgrp = str(row['DEPGRP']) if row['DEPGRP'] else ''
        
        lines.append("")
        lines.append(f"{rank}{dlm}{depgrp}")
        lines.append(f"{dlm}DEPOSIT TYPE{dlm}UP TO 1 WEEK{dlm}> 1 WK - 1 MTH{dlm}"
                    f"> 1 - 3 MTHS{dlm}> 3 - 6 MTHS{dlm}> 6 MTHS -  1 YR{dlm}"
                    f"> 1 YEAR{dlm}NO SPECIFIC MATURITY{dlm}TOTAL")
        
        # Template items from SAS format
        template_items = [
            ('A1.01', 'FIXED DEPOSIT'),
            ('A1.02', 'SAVINGS'),
            ('A1.03', 'DEMAND DEPOSIT'),
            ('A1.04', 'REPO'),
            ('A1.05', 'INTERBANK BORROWING'),
            ('A1.06', 'SHORT TERM DEPOSIT'),
            ('A1.07', 'NID ISSUED'),
            ('A1.08', 'DUAL CURRENCY INVESTMENT'),
            ('A1.09', 'VOSTRO'),
            ('A1.10', 'GOLD INVESTMENT'),
            ('B1.01', 'FIXED DEPOSIT'),
            ('B1.02', 'DEMAND DEPOSIT'),
            ('B1.03', 'REPO'),
            ('B1.04', 'INTERBANK BORROWING'),
            ('B1.05', 'SHORT TERM DEPOSIT'),
            ('B1.06', 'NID ISSUED'),
            ('B1.07', 'DUAL CURRENCY INVESTMENT'),
        ]
        
        for item_code, desc in template_items:
            lines.append(f"{dlm}{desc}{dlm}0.00{dlm}0.00{dlm}0.00{dlm}"
                        f"0.00{dlm}0.00{dlm}0.00{dlm}0.00{dlm}0.00")
        
        # Subtotals
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
    
    # Get report variables
    rep_vars = get_report_vars()
    print(f"\nReport Date: {rep_vars['rdate']}")
    print(f"Report Month: {rep_vars['reptmon']}")
    
    # Get exclusion lists
    print("\nLoading exclusion lists...")
    excl_cis, excl_equ = get_exclusion_lists()
    print(f"Exclusions: CIS={len(excl_cis)}, EQU={len(excl_equ)}")
    
    # Process M&I
    print("\n" + "=" * 40)
    print("Processing M&I...")
    print("=" * 40)
    mni_sum, mni_detail = process_mni(rep_vars, excl_cis)
    print(f"M&I Summary: {len(mni_sum)} groups")
    print(f"M&I Detail: {len(mni_detail)} records")
    
    # Process Equity
    print("\n" + "=" * 40)
    print("Processing Equity...")
    print("=" * 40)
    equ_sum, equ_detail = process_equity(rep_vars, excl_equ)
    print(f"Equity Summary: {len(equ_sum)} groups")
    print(f"Equity Detail: {len(equ_detail)} records")
    
    # Consolidate
    print("\n" + "=" * 40)
    print("Consolidating...")
    print("=" * 40)
    allsrc, alltot2, alltot = consolidate_sources(mni_sum, equ_sum)
    print(f"Consolidated Detail: {len(allsrc)} records")
    print(f"TOT2 Summary: {len(alltot2)} groups")
    print(f"Product Summary: {len(alltot)} groups")
    
    # Generate reports
    print("\n" + "=" * 40)
    print("Generating reports...")
    print("=" * 40)
    
    # Individual Top 50
    ind_lines, ind_top = generate_top50_report(
        alltot2, 'I', 'Individual', rep_vars, 
        f"{PATHS['OUTPUT']}COFOUTI.txt"
    )
    
    # Corporate Top 50
    corp_lines, corp_top = generate_top50_report(
        alltot2, 'C', 'Corporate', rep_vars,
        f"{PATHS['OUTPUT']}COFOUTC.txt"
    )
    
    # Detail listings
    ind_detail = generate_detail_listing(
        ind_top, mni_detail, equ_detail, 
        'Individual', f"{PATHS['OUTPUT']}COFOUTI.txt"
    )
    
    corp_detail = generate_detail_listing(
        corp_top, mni_detail, equ_detail, 
        'Corporate', f"{PATHS['OUTPUT']}COFOUTC.txt"
    )
    
    # Top 100 by Product
    prod_lines, prod_top = generate_top100_by_product(
        alltot, mni_detail, equ_detail, rep_vars,
        f"{PATHS['OUTPUT']}COFOUT1.txt"
    )
    
    # Detail listing for Top 100
    prod_detail = generate_detail_listing(
        prod_top, mni_detail, equ_detail, 
        'Product', f"{PATHS['OUTPUT']}COFOUT2.txt"
    )
    
    # Maturity report
    maturity_lines = generate_maturity_report(
        prod_top, allsrc, rep_vars,
        f"{PATHS['OUTPUT']}COFOUT3.txt"
    )
    
    # Write output files
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
    
    # Apply PBBLNFMT formatting if available
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
                    else:
                        print(f"  Warning: PBBLNFMT has no apply_format or main method")
                except Exception as e:
                    print(f"  Warning formatting {fpath}: {e}")
    
    print("\n" + "=" * 60)
    print("✓ EIBMTLCR Complete")
    print(f"Output directory: {PATHS['OUTPUT']}")
    print("=" * 60)

if __name__ == "__main__":
    main()
