"""
EIBQINST - Trustee and Client Account Quarterly Reporting
Processes trustee and client accounts with balance thresholds (>60k/<=60k)
Includes PBBDPFMT format mappings for product codes
Converted from SAS to Python with Polars
"""

import polars as pl
import pyreadstat
from datetime import datetime, timedelta
from pathlib import Path

# Import PBBDPFMT (assumed to be in the same directory)
try:
    from PBBDPFMT import *  # Import product code formats if needed
    print("PBBDPFMT imported successfully")
except ImportError:
    print("Warning: PBBDPFMT not found, using default product codes")

# =============================================================================
# CONFIG
# =============================================================================
PATHS = {
    'PIDMS': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibqinst/',
    'SACA': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibqinst/',
    'DEPOSIT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibqinst/deposit/',
    'DEPOSIX': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibqinst/deposix/',
    'UNCLAIM': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/eibqinst/',
    'OUTPUT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/eibqinst/'
}
for p in PATHS.values(): Path(p).mkdir(exist_ok=True)

# Product code filters (from PBBDPFMT)
PROD_CODES = [
    '42110','42310','42120','42320','42130','42133','42132','42180',
    '42610','42630','34180','42199','42699'
]

# =============================================================================
# DATE PROCESSING (Following SAS REPTDATE logic)
# =============================================================================
def get_dates():
    """Calculate report dates following SAS logic exactly"""
    today = datetime.now().date()
    
    # REPTDATE = INPUT('01'||PUT(MONTH(TODAY()),Z2.)||PUT(YEAR(TODAY()), 4.), DDMMYY8.)-1
    first_of_month = datetime(today.year, today.month, 1).date()
    reptdate = first_of_month - timedelta(days=1)
    
    # SELECT(DAY(REPTDATE))
    day = reptdate.day
    if day == 8:
        sdd, wk, wk1 = 1, '1', '4'
    elif day == 15:
        sdd, wk, wk1 = 9, '2', '1'
    elif day == 22:
        sdd, wk, wk1 = 16, '3', '2'
    else:
        sdd, wk, wk1 = 23, '4', '3'
        wk2, wk3 = '2', '1'
    
    mm = reptdate.month
    
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm
    
    mm2 = mm - 1
    if mm2 == 0:
        mm2 = 12
    
    sdate = datetime(reptdate.year, mm, sdd).date()
    
    return {
        'reptdate': reptdate,
        'nowk': wk,
        'reptmon': f"{mm:02d}",
        'reptyear': str(reptdate.year),
        'sdate': sdate,
        'sdesc': 'PUBLIC BANK BERHAD',
        'mm1': mm1,
        'mm2': mm2,
        'sdd': sdd
    }

# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

def standardize_acctno(df):
    """Standardize ACCTNO column to string type for consistent joins"""
    if 'acctno' in df.columns:
        df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
    return df

def load_float():
    """Load FLOAT data with PROC SUMMARY equivalent"""
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['PIDMS']}float.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        result = df.group_by('acctno').agg([
            pl.col('float').cast(pl.Float64, strict=False).fill_null(0).sum().alias('float')
        ])
        return result
    except Exception as e:
        print(f"Error loading FLOAT: {e}")
        return pl.DataFrame()

def load_ibgpidm():
    """Load IBGPIDM data with PROC SUMMARY equivalent"""
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['DEPOSIT']}ibgpidm.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        result = df.group_by('acctno').agg([
            pl.col('ibgamt').cast(pl.Float64, strict=False).fill_null(0).sum().alias('ibgamt')
        ])
        return result
    except Exception as e:
        print(f"Error loading IBGPIDM: {e}")
        return pl.DataFrame()

def load_remit(d):
    """Load REMIT and UNCLAIM data"""
    try:
        remit, meta = pyreadstat.read_sas7bdat(f"{PATHS['DEPOSIT']}remit.sas7bdat")
        remit = pl.DataFrame(remit)
        remit.columns = [col.lower() for col in remit.columns]
        
        unclaim_file = f"{PATHS['UNCLAIM']}unclaim{d['reptyear']}.sas7bdat"
        if not Path(unclaim_file).exists():
            unclaim_file = f"{PATHS['UNCLAIM']}unclaim.sas7bdat"
        
        unclaim, meta = pyreadstat.read_sas7bdat(unclaim_file)
        unclaim = pl.DataFrame(unclaim)
        unclaim.columns = [col.lower() for col in unclaim.columns]
        
        if 'ledgbal' in unclaim.columns:
            unclaim = unclaim.rename({'ledgbal': 'unclaimx'})
        
        if 'paymode' in remit.columns:
            remit = remit.with_columns(pl.col('paymode').cast(pl.Utf8).str.strip_chars())
        if 'paymode' in unclaim.columns:
            unclaim = unclaim.with_columns(pl.col('paymode').cast(pl.Utf8).str.strip_chars())
        
        if 'ledgbal' in remit.columns:
            remit = remit.with_columns(pl.col('ledgbal').cast(pl.Float64, strict=False).fill_null(0))
        else:
            remit = remit.with_columns(pl.lit(0.0).alias('ledgbal'))
        
        if 'unclaimx' in unclaim.columns:
            unclaim = unclaim.with_columns(pl.col('unclaimx').cast(pl.Float64, strict=False).fill_null(0))
        else:
            unclaim = unclaim.with_columns(pl.lit(0.0).alias('unclaimx'))
        
        if 'unclaimx' not in remit.columns:
            remit = remit.with_columns(pl.lit(0.0).alias('unclaimx'))
        
        if 'ledgbal' not in unclaim.columns:
            unclaim = unclaim.with_columns(pl.lit(0.0).alias('ledgbal'))
        
        common_cols = ['paymode', 'ledgbal', 'unclaimx']
        remit_subset = remit.select(common_cols)
        unclaim_subset = unclaim.select(common_cols)
        combined = pl.concat([remit_subset, unclaim_subset])
        
        summary = combined.group_by('paymode').agg([
            pl.col('ledgbal').sum().alias('plusbal'),
            pl.col('unclaimx').sum().alias('unclaim')
        ])
        
        result = summary.with_columns(pl.col('paymode').alias('acctno'))
        
        if 'acctno' in result.columns:
            result = result.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        keep_cols = ['acctno', 'plusbal', 'unclaim']
        for col in keep_cols:
            if col not in result.columns:
                result = result.with_columns(pl.lit(0.0).alias(col))
        
        return result.select(keep_cols)
    except Exception as e:
        print(f"Error loading REMIT/UNCLAIM: {e}")
        return pl.DataFrame()

def load_saca_trustee():
    """Load SA/CA/FD data for TRUSTEE processing (WITH PURPOSE filter)"""
    dfs = []
    
    # Load SAVING with PURPOSE filter
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}saving.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        df = df.filter(pl.col('purpose').cast(pl.Utf8).is_in(['5', '6']))
        
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        keep_cols = ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
        for col in keep_cols:
            if col not in df.columns:
                if col == 'name':
                    df = df.with_columns(pl.lit('').alias('name'))
                elif col in ['curbal', 'intpaybl']:
                    df = df.with_columns(pl.lit(0.0).alias(col))
                else:
                    df = df.with_columns(pl.lit('').alias(col))
        
        # Cast to consistent types
        df = df.with_columns([
            pl.col('branch').cast(pl.Utf8).fill_null(''),
            pl.col('acctno').cast(pl.Utf8).fill_null(''),
            pl.col('name').cast(pl.Utf8).fill_null(''),
            pl.col('purpose').cast(pl.Utf8).fill_null(''),
            pl.col('product').cast(pl.Utf8).fill_null(''),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0)
        ])
        
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading SAVING (trustee): {e}")
    
    # Load CURRENT with PURPOSE filter
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}current.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        df = df.filter(pl.col('purpose').cast(pl.Utf8).is_in(['5', '6']))
        
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        df = df.with_columns(pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0))
        
        if 'curcode' in df.columns and 'forate' in df.columns:
            df = df.with_columns([
                pl.when(pl.col('curcode').cast(pl.Utf8) != 'MYR')
                  .then((pl.col('intpaybl') * pl.col('forate').cast(pl.Float64, strict=False).fill_null(1)).round(2))
                  .otherwise(pl.col('intpaybl')).alias('intpaybl')
            ])
        
        keep_cols = ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
        for col in keep_cols:
            if col not in df.columns:
                if col == 'name':
                    df = df.with_columns(pl.lit('').alias('name'))
                elif col in ['curbal', 'intpaybl']:
                    df = df.with_columns(pl.lit(0.0).alias(col))
                else:
                    df = df.with_columns(pl.lit('').alias(col))
        
        # Cast to consistent types
        df = df.with_columns([
            pl.col('branch').cast(pl.Utf8).fill_null(''),
            pl.col('acctno').cast(pl.Utf8).fill_null(''),
            pl.col('name').cast(pl.Utf8).fill_null(''),
            pl.col('purpose').cast(pl.Utf8).fill_null(''),
            pl.col('product').cast(pl.Utf8).fill_null(''),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0)
        ])
        
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading CURRENT (trustee): {e}")
    
    # Load FD with PURPOSE filter
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}fd.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        df = df.filter(pl.col('purpose').cast(pl.Utf8).is_in(['5', '6']))
        
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        df = df.with_columns(pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0))
        
        if 'curcode' in df.columns and 'forate' in df.columns:
            df = df.with_columns([
                pl.when(pl.col('curcode').cast(pl.Utf8) != 'MYR')
                  .then((pl.col('intpaybl') * pl.col('forate').cast(pl.Float64, strict=False).fill_null(1)).round(2))
                  .otherwise(pl.col('intpaybl')).alias('intpaybl')
            ])
        
        keep_cols = ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
        for col in keep_cols:
            if col not in df.columns:
                if col == 'name':
                    df = df.with_columns(pl.lit('').alias('name'))
                elif col in ['curbal', 'intpaybl']:
                    df = df.with_columns(pl.lit(0.0).alias(col))
                else:
                    df = df.with_columns(pl.lit('').alias(col))
        
        # Cast to consistent types
        df = df.with_columns([
            pl.col('branch').cast(pl.Utf8).fill_null(''),
            pl.col('acctno').cast(pl.Utf8).fill_null(''),
            pl.col('name').cast(pl.Utf8).fill_null(''),
            pl.col('purpose').cast(pl.Utf8).fill_null(''),
            pl.col('product').cast(pl.Utf8).fill_null(''),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0)
        ])
        
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading FD (trustee): {e}")
    
    if dfs:
        result = pl.concat(dfs)
        if 'acctno' in result.columns:
            result = result.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        return result
    else:
        return pl.DataFrame()

def load_saca_client():
    """Load SA/CA/FD data for CLIENT processing (WITHOUT PURPOSE filter)"""
    dfs = []
    
    # Load SAVING (all purposes)
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}saving.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        keep_cols = ['acctno', 'branch', 'product', 'purpose', 'curbal', 'intpaybl', 'curcode', 'forate', 'name']
        for col in keep_cols:
            if col not in df.columns:
                if col == 'name':
                    df = df.with_columns(pl.lit('').alias('name'))
                elif col in ['curbal', 'intpaybl', 'forate']:
                    df = df.with_columns(pl.lit(0.0).alias(col))
                elif col in ['branch', 'product', 'purpose', 'curcode']:
                    df = df.with_columns(pl.lit('').alias(col))
                else:
                    df = df.with_columns(pl.lit(None).alias(col))
        
        # Cast all columns to consistent types
        df = df.with_columns([
            pl.col('acctno').cast(pl.Utf8).fill_null(''),
            pl.col('branch').cast(pl.Utf8).fill_null(''),
            pl.col('product').cast(pl.Utf8).fill_null(''),
            pl.col('purpose').cast(pl.Utf8).fill_null(''),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('curcode').cast(pl.Utf8).fill_null(''),
            pl.col('forate').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('name').cast(pl.Utf8).fill_null('')
        ])
        
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading SAVING (client): {e}")
    
    # Load FD (all purposes)
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}fd.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        keep_cols = ['acctno', 'branch', 'product', 'purpose', 'curbal', 'intpaybl', 'curcode', 'forate', 'name']
        for col in keep_cols:
            if col not in df.columns:
                if col == 'name':
                    df = df.with_columns(pl.lit('').alias('name'))
                elif col in ['curbal', 'intpaybl', 'forate']:
                    df = df.with_columns(pl.lit(0.0).alias(col))
                elif col in ['branch', 'product', 'purpose', 'curcode']:
                    df = df.with_columns(pl.lit('').alias(col))
                else:
                    df = df.with_columns(pl.lit(None).alias(col))
        
        # Cast all columns to consistent types
        df = df.with_columns([
            pl.col('acctno').cast(pl.Utf8).fill_null(''),
            pl.col('branch').cast(pl.Utf8).fill_null(''),
            pl.col('product').cast(pl.Utf8).fill_null(''),
            pl.col('purpose').cast(pl.Utf8).fill_null(''),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('curcode').cast(pl.Utf8).fill_null(''),
            pl.col('forate').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('name').cast(pl.Utf8).fill_null('')
        ])
        
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading FD (client): {e}")
    
    # Load CURRENT (all purposes)
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}current.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        if 'acctno' in df.columns:
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        keep_cols = ['acctno', 'branch', 'product', 'purpose', 'curbal', 'intpaybl', 'curcode', 'forate', 'name']
        for col in keep_cols:
            if col not in df.columns:
                if col == 'name':
                    df = df.with_columns(pl.lit('').alias('name'))
                elif col in ['curbal', 'intpaybl', 'forate']:
                    df = df.with_columns(pl.lit(0.0).alias(col))
                elif col in ['branch', 'product', 'purpose', 'curcode']:
                    df = df.with_columns(pl.lit('').alias(col))
                else:
                    df = df.with_columns(pl.lit(None).alias(col))
        
        # Cast all columns to consistent types
        df = df.with_columns([
            pl.col('acctno').cast(pl.Utf8).fill_null(''),
            pl.col('branch').cast(pl.Utf8).fill_null(''),
            pl.col('product').cast(pl.Utf8).fill_null(''),
            pl.col('purpose').cast(pl.Utf8).fill_null(''),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('curcode').cast(pl.Utf8).fill_null(''),
            pl.col('forate').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('name').cast(pl.Utf8).fill_null('')
        ])
        
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading CURRENT (client): {e}")
    
    if dfs:
        result = pl.concat(dfs)
        
        # Cast numeric columns
        result = result.with_columns([
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0)
        ])
        
        # IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE,.01)
        if 'curcode' in result.columns and 'forate' in result.columns:
            result = result.with_columns([
                pl.when(pl.col('curcode').cast(pl.Utf8) != 'MYR')
                  .then((pl.col('intpaybl') * pl.col('forate').cast(pl.Float64, strict=False).fill_null(1)).round(2))
                  .otherwise(pl.col('intpaybl')).alias('intpaybl')
            ])
        
        # PROC SORT DATA=DEPOSIT NODUPKEYS; BY ACCTNO;
        result = result.unique(subset=['acctno'])
        
        if 'acctno' in result.columns:
            result = result.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        return result
    else:
        return pl.DataFrame()

def load_dep(d):
    """Load DEP data following SAS logic"""
    try:
        dfs = []
        
        # DEP.SAVG&REPTMON&NOWK
        savg_file = f"{PATHS['DEPOSIT']}savg{d['reptmon']}{d['nowk']}.sas7bdat"
        if Path(savg_file).exists():
            df, meta = pyreadstat.read_sas7bdat(savg_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            if 'acctno' in df.columns:
                df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
            
            keep_cols = ['acctno', 'amtind', 'prodcd', 'product']
            for col in keep_cols:
                if col not in df.columns:
                    if col == 'amtind':
                        df = df.with_columns(pl.lit('').alias('amtind'))
                    elif col == 'product':
                        df = df.with_columns(pl.lit('').alias('product'))
                    elif col == 'prodcd':
                        df = df.with_columns(pl.lit('').alias('prodcd'))
            dfs.append(df.select(keep_cols))
        
        # DEP.CURN&REPTMON&NOWK
        curn_file = f"{PATHS['DEPOSIT']}curn{d['reptmon']}{d['nowk']}.sas7bdat"
        if Path(curn_file).exists():
            df, meta = pyreadstat.read_sas7bdat(curn_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            if 'acctno' in df.columns:
                df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
            
            keep_cols = ['acctno', 'amtind', 'prodcd', 'product']
            for col in keep_cols:
                if col not in df.columns:
                    if col == 'amtind':
                        df = df.with_columns(pl.lit('').alias('amtind'))
                    elif col == 'product':
                        df = df.with_columns(pl.lit('').alias('product'))
                    elif col == 'prodcd':
                        df = df.with_columns(pl.lit('').alias('prodcd'))
            dfs.append(df.select(keep_cols))
        
        # DEP.FDMTHLY (with rename)
        fdmthly_file = f"{PATHS['DEPOSIT']}fdmthly.sas7bdat"
        if Path(fdmthly_file).exists():
            df, meta = pyreadstat.read_sas7bdat(fdmthly_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            if 'acctno' in df.columns:
                df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
            
            # RENAME=(BIC=PRODCD ACCTTYPE=PRODUCT)
            if 'bic' in df.columns:
                df = df.rename({'bic': 'prodcd'})
            if 'accttype' in df.columns:
                df = df.rename({'accttype': 'product'})
            
            keep_cols = ['acctno', 'amtind', 'prodcd', 'product']
            for col in keep_cols:
                if col not in df.columns:
                    if col == 'amtind':
                        df = df.with_columns(pl.lit('').alias('amtind'))
                    elif col == 'product':
                        df = df.with_columns(pl.lit('').alias('product'))
                    elif col == 'prodcd':
                        df = df.with_columns(pl.lit('').alias('prodcd'))
            dfs.append(df.select(keep_cols))
        
        if not dfs:
            return pl.DataFrame()
        
        combined = pl.concat(dfs)
        
        # IF PRODCD IN (list)
        combined = combined.filter(pl.col('prodcd').cast(pl.Utf8).is_in(PROD_CODES))
        
        # IF PRODCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE
        combined = combined.filter(
            ~((pl.col('prodcd').cast(pl.Utf8).is_in(['42199', '42699'])) & 
              (~pl.col('product').cast(pl.Int64, strict=False).fill_null(0).is_in([72, 413])))
        )
        
        # PROC SORT DATA=DEP NODUPKEYS; BY ACCTNO
        result = combined.unique(subset=['acctno'])
        if 'acctno' in result.columns:
            result = result.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
        return result
    except Exception as e:
        print(f"Error loading DEP: {e}")
        return pl.DataFrame()

def load_client():
    """Load CLIENT data"""
    try:
        client_file = f"{PATHS['DEPOSIT']}client.sas7bdat"
        if Path(client_file).exists():
            df, meta = pyreadstat.read_sas7bdat(client_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            if 'acctno' not in df.columns:
                print("Error: 'acctno' column not found in CLIENT file")
                return pl.DataFrame()
            
            df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip_chars())
            
            if 'name' in df.columns:
                df = df.with_columns(pl.col('name').cast(pl.Utf8).str.slice(0, 10).alias('key'))
            
            return df.unique(subset=['acctno'])
        else:
            print(f"Warning: CLIENT SAS file not found at {client_file}")
            return pl.DataFrame()
    except Exception as e:
        print(f"Error loading CLIENT: {e}")
        return pl.DataFrame()

# =============================================================================
# MAIN PROCESSING
# =============================================================================
def main():
    print("="*60)
    print("EIBQINST - Trustee and Client Account Quarterly Reporting")
    print("="*60)
    
    d = get_dates()
    print(f"\nReport Date: {d['reptdate']} (Week: {d['nowk']})")
    
    # Load all data
    print("\nLoading data...")
    float_df = load_float()
    print(f"  FLOAT: {len(float_df)} records")
    
    ibg_df = load_ibgpidm()
    print(f"  IBGPIDM: {len(ibg_df)} records")
    
    remit_df = load_remit(d)
    print(f"  REMIT/UNCLAIM: {len(remit_df)} records")
    
    dep_df = load_dep(d)
    print(f"  DEP: {len(dep_df)} records")
    
    client_df = load_client()
    print(f"  CLIENT: {len(client_df)} records")
    
    # ========== TRUSTEE ACCOUNTS PROCESSING ==========
    print("\n" + "="*60)
    print("Processing Trustee Accounts...")
    print("="*60)
    
    trustee = None
    client = None
    
    # Load trustee-specific data (WITH PURPOSE filter)
    saca_trustee_df = load_saca_trustee()
    print(f"  Trustee SA/CA/FD (with purpose filter): {len(saca_trustee_df)} records")
    
    if not saca_trustee_df.is_empty():
        # Merge with FLOAT
        if not float_df.is_empty():
            trustee = saca_trustee_df.join(float_df, on='acctno', how='left')
        else:
            trustee = saca_trustee_df.with_columns(pl.lit(0.0).alias('float'))
        
        trustee = trustee.with_columns([
            pl.col('float').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0)
        ])
        
        trustee = trustee.with_columns([
            (pl.col('curbal') - pl.col('float')).alias('avbal'),
            (pl.col('curbal') - pl.col('float') + pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0)).alias('avbaltt')
        ])
        
        # Merge with DEP
        if not dep_df.is_empty():
            # Rename product column in dep to avoid conflict
            dep_for_trustee = dep_df.rename({
                'product': 'dep_product',
                'amtind': 'amtind_dep'
            })
            trustee = trustee.join(dep_for_trustee, on='acctno', how='inner')
        
        if not trustee.is_empty():
            # Ensure we have product and amtind columns
            if 'dep_product' in trustee.columns:
                trustee = trustee.with_columns(pl.col('dep_product').alias('product'))
            if 'amtind_dep' in trustee.columns:
                trustee = trustee.with_columns(pl.col('amtind_dep').alias('amtind'))
            elif 'amtind' not in trustee.columns:
                trustee = trustee.with_columns(pl.lit('').alias('amtind'))
            
            # Merge with REMIT
            if not remit_df.is_empty():
                trustee = trustee.join(remit_df, on='acctno', how='left')
            else:
                trustee = trustee.with_columns([
                    pl.lit(0.0).alias('plusbal'),
                    pl.lit(0.0).alias('unclaim')
                ])
            
            trustee = trustee.with_columns([
                pl.col('plusbal').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('unclaim').cast(pl.Float64, strict=False).fill_null(0)
            ])
            
            trustee = trustee.with_columns([
                (pl.col('avbal') + pl.col('plusbal') + pl.col('unclaim') + 
                 pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0)).alias('avbaltt')
            ])
            
            # Add SI
            trustee = trustee.with_columns(pl.lit(0).alias('si'))
            trustee = trustee.with_columns((pl.col('avbaltt') + pl.col('si')).alias('avbaltt'))
            
            # Add IBGAMT
            if not ibg_df.is_empty():
                trustee = trustee.join(ibg_df, on='acctno', how='left')
            else:
                trustee = trustee.with_columns(pl.lit(0.0).alias('ibgamt'))
            
            trustee = trustee.with_columns([
                pl.col('ibgamt').cast(pl.Float64, strict=False).fill_null(0),
                (pl.col('avbaltt') + pl.col('ibgamt').cast(pl.Float64, strict=False).fill_null(0)).alias('avbaltt')
            ])
            
            # Split by threshold
            trustee_high = trustee.filter(pl.col('avbaltt') > 60000)
            trustee_low = trustee.filter(pl.col('avbaltt') <= 60000)
            
            print(f"\nTrustee >60k: {len(trustee_high)} accounts")
            print(f"Trustee <=60k: {len(trustee_low)} accounts")
            
            # Write output files
            def write_output(df, title, filename):
                if df.is_empty():
                    return
                
                lines = []
                lines.append(" ")
                lines.append(title)
                lines.append(" ")
                lines.append("BRANCH;ACCTNO;NAME;PURPOSE;AVBAL;INTPAYBL;PRODUCT;AMTIND;PLUSBAL;UNCLAIM;SI;IBGAMT;AVBALTT;")
                
                for row in df.rows(named=True):
                    line = (f"{row.get('branch','')};{row.get('acctno','')};{row.get('name','')};"
                           f"{row.get('purpose','')};{row.get('avbal',0):.2f};{row.get('intpaybl',0):.2f};"
                           f"{row.get('product','')};{row.get('amtind','')};{row.get('plusbal',0):.2f};"
                           f"{row.get('unclaim',0):.2f};{row.get('si',0)};{row.get('ibgamt',0):.2f};"
                           f"{row.get('avbaltt',0):.2f};")
                    lines.append(line)
                
                output_file = Path(f"{PATHS['OUTPUT']}{filename}")
                output_file.write_text('\n'.join(lines))
                print(f"  Written to {output_file}")
            
            print("\nWriting Trustee output files...")
            write_output(trustee_high, "TRUSTEE >60000", "trustee_high.txt")
            write_output(trustee_low, "TRUSTEE <=60000", "trustee_low.txt")
            
            # Print summary by branch
            if not trustee_high.is_empty():
                print("\nTRUSTEE >60000 by Branch:")
                for r in trustee_high.group_by('branch').agg(pl.col('avbaltt').sum()).sort('branch').rows():
                    print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
            
            if not trustee_low.is_empty():
                print("\nTRUSTEE <=60000 by Branch:")
                for r in trustee_low.group_by('branch').agg(pl.col('avbaltt').sum()).sort('branch').rows():
                    print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
    
    # ========== CLIENT ACCOUNTS PROCESSING ==========
    print("\n" + "="*60)
    print("Processing Client Accounts...")
    print("="*60)
    
    # Load client-specific data (WITHOUT PURPOSE filter)
    saca_client_df = load_saca_client()
    print(f"  Client SA/CA/FD (without purpose filter): {len(saca_client_df)} records")
    
    if not client_df.is_empty() and not saca_client_df.is_empty():
        # Debug: Check overlap
        client_accts = set(client_df['acctno'].to_list())
        saca_client_accts = set(saca_client_df['acctno'].to_list())
        overlap = client_accts.intersection(saca_client_accts)
        print(f"  Debug - Client accounts: {len(client_accts)}")
        print(f"  Debug - SACA client accounts: {len(saca_client_accts)}")
        print(f"  Debug - Overlap: {len(overlap)}")
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) DEPOSIT(IN=B); BY ACCTNO; IF A & B; RUN;
        client = client_df.join(saca_client_df, on='acctno', how='inner')
        
        print(f"  Debug - Client after join with SACA: {len(client)}")
        
        if not client.is_empty():
            # Merge with FLOAT
            if not float_df.is_empty():
                client = client.join(float_df, on='acctno', how='left')
            else:
                client = client.with_columns(pl.lit(0.0).alias('float'))
            
            client = client.with_columns([
                pl.col('float').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0)
            ])
            
            client = client.with_columns([
                (pl.col('curbal') - pl.col('float')).alias('avbal'),
                (pl.col('curbal') - pl.col('float') + pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0)).alias('avbaltt')
            ])
            
            # Merge with DEP - rename columns to avoid conflicts
            if not dep_df.is_empty():
                dep_for_client = dep_df.rename({
                    'product': 'dep_product',
                    'amtind': 'amtind_dep'
                })
                client = client.join(dep_for_client, on='acctno', how='inner')
            
            print(f"  Debug - Client after join with DEP: {len(client)}")
            
            # Ensure we have product and amtind columns
            if 'dep_product' in client.columns:
                client = client.with_columns(pl.col('dep_product').alias('product'))
            if 'amtind_dep' in client.columns:
                client = client.with_columns(pl.col('amtind_dep').alias('amtind'))
            elif 'amtind' not in client.columns:
                client = client.with_columns(pl.lit('').alias('amtind'))
            
            # Merge with REMIT
            if not remit_df.is_empty():
                client = client.join(remit_df, on='acctno', how='left')
            else:
                client = client.with_columns([
                    pl.lit(0.0).alias('plusbal'),
                    pl.lit(0.0).alias('unclaim')
                ])
            
            client = client.with_columns([
                pl.col('plusbal').cast(pl.Float64, strict=False).fill_null(0),
                pl.col('unclaim').cast(pl.Float64, strict=False).fill_null(0)
            ])
            
            client = client.with_columns([
                (pl.col('avbal') + pl.col('plusbal') + pl.col('unclaim') + 
                 pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0)).alias('avbaltt')
            ])
            
            # Add SI
            client = client.with_columns(pl.lit(0).alias('si'))
            client = client.with_columns((pl.col('avbaltt') + pl.col('si')).alias('avbaltt'))
            
            # Add IBGAMT
            if not ibg_df.is_empty():
                client = client.join(ibg_df, on='acctno', how='left')
            else:
                client = client.with_columns(pl.lit(0.0).alias('ibgamt'))
            
            client = client.with_columns([
                pl.col('ibgamt').cast(pl.Float64, strict=False).fill_null(0),
                (pl.col('avbaltt') + pl.col('ibgamt').cast(pl.Float64, strict=False).fill_null(0)).alias('avbaltt')
            ])
            
            # Split by threshold
            client_high = client.filter(pl.col('avbaltt') > 60000)
            client_low = client.filter(pl.col('avbaltt') <= 60000)
            
            print(f"\nClient >60k: {len(client_high)} accounts")
            print(f"Client <=60k: {len(client_low)} accounts")
            
            # Write output files
            def write_client_output(df, title, filename):
                if df.is_empty():
                    return
                
                lines = []
                lines.append(" ")
                lines.append(title)
                lines.append(" ")
                lines.append("BRANCH;ACCTNO;NAME;PURPOSE;AVBAL;INTPAYBL;PRODUCT;AMTIND;PLUSBAL;UNCLAIM;SI;IBGAMT;AVBALTT;")
                
                for row in df.rows(named=True):
                    product_val = row.get('product', row.get('dep_product', ''))
                    amtind_val = row.get('amtind', row.get('amtind_dep', ''))
                    
                    line = (f"{row.get('branch','')};{row.get('acctno','')};{row.get('name','')};"
                           f"{row.get('purpose','')};{row.get('avbal',0):.2f};{row.get('intpaybl',0):.2f};"
                           f"{product_val};{amtind_val};{row.get('plusbal',0):.2f};"
                           f"{row.get('unclaim',0):.2f};{row.get('si',0)};{row.get('ibgamt',0):.2f};"
                           f"{row.get('avbaltt',0):.2f};")
                    lines.append(line)
                
                output_file = Path(f"{PATHS['OUTPUT']}{filename}")
                output_file.write_text('\n'.join(lines))
                print(f"  Written to {output_file}")
            
            print("\nWriting Client output files...")
            write_client_output(client_high, "CLIENT >60000", "client_high.txt")
            write_client_output(client_low, "CLIENT <=60000", "client_low.txt")
            
            # Print summary by branch
            if not client_high.is_empty():
                print("\nCLIENT >60000 by Branch:")
                for r in client_high.group_by('branch').agg(pl.col('avbaltt').sum()).sort('branch').rows():
                    print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
            
            if not client_low.is_empty():
                print("\nCLIENT <=60000 by Branch:")
                for r in client_low.group_by('branch').agg(pl.col('avbaltt').sum()).sort('branch').rows():
                    print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
    
    # ========== DUPLICATE ACCOUNTS CHECK ==========
    print("\n" + "="*60)
    print("Checking for duplicate accounts...")
    print("="*60)
    
    if (trustee is not None and client is not None and 
        not trustee.is_empty() and not client.is_empty()):
        
        all_acc = pl.concat([
            trustee.select(['acctno']).with_columns(pl.lit('TRUSTEE').alias('source')),
            client.select(['acctno']).with_columns(pl.lit('CLIENT').alias('source'))
        ])
        
        dup = all_acc.group_by('acctno').agg([
            pl.col('source').alias('sources'),
            pl.count().alias('count')
        ]).filter(pl.col('count') > 1)
        
        if not dup.is_empty():
            print(f"\nFound {len(dup)} duplicate accounts:")
            for r in dup.rows(named=True):
                print(f"  Account {r['acctno']} appears in: {', '.join(r['sources'])}")
        else:
            print("\nNo duplicate accounts found")
    
    # ========== SUMMARY ==========
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    if trustee is not None and not trustee.is_empty():
        print(f"\nTrustee Accounts:")
        print(f"  Total: RM {trustee['avbaltt'].sum():,.2f}")
        print(f"  >60k: RM {trustee_high['avbaltt'].sum():,.2f} ({len(trustee_high)} accounts)")
        print(f"  <=60k: RM {trustee_low['avbaltt'].sum():,.2f} ({len(trustee_low)} accounts)")
    
    if client is not None and not client.is_empty():
        print(f"\nClient Accounts:")
        print(f"  Total: RM {client['avbaltt'].sum():,.2f}")
        print(f"  >60k: RM {client_high['avbaltt'].sum():,.2f} ({len(client_high)} accounts)")
        print(f"  <=60k: RM {client_low['avbaltt'].sum():,.2f} ({len(client_low)} accounts)")
    
    print("\n" + "="*60)
    print("✓ EIBQINST Complete")
    print("="*60)

if __name__ == "__main__":
    main()
