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
    # This is first day of current month - 1 = last day of previous month
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
    
    # MM = MONTH(REPTDATE)
    mm = reptdate.month
    
    # IF WK = '1' THEN DO; MM1 = MM - 1; IF MM1 = 0 THEN MM1 = 12; END; ELSE MM1 = MM;
    if wk == '1':
        mm1 = mm - 1
        if mm1 == 0:
            mm1 = 12
    else:
        mm1 = mm
    
    # MM2 = MM - 1; IF MM2 = 0 THEN MM2 = 12;
    mm2 = mm - 1
    if mm2 == 0:
        mm2 = 12
    
    # SDATE = MDY(MM,SDD,YEAR(REPTDATE))
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
        df = df.with_columns(pl.col('acctno').cast(pl.Utf8).str.strip())
    return df

def load_float():
    """DATA FLOAT; SET PIDMS.FLOAT; RUN;
       PROC SUMMARY DATA=FLOAT NWAY; CLASS ACCTNO; VAR FLOAT; OUTPUT OUT=FLOAT SUM=; RUN;"""
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['PIDMS']}float.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        # Standardize acctno
        df = standardize_acctno(df)
        
        # PROC SUMMARY equivalent
        result = df.group_by('acctno').agg([
            pl.col('float').cast(pl.Float64, strict=False).fill_null(0).sum().alias('float')
        ])
        return result
    except Exception as e:
        print(f"Error loading FLOAT: {e}")
        return pl.DataFrame()

def load_ibgpidm():
    """DATA IBGPIDM; INFILE IBGPIDM FIRSTOBS=1; INPUT @01 ACCTNO 10. @12 IBGAMT 16.2; RUN;
       PROC SORT; BY ACCTNO;
       PROC SUMMARY DATA=IBGPIDM NWAY; BY ACCTNO; VAR IBGAMT; OUTPUT OUT=DEPOSIT.IBGPIDM SUM=; RUN;"""
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['DEPOSIT']}ibgpidm.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        # Standardize acctno
        df = standardize_acctno(df)
        
        # PROC SUMMARY equivalent
        result = df.group_by('acctno').agg([
            pl.col('ibgamt').cast(pl.Float64, strict=False).fill_null(0).sum().alias('ibgamt')
        ])
        return result
    except Exception as e:
        print(f"Error loading IBGPIDM: {e}")
        return pl.DataFrame()

def load_remit(d):
    """DATA REMIT; SET DEPOSIT.REMIT UNCLAIM.UNCLAIM&REPTYEAR(RENAME=(LEDGBAL=UNCLAIMX)); RUN;
       PROC SUMMARY DATA=REMIT NWAY; CLASS PAYMODE; VAR LEDGBAL UNCLAIMX; OUTPUT OUT=REMIT SUM=PLUSBAL UNCLAIM; RUN;
       PROC SORT DATA=DEPOSIT.REMIT OUT=REMITORI NODUPKEYS; BY PAYMODE; RUN;
       DATA REMIT; MERGE REMIT REMITORI; BY PAYMODE; RUN;
       DATA REMIT; SET REMIT; FORMAT ACCTNO 10.; ACCTNO = PAYMODE; DROP PAYMODE LEDGBAL UNCLAIMX; RUN;"""
    try:
        # Load REMIT
        remit, meta = pyreadstat.read_sas7bdat(f"{PATHS['DEPOSIT']}remit.sas7bdat")
        remit = pl.DataFrame(remit)
        remit.columns = [col.lower() for col in remit.columns]
        
        # Load UNCLAIM
        unclaim_file = f"{PATHS['UNCLAIM']}unclaim{d['reptyear']}.sas7bdat"
        if not Path(unclaim_file).exists():
            unclaim_file = f"{PATHS['UNCLAIM']}unclaim.sas7bdat"
        
        unclaim, meta = pyreadstat.read_sas7bdat(unclaim_file)
        unclaim = pl.DataFrame(unclaim)
        unclaim.columns = [col.lower() for col in unclaim.columns]
        
        # Rename LEDGBAL to UNCLAIMX in unclaim
        if 'ledgbal' in unclaim.columns:
            unclaim = unclaim.rename({'ledgbal': 'unclaimx'})
        
        # Ensure both have consistent types for concatenation
        if 'paymode' in remit.columns:
            remit = remit.with_columns(pl.col('paymode').cast(pl.Utf8).str.strip())
        if 'paymode' in unclaim.columns:
            unclaim = unclaim.with_columns(pl.col('paymode').cast(pl.Utf8).str.strip())
        
        if 'ledgbal' in remit.columns:
            remit = remit.with_columns(pl.col('ledgbal').cast(pl.Float64, strict=False).fill_null(0))
        else:
            remit = remit.with_columns(pl.lit(0.0).alias('ledgbal'))
        
        if 'unclaimx' in unclaim.columns:
            unclaim = unclaim.with_columns(pl.col('unclaimx').cast(pl.Float64, strict=False).fill_null(0))
        else:
            unclaim = unclaim.with_columns(pl.lit(0.0).alias('unclaimx'))
        
        # Add unclaimx to remit if not exists
        if 'unclaimx' not in remit.columns:
            remit = remit.with_columns(pl.lit(0.0).alias('unclaimx'))
        
        # Add ledgbal to unclaim if not exists
        if 'ledgbal' not in unclaim.columns:
            unclaim = unclaim.with_columns(pl.lit(0.0).alias('ledgbal'))
        
        # Select common columns and concatenate (SET DEPOSIT.REMIT UNCLAIM.UNCLAIM)
        common_cols = ['paymode', 'ledgbal', 'unclaimx']
        remit_subset = remit.select(common_cols)
        unclaim_subset = unclaim.select(common_cols)
        combined = pl.concat([remit_subset, unclaim_subset])
        
        # PROC SUMMARY DATA=REMIT NWAY; CLASS PAYMODE; VAR LEDGBAL UNCLAIMX; OUTPUT SUM=PLUSBAL UNCLAIM
        summary = combined.group_by('paymode').agg([
            pl.col('ledgbal').sum().alias('plusbal'),
            pl.col('unclaimx').sum().alias('unclaim')
        ])
        
        # PROC SORT DATA=DEPOSIT.REMIT OUT=REMITORI NODUPKEYS; BY PAYMODE
        remitori = remit.unique(subset=['paymode'])
        
        # DATA REMIT; MERGE REMIT REMITORI; BY PAYMODE
        result = summary.join(remitori, on='paymode', how='left')
        
        # DATA REMIT; SET REMIT; ACCTNO = PAYMODE; DROP PAYMODE LEDGBAL UNCLAIMX
        result = result.with_columns(pl.col('paymode').alias('acctno'))
        
        # Standardize acctno
        result = standardize_acctno(result)
        
        # Keep only needed columns
        keep_cols = ['acctno', 'plusbal', 'unclaim']
        for col in keep_cols:
            if col not in result.columns:
                result = result.with_columns(pl.lit(0.0).alias(col))
        
        return result.select(keep_cols)
    except Exception as e:
        print(f"Error loading REMIT/UNCLAIM: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame()

def load_saca():
    """DATA SA; SET SACA.SAVING; WHERE PURPOSE IN ('5','6'); KEEP BRANCH ACCTNO NAME PURPOSE PRODUCT CURBAL INTPAYBL; RUN;
       DATA CA; SET SACA.CURRENT; WHERE PURPOSE IN ('5','6'); IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE,.01); KEEP...; RUN;
       DATA FD; SET SACA.FD; WHERE PURPOSE IN ('5','6'); IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE,.01); KEEP...; RUN;
       DATA DEPOSIX.MERGE; SET SA CA FD; RUN;"""
    
    dfs = []
    
    # Load SAVING
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}saving.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        # Standardize acctno
        df = standardize_acctno(df)
        
        # WHERE PURPOSE IN ('5','6')
        df = df.filter(pl.col('purpose').cast(pl.Utf8).is_in(['5', '6']))
        
        # Ensure intpaybl exists
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        # KEEP BRANCH ACCTNO NAME PURPOSE PRODUCT CURBAL INTPAYBL
        keep_cols = ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
        for col in keep_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading SAVING: {e}")
    
    # Load CURRENT
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}current.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        # Standardize acctno
        df = standardize_acctno(df)
        
        # WHERE PURPOSE IN ('5','6')
        df = df.filter(pl.col('purpose').cast(pl.Utf8).is_in(['5', '6']))
        
        # Ensure intpaybl exists
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        # Cast to float
        df = df.with_columns(pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0))
        
        # IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE,.01)
        if 'curcode' in df.columns and 'forate' in df.columns:
            df = df.with_columns([
                pl.when(pl.col('curcode').cast(pl.Utf8) != 'MYR')
                  .then((pl.col('intpaybl') * pl.col('forate').cast(pl.Float64, strict=False).fill_null(1)).round(2))
                  .otherwise(pl.col('intpaybl')).alias('intpaybl')
            ])
        
        # KEEP BRANCH ACCTNO NAME PURPOSE PRODUCT CURBAL INTPAYBL
        keep_cols = ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
        for col in keep_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading CURRENT: {e}")
    
    # Load FD
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}fd.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        # Standardize acctno
        df = standardize_acctno(df)
        
        # WHERE PURPOSE IN ('5','6')
        df = df.filter(pl.col('purpose').cast(pl.Utf8).is_in(['5', '6']))
        
        # FD uses INTPAY instead of INTPAYBL
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0.0).alias('intpaybl'))
        
        # Cast to float
        df = df.with_columns(pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0))
        
        # IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE,.01)
        if 'curcode' in df.columns and 'forate' in df.columns:
            df = df.with_columns([
                pl.when(pl.col('curcode').cast(pl.Utf8) != 'MYR')
                  .then((pl.col('intpaybl') * pl.col('forate').cast(pl.Float64, strict=False).fill_null(1)).round(2))
                  .otherwise(pl.col('intpaybl')).alias('intpaybl')
            ])
        
        # KEEP BRANCH ACCTNO NAME PURPOSE PRODUCT CURBAL INTPAYBL
        keep_cols = ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
        for col in keep_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        dfs.append(df.select(keep_cols))
    except Exception as e:
        print(f"Error loading FD: {e}")
    
    # DATA DEPOSIX.MERGE; SET SA CA FD; RUN;
    if dfs:
        result = pl.concat(dfs)
        # Ensure acctno is string type
        result = standardize_acctno(result)
        return result
    else:
        return pl.DataFrame()

def load_dep(d):
    """DATA DEP; SET DEP.SAVG&REPTMON&NOWK(KEEP=ACCTNO AMTIND PRODCD PRODUCT)
                        DEP.CURN&REPTMON&NOWK(KEEP=ACCTNO AMTIND PRODCD PRODUCT)
                        DEP.FDMTHLY(KEEP=ACCTNO AMTIND BIC ACCTTYPE RENAME=(BIC=PRODCD ACCTTYPE=PRODUCT));
      IF PRODCD IN (...); IF PRODCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE; RUN;"""
    try:
        dfs = []
        
        # DEP.SAVG&REPTMON&NOWK
        savg_file = f"{PATHS['DEPOSIT']}savg{d['reptmon']}{d['nowk']}.sas7bdat"
        if Path(savg_file).exists():
            df, meta = pyreadstat.read_sas7bdat(savg_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            # Standardize acctno
            df = standardize_acctno(df)
            
            keep_cols = ['acctno', 'amtind', 'prodcd', 'product']
            for col in keep_cols:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))
            dfs.append(df.select(keep_cols))
        
        # DEP.CURN&REPTMON&NOWK
        curn_file = f"{PATHS['DEPOSIT']}curn{d['reptmon']}{d['nowk']}.sas7bdat"
        if Path(curn_file).exists():
            df, meta = pyreadstat.read_sas7bdat(curn_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            # Standardize acctno
            df = standardize_acctno(df)
            
            keep_cols = ['acctno', 'amtind', 'prodcd', 'product']
            for col in keep_cols:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))
            dfs.append(df.select(keep_cols))
        
        # DEP.FDMTHLY (with rename)
        fdmthly_file = f"{PATHS['DEPOSIT']}fdmthly.sas7bdat"
        if Path(fdmthly_file).exists():
            df, meta = pyreadstat.read_sas7bdat(fdmthly_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            # Standardize acctno
            df = standardize_acctno(df)
            
            # RENAME=(BIC=PRODCD ACCTTYPE=PRODUCT)
            if 'bic' in df.columns:
                df = df.rename({'bic': 'prodcd'})
            if 'accttype' in df.columns:
                df = df.rename({'accttype': 'product'})
            
            keep_cols = ['acctno', 'amtind', 'prodcd', 'product']
            for col in keep_cols:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))
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
        return standardize_acctno(result)
    except Exception as e:
        print(f"Error loading DEP: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame()

def load_client():
    """DATA DEPOSIT.CLIENT; INFILE CLIENT; INPUT @002 ACCTNO 10. @;
       IF COMPRESS(ACCTNO, "1234567890") = ' ' THEN DO; INPUT @021 NAME $40.; OUTPUT; END;
       KEY = SUBSTR(NAME,1,10); RUN;"""
    try:
        client_file = f"{PATHS['DEPOSIT']}client.sas7bdat"
        if Path(client_file).exists():
            df, meta = pyreadstat.read_sas7bdat(client_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            if 'acctno' not in df.columns:
                print("Error: 'acctno' column not found in CLIENT file")
                return pl.DataFrame()
            
            # Standardize acctno
            df = standardize_acctno(df)
            
            # Add KEY = SUBSTR(NAME,1,10)
            if 'name' in df.columns:
                df = df.with_columns(pl.col('name').cast(pl.Utf8).str.slice(0, 10).alias('key'))
            
            # PROC SORT DATA=DEPOSIT.CLIENT NODUPKEYS; BY ACCTNO
            return df.unique(subset=['acctno'])
        else:
            print(f"Warning: CLIENT SAS file not found at {client_file}")
            return pl.DataFrame()
    except Exception as e:
        print(f"Error loading CLIENT: {e}")
        return pl.DataFrame()

# =============================================================================
# MAIN PROCESSING - Following SAS logic exactly
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
    
    saca_df = load_saca()
    print(f"  SA/CA/FD: {len(saca_df)} records")
    
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
    
    if not saca_df.is_empty():
        # DATA DEPOSIX.MERGE; MERGE DEPOSIX.MERGE(IN=A) FLOAT(IN=B); BY ACCTNO;
        # AVBAL = SUM(CURBAL,(-1)*FLOAT); AVBALTT = SUM(AVBAL,INTPAYBL); IF A; RUN;
        if not float_df.is_empty():
            trustee = saca_df.join(float_df, on='acctno', how='left')
        else:
            trustee = saca_df.with_columns(pl.lit(0.0).alias('float'))
        
        trustee = trustee.with_columns([
            pl.col('float').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0)
        ])
        
        trustee = trustee.with_columns([
            (pl.col('curbal') - pl.col('float')).alias('avbal'),
            (pl.col('curbal') - pl.col('float') + pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0)).alias('avbaltt')
        ])
        
        # DATA DEPOSIX.MERGE; MERGE MERGEX (IN=A) DEP (IN=B); BY ACCTNO; IF A & B; RUN;
        if not dep_df.is_empty():
            trustee = trustee.join(dep_df, on='acctno', how='inner')
        else:
            print("  Warning: No DEP data, skipping trustee processing")
            trustee = pl.DataFrame()
        
        if not trustee.is_empty():
            # DATA DEPOSIX.MERGE; MERGE MERGE(IN=A) REMIT; BY ACCTNO;
            # AVBALTT = SUM(AVBAL,PLUSBAL,UNCLAIM,INTPAYBL); IF A; RUN;
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
            
            # DATA DEPOSIX.MERGE; SET DEPOSIX.MERGE; SI = 0; AVBALTT = SUM(AVBALTT,SI); RUN;
            trustee = trustee.with_columns(pl.lit(0).alias('si'))
            trustee = trustee.with_columns((pl.col('avbaltt') + pl.col('si')).alias('avbaltt'))
            
            # DATA DEPOSIX.MERGE; MERGE DEPOSIX.MERGE(IN=A) DEPOSIT.IBGPIDM(IN=B RENAME=(IBGAMT=IBGAMTX));
            # BY ACCTNO; IF A; IBGAMT = IBGAMTX; AVBALTT = SUM(AVBALTT,IBGAMT); DROP IBGAMTX; RUN;
            if not ibg_df.is_empty():
                trustee = trustee.join(ibg_df, on='acctno', how='left')
            else:
                trustee = trustee.with_columns(pl.lit(0.0).alias('ibgamt'))
            
            trustee = trustee.with_columns([
                pl.col('ibgamt').cast(pl.Float64, strict=False).fill_null(0),
                (pl.col('avbaltt') + pl.col('ibgamt').cast(pl.Float64, strict=False).fill_null(0)).alias('avbaltt')
            ])
            
            # Split by threshold (Note: SAS uses 60000 in PROC PRINT WHERE clause)
            trustee_high = trustee.filter(pl.col('avbaltt') > 60000)
            trustee_low = trustee.filter(pl.col('avbaltt') <= 60000)
            
            print(f"\nTrustee >60k: {len(trustee_high)} accounts")
            print(f"Trustee <=60k: {len(trustee_low)} accounts")
            
            # Write output files (following SAS DATA _NULL_ with FILE FC2B)
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
            
            # Print summary by branch (PROC PRINT equivalent)
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
    
    if (not client_df.is_empty() and not saca_df.is_empty() and 
        not dep_df.is_empty()):
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) DEPOSIT(IN=B); BY ACCTNO; IF A & B; RUN;
        client = client_df.join(saca_df, on='acctno', how='inner')
        
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
            
            # Merge with DEP
            client = client.join(dep_df, on='acctno', how='inner')
            
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
                    line = (f"{row.get('branch','')};{row.get('acctno','')};{row.get('name','')};"
                           f"{row.get('purpose','')};{row.get('avbal',0):.2f};{row.get('intpaybl',0):.2f};"
                           f"{row.get('product','')};{row.get('amtind','')};{row.get('plusbal',0):.2f};"
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
        
        # DATA DUPLI; SET DEPOSIX.MERGE DEPOSIT.CLIENT; RUN;
        all_acc = pl.concat([
            trustee.select(['acctno']).with_columns(pl.lit('TRUSTEE').alias('source')),
            client.select(['acctno']).with_columns(pl.lit('CLIENT').alias('source'))
        ])
        
        # PROC SORT DATA=DUPLI; BY ACCTNO;
        # DATA DUPLI DUPLI2; SET DUPLI; BY ACCTNO; IF FIRST.ACCTNO THEN OUTPUT DUPLI; ELSE OUTPUT DUPLI2; RUN;
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
