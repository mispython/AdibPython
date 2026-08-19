"""
EIBQINST - Trustee and Client Account Quarterly Reporting
Processes trustee and client accounts with balance thresholds (>60k/<=60k)
Includes PBBDPFMT format mappings for product codes
"""

import polars as pl
import pyreadstat
from datetime import datetime, timedelta
from pathlib import Path

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
# DATE PROCESSING
# =============================================================================
def get_dates():
    """Calculate report dates and week parameters"""
    today = datetime.now().date()
    # Last day of previous month
    reptdate = (datetime(today.year, today.month, 1) - timedelta(days=1)).date()
    
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
    mm1 = mm - 1 if wk == '1' and mm > 1 else (12 if wk == '1' and mm == 1 else mm)
    mm2 = mm - 1 if mm > 1 else 12
    
    sdate = datetime(reptdate.year, mm, sdd).date()
    
    return {
        'reptdate': reptdate,
        'nowk': wk,
        'reptmon': f"{mm:02d}",
        'reptyear': str(reptdate.year),
        'sdate': sdate,
        'sdesc': 'PUBLIC BANK BERHAD'
    }

# =============================================================================
# DATA LOADING
# =============================================================================
def load_float():
    """Load FLOAT data from PIDMS"""
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['PIDMS']}float.sas7bdat")
        df = pl.DataFrame(df)
        # Convert column names to lowercase
        df.columns = [col.lower() for col in df.columns]
        return df.group_by('acctno').agg(pl.col('float').sum())
    except Exception as e:
        print(f"Error loading FLOAT: {e}")
        return pl.DataFrame()

def load_ibgpidm():
    """Load IBGPIDM data from SAS dataset"""
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['DEPOSIT']}ibgpidm.sas7bdat")
        df = pl.DataFrame(df)
        # Convert column names to lowercase
        df.columns = [col.lower() for col in df.columns]
        return df.group_by('acctno').agg(pl.col('ibgamt').sum())
    except Exception as e:
        print(f"Error loading IBGPIDM: {e}")
        return pl.DataFrame()

def load_remit():
    """Load REMIT and UNCLAIM data"""
    try:
        remit, meta = pyreadstat.read_sas7bdat(f"{PATHS['DEPOSIT']}remit.sas7bdat")
        remit = pl.DataFrame(remit)
        remit.columns = [col.lower() for col in remit.columns]
        
        # Add missing columns if they don't exist
        if 'unclaimx' not in remit.columns:
            remit = remit.with_columns(pl.lit(0).alias('unclaimx'))
        
        unclaim, meta = pyreadstat.read_sas7bdat(f"{PATHS['UNCLAIM']}unclaim.sas7bdat")
        unclaim = pl.DataFrame(unclaim)
        unclaim.columns = [col.lower() for col in unclaim.columns]
        
        # Rename and ensure same columns
        if 'ledgbal' in unclaim.columns:
            unclaim = unclaim.rename({'ledgbal': 'unclaimx'})
        else:
            unclaim = unclaim.with_columns(pl.lit(0).alias('unclaimx'))
        
        # Add missing columns to unclaim to match remit
        for col in remit.columns:
            if col not in unclaim.columns:
                unclaim = unclaim.with_columns(pl.lit(None).alias(col))
        
        # Add missing columns to remit to match unclaim
        for col in unclaim.columns:
            if col not in remit.columns:
                remit = remit.with_columns(pl.lit(None).alias(col))
        
        # Ensure ledgbal exists in both
        if 'ledgbal' not in remit.columns:
            remit = remit.with_columns(pl.lit(0).alias('ledgbal'))
        if 'ledgbal' not in unclaim.columns:
            unclaim = unclaim.with_columns(pl.lit(0).alias('ledgbal'))
        
        # Ensure paymode exists in both
        if 'paymode' not in remit.columns:
            remit = remit.with_columns(pl.lit(None).alias('paymode'))
        if 'paymode' not in unclaim.columns:
            unclaim = unclaim.with_columns(pl.lit(None).alias('paymode'))
        
        combined = pl.concat([remit, unclaim])
        
        # Handle null paymode
        combined = combined.filter(pl.col('paymode').is_not_null())
        
        if combined.is_empty():
            return pl.DataFrame()
        
        summary = combined.group_by('paymode').agg([
            pl.col('ledgbal').sum().alias('plusbal'),
            pl.col('unclaimx').sum().alias('unclaim')
        ])
        
        # Get original for other fields
        orig = remit.unique(subset=['paymode'])
        result = summary.join(orig, on='paymode', how='left')
        result = result.with_columns(pl.col('paymode').alias('acctno'))
        return result
    except Exception as e:
        print(f"Error loading REMIT/UNCLAIM: {e}")
        return pl.DataFrame()

def load_saving():
    """Load SAVING accounts with purpose 5/6"""
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}saving.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        # Check if intpaybl exists, if not use intpay or create it
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0).alias('intpaybl'))
        
        # Select only needed columns
        needed_cols = ['branch','acctno','name','purpose','product','curbal','intpaybl']
        for col in needed_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        
        return df.filter(pl.col('purpose').cast(pl.Utf8).is_in(['5','6'])) \
                 .select(needed_cols)
    except Exception as e:
        print(f"Error loading SAVING: {e}")
        return pl.DataFrame()

def load_current():
    """Load CURRENT accounts with purpose 5/6 and FX conversion"""
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}current.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        # Check if intpaybl exists, if not use intpay or create it
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0).alias('intpaybl'))
        
        # Cast numeric columns
        df = df.with_columns([
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('forate').cast(pl.Float64, strict=False).fill_null(1)
        ])
        
        # Apply FX conversion
        df = df.with_columns([
            pl.when(pl.col('curcode').cast(pl.Utf8) != 'MYR')
              .then((pl.col('intpaybl') * pl.col('forate')).round(2))
              .otherwise(pl.col('intpaybl')).alias('intpaybl')
        ])
        
        # Select only needed columns
        needed_cols = ['branch','acctno','name','purpose','product','curbal','intpaybl']
        for col in needed_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        
        return df.filter(pl.col('purpose').cast(pl.Utf8).is_in(['5','6'])) \
                 .select(needed_cols)
    except Exception as e:
        print(f"Error loading CURRENT: {e}")
        return pl.DataFrame()

def load_fd():
    """Load FD accounts with purpose 5/6 and FX conversion"""
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}fd.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        
        # Check if intpaybl exists, if not use intpay or create it
        if 'intpaybl' not in df.columns:
            if 'intpay' in df.columns:
                df = df.rename({'intpay': 'intpaybl'})
            else:
                df = df.with_columns(pl.lit(0).alias('intpaybl'))
        
        # Check if curbal exists
        if 'curbal' not in df.columns:
            if 'curbalus' in df.columns:
                df = df.rename({'curbalus': 'curbal'})
            else:
                df = df.with_columns(pl.lit(0).alias('curbal'))
        
        # Cast numeric columns
        df = df.with_columns([
            pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0),
            pl.col('forate').cast(pl.Float64, strict=False).fill_null(1),
            pl.col('curbal').cast(pl.Float64, strict=False).fill_null(0)
        ])
        
        # Apply FX conversion
        df = df.with_columns([
            pl.when(pl.col('curcode').cast(pl.Utf8) != 'MYR')
              .then((pl.col('intpaybl') * pl.col('forate')).round(2))
              .otherwise(pl.col('intpaybl')).alias('intpaybl')
        ])
        
        # Select only needed columns
        needed_cols = ['branch','acctno','name','product','purpose','curbal','intpaybl']
        for col in needed_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        
        return df.filter(pl.col('purpose').cast(pl.Utf8).is_in(['5','6'])) \
                 .select(needed_cols)
    except Exception as e:
        print(f"Error loading FD: {e}")
        return pl.DataFrame()

def load_dep(d):
    """Load DEP data from monthly files"""
    try:
        dfs = []
        
        # SAVG
        savg_file = f"{PATHS['DEPOSIT']}savg{d['reptmon']}{d['nowk']}.sas7bdat"
        if Path(savg_file).exists():
            df, meta = pyreadstat.read_sas7bdat(savg_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            # Ensure required columns exist
            for col in ['acctno','amtind','prodcd','product']:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))
            
            dfs.append(df.select(['acctno','amtind','prodcd','product']))
        
        # CURN
        curn_file = f"{PATHS['DEPOSIT']}curn{d['reptmon']}{d['nowk']}.sas7bdat"
        if Path(curn_file).exists():
            df, meta = pyreadstat.read_sas7bdat(curn_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            # Ensure required columns exist
            for col in ['acctno','amtind','prodcd','product']:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))
            
            dfs.append(df.select(['acctno','amtind','prodcd','product']))
        
        # FDMTHLY
        fd_file = f"{PATHS['DEPOSIT']}fdmthly.sas7bdat"
        if Path(fd_file).exists():
            df, meta = pyreadstat.read_sas7bdat(fd_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            # Rename columns if needed
            if 'bic' in df.columns and 'prodcd' not in df.columns:
                df = df.rename({'bic': 'prodcd'})
            if 'accttype' in df.columns and 'product' not in df.columns:
                df = df.rename({'accttype': 'product'})
            
            # Ensure required columns exist
            for col in ['acctno','amtind','prodcd','product']:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))
            
            dfs.append(df.select(['acctno','amtind','prodcd','product']))
        
        if not dfs:
            return pl.DataFrame()
        
        combined = pl.concat(dfs)
        # Filter product codes
        combined = combined.filter(pl.col('prodcd').cast(pl.Utf8).is_in(PROD_CODES))
        # Special filter for 42199/42699
        combined = combined.filter(
            ~((pl.col('prodcd').cast(pl.Utf8).is_in(['42199','42699'])) & 
              (~pl.col('product').cast(pl.Int64, strict=False).is_in([72,413])))
        )
        return combined.unique(subset=['acctno'])
    except Exception as e:
        print(f"Error loading DEP: {e}")
        return pl.DataFrame()

def load_client():
    """Load CLIENT file"""
    try:
        # Try SAS format
        client_file = f"{PATHS['DEPOSIT']}client.sas7bdat"
        if Path(client_file).exists():
            df, meta = pyreadstat.read_sas7bdat(client_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            
            # Ensure required columns exist
            if 'acctno' not in df.columns:
                print("Error: 'acctno' column not found in CLIENT file")
                return pl.DataFrame()
            
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
    
    # ========== PART 1: TRUSTEE ACCOUNTS ==========
    print("\nProcessing Trustee Accounts...")
    
    # Load FLOAT
    float_df = load_float()
    print(f"  FLOAT: {len(float_df)} records loaded")
    
    # Load IBGPIDM
    ibg_df = load_ibgpidm()
    print(f"  IBGPIDM: {len(ibg_df)} records loaded")
    
    # Load REMIT/UNCLAIM
    remit_df = load_remit()
    print(f"  REMIT: {len(remit_df)} records loaded")
    
    # Load SA/CA/FD
    sa = load_saving()
    ca = load_current()
    fd = load_fd()
    
    # Concatenate non-empty DataFrames
    dfs_to_concat = []
    if not sa.is_empty():
        dfs_to_concat.append(sa)
    if not ca.is_empty():
        dfs_to_concat.append(ca)
    if not fd.is_empty():
        dfs_to_concat.append(fd)
    
    if dfs_to_concat:
        saca = pl.concat(dfs_to_concat)
    else:
        saca = pl.DataFrame()
    
    print(f"  SA/CA/FD: {len(saca)} records loaded")
    print(f"    SA: {len(sa)}, CA: {len(ca)}, FD: {len(fd)}")
    
    trustee = None
    client = None
    
    if not saca.is_empty():
        # Merge with FLOAT
        if not float_df.is_empty():
            trustee = saca.join(float_df, on='acctno', how='left')
            trustee = trustee.with_columns([
                pl.col('float').fill_null(0),
                (pl.col('curbal').fill_null(0) - pl.col('float').fill_null(0)).alias('avbal')
            ])
        else:
            trustee = saca.with_columns([
                pl.lit(0).alias('float'),
                pl.col('curbal').fill_null(0).alias('avbal')
            ])
        
        # Merge with DEP
        dep = load_dep(d)
        print(f"  DEP: {len(dep)} records loaded")
        
        if not dep.is_empty():
            trustee = trustee.join(dep, on='acctno', how='inner')
        else:
            print("  Warning: No DEP data found, skipping trustee processing")
            trustee = pl.DataFrame()
        
        if not trustee.is_empty():
            # Merge with REMIT
            if not remit_df.is_empty():
                trustee = trustee.join(remit_df, on='acctno', how='left')
                trustee = trustee.with_columns([
                    pl.col('plusbal').fill_null(0),
                    pl.col('unclaim').fill_null(0)
                ])
            else:
                trustee = trustee.with_columns([
                    pl.lit(0).alias('plusbal'),
                    pl.lit(0).alias('unclaim')
                ])
            
            # Calculate AVBALTT
            trustee = trustee.with_columns([
                (pl.col('avbal') + pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0) + 
                 pl.col('plusbal') + pl.col('unclaim')).alias('avbaltt')
            ])
            
            # Add SI (always 0)
            trustee = trustee.with_columns(pl.lit(0).alias('si'))
            trustee = trustee.with_columns((pl.col('avbaltt') + pl.col('si')).alias('avbaltt'))
            
            # Add IBGAMT
            if not ibg_df.is_empty():
                trustee = trustee.join(ibg_df, on='acctno', how='left')
                trustee = trustee.with_columns([
                    pl.col('ibgamt').fill_null(0),
                    (pl.col('avbaltt') + pl.col('ibgamt').fill_null(0)).alias('avbaltt')
                ])
            else:
                trustee = trustee.with_columns([
                    pl.lit(0).alias('ibgamt'),
                    pl.col('avbaltt').alias('avbaltt')
                ])
            
            # Split by threshold
            trustee_high = trustee.filter(pl.col('avbaltt') > 60000)
            trustee_low = trustee.filter(pl.col('avbaltt') <= 60000)
            
            print(f"  Trustee >60k: {len(trustee_high)} accounts")
            print(f"  Trustee <=60k: {len(trustee_low)} accounts")
            
            # Write text file outputs
            def write_txt(df, title, filename):
                if df.is_empty(): return
                lines = [f"{title}\n", "BRANCH;ACCTNO;NAME;PURPOSE;AVBAL;INTPAYBL;PRODUCT;AMTIND;PLUSBAL;UNCLAIM;SI;IBGAMT;AVBALTT\n"]
                for r in df.rows(named=True):
                    lines.append(
                        f"{r.get('branch','')};{r.get('acctno','')};{r.get('name','')};{r.get('purpose','')};"
                        f"{r.get('avbal',0):.2f};{r.get('intpaybl',0):.2f};{r.get('product','')};{r.get('amtind','')};"
                        f"{r.get('plusbal',0):.2f};{r.get('unclaim',0):.2f};{r.get('si',0):.2f};{r.get('ibgamt',0):.2f};{r.get('avbaltt',0):.2f}\n"
                    )
                output_file = Path(f"{PATHS['OUTPUT']}{filename}")
                output_file.write_text(''.join(lines))
                print(f"  Written to {output_file}")
            
            write_txt(trustee_high, "TRUSTEE >60000", "trustee_high.txt")
            write_txt(trustee_low, "TRUSTEE <=60000", "trustee_low.txt")
            
            # Print reports
            if not trustee_high.is_empty():
                print("\nTRUSTEE >60000 by Branch:")
                for r in trustee_high.group_by('branch').agg(pl.col('avbaltt').sum()).sort('branch').rows():
                    print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
            
            if not trustee_low.is_empty():
                print("\nTRUSTEE <=60000 by Branch:")
                for r in trustee_low.group_by('branch').agg(pl.col('avbaltt').sum()).sort('branch').rows():
                    print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
    
    # ========== PART 2: CLIENT ACCOUNTS ==========
    print("\nProcessing Client Accounts...")
    
    client_df = load_client()
    print(f"  CLIENT master: {len(client_df)} records loaded")
    
    if (not client_df.is_empty() and not saca.is_empty() and 
        'dep' in locals() and not dep.is_empty()):
        # Merge client with deposit data
        client = client_df.join(saca, on='acctno', how='inner')
        
        if not float_df.is_empty():
            client = client.join(float_df, on='acctno', how='left')
            client = client.with_columns([
                pl.col('float').fill_null(0),
                (pl.col('curbal').fill_null(0) - pl.col('float').fill_null(0)).alias('avbal')
            ])
        else:
            client = client.with_columns([
                pl.lit(0).alias('float'),
                pl.col('curbal').fill_null(0).alias('avbal')
            ])
        
        # Merge with DEP
        client = client.join(dep, on='acctno', how='inner')
        
        # Merge with REMIT
        if not remit_df.is_empty():
            client = client.join(remit_df, on='acctno', how='left')
            client = client.with_columns([
                pl.col('plusbal').fill_null(0),
                pl.col('unclaim').fill_null(0)
            ])
        else:
            client = client.with_columns([
                pl.lit(0).alias('plusbal'),
                pl.lit(0).alias('unclaim')
            ])
        
        # Calculate AVBALTT
        client = client.with_columns([
            (pl.col('avbal') + pl.col('intpaybl').cast(pl.Float64, strict=False).fill_null(0) + 
             pl.col('plusbal') + pl.col('unclaim')).alias('avbaltt')
        ])
        
        # Add SI
        client = client.with_columns(pl.lit(0).alias('si'))
        client = client.with_columns((pl.col('avbaltt') + pl.col('si')).alias('avbaltt'))
        
        # Add IBGAMT
        if not ibg_df.is_empty():
            client = client.join(ibg_df, on='acctno', how='left')
            client = client.with_columns([
                pl.col('ibgamt').fill_null(0),
                (pl.col('avbaltt') + pl.col('ibgamt').fill_null(0)).alias('avbaltt')
            ])
        else:
            client = client.with_columns([
                pl.lit(0).alias('ibgamt'),
                pl.col('avbaltt').alias('avbaltt')
            ])
        
        # Split by threshold
        client_high = client.filter(pl.col('avbaltt') > 60000)
        client_low = client.filter(pl.col('avbaltt') <= 60000)
        
        print(f"  Client >60k: {len(client_high)} accounts")
        print(f"  Client <=60k: {len(client_low)} accounts")
        
        # Write text file outputs
        def write_client_txt(df, title, filename):
            if df.is_empty(): return
            lines = [f"{title}\n", "BRANCH;ACCTNO;NAME;PURPOSE;AVBAL;INTPAYBL;PRODUCT;AMTIND;PLUSBAL;UNCLAIM;SI;IBGAMT;AVBALTT\n"]
            for r in df.rows(named=True):
                lines.append(
                    f"{r.get('branch','')};{r.get('acctno','')};{r.get('name','')};{r.get('purpose','')};"
                    f"{r.get('avbal',0):.2f};{r.get('intpaybl',0):.2f};{r.get('product','')};{r.get('amtind','')};"
                    f"{r.get('plusbal',0):.2f};{r.get('unclaim',0):.2f};{r.get('si',0):.2f};{r.get('ibgamt',0):.2f};{r.get('avbaltt',0):.2f}\n"
                )
            output_file = Path(f"{PATHS['OUTPUT']}{filename}")
            output_file.write_text(''.join(lines))
            print(f"  Written to {output_file}")
        
        write_client_txt(client_high, "CLIENT >60000", "client_high.txt")
        write_client_txt(client_low, "CLIENT <=60000", "client_low.txt")
        
        # Print reports
        if not client_high.is_empty():
            print("\nCLIENT >60000 by Branch:")
            for r in client_high.group_by('branch').agg(pl.col('avbaltt').sum()).sort('branch').rows():
                print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
        
        if not client_low.is_empty():
            print("\nCLIENT <=60000 by Branch:")
            for r in client_low.group_by('branch').agg(pl.col('avbaltt').sum()).sort('branch').rows():
                print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
    
    # ========== PART 3: DUPLICATE ACCOUNTS ==========
    print("\nChecking for duplicate accounts...")
    if (trustee is not None and client is not None and 
        not trustee.is_empty() and not client.is_empty()):
        all_acc = pl.concat([
            trustee.select(['acctno']).with_columns(pl.lit('TRUSTEE').alias('src')),
            client.select(['acctno']).with_columns(pl.lit('CLIENT').alias('src'))
        ])
        
        dup = all_acc.group_by('acctno').agg([
            pl.col('src').alias('sources'),
            pl.count().alias('count')
        ]).filter(pl.col('count') > 1)
        
        if not dup.is_empty():
            print(f"  Found {len(dup)} duplicate accounts:")
            for r in dup.rows(named=True):
                print(f"    {r['acctno']} appears in: {', '.join(r['sources'])}")
        else:
            print("  No duplicate accounts found")
    
    # ========== SUMMARY ==========
    print("\n"+"="*60)
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
    
    print("\n"+"="*60)
    print("✓ EIBQINST Complete")

if __name__ == "__main__":
    main()
