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
    """Load IBGPIDM data from text file"""
    try:
        # Read text file with fixed width format
        df = pl.read_csv(
            f"{PATHS['DEPOSIT']}ibgpidm.txt",
            has_header=False,
            separator='\t'  # Adjust separator as needed
        )
        # Assume first column is ACCTNO and second is IBGAMT
        if df.width >= 2:
            df.columns = ['acctno', 'ibgamt'] + [f'col_{i}' for i in range(2, df.width)]
            return df.group_by('acctno').agg(pl.col('ibgamt').sum())
        return pl.DataFrame()
    except Exception as e:
        print(f"Error loading IBGPIDM: {e}")
        return pl.DataFrame()

def load_remit():
    """Load REMIT and UNCLAIM data"""
    try:
        remit, meta = pyreadstat.read_sas7bdat(f"{PATHS['DEPOSIT']}remit.sas7bdat")
        remit = pl.DataFrame(remit)
        remit.columns = [col.lower() for col in remit.columns]
        
        unclaim, meta = pyreadstat.read_sas7bdat(f"{PATHS['UNCLAIM']}unclaim.sas7bdat")
        unclaim = pl.DataFrame(unclaim)
        unclaim.columns = [col.lower() for col in unclaim.columns]
        unclaim = unclaim.rename({'ledgbal': 'unclaimx'})
        
        combined = pl.concat([remit, unclaim])
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
        return df.filter(pl.col('purpose').cast(str).is_in(['5','6'])) \
                 .select(['branch','acctno','name','purpose','product','curbal','intpaybl'])
    except Exception as e:
        print(f"Error loading SAVING: {e}")
        return pl.DataFrame()

def load_current():
    """Load CURRENT accounts with purpose 5/6 and FX conversion"""
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}current.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        df = df.filter(pl.col('purpose').cast(str).is_in(['5','6']))
        df = df.with_columns([
            pl.when(pl.col('curcode') != 'MYR')
              .then((pl.col('intpaybl') * pl.col('forate')).round(2))
              .otherwise(pl.col('intpaybl')).alias('intpaybl')
        ])
        return df.select(['branch','acctno','name','purpose','product','curbal','intpaybl'])
    except Exception as e:
        print(f"Error loading CURRENT: {e}")
        return pl.DataFrame()

def load_fd():
    """Load FD accounts with purpose 5/6 and FX conversion"""
    try:
        df, meta = pyreadstat.read_sas7bdat(f"{PATHS['SACA']}fd.sas7bdat")
        df = pl.DataFrame(df)
        df.columns = [col.lower() for col in df.columns]
        df = df.filter(pl.col('purpose').cast(str).is_in(['5','6']))
        df = df.with_columns([
            pl.when(pl.col('curcode') != 'MYR')
              .then((pl.col('intpaybl') * pl.col('forate')).round(2))
              .otherwise(pl.col('intpaybl')).alias('intpaybl')
        ])
        return df.select(['branch','acctno','name','product','purpose','curbal','intpaybl'])
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
            dfs.append(df.select(['acctno','amtind','prodcd','product']))
        
        # CURN
        curn_file = f"{PATHS['DEPOSIT']}curn{d['reptmon']}{d['nowk']}.sas7bdat"
        if Path(curn_file).exists():
            df, meta = pyreadstat.read_sas7bdat(curn_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            dfs.append(df.select(['acctno','amtind','prodcd','product']))
        
        # FDMTHLY
        fd_file = f"{PATHS['DEPOSIT']}fdmthly.sas7bdat"
        if Path(fd_file).exists():
            df, meta = pyreadstat.read_sas7bdat(fd_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            df = df.rename({'bic': 'prodcd', 'accttype': 'product'})
            dfs.append(df.select(['acctno','amtind','prodcd','product']))
        
        if not dfs:
            return pl.DataFrame()
        
        combined = pl.concat(dfs)
        # Filter product codes
        combined = combined.filter(pl.col('prodcd').cast(str).is_in(PROD_CODES))
        # Special filter for 42199/42699
        combined = combined.filter(
            ~((pl.col('prodcd').cast(str).is_in(['42199','42699'])) & 
              (~pl.col('product').cast(int).is_in([72,413])))
        )
        return combined.unique(subset=['acctno'])
    except Exception as e:
        print(f"Error loading DEP: {e}")
        return pl.DataFrame()

def load_client():
    """Load CLIENT file"""
    try:
        # Try SAS format first
        client_file = f"{PATHS['DEPOSIT']}client.sas7bdat"
        if Path(client_file).exists():
            df, meta = pyreadstat.read_sas7bdat(client_file)
            df = pl.DataFrame(df)
            df.columns = [col.lower() for col in df.columns]
            return df.unique(subset=['acctno'])
        else:
            # Fallback to text file
            df = pl.read_csv(
                f"{PATHS['DEPOSIT']}client.txt",
                has_header=False,
                skip_rows=0
            )
            # Parse fixed positions: ACCTNO at 2 (len 10), NAME at 21 (len 40)
            data = []
            for row in df.rows():
                line = row[0]
                acct = line[1:11].strip()
                if acct.replace(' ', '').isdigit():
                    name = line[20:60].strip()
                    data.append({'acctno': int(acct), 'name': name, 'key': name[:10]})
            return pl.DataFrame(data).unique(subset=['acctno'])
    except Exception as e:
        print(f"Error loading CLIENT: {e}")
        return pl.DataFrame()

# =============================================================================
# MAIN PROCESSING
# =============================================================================
def main():
    print("="*60)
    print("EIBQINST - Trustee and Client Account Reporting")
    print("="*60)
    
    d = get_dates()
    print(f"\nReport Date: {d['reptdate']} (Week: {d['nowk']})")
    
    # ========== PART 1: TRUSTEE ACCOUNTS ==========
    print("\nProcessing Trustee Accounts...")
    
    # Load FLOAT
    float_df = load_float()
    print(f"  FLOAT: {len(float_df)}")
    
    # Load IBGPIDM
    ibg_df = load_ibgpidm()
    print(f"  IBGPIDM: {len(ibg_df)}")
    
    # Load REMIT/UNCLAIM
    remit_df = load_remit()
    print(f"  REMIT: {len(remit_df)}")
    
    # Load SA/CA/FD
    sa = load_saving()
    ca = load_current()
    fd = load_fd()
    saca = pl.concat([sa, ca, fd]) if any(len(df)>0 for df in [sa,ca,fd]) else pl.DataFrame()
    print(f"  SA/CA/FD: {len(saca)}")
    
    trustee = None
    client = None
    
    if not saca.is_empty():
        # Merge with FLOAT
        trustee = saca.join(float_df, on='acctno', how='left')
        trustee = trustee.with_columns([
            pl.col('float').fill_null(0),
            (pl.col('curbal').fill_null(0) - pl.col('float').fill_null(0)).alias('avbal')
        ])
        
        # Merge with DEP
        dep = load_dep(d)
        trustee = trustee.join(dep, on='acctno', how='inner')
        
        # Merge with REMIT
        trustee = trustee.join(remit_df, on='acctno', how='left')
        trustee = trustee.with_columns([
            pl.col('plusbal').fill_null(0),
            pl.col('unclaim').fill_null(0)
        ])
        
        # Calculate AVBALTT
        trustee = trustee.with_columns([
            (pl.col('avbal') + pl.col('intpaybl').fill_null(0) + 
             pl.col('plusbal') + pl.col('unclaim')).alias('avbaltt')
        ])
        
        # Add SI (always 0)
        trustee = trustee.with_columns(pl.lit(0).alias('si'))
        trustee = trustee.with_columns((pl.col('avbaltt') + pl.col('si')).alias('avbaltt'))
        
        # Add IBGAMT
        trustee = trustee.join(ibg_df, on='acctno', how='left')
        trustee = trustee.with_columns([
            pl.col('ibgamt').fill_null(0),
            (pl.col('avbaltt') + pl.col('ibgamt').fill_null(0)).alias('avbaltt')
        ])
        
        # Split by threshold
        trustee_high = trustee.filter(pl.col('avbaltt') > 60000)
        trustee_low = trustee.filter(pl.col('avbaltt') <= 60000)
        
        print(f"  Trustee >60k: {len(trustee_high)}")
        print(f"  Trustee <=60k: {len(trustee_low)}")
        
        # Write text file outputs
        def write_txt(df, title, filename):
            if df.is_empty(): return
            lines = [f"{title}\n", "BRANCH;ACCTNO;NAME;PURPOSE;AVBAL;INTPAYBL;PRODUCT;AMTIND;PLUSBAL;UNCLAIM;SI;IBGAMT;AVBALTT\n"]
            for r in df.rows(named=True):
                lines.append(
                    f"{r.get('branch','')};{r.get('acctno','')};{r.get('name','')};{r.get('purpose','')};"
                    f"{r.get('avbal',0)};{r.get('intpaybl',0)};{r.get('product','')};{r.get('amtind','')};"
                    f"{r.get('plusbal',0)};{r.get('unclaim',0)};{r.get('si',0)};{r.get('ibgamt',0)};{r.get('avbaltt',0)}\n"
                )
            Path(f"{PATHS['OUTPUT']}{filename}").write_text(''.join(lines))
        
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
    print(f"  CLIENT master: {len(client_df)}")
    
    if not client_df.is_empty() and not saca.is_empty():
        # Merge client with deposit data
        client = client_df.join(saca, on='acctno', how='inner')
        client = client.join(float_df, on='acctno', how='left')
        client = client.with_columns([
            pl.col('float').fill_null(0),
            (pl.col('curbal').fill_null(0) - pl.col('float').fill_null(0)).alias('avbal')
        ])
        
        # Merge with DEP
        client = client.join(dep, on='acctno', how='inner')
        
        # Merge with REMIT
        client = client.join(remit_df, on='acctno', how='left')
        client = client.with_columns([
            pl.col('plusbal').fill_null(0),
            pl.col('unclaim').fill_null(0)
        ])
        
        # Calculate AVBALTT
        client = client.with_columns([
            (pl.col('avbal') + pl.col('intpaybl').fill_null(0) + 
             pl.col('plusbal') + pl.col('unclaim')).alias('avbaltt')
        ])
        
        # Add SI
        client = client.with_columns(pl.lit(0).alias('si'))
        client = client.with_columns((pl.col('avbaltt') + pl.col('si')).alias('avbaltt'))
        
        # Add IBGAMT
        client = client.join(ibg_df, on='acctno', how='left')
        client = client.with_columns([
            pl.col('ibgamt').fill_null(0),
            (pl.col('avbaltt') + pl.col('ibgamt').fill_null(0)).alias('avbaltt')
        ])
        
        # Split by threshold
        client_high = client.filter(pl.col('avbaltt') > 60000)
        client_low = client.filter(pl.col('avbaltt') <= 60000)
        
        print(f"  Client >60k: {len(client_high)}")
        print(f"  Client <=60k: {len(client_low)}")
        
        # Write text file outputs
        def write_client_txt(df, title, filename):
            if df.is_empty(): return
            lines = [f"{title}\n", "BRANCH;ACCTNO;NAME;PURPOSE;AVBAL;INTPAYBL;PRODUCT;AMTIND;PLUSBAL;UNCLAIM;SI;IBGAMT;AVBALTT\n"]
            for r in df.rows(named=True):
                lines.append(
                    f"{r.get('branch','')};{r.get('acctno','')};{r.get('name','')};{r.get('purpose','')};"
                    f"{r.get('avbal',0)};{r.get('intpaybl',0)};{r.get('product','')};{r.get('amtind','')};"
                    f"{r.get('plusbal',0)};{r.get('unclaim',0)};{r.get('si',0)};{r.get('ibgamt',0)};{r.get('avbaltt',0)}\n"
                )
            Path(f"{PATHS['OUTPUT']}{filename}").write_text(''.join(lines))
        
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
    if trustee is not None and client is not None and not trustee.is_empty() and not client.is_empty():
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
