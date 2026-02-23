"""
EIIQINST - Islamic Trustee and Client Account Quarterly Reporting
Processes Islamic trustee and client accounts with balance thresholds (>60k/<=60k)
Includes PBBDPFMT format mappings for product codes (Islamic version)
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path

# =============================================================================
# CONFIG
# =============================================================================
PATHS = {
    'PIDMS': 'data/pidms/',
    'SACA': 'data/saca/',
    'DEPOSIT': 'data/deposit/',
    'DEPOSIX': 'data/deposix/',
    'UNCLAIM': 'data/unclaim/',
    'OUTPUT': 'output/'
}
for p in PATHS.values(): Path(p).mkdir(exist_ok=True)

# Product code filters (from PBBDPFMT) - same as conventional
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
    reptdate = datetime(today.year, today.month, 1) - timedelta(days=1)
    reptdate = reptdate.date()
    
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
        df = pl.read_parquet(f"{PATHS['PIDMS']}FLOAT.parquet")
        return df.group_by('ACCTNO').agg(pl.col('FLOAT').sum())
    except:
        return pl.DataFrame()

def load_ibgpidm():
    """Load IBGPIDM data"""
    try:
        df = pl.read_parquet(f"{PATHS['DEPOSIT']}IBGPIDM.parquet")
        return df.group_by('ACCTNO').agg(pl.col('IBGAMT').sum())
    except:
        return pl.DataFrame()

def load_remit(d):
    """Load REMIT and UNCLAIM data"""
    try:
        remit = pl.read_parquet(f"{PATHS['DEPOSIT']}REMIT.parquet")
        unclaim = pl.read_parquet(f"{PATHS['UNCLAIM']}UNCLAIM{d['reptyear']}.parquet")
        unclaim = unclaim.rename({'LEDGBAL': 'UNCLAIMX'})
        
        combined = pl.concat([remit, unclaim])
        summary = combined.group_by('PAYMODE').agg([
            pl.col('LEDGBAL').sum().alias('PLUSBAL'),
            pl.col('UNCLAIMX').sum().alias('UNCLAIM')
        ])
        
        # Get original for other fields
        orig = remit.unique(subset=['PAYMODE'])
        result = summary.join(orig, on='PAYMODE', how='left')
        result = result.with_columns(pl.col('PAYMODE').alias('ACCTNO'))
        return result
    except:
        return pl.DataFrame()

def load_saving():
    """Load SAVING accounts with purpose 5/6"""
    try:
        df = pl.read_parquet(f"{PATHS['SACA']}SAVING.parquet")
        return df.filter(pl.col('PURPOSE').cast(str).is_in(['5','6'])) \
                 .select(['BRANCH','ACCTNO','NAME','PURPOSE','PRODUCT','CURBAL','INTPAYBL'])
    except:
        return pl.DataFrame()

def load_current():
    """Load CURRENT accounts with purpose 5/6 and FX conversion"""
    try:
        df = pl.read_parquet(f"{PATHS['SACA']}CURRENT.parquet")
        df = df.filter(pl.col('PURPOSE').cast(str).is_in(['5','6']))
        # Take highest balance for duplicate accounts (as per SAS)
        df = df.sort(['ACCTNO', 'CURBAL'], descending=[False, True])
        df = df.unique(subset=['ACCTNO'], keep='first')
        
        df = df.with_columns([
            pl.when(pl.col('CURCODE') != 'MYR')
              .then((pl.col('INTPAYBL') * pl.col('FORATE')).round(2))
              .otherwise(pl.col('INTPAYBL')).alias('INTPAYBL')
        ])
        return df.select(['BRANCH','ACCTNO','NAME','PURPOSE','PRODUCT','CURBAL','INTPAYBL'])
    except:
        return pl.DataFrame()

def load_fd():
    """Load FD accounts with purpose 5/6 and FX conversion"""
    try:
        df = pl.read_parquet(f"{PATHS['SACA']}FD.parquet")
        df = df.filter(pl.col('PURPOSE').cast(str).is_in(['5','6']))
        df = df.with_columns([
            pl.when(pl.col('CURCODE') != 'MYR')
              .then((pl.col('INTPAYBL') * pl.col('FORATE')).round(2))
              .otherwise(pl.col('INTPAYBL')).alias('INTPAYBL')
        ])
        return df.select(['BRANCH','ACCTNO','NAME','PRODUCT','PURPOSE','CURBAL','INTPAYBL'])
    except:
        return pl.DataFrame()

def load_dep(d):
    """Load DEP data from monthly files"""
    try:
        dfs = []
        # SAVG
        if Path(f"{PATHS['DEPOSIT']}SAVG{d['reptmon']}{d['nowk']}.parquet").exists():
            df = pl.read_parquet(f"{PATHS['DEPOSIT']}SAVG{d['reptmon']}{d['nowk']}.parquet")
            dfs.append(df.select(['ACCTNO','AMTIND','PRODCD','PRODUCT']))
        
        # CURN
        if Path(f"{PATHS['DEPOSIT']}CURN{d['reptmon']}{d['nowk']}.parquet").exists():
            df = pl.read_parquet(f"{PATHS['DEPOSIT']}CURN{d['reptmon']}{d['nowk']}.parquet")
            dfs.append(df.select(['ACCTNO','AMTIND','PRODCD','PRODUCT']))
        
        # FDMTHLY
        if Path(f"{PATHS['DEPOSIT']}FDMTHLY.parquet").exists():
            df = pl.read_parquet(f"{PATHS['DEPOSIT']}FDMTHLY.parquet")
            df = df.rename({'BIC': 'PRODCD', 'ACCTTYPE': 'PRODUCT'})
            dfs.append(df.select(['ACCTNO','AMTIND','PRODCD','PRODUCT']))
        
        if not dfs:
            return pl.DataFrame()
        
        combined = pl.concat(dfs)
        # Filter product codes
        combined = combined.filter(pl.col('PRODCD').cast(str).is_in(PROD_CODES))
        # Special filter for 42199/42699
        combined = combined.filter(
            ~((pl.col('PRODCD').cast(str).is_in(['42199','42699'])) & 
              (~pl.col('PRODUCT').cast(int).is_in([72,413])))
        )
        return combined.unique(subset=['ACCTNO'])
    except:
        return pl.DataFrame()

def load_client():
    """Load CLIENT file"""
    try:
        df = pl.read_csv(
            f"{PATHS['DEPOSIT']}CLIENT.txt",
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
                data.append({'ACCTNO': int(acct), 'NAME': name, 'KEY': name[:10]})
        return pl.DataFrame(data).unique(subset=['ACCTNO'])
    except:
        return pl.DataFrame()

def load_uma():
    """Load UMA data for SASA merge"""
    try:
        df = pl.read_parquet(f"{PATHS['DEPOSIT']}UMA.parquet")
        return df
    except:
        return pl.DataFrame()

# =============================================================================
# MAIN PROCESSING
# =============================================================================
def main():
    print("="*60)
    print("EIIQINST - Islamic Trustee and Client Account Reporting")
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
    remit_df = load_remit(d)
    print(f"  REMIT: {len(remit_df)}")
    
    # Load SA/CA/FD
    sa = load_saving()
    ca = load_current()
    fd = load_fd()
    saca = pl.concat([sa, ca, fd]) if any(len(df)>0 for df in [sa,ca,fd]) else pl.DataFrame()
    print(f"  SA/CA/FD: {len(saca)}")
    
    if not saca.is_empty():
        # Merge with FLOAT
        trustee = saca.join(float_df, on='ACCTNO', how='left')
        trustee = trustee.with_columns([
            pl.col('FLOAT').fill_null(0),
            (pl.col('CURBAL').fill_null(0) - pl.col('FLOAT').fill_null(0)).alias('AVBAL')
        ])
        trustee = trustee.with_columns(
            (pl.col('AVBAL') + pl.col('INTPAYBL').fill_null(0)).alias('AVBALTT')
        )
        
        # Sort for next merge
        trustee = trustee.sort('ACCTNO')
        
        # Merge with DEP
        dep = load_dep(d)
        trustee = trustee.join(dep, on='ACCTNO', how='inner')
        
        # Merge with REMIT
        trustee = trustee.join(remit_df, on='ACCTNO', how='left')
        trustee = trustee.with_columns([
            pl.col('PLUSBAL').fill_null(0),
            pl.col('UNCLAIM').fill_null(0)
        ])
        
        # Recalculate AVBALTT with REMIT
        trustee = trustee.with_columns(
            (pl.col('AVBAL') + pl.col('INTPAYBL').fill_null(0) + 
             pl.col('PLUSBAL') + pl.col('UNCLAIM')).alias('AVBALTT')
        )
        
        # Add SI (always 0)
        trustee = trustee.with_columns(pl.lit(0).alias('SI'))
        trustee = trustee.with_columns((pl.col('AVBALTT') + pl.col('SI')).alias('AVBALTT'))
        
        # Add IBGAMT
        trustee = trustee.join(ibg_df, on='ACCTNO', how='left')
        trustee = trustee.with_columns([
            pl.col('IBGAMT').fill_null(0),
            (pl.col('AVBALTT') + pl.col('IBGAMT').fill_null(0)).alias('AVBALTT')
        ])
        
        # Split by threshold
        trustee_high = trustee.filter(pl.col('AVBALTT') > 60000)
        trustee_low = trustee.filter(pl.col('AVBALTT') <= 60000)
        
        print(f"  Trustee >60k: {len(trustee_high)}")
        print(f"  Trustee ≤60k: {len(trustee_low)}")
        
        # Write CSV outputs
        def write_csv(df, title, filename):
            if df.is_empty(): return
            lines = [f"\n{title}\n", "BRANCH;ACCTNO;NAME;PURPOSE;AVBAL;INTPAYBL;PRODUCT;AMTIND;PLUSBAL;UNCLAIM;SI;IBGAMT;AVBALTT;"]
            for r in df.rows(named=True):
                lines.append(
                    f"{r.get('BRANCH','')};{r.get('ACCTNO','')};{r.get('NAME','')};{r.get('PURPOSE','')};"
                    f"{r.get('AVBAL',0)};{r.get('INTPAYBL',0)};{r.get('PRODUCT','')};{r.get('AMTIND','')};"
                    f"{r.get('PLUSBAL',0)};{r.get('UNCLAIM',0)};{r.get('SI',0)};{r.get('IBGAMT',0)};{r.get('AVBALTT',0)};"
                )
            Path(f"{PATHS['OUTPUT']}{filename}").write_text('\n'.join(lines))
        
        write_csv(trustee_high, "TRUSTEE >60000", "islamic_trustee_high.csv")
        write_csv(trustee_low, "TRUSTEE ≤60000", "islamic_trustee_low.csv")
        
        # Print reports
        if not trustee_high.is_empty():
            print("\nTRUSTEE >60000 by Branch:")
            for r in trustee_high.group_by('BRANCH').agg(pl.col('AVBALTT').sum()).sort('BRANCH').rows():
                print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
        
        if not trustee_low.is_empty():
            print("\nTRUSTEE ≤60000 by Branch:")
            for r in trustee_low.group_by('BRANCH').agg(pl.col('AVBALTT').sum()).sort('BRANCH').rows():
                print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
    
    # ========== PART 2: CLIENT ACCOUNTS ==========
    print("\nProcessing Client Accounts...")
    
    client_df = load_client()
    print(f"  CLIENT master: {len(client_df)}")
    
    # Load SASA (SAVING + UMA)
    uma = load_uma()
    sasa = pl.concat([sa, uma]) if not uma.is_empty() else sa
    if not sasa.is_empty():
        sasa = sasa.sort('ACCTNO')
        print(f"  SASA: {len(sasa)}")
    
    if not client_df.is_empty() and not saca.is_empty():
        # Create DEPOSIT dataset (SA/CA/FD merged with FLOAT)
        deposit = saca.join(float_df, on='ACCTNO', how='left')
        deposit = deposit.with_columns([
            pl.col('FLOAT').fill_null(0),
            (pl.col('CURBAL').fill_null(0) - pl.col('FLOAT').fill_null(0)).alias('AVBAL')
        ])
        deposit = deposit.with_columns(
            (pl.col('AVBAL') + pl.col('INTPAYBL').fill_null(0)).alias('AVBALTT')
        )
        deposit = deposit.unique(subset=['ACCTNO'])
        
        # Merge client with deposit
        client = client_df.join(deposit, on='ACCTNO', how='inner')
        client = client.select(['BRANCH','ACCTNO','NAME','PRODUCT','AVBAL','INTPAYBL','AVBALTT','CURBAL','FLOAT'])
        
        # Merge with DEP
        client = client.join(dep, on='ACCTNO', how='inner')
        
        # Merge with REMIT
        client = client.join(remit_df.drop('NAME'), on='ACCTNO', how='left')
        client = client.with_columns([
            pl.col('PLUSBAL').fill_null(0),
            pl.col('UNCLAIM').fill_null(0)
        ])
        
        # Recalculate AVBALTT with REMIT
        client = client.with_columns(
            (pl.col('AVBAL') + pl.col('INTPAYBL').fill_null(0) + 
             pl.col('PLUSBAL') + pl.col('UNCLAIM')).alias('AVBALTT')
        )
        
        # Add SI
        client = client.with_columns(pl.lit(0).alias('SI'))
        client = client.with_columns((pl.col('AVBALTT') + pl.col('SI')).alias('AVBALTT'))
        
        # Add IBGAMT
        client = client.join(ibg_df, on='ACCTNO', how='left')
        client = client.with_columns([
            pl.col('IBGAMT').fill_null(0),
            (pl.col('AVBALTT') + pl.col('IBGAMT').fill_null(0)).alias('AVBALTT')
        ])
        
        # Split by threshold
        client_high = client.filter(pl.col('AVBALTT') > 60000)
        client_low = client.filter(pl.col('AVBALTT') <= 60000)
        
        print(f"  Client >60k: {len(client_high)}")
        print(f"  Client ≤60k: {len(client_low)}")
        
        # Write CSV outputs
        def write_client_csv(df, title, filename):
            if df.is_empty(): return
            lines = [f"\n{title}\n", "BRANCH;ACCTNO;NAME;PURPOSE;AVBAL;INTPAYBL;PRODUCT;AMTIND;PLUSBAL;UNCLAIM;SI;IBGAMT;AVBALTT;"]
            for r in df.rows(named=True):
                lines.append(
                    f"{r.get('BRANCH','')};{r.get('ACCTNO','')};{r.get('NAME','')};{r.get('PURPOSE','')};"
                    f"{r.get('AVBAL',0)};{r.get('INTPAYBL',0)};{r.get('PRODUCT','')};{r.get('AMTIND','')};"
                    f"{r.get('PLUSBAL',0)};{r.get('UNCLAIM',0)};{r.get('SI',0)};{r.get('IBGAMT',0)};{r.get('AVBALTT',0)};"
                )
            Path(f"{PATHS['OUTPUT']}{filename}").write_text('\n'.join(lines))
        
        write_client_csv(client_high, "CLIENT >60000", "islamic_client_high.csv")
        write_client_csv(client_low, "CLIENT ≤60000", "islamic_client_low.csv")
        
        # Print reports
        if not client_high.is_empty():
            print("\nCLIENT >60000 by Branch:")
            for r in client_high.group_by('BRANCH').agg(pl.col('AVBALTT').sum()).sort('BRANCH').rows():
                print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
        
        if not client_low.is_empty():
            print("\nCLIENT ≤60000 by Branch:")
            for r in client_low.group_by('BRANCH').agg(pl.col('AVBALTT').sum()).sort('BRANCH').rows():
                print(f"  Branch {r[0]}: RM {r[1]:,.2f}")
    
    # ========== PART 3: DUPLICATE ACCOUNTS ==========
    print("\nChecking for duplicate accounts...")
    if 'trustee' in locals() and 'client' in locals() and not trustee.is_empty() and not client.is_empty():
        all_acc = pl.concat([
            trustee.select(['ACCTNO']).with_columns(pl.lit('TRUSTEE').alias('SRC')),
            client.select(['ACCTNO']).with_columns(pl.lit('CLIENT').alias('SRC'))
        ])
        
        dup = all_acc.group_by('ACCTNO').agg([
            pl.col('SRC').alias('SOURCES'),
            pl.count().alias('COUNT')
        ]).filter(pl.col('COUNT') > 1)
        
        if not dup.is_empty():
            print(f"  Found {len(dup)} duplicate accounts:")
            for r in dup.rows(named=True):
                print(f"    {r['ACCTNO']} appears in: {', '.join(r['SOURCES'])}")
        else:
            print("  No duplicate accounts found")
    
    # ========== SUMMARY ==========
    print("\n"+"="*60)
    print("SUMMARY")
    print("="*60)
    
    if 'trustee' in locals() and not trustee.is_empty():
        print(f"\nTrustee Accounts:")
        print(f"  Total: RM {trustee['AVBALTT'].sum():,.2f}")
        print(f"  >60k: RM {trustee_high['AVBALTT'].sum():,.2f} ({len(trustee_high)} accounts)")
        print(f"  ≤60k: RM {trustee_low['AVBALTT'].sum():,.2f} ({len(trustee_low)} accounts)")
    
    if 'client' in locals() and not client.is_empty():
        print(f"\nClient Accounts:")
        print(f"  Total: RM {client['AVBALTT'].sum():,.2f}")
        print(f"  >60k: RM {client_high['AVBALTT'].sum():,.2f} ({len(client_high)} accounts)")
        print(f"  ≤60k: RM {client_low['AVBALTT'].sum():,.2f} ({len(client_low)} accounts)")
    
    print("\n"+"="*60)
    print("✓ EIIQINST Complete")

if __name__ == "__main__":
    main()
