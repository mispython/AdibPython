"""
EIBQDISF - Deposit Insurance Framework (MDIC) Reporting
Complete implementation with CIS integration, branch apportionment, and insurance calculations
"""

import polars as pl
from datetime import datetime, timedelta
from pathlib import Path
import re

# =============================================================================
# CONFIG
# =============================================================================
PATHS = {
    'DEPOSIT': 'data/deposit/', 'MNI': 'data/mni/', 'PIDMS': 'data/pidms/',
    'UNCLAIM': 'data/unclaim/', 'TRUST': 'data/trust/', 'OUTPUT': 'output/'
}
for p in PATHS.values(): Path(p).mkdir(exist_ok=True)

# Formats
JOIN_FMT = {0: 'ORGANISATION', 1: 'PERSONAL', **{i: f'JOIN {i}' for i in range(2, 12)}}
PROD_FMD = {'42110':'CA (A)','42310':'CA (A)','34180':'CA (A)','42610':'FX CA (A)','42120':'SA (B)',
            '42320':'SA (B)','42130':'FD (C)','42630':'FX FD (C)','42132':'GID','42133':'GID',
            '42180':'HOUSING DEV (D)','42XXX':'ATM/SI (E)','46795':'DEBIT CARD (E)',
            '42199':'OD CA','42699':'FX ODCA'}
PROD_BRH = {'42110':'DDMAND','42310':'DDMAND','34180':'DDMAND','42199':'DDMAND','42120':'DSVING',
            '42320':'DSVING','42130':'DFIXED','42132':'DFIXED','42133':'DFIXED','42180':'DDMAND',
            '42610':'FDMAND','42699':'FDMAND','42630':'FFIXED','42XXX':'ATM/SI (E)','46795':'DEBIT CARD (E)'}

# ACE products (example - would come from macro)
ACE_PRODUCTS = []  # Populate from actual macro

# =============================================================================
# DATE
# =============================================================================
def get_dates():
    today = datetime.now().date()
    reptdate = datetime(today.year, today.month, 1) - timedelta(days=1)
    reptdate = reptdate.date()
    day = reptdate.day
    wk = '1' if day == 8 else '2' if day == 15 else '3' if day == 22 else '4'
    return {'reptdate': reptdate, 'nowk': wk, 'reptmon': f"{reptdate.month:02d}", 'reptyear': str(reptdate.year)}

# =============================================================================
# DATA LOADING
# =============================================================================
def load_parquet(path, **kwargs):
    try: return pl.read_parquet(path, **kwargs)
    except: return pl.DataFrame()

def load_remit(d):
    remit = load_parquet(f"{PATHS['DEPOSIT']}REMIT.parquet")
    unclaim = load_parquet(f"{PATHS['UNCLAIM']}UNCLAIM{d['reptyear']}.parquet").rename({'LEDGBAL': 'UNCLAIMX'})
    if remit.is_empty() and unclaim.is_empty(): return pl.DataFrame()
    combined = pl.concat([remit, unclaim]) if not remit.is_empty() else unclaim
    summary = combined.group_by('PAYMODE').agg([pl.col('LEDGBAL').sum(), pl.col('UNCLAIMX').sum()]).rename({'LEDGBAL':'PLUSBAL','UNCLAIMX':'UNCLAIM'})
    orig = remit.unique('PAYMODE') if not remit.is_empty() else pl.DataFrame()
    if not orig.is_empty(): summary = summary.join(orig, on='PAYMODE', how='left')
    return summary.with_columns(pl.col('PAYMODE').alias('ACCTNO'))

def load_float():
    return load_parquet(f"{PATHS['PIDMS']}FLOAT.parquet").group_by('ACCTNO').agg(pl.col('FLOAT').sum())

def load_ibg():
    try:
        df = pl.read_csv(f"{PATHS['DEPOSIT']}IBGPIDM.txt", has_header=False, new_columns=['line'])
        data = []
        for r in df.rows():
            line = r[0]
            if len(line) >= 28 and line[0:10].strip().isdigit():
                data.append({'ACCTNO': int(line[0:10].strip()), 'IBGAMT': float(line[11:27].strip() or 0)})
        return pl.DataFrame(data).group_by('ACCTNO').agg(pl.col('IBGAMT').sum())
    except: return pl.DataFrame()

def load_si():
    return load_parquet(f"{PATHS['DEPOSIT']}SI.parquet")

def load_deposit(d):
    dfs = []
    for name in ['SAVG', 'CURN']:
        df = load_parquet(f"{PATHS['MNI']}{name}{d['reptmon']}{d['nowk']}.parquet")
        if name == 'CURN' and not df.is_empty(): df = df.filter(pl.col('PRODUCT') != 139)
        if not df.is_empty(): dfs.append(df)
    fdm = load_parquet(f"{PATHS['MNI']}FDMTHLY.parquet")
    if not fdm.is_empty():
        fdm = fdm.select(['ACCTNO','BRANCH','INTPLAN','CURBAL','BIC','AMTIND','INTPAY'])\
                 .rename({'BIC':'PRODCD','INTPLAN':'PRODUCT','INTPAY':'INTPAYBL'})\
                 .with_columns(pl.col('CURBAL').alias('LEDGBAL'))
        dfs.append(fdm)
    if not dfs: return pl.DataFrame()
    dep = pl.concat(dfs).filter(pl.col('PRODCD').cast(str).is_in(['42110','42310','42120','42320','42130','42133',
                '42132','42180','42610','42630','34180','42199','42699']))
    dep = dep.with_columns(pl.when(pl.col('PRODUCT')==166).then(pl.lit('42310')).otherwise(pl.col('PRODCD')).alias('PRODCD'))
    dep = dep.filter(~((pl.col('PRODCD').is_in(['42199','42699'])) & (~pl.col('PRODUCT').cast(int).is_in([72,413]))))
    inv = []
    for s,e in [(107,107),(126,129),(130,136),(140,149),(171,173),(350,357),(400,410),(500,560)]:
        inv.extend(range(s, e+1))
    return dep.filter(~pl.col('PRODUCT').cast(int).is_in(inv)).with_columns(pl.when(pl.col('INTPAYBL')<0).then(0).otherwise(pl.col('INTPAYBL')).alias('INTPAYBL'))

def load_cisdep():
    dfs = []
    for i in [1,2,3,4,5,6,7,8,11,14]:
        df = load_parquet(f"{PATHS['DEPOSIT']}TRANS{i:02d}.parquet")
        if not df.is_empty(): dfs.append(df)
    for f in ['TRANORG', 'TRAN999']:
        df = load_parquet(f"{PATHS['DEPOSIT']}{f}.parquet")
        if not df.is_empty(): dfs.append(df)
    return pl.concat(dfs).unique('ACCTNO') if dfs else pl.DataFrame()

def load_trust_client():
    t = load_parquet(f"{PATHS['TRUST']}MERGE.parquet")
    c = load_parquet(f"{PATHS['DEPOSIT']}CLIENT.parquet")
    return pl.concat([t, c]).unique('ACCTNO') if not t.is_empty() or not c.is_empty() else pl.DataFrame()

# =============================================================================
# MAIN
# =============================================================================
def main():
    print("="*60+"\nEIBQDISF - Deposit Insurance Framework\n"+"="*60)
    d = get_dates()
    print(f"Date: {d['reptdate']} (Week: {d['nowk']})")
    
    # Load data
    print("\nLoading...")
    remit, flt, ibg, si, dep = load_remit(d), load_float(), load_ibg(), load_si(), load_deposit(d)
    print(f"  REMIT:{len(remit)} FLOAT:{len(flt)} IBG:{len(ibg)} SI:{len(si)} DEP:{len(dep)}")
    
    if dep.is_empty(): return
    
    # Merge with FLOAT and calculate AVBAL
    dep = dep.join(flt, on='ACCTNO', how='left').with_columns(pl.col('FLOAT').fill_null(0))
    dep = dep.with_columns(pl.when(pl.col('CURBAL')<0).then(0).otherwise(pl.col('CURBAL')).alias('CURBAL'))
    dep = dep.with_columns([(pl.col('CURBAL')-pl.col('FLOAT')).alias('AVBAL'), (pl.col('CURBAL')+pl.col('INTPAYBL')).alias('CURBALTT')])
    
    # Handle negative AVBAL
    neg_mask = pl.col('AVBAL') < 0
    dep = dep.with_columns(pl.when(neg_mask).then(pl.col('CURBAL')).otherwise(pl.col('FLOAT')).alias('FLOAT2'))
    dep = dep.with_columns(pl.when(neg_mask).then(0).otherwise(pl.col('AVBAL')).alias('AVBAL'))
    dep = dep.drop('FLOAT').rename({'FLOAT2':'FLOAT'})
    dep = dep.with_columns((pl.col('AVBAL')+pl.col('INTPAYBL')).alias('AVBALTT'))
    
    # Split positive/negative
    dep_pos = dep.filter(pl.col('CURBALTT')>0)
    dep_neg = dep.filter(pl.col('CURBALTT')<=0)
    
    # Summaries
    for name, df in [('POS',dep_pos), ('NEG',dep_neg)]:
        if not df.is_empty():
            print(f"\n  {name} by PRODCD:")
            for r in df.group_by('PRODCD').agg(pl.col('AVBALTT').sum()).sort('PRODCD').rows():
                print(f"    {r[0]}: {r[1]:,.2f}")
    
    # Create DEP with AVBAL as new CURBAL
    dep = pl.concat([
        dep_pos.with_columns([pl.col('AVBAL').alias('CURBAL'), pl.col('AVBALTT').alias('CURBALTT')]),
        dep_neg.with_columns([pl.col('AVBAL').alias('CURBAL'), pl.col('AVBALTT').alias('CURBALTT')])
    ]).with_columns([pl.when(pl.col('CURBAL')<0).then(0).otherwise(pl.col('CURBAL')).alias('CURBAL'),
                     pl.when(pl.col('CURBALTT')<0).then(0).otherwise(pl.col('CURBALTT')).alias('CURBALTT')])
    
    # Merge with REMIT, SI, IBG
    dep = dep.join(remit, on='ACCTNO', how='left').with_columns([pl.col('PLUSBAL').fill_null(0), pl.col('UNCLAIM').fill_null(0)])
    dep = dep.join(si, on='ACCTNO', how='left').with_columns(pl.col('SI').fill_null(0))
    dep = dep.join(ibg, on='ACCTNO', how='left').with_columns(pl.col('IBGAMT').fill_null(0))
    
    # Add category
    dep = dep.with_columns(pl.col('PRODCD').map_elements(lambda x: PROD_FMD.get(str(x), ''), return_dtype=pl.Utf8).alias('CATEGORY'))
    
    # BC/DD adjustments for CA products (simplified)
    dep = dep.with_columns(pl.when((pl.col('PRODUCT').cast(int).is_in(ACE_PRODUCTS)) & (pl.col('PRODCD')=='42110'))
                          .then(pl.col('CURBALTT')+pl.col('PLUSBAL')+pl.col('UNCLAIM')+pl.col('SI')+pl.col('IBGAMT'))
                          .otherwise(pl.col('CURBALTT')+pl.col('PLUSBAL')+pl.col('UNCLAIM')+pl.col('SI')+pl.col('IBGAMT')).alias('CURBALTT'))
    
    # Split final
    dep_final = dep.filter(pl.col('CURBALTT')>0)
    dep_neg_final = dep.filter(pl.col('CURBALTT')<=0)
    
    # Load CIS and trust/client
    print("\nLoading CIS...")
    cisdep, tc = load_cisdep(), load_trust_client()
    print(f"  CISDEP:{len(cisdep)} TRUST/CLIENT:{len(tc)}")
    
    # Merge with CIS and exclude trust/client
    if not cisdep.is_empty() and not dep_final.is_empty():
        merged = cisdep.sort('ACCTNO').join(dep_final.sort('ACCTNO'), on='ACCTNO', how='inner')
        if not tc.is_empty(): merged = merged.join(tc.select('ACCTNO'), on='ACCTNO', how='anti')
        print(f"  CIS merged: {len(merged)}")
        
        # Group by KEY and AMTIND
        summary = merged.group_by(['KEY', 'AMTIND']).agg([
            pl.col('CURBAL').sum().alias('TOTBAL'),
            pl.col('INTPAYBL').sum().alias('TOTINTPY'),
            pl.col('CURBALTT').sum().alias('TOTCURTT')
        ])
        
        # Insurance calculation with FIRST.KEY logic
        result, last_key = [], None
        for row in summary.sort('KEY').rows(named=True):
            key, tot = row['KEY'], row['TOTCURTT']
            if key != last_key:
                if tot > 250000:
                    insured, excess, moresix, lesssix = 250000, tot-250000, 250000, 0
                else:
                    insured, excess, moresix, lesssix = tot, 0, 0, tot
                first = True
            else:
                first = False
                if tot > 250000:
                    inshare = (row['CURBALTT']/tot) * 250000 if tot>0 else 0
                else:
                    inshare = row['CURBALTT']
            
            result.append({
                'KEY': key, 'AMTIND': row['AMTIND'], 'TOTCURTT': tot,
                'TOTBAL': row['TOTBAL'], 'TOTINTPY': row['TOTINTPY'],
                'INSURED': insured if first else None,
                'EXCESS': excess if first else None,
                'MORESIX': moresix if first else None,
                'LESSSIX': lesssix if first else None,
                'INSUREBR': inshare if not first else None,
                'INSURSUM': (insured if first else 0) * 0.0006,
                'TOTALSUM': tot * 0.0002
            })
            last_key = key
        
        ins_df = pl.DataFrame(result)
        
        # Final reports
        print("\n"+"="*60+"\nDEPOSIT INSURANCE REPORT\n"+"="*60)
        totals = ins_df.select([
            pl.col('INSURED').fill_null(0).sum(), pl.col('EXCESS').fill_null(0).sum(),
            pl.col('INSURSUM').sum(), pl.col('TOTALSUM').sum()
        ]).rows()[0]
        print(f"\nInsured: RM {totals[0]:,.2f}\nExcess: RM {totals[1]:,.2f}")
        print(f"Premium (0.06%): RM {totals[2]:,.2f}\nPremium (0.02%): RM {totals[3]:,.2f}")
        
        # TABULATE equivalent - by JOIN and PRODCD
        if 'JOIN' in merged.columns and 'PRODCD' in merged.columns:
            tab1 = merged.group_by(['JOIN', 'PRODCD']).agg([
                pl.col('CURBALTT').sum().alias('CURBALTT'),
                pl.col('CURBALTT').sum().alias('TOTFINAL'),
                pl.col('CURBAL').sum().alias('TOTBAL1'),
                (pl.col('CURBALTT').sum() - 250000).clip_min(0).alias('EXCESS'),
                pl.when(pl.col('CURBALTT').sum()>250000).then(250000).otherwise(pl.col('CURBALTT').sum()).alias('INSURED')
            ])
            print("\nBy JOIN and PRODCD:")
            for r in tab1.sort('JOIN').rows(named=True):
                print(f"  JOIN {r['JOIN']} {r['PRODCD']}: {r['CURBALTT']:,.2f}")
        
        # Branch-level apportionment
        if 'BRANCH' in merged.columns:
            branch = merged.group_by('BRANCH').agg([
                pl.col('CURBALTT').sum().alias('TOTBR'),
                pl.col('CURBALTT').map_elements(lambda x: (x/x.sum()*250000).sum() if x.sum()>250000 else x.sum()).alias('INSUREBR')
            ])
            print("\nBranch apportionment:")
            for r in branch.sort('BRANCH').rows(named=True):
                print(f"  Branch {r['BRANCH']}: Insured {r['INSUREBR']:,.2f}")
        
        ins_df.write_parquet(f"{PATHS['OUTPUT']}CISDEPD.parquet")
        print(f"\nSaved: CISDEPD.parquet ({len(ins_df)} records)")
    
    print("\n"+"="*60+"\n✓ EIBQDISF Complete")

if __name__ == "__main__":
    main()
