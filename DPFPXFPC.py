"""
EIIQINST - Islamic Trustee and Client Account Quarterly Reporting
Processes Islamic trustee and client accounts with balance thresholds (>60k/<=60k)
Includes PBBDPFMT format mappings for product codes (Islamic version)
"""

import pyreadstat
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from PBBDPFMT import *

# =============================================================================
# CONFIGURATION
# =============================================================================
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS'
INPUT_PATH = f'{BASE_PATH}/input/prod/EIIQINST'
OUTPUT_PATH = f'{BASE_PATH}/output/EIIQINST'

Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)

PROD_CODES = [
    '42110', '42310', '42120', '42320', '42130', '42133', '42132', '42180',
    '42610', '42630', '34180', '42199', '42699'
]

# =============================================================================
# DATE PROCESSING
# =============================================================================
def get_dates():
    """Calculate report dates matching SAS logic"""
    today = datetime.now()
    first_of_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    reptdate = first_of_month - timedelta(days=1)
    
    day = reptdate.day
    if day == 8:
        sdd, wk = 1, '1'
    elif day == 15:
        sdd, wk = 9, '2'
    elif day == 22:
        sdd, wk = 16, '3'
    else:
        sdd, wk = 23, '4'
    
    mm = reptdate.month
    
    return {
        'nowk': wk,
        'reptmon': f"{mm:02d}",
        'reptyear': str(reptdate.year),
        'sdate': datetime(reptdate.year, mm, sdd),
        'sdesc': 'PUBLIC BANK BERHAD'
    }

# =============================================================================
# DATA LOADING UTILITIES
# =============================================================================
def read_sas_file(filepath):
    """Read SAS file and convert column names to lowercase"""
    try:
        df, _ = pyreadstat.read_sas7bdat(filepath)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        print(f"  Warning: Could not read {filepath}: {e}")
        return pd.DataFrame()

def standardize_acctno(df):
    """Standardize ACCTNO column to string type for consistent merging"""
    if 'acctno' in df.columns:
        df = df.copy()
        # Handle potential NaN values and convert to proper string format
        df['acctno'] = df['acctno'].fillna(0).astype('int64').astype(str).str.strip()
    return df

def load_float():
    """Load FLOAT data from PIDMS"""
    df = read_sas_file(f"{INPUT_PATH}/float.sas7bdat")
    if not df.empty and 'acctno' in df.columns and 'float' in df.columns:
        return standardize_acctno(df.groupby('acctno')['float'].sum().reset_index())
    return pd.DataFrame()

def load_ibgpidm():
    """Load IBGPIDM data from text file"""
    filepath = f"{INPUT_PATH}/IBGPIDM.txt"
    if not Path(filepath).exists():
        print(f"  Warning: IBGPIDM text file not found")
        return pd.DataFrame()
    
    try:
        data = []
        with open(filepath, 'r') as f:
            for line in f:
                if len(line) >= 28:
                    acct_str = line[0:10].strip()
                    ibgamt_str = line[11:27].strip()
                    if acct_str and ibgamt_str:
                        try:
                            data.append({
                                'acctno': str(int(float(acct_str))),
                                'ibgamt': float(ibgamt_str)
                            })
                        except ValueError:
                            continue
        
        df = pd.DataFrame(data)
        if not df.empty:
            return standardize_acctno(df.groupby('acctno')['ibgamt'].sum().reset_index())
    except Exception as e:
        print(f"  Error reading IBGPIDM: {e}")
    
    return pd.DataFrame()

def load_remit(d):
    """Load REMIT and UNCLAIM data"""
    remit = read_sas_file(f"{INPUT_PATH}/remit.sas7bdat")
    unclaim = read_sas_file(f"{INPUT_PATH}/unclaim{d['reptyear']}.sas7bdat")
    
    if remit.empty:
        return pd.DataFrame()
    
    if not unclaim.empty and 'ledgbal' in unclaim.columns:
        unclaim = unclaim.rename(columns={'ledgbal': 'unclaimx'})
    
    combined = pd.concat([remit, unclaim], ignore_index=True) if not unclaim.empty else remit.copy()
    if 'unclaimx' not in combined.columns:
        combined['unclaimx'] = 0
    
    if 'paymode' in combined.columns:
        summary = combined.groupby('paymode').agg({
            'ledgbal': 'sum',
            'unclaimx': 'sum'
        }).reset_index()
        summary.columns = ['paymode', 'plusbal', 'unclaim']
        
        orig = remit.drop_duplicates(subset=['paymode'])
        result = summary.merge(orig, on='paymode', how='left')
        result['acctno'] = result['paymode'].astype(str).str.strip()
        result = result.drop(columns=['paymode', 'ledgbal', 'unclaimx'], errors='ignore')
        return standardize_acctno(result)
    
    return pd.DataFrame()

def load_account_data():
    """Load SA/CA/FD accounts with purpose 5/6"""
    dfs = []
    
    # SAVING
    saving = read_sas_file(f"{INPUT_PATH}/saving.sas7bdat")
    if not saving.empty and 'purpose' in saving.columns:
        saving = saving[saving['purpose'].astype(str).isin(['5', '6'])].copy()
        cols = ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
        saving = saving[[c for c in cols if c in saving.columns]]
        dfs.append(saving)
    
    # CURRENT
    current = read_sas_file(f"{INPUT_PATH}/current.sas7bdat")
    if not current.empty and 'purpose' in current.columns:
        current = current[current['purpose'].astype(str).isin(['5', '6'])].copy()
        if 'acctno' in current.columns and 'curbal' in current.columns:
            current = current.sort_values(['acctno', 'curbal'], ascending=[True, False])
            current = current.drop_duplicates(subset=['acctno'], keep='first')
        if all(col in current.columns for col in ['curcode', 'intpaybl', 'forate']):
            current.loc[:, 'intpaybl'] = current.apply(
                lambda x: round(x['intpaybl'] * x['forate'], 2) if x['curcode'] != 'MYR' else x['intpaybl'],
                axis=1
            )
        cols = ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
        current = current[[c for c in cols if c in current.columns]]
        dfs.append(current)
    
    # FD
    fd = read_sas_file(f"{INPUT_PATH}/fd.sas7bdat")
    if not fd.empty and 'purpose' in fd.columns:
        fd = fd[fd['purpose'].astype(str).isin(['5', '6'])].copy()
        if all(col in fd.columns for col in ['curcode', 'intpaybl', 'forate']):
            fd.loc[:, 'intpaybl'] = fd.apply(
                lambda x: round(x['intpaybl'] * x['forate'], 2) if x['curcode'] != 'MYR' else x['intpaybl'],
                axis=1
            )
        cols = ['branch', 'acctno', 'name', 'product', 'purpose', 'curbal', 'intpaybl']
        fd = fd[[c for c in cols if c in fd.columns]]
        dfs.append(fd)
    
    if dfs:
        return standardize_acctno(pd.concat(dfs, ignore_index=True))
    return pd.DataFrame()

def load_dep(d):
    """Load DEP data from monthly files"""
    dfs = []
    
    # SAVG
    savg_path = f"{INPUT_PATH}/savg{d['reptmon']}{d['nowk']}.sas7bdat"
    if Path(savg_path).exists():
        df = read_sas_file(savg_path)
        if not df.empty:
            cols = [c for c in ['acctno', 'amtind', 'prodcd', 'product'] if c in df.columns]
            if cols:
                dfs.append(df[cols])
    
    # CURN
    curn_path = f"{INPUT_PATH}/curn{d['reptmon']}{d['nowk']}.sas7bdat"
    if Path(curn_path).exists():
        df = read_sas_file(curn_path)
        if not df.empty:
            cols = [c for c in ['acctno', 'amtind', 'prodcd', 'product'] if c in df.columns]
            if cols:
                dfs.append(df[cols])
    
    # FDMTHLY
    fdmthly_path = f"{INPUT_PATH}/fdmthly.sas7bdat"
    if Path(fdmthly_path).exists():
        df = read_sas_file(fdmthly_path)
        if not df.empty:
            df = df.rename(columns={'bic': 'prodcd', 'accttype': 'product'})
            cols = [c for c in ['acctno', 'amtind', 'prodcd', 'product'] if c in df.columns]
            if cols:
                dfs.append(df[cols])
    
    if not dfs:
        return pd.DataFrame()
    
    combined = pd.concat(dfs, ignore_index=True)
    
    if 'prodcd' in combined.columns:
        combined = combined.copy()
        combined['prodcd'] = combined['prodcd'].astype(str)
        combined = combined[combined['prodcd'].isin(PROD_CODES)]
        
        if 'product' in combined.columns:
            mask = ~((combined['prodcd'].isin(['42199', '42699'])) & 
                     (~combined['product'].astype(str).isin(['72', '413'])))
            combined = combined[mask]
    
    if 'acctno' in combined.columns:
        return standardize_acctno(combined.drop_duplicates(subset=['acctno']))
    return pd.DataFrame()

def load_client():
    """Load CLIENT file from SAS dataset"""
    filepath = f"{INPUT_PATH}/client.sas7bdat"
    if not Path(filepath).exists():
        print(f"  Warning: CLIENT file not found")
        return pd.DataFrame()
    
    df = read_sas_file(filepath)
    if not df.empty:
        return standardize_acctno(df)
    return pd.DataFrame()

# =============================================================================
# OUTPUT UTILITIES
# =============================================================================
def write_text_output(df, title, filename):
    """Write DataFrame to text file in SAS format"""
    if df.empty:
        return
    
    lines = [" ", title, " "]
    header = "BRANCH;ACCTNO;NAME;PURPOSE;AVBAL;INTPAYBL;PRODUCT;AMTIND;PLUSBAL;UNCLAIM;SI;IBGAMT;AVBALTT;"
    lines.append(header)
    
    for _, r in df.iterrows():
        line = (
            f"{r.get('branch', '')};{r.get('acctno', '')};{r.get('name', '')};{r.get('purpose', '')};"
            f"{r.get('avbal', 0):.2f};{r.get('intpaybl', 0):.2f};{r.get('product', '')};{r.get('amtind', '')};"
            f"{r.get('plusbal', 0):.2f};{r.get('unclaim', 0):.2f};{r.get('si', 0):.2f};"
            f"{r.get('ibgamt', 0):.2f};{r.get('avbaltt', 0):.2f};"
        )
        lines.append(line)
    
    output_path = Path(f"{OUTPUT_PATH}/{filename}")
    output_path.write_text('\n'.join(lines))
    print(f"  Output: {output_path}")

def print_branch_summary(df, title):
    """Print branch summary"""
    if not df.empty and 'branch' in df.columns:
        print(f"\n{title} by Branch:")
        for branch, total in df.groupby('branch')['avbaltt'].sum().sort_index().items():
            # Convert branch to int if possible to remove decimals
            branch_display = int(branch) if float(branch).is_integer() else branch
            print(f"  Branch {branch_display}: RM {total:,.2f}")

# =============================================================================
# MAIN PROCESSING
# =============================================================================
def process_trustee_accounts(d, float_df, ibg_df, remit_df, dep):
    """Process trustee accounts"""
    print("\nProcessing Trustee Accounts...")
    
    saca = load_account_data()
    print(f"  SA/CA/FD: {len(saca)} rows")
    
    if saca.empty:
        return None
    
    # Merge with FLOAT
    trustee = saca.merge(float_df, on='acctno', how='left') if not float_df.empty else saca.copy()
    if 'float' not in trustee.columns:
        trustee['float'] = 0
    
    # Calculate AVBAL and AVBALTT
    trustee = trustee.copy()
    trustee['float'] = trustee['float'].fillna(0)
    trustee['curbal'] = trustee['curbal'].fillna(0)
    trustee['intpaybl'] = trustee['intpaybl'].fillna(0)
    trustee['avbal'] = trustee['curbal'] - trustee['float']
    trustee['avbaltt'] = trustee['avbal'] + trustee['intpaybl']
    
    # Merge with DEP
    if not dep.empty:
        trustee = trustee.merge(dep, on='acctno', how='inner')
    
    # Merge with REMIT
    if not remit_df.empty:
        remit_cols = [c for c in ['acctno', 'plusbal', 'unclaim'] if c in remit_df.columns]
        if remit_cols:
            trustee = trustee.merge(remit_df[remit_cols], on='acctno', how='left')
    if 'plusbal' not in trustee.columns:
        trustee['plusbal'] = 0
    if 'unclaim' not in trustee.columns:
        trustee['unclaim'] = 0
    
    trustee = trustee.copy()
    trustee['plusbal'] = trustee['plusbal'].fillna(0)
    trustee['unclaim'] = trustee['unclaim'].fillna(0)
    trustee['avbaltt'] = trustee['avbal'] + trustee['plusbal'] + trustee['unclaim'] + trustee['intpaybl']
    
    # Add SI
    trustee['si'] = 0
    trustee['avbaltt'] += trustee['si']
    
    # Merge with IBGPIDM
    if not ibg_df.empty:
        trustee = trustee.merge(ibg_df, on='acctno', how='left')
    if 'ibgamt' not in trustee.columns:
        trustee['ibgamt'] = 0
    trustee = trustee.copy()
    trustee['ibgamt'] = trustee['ibgamt'].fillna(0)
    trustee['avbaltt'] += trustee['ibgamt']
    
    # Split by threshold
    high = trustee[trustee['avbaltt'] > 60000]
    low = trustee[trustee['avbaltt'] <= 60000]
    
    print(f"  Trustee >60k: {len(high)} accounts")
    print(f"  Trustee <=60k: {len(low)} accounts")
    
    write_text_output(high, "TRUSTEE >60000", "islamic_trustee_high.txt")
    write_text_output(low, "TRUSTEE <=60000", "islamic_trustee_low.txt")
    
    print_branch_summary(high, "TRUSTEE >60000")
    print_branch_summary(low, "TRUSTEE <=60000")
    
    return trustee, high, low

def process_client_accounts(client_df):
    """Process client accounts from pre-processed data"""
    print("\nProcessing Client Accounts...")
    print(f"  CLIENT master: {len(client_df)} rows")
    
    if client_df.empty:
        return None
    
    # Ensure required columns exist
    client_df = client_df.copy()
    for col, default in [('si', 0), ('ibgamt', 0), ('plusbal', 0), ('unclaim', 0),
                          ('amtind', ''), ('purpose', '')]:
        if col not in client_df.columns:
            client_df[col] = default
    
    # Split by threshold
    high = client_df[client_df['avbaltt'] > 60000]
    low = client_df[client_df['avbaltt'] <= 60000]
    
    print(f"  Client >60k: {len(high)} accounts")
    print(f"  Client <=60k: {len(low)} accounts")
    
    write_text_output(high, "CLIENT >60000", "islamic_client_high.txt")
    write_text_output(low, "CLIENT <=60000", "islamic_client_low.txt")
    
    print_branch_summary(high, "CLIENT >60000")
    print_branch_summary(low, "CLIENT <=60000")
    
    return client_df, high, low

def check_duplicates(trustee, client):
    """Check for duplicate accounts between trustee and client"""
    print("\nChecking for duplicate accounts...")
    
    if trustee is None or client is None or trustee.empty or client.empty:
        print("  No data to check")
        return
    
    trustee_src = trustee[['acctno']].copy()
    trustee_src['src'] = 'TRUSTEE'
    client_src = client[['acctno']].copy()
    client_src['src'] = 'CLIENT'
    
    all_acc = pd.concat([trustee_src, client_src])
    dup = all_acc[all_acc.duplicated(subset=['acctno'], keep=False)]
    
    if not dup.empty:
        print(f"  Found {len(dup)} duplicate accounts:")
        for acctno, row in dup.groupby('acctno')['src'].apply(list).items():
            print(f"    {acctno} appears in: {', '.join(row)}")
    else:
        print("  No duplicate accounts found")

def print_summary(trustee_data, client_data):
    """Print final summary"""
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if trustee_data:
        trustee, high, low = trustee_data
        print(f"\nTrustee Accounts:")
        print(f"  Total: RM {trustee['avbaltt'].sum():,.2f}")
        print(f"  >60k: RM {high['avbaltt'].sum():,.2f} ({len(high)} accounts)")
        print(f"  <=60k: RM {low['avbaltt'].sum():,.2f} ({len(low)} accounts)")
    
    if client_data:
        client, high, low = client_data
        print(f"\nClient Accounts:")
        print(f"  Total: RM {client['avbaltt'].sum():,.2f}")
        print(f"  >60k: RM {high['avbaltt'].sum():,.2f} ({len(high)} accounts)")
        print(f"  <=60k: RM {low['avbaltt'].sum():,.2f} ({len(low)} accounts)")

def main():
    print("=" * 60)
    print("EIIQINST - Islamic Trustee and Client Account Reporting")
    print("=" * 60)
    
    d = get_dates()
    print(f"\nReport Period: {d['reptmon']}/{d['reptyear']} (Week: {d['nowk']})")
    print(f"SDESC: {d['sdesc']}")
    
    # Load data
    print("\nLoading data...")
    float_df = load_float()
    print(f"  FLOAT: {len(float_df)} rows")
    
    ibg_df = load_ibgpidm()
    print(f"  IBGPIDM: {len(ibg_df)} rows")
    
    remit_df = load_remit(d)
    print(f"  REMIT: {len(remit_df)} rows")
    
    dep = load_dep(d)
    print(f"  DEP: {len(dep)} rows")
    
    client_df = load_client()
    print(f"  CLIENT: {len(client_df)} rows")
    
    # Process accounts
    trustee_data = process_trustee_accounts(d, float_df, ibg_df, remit_df, dep)
    client_data = process_client_accounts(client_df)
    
    # Check duplicates
    if trustee_data and client_data:
        check_duplicates(trustee_data[0], client_data[0])
    
    # Print summary
    print_summary(trustee_data, client_data)
    
    print("\n" + "=" * 60)
    print("✓ EIIQINST Complete")

if __name__ == "__main__":
    main()
