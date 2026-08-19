"""
EIIQINST - Islamic Trustee and Client Account Quarterly Reporting
Processes Islamic trustee and client accounts with balance thresholds (>60k/<=60k)
Includes PBBDPFMT format mappings for product codes (Islamic version)
"""

import pyreadstat
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from PBBDPFMT import *  # Import existing PBBDPFMT program

# =============================================================================
# CONFIG
# =============================================================================
PATHS = {
    'PIDMS': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/',
    'SACA': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/',
    'DEPOSIT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/',
    'DEPOSIX': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQINST/deposix/',
    'UNCLAIM': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/',
    'OUTPUT': '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQINST/'
}
for p in PATHS.values():
    Path(p).mkdir(parents=True, exist_ok=True)

# Product code filters (from PBBDPFMT)
PROD_CODES = [
    '42110', '42310', '42120', '42320', '42130', '42133', '42132', '42180',
    '42610', '42630', '34180', '42199', '42699'
]

# =============================================================================
# DATE PROCESSING (matches SAS REPTDATE logic)
# =============================================================================
def get_dates():
    """Calculate report dates matching SAS logic exactly"""
    today = datetime.now()
    
    # REPTDATE = INPUT('01'||PUT(MONTH(TODAY()),Z2.)||PUT(YEAR(TODAY()),4.),DDMMYY8.)-1
    # This creates first day of current month, then subtracts 1 day
    first_of_month = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
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
    
    # IF WK = '1' THEN DO; MM1 = MM - 1; IF MM1 = 0 THEN MM1 = 12; END;
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
    sdate = datetime(reptdate.year, mm, sdd)
    
    # SDESC = 'PUBLIC BANK BERHAD'
    sdesc = 'PUBLIC BANK BERHAD'
    
    return {
        'nowk': wk,
        'reptmon': f"{mm:02d}",
        'reptyear': str(reptdate.year),
        'sdate': sdate,
        'sdesc': sdesc
    }

# =============================================================================
# DATA LOADING (using pyreadstat for SAS files)
# =============================================================================
def read_sas_file(filepath):
    """Read SAS file using pyreadstat and convert column names to lowercase"""
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        # Convert all column names to lowercase
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        print(f"  Warning: Could not read {filepath}: {e}")
        return pd.DataFrame()

def load_float():
    """Load FLOAT data from PIDMS (DATA FLOAT; SET PIDMS.FLOAT; PROC SUMMARY)"""
    df = read_sas_file(f"{PATHS['PIDMS']}float.sas7bdat")
    if not df.empty and 'acctno' in df.columns and 'float' in df.columns:
        # PROC SUMMARY DATA=FLOAT NWAY; CLASS ACCTNO; VAR FLOAT; OUTPUT OUT=FLOAT SUM=;
        return df.groupby('acctno')['float'].sum().reset_index()
    return pd.DataFrame()

def load_ibgpidm():
    """Load IBGPIDM data (PROC SUMMARY DATA=IBGPIDM NWAY; BY ACCTNO; VAR IBGAMT)"""
    df = read_sas_file(f"{PATHS['DEPOSIT']}ibgpidm.sas7bdat")
    if not df.empty and 'acctno' in df.columns and 'ibgamt' in df.columns:
        return df.groupby('acctno')['ibgamt'].sum().reset_index()
    return pd.DataFrame()

def load_remit(d):
    """Load REMIT and UNCLAIM data (DATA REMIT; SET DEPOSIT.REMIT UNCLAIM.UNCLAIM&REPTYEAR)"""
    remit = read_sas_file(f"{PATHS['DEPOSIT']}remit.sas7bdat")
    unclaim = read_sas_file(f"{PATHS['UNCLAIM']}unclaim{d['reptyear']}.sas7bdat")
    
    if remit.empty:
        return pd.DataFrame()
    
    # Rename LEDGBAL to UNCLAIMX in unclaim data
    if not unclaim.empty and 'ledgbal' in unclaim.columns:
        unclaim = unclaim.rename(columns={'ledgbal': 'unclaimx'})
    
    # Combine REMIT and UNCLAIM data
    if not unclaim.empty:
        combined = pd.concat([remit, unclaim], ignore_index=True)
    else:
        combined = remit.copy()
        combined['unclaimx'] = 0
    
    # PROC SUMMARY DATA=REMIT NWAY; CLASS PAYMODE; VAR LEDGBAL UNCLAIMX;
    if 'paymode' in combined.columns:
        summary = combined.groupby('paymode').agg({
            'ledgbal': 'sum',
            'unclaimx': 'sum'
        }).reset_index()
        summary.columns = ['paymode', 'plusbal', 'unclaim']
        
        # Get original for other fields (PROC SORT DATA=DEPOSIT.REMIT OUT=REMITORI NODUPKEYS)
        orig = remit.drop_duplicates(subset=['paymode'])
        result = summary.merge(orig, on='paymode', how='left')
        
        # ACCTNO = PAYMODE
        result['acctno'] = result['paymode']
        result = result.drop(columns=['paymode', 'ledgbal', 'unclaimx'], errors='ignore')
        return result
    
    return pd.DataFrame()

def load_saving():
    """Load SAVING accounts with purpose 5/6 (DATA SA; SET SACA.SAVING; WHERE PURPOSE IN ('5','6'))"""
    df = read_sas_file(f"{PATHS['SACA']}saving.sas7bdat")
    if not df.empty and 'purpose' in df.columns:
        df = df[df['purpose'].astype(str).isin(['5', '6'])]
        # KEEP BRANCH ACCTNO NAME PURPOSE PRODUCT CURBAL INTPAYBL
        cols = ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
        cols = [c for c in cols if c in df.columns]
        return df[cols]
    return pd.DataFrame()

def load_current():
    """Load CURRENT accounts with purpose 5/6 and FX conversion (DATA CA)"""
    df = read_sas_file(f"{PATHS['SACA']}current.sas7bdat")
    if not df.empty and 'purpose' in df.columns:
        df = df[df['purpose'].astype(str).isin(['5', '6'])]
        
        # Take highest balance for duplicate accounts (PROC SORT DATA=CURRENT;BY ACCTNO DESCENDING CURBAL)
        if 'acctno' in df.columns and 'curbal' in df.columns:
            df = df.sort_values(['acctno', 'curbal'], ascending=[True, False])
            df = df.drop_duplicates(subset=['acctno'], keep='first')
        
        # IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE,.01)
        if all(col in df.columns for col in ['curcode', 'intpaybl', 'forate']):
            df['intpaybl'] = df.apply(
                lambda x: round(x['intpaybl'] * x['forate'], 2) if x['curcode'] != 'MYR' else x['intpaybl'],
                axis=1
            )
        
        # KEEP BRANCH ACCTNO NAME PURPOSE PRODUCT CURBAL INTPAYBL
        cols = ['branch', 'acctno', 'name', 'purpose', 'product', 'curbal', 'intpaybl']
        cols = [c for c in cols if c in df.columns]
        return df[cols]
    return pd.DataFrame()

def load_fd():
    """Load FD accounts with purpose 5/6 and FX conversion (DATA FD)"""
    df = read_sas_file(f"{PATHS['SACA']}fd.sas7bdat")
    if not df.empty and 'purpose' in df.columns:
        df = df[df['purpose'].astype(str).isin(['5', '6'])]
        
        # IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE,.01)
        if all(col in df.columns for col in ['curcode', 'intpaybl', 'forate']):
            df['intpaybl'] = df.apply(
                lambda x: round(x['intpaybl'] * x['forate'], 2) if x['curcode'] != 'MYR' else x['intpaybl'],
                axis=1
            )
        
        # KEEP BRANCH ACCTNO NAME PRODUCT PURPOSE CURBAL INTPAYBL
        cols = ['branch', 'acctno', 'name', 'product', 'purpose', 'curbal', 'intpaybl']
        cols = [c for c in cols if c in df.columns]
        return df[cols]
    return pd.DataFrame()

def load_dep(d):
    """Load DEP data from monthly files (DATA DEP; SET DEP.SAVG&REPTMON&NOWK ...)"""
    dfs = []
    
    # DEP.SAVG&REPTMON&NOWK
    savg_path = f"{PATHS['DEPOSIT']}savg{d['reptmon']}{d['nowk']}.sas7bdat"
    if Path(savg_path).exists():
        df = read_sas_file(savg_path)
        if not df.empty:
            # KEEP=ACCTNO AMTIND PRODCD PRODUCT
            cols = ['acctno', 'amtind', 'prodcd', 'product']
            cols = [c for c in cols if c in df.columns]
            if cols:
                dfs.append(df[cols])
    
    # DEP.CURN&REPTMON&NOWK
    curn_path = f"{PATHS['DEPOSIT']}curn{d['reptmon']}{d['nowk']}.sas7bdat"
    if Path(curn_path).exists():
        df = read_sas_file(curn_path)
        if not df.empty:
            # KEEP=ACCTNO AMTIND PRODCD PRODUCT
            cols = ['acctno', 'amtind', 'prodcd', 'product']
            cols = [c for c in cols if c in df.columns]
            if cols:
                dfs.append(df[cols])
    
    # DEP.FDMTHLY with RENAME=(BIC=PRODCD ACCTTYPE=PRODUCT)
    fdmthly_path = f"{PATHS['DEPOSIT']}fdmthly.sas7bdat"
    if Path(fdmthly_path).exists():
        df = read_sas_file(fdmthly_path)
        if not df.empty:
            # RENAME=(BIC=PRODCD ACCTTYPE=PRODUCT)
            if 'bic' in df.columns:
                df = df.rename(columns={'bic': 'prodcd'})
            if 'accttype' in df.columns:
                df = df.rename(columns={'accttype': 'product'})
            # KEEP=ACCTNO AMTIND BIC ACCTTYPE
            cols = ['acctno', 'amtind', 'prodcd', 'product']
            cols = [c for c in cols if c in df.columns]
            if cols:
                dfs.append(df[cols])
    
    if not dfs:
        return pd.DataFrame()
    
    combined = pd.concat(dfs, ignore_index=True)
    
    # IF PRODCD IN ('42110','42310',...) 
    if 'prodcd' in combined.columns:
        combined['prodcd'] = combined['prodcd'].astype(str)
        combined = combined[combined['prodcd'].isin(PROD_CODES)]
        
        # IF PRODCD IN ('42199','42699') AND PRODUCT NOT IN (72,413) THEN DELETE
        if 'product' in combined.columns:
            mask = ~((combined['prodcd'].isin(['42199', '42699'])) & 
                     (~combined['product'].astype(str).isin(['72', '413'])))
            combined = combined[mask]
    
    # PROC SORT DATA=DEP NODUPKEYS; BY ACCTNO;
    if 'acctno' in combined.columns:
        return combined.drop_duplicates(subset=['acctno'])
    return pd.DataFrame()

def load_client():
    """Load CLIENT file (DATA DEPOSIT.CLIENT; INFILE CLIENT)"""
    # Try to read as SAS file first
    client_path = f"{PATHS['DEPOSIT']}client.sas7bdat"
    df = read_sas_file(client_path)
    
    if df.empty:
        # If not SAS, try reading as text file with fixed positions
        txt_path = f"{PATHS['DEPOSIT']}client.txt"
        if Path(txt_path).exists():
            try:
                with open(txt_path, 'r') as f:
                    lines = f.readlines()
                
                data = []
                for line in lines:
                    if len(line) >= 21:
                        # INPUT @002 ACCTNO 10.
                        acct_str = line[1:11].strip()
                        # Check if ACCTNO is numeric
                        if acct_str.replace(' ', '').isdigit():
                            acctno = int(acct_str)
                            # INPUT @021 NAME $40.
                            name = line[20:60].strip()
                            if name:
                                data.append({
                                    'acctno': acctno,
                                    'name': name,
                                    'key': name[:10]
                                })
                
                df = pd.DataFrame(data)
            except Exception as e:
                print(f"  Warning: Could not read client.txt: {e}")
                return pd.DataFrame()
    
    if not df.empty:
        # PROC SORT DATA=DEPOSIT.CLIENT NODUPKEYS; BY ACCTNO;
        if 'acctno' in df.columns:
            df = df.drop_duplicates(subset=['acctno'])
        
        # KEY = SUBSTR(NAME,1,10)
        if 'name' in df.columns:
            df['key'] = df['name'].str[:10]
        
        return df
    
    return pd.DataFrame()

def load_uma():
    """Load UMA data for SASA merge (DATA SASA; SET SACA.SAVING DEP.UMA)"""
    df = read_sas_file(f"{PATHS['DEPOSIT']}uma.sas7bdat")
    return df

# =============================================================================
# MAIN PROCESSING
# =============================================================================
def main():
    print("=" * 60)
    print("EIIQINST - Islamic Trustee and Client Account Reporting")
    print("=" * 60)
    
    d = get_dates()
    print(f"\nReport Period: {d['reptmon']}/{d['reptyear']} (Week: {d['nowk']})")
    print(f"SDESC: {d['sdesc']}")
    
    # ========== PART 1: TRUSTEE ACCOUNTS ==========
    print("\nProcessing Trustee Accounts...")
    
    # Load FLOAT
    float_df = load_float()
    print(f"  FLOAT: {len(float_df)} rows")
    
    # Load IBGPIDM
    ibg_df = load_ibgpidm()
    print(f"  IBGPIDM: {len(ibg_df)} rows")
    
    # Load REMIT/UNCLAIM
    remit_df = load_remit(d)
    print(f"  REMIT: {len(remit_df)} rows")
    
    # Load SA/CA/FD
    sa = load_saving()
    ca = load_current()
    fd = load_fd()
    
    # DATA DEPOSIX.MERGE; SET SA CA FD;
    saca_list = [df for df in [sa, ca, fd] if not df.empty]
    saca = pd.concat(saca_list, ignore_index=True) if saca_list else pd.DataFrame()
    print(f"  SA/CA/FD: {len(saca)} rows")
    
    if not saca.empty:
        # PROC SORT DATA=DEPOSIX.MERGE; BY ACCTNO;
        saca = saca.sort_values('acctno')
        
        # DATA DEPOSIX.MERGE; MERGE DEPOSIX.MERGE(IN=A) FLOAT(IN=B); BY ACCTNO;
        if not float_df.empty:
            trustee = saca.merge(float_df, on='acctno', how='left')
        else:
            trustee = saca.copy()
            trustee['float'] = 0
        
        # AVBAL = SUM(CURBAL,(-1)*FLOAT)
        trustee['float'] = trustee['float'].fillna(0) if 'float' in trustee.columns else 0
        trustee['curbal'] = trustee['curbal'].fillna(0) if 'curbal' in trustee.columns else 0
        trustee['intpaybl'] = trustee['intpaybl'].fillna(0) if 'intpaybl' in trustee.columns else 0
        
        trustee['avbal'] = trustee['curbal'] - trustee['float']
        # AVBALTT = SUM(AVBAL,INTPAYBL)
        trustee['avbaltt'] = trustee['avbal'] + trustee['intpaybl']
        
        # IF A (keep only records from DEPOSIX.MERGE)
        # (already handled by left merge)
        
        # PROC SORT DATA=DEPOSIX.MERGE OUT=MERGEX; BY ACCTNO;
        trustee = trustee.sort_values('acctno')
        
        # Load DEP
        dep = load_dep(d)
        
        # DATA DEPOSIX.MERGE; MERGE MERGEX (IN=A) DEP (IN=B); BY ACCTNO; IF A & B;
        if not dep.empty:
            trustee = trustee.merge(dep, on='acctno', how='inner')
        
        # Merge with REMIT
        # DATA DEPOSIX.MERGE; MERGE MERGE(IN=A) REMIT; BY ACCTNO;
        if not remit_df.empty:
            remit_cols = ['acctno', 'plusbal', 'unclaim']
            remit_cols = [c for c in remit_cols if c in remit_df.columns]
            if remit_cols:
                trustee = trustee.merge(remit_df[remit_cols], on='acctno', how='left')
                trustee['plusbal'] = trustee['plusbal'].fillna(0) if 'plusbal' in trustee.columns else 0
                trustee['unclaim'] = trustee['unclaim'].fillna(0) if 'unclaim' in trustee.columns else 0
            else:
                trustee['plusbal'] = 0
                trustee['unclaim'] = 0
        else:
            trustee['plusbal'] = 0
            trustee['unclaim'] = 0
        
        # AVBALTT = SUM(AVBAL,PLUSBAL,UNCLAIM,INTPAYBL)
        trustee['avbaltt'] = (
            trustee['avbal'] + 
            trustee['plusbal'] + 
            trustee['unclaim'] + 
            trustee['intpaybl']
        )
        
        # SI = 0; AVBALTT = SUM(AVBALTT,SI)
        trustee['si'] = 0
        trustee['avbaltt'] = trustee['avbaltt'] + trustee['si']
        
        # Merge with IBGPIDM
        # DATA DEPOSIX.MERGE; MERGE DEPOSIX.MERGE(IN=A) DEPOSIT.IBGPIDM(IN=B RENAME=(IBGAMT=IBGAMTX));
        if not ibg_df.empty:
            trustee = trustee.merge(ibg_df, on='acctno', how='left')
            trustee['ibgamt'] = trustee['ibgamt'].fillna(0) if 'ibgamt' in trustee.columns else 0
        else:
            trustee['ibgamt'] = 0
        
        # AVBALTT = SUM(AVBALTT,IBGAMT)
        trustee['avbaltt'] = trustee['avbaltt'] + trustee['ibgamt']
        
        # Split by threshold
        # WHERE AVBALTT > 60000
        trustee_high = trustee[trustee['avbaltt'] > 60000]
        # WHERE AVBALTT <= 60000
        trustee_low = trustee[trustee['avbaltt'] <= 60000]
        
        print(f"  Trustee >60k: {len(trustee_high)} accounts")
        print(f"  Trustee <=60k: {len(trustee_low)} accounts")
        
        # ========== WRITE TEXT OUTPUTS ==========
        def write_txt(df, title, filename):
            if df.empty:
                return
            lines = []
            lines.append(" ")
            lines.append(title)
            lines.append(" ")
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
            
            output_path = Path(f"{PATHS['OUTPUT']}{filename}")
            output_path.write_text('\n'.join(lines))
            print(f"  Output written to: {output_path}")
        
        write_txt(trustee_high, "TRUSTEE >60000", "islamic_trustee_high.txt")
        write_txt(trustee_low, "TRUSTEE <=60000", "islamic_trustee_low.txt")
        
        # Print reports
        if not trustee_high.empty and 'branch' in trustee_high.columns:
            print("\nTRUSTEE >60000 by Branch:")
            branch_summary = trustee_high.groupby('branch')['avbaltt'].sum().sort_index()
            for branch, total in branch_summary.items():
                print(f"  Branch {branch}: RM {total:,.2f}")
        
        if not trustee_low.empty and 'branch' in trustee_low.columns:
            print("\nTRUSTEE <=60000 by Branch:")
            branch_summary = trustee_low.groupby('branch')['avbaltt'].sum().sort_index()
            for branch, total in branch_summary.items():
                print(f"  Branch {branch}: RM {total:,.2f}")
    
    # ========== PART 2: CLIENT ACCOUNTS ==========
    print("\nProcessing Client Accounts...")
    
    client_df = load_client()
    print(f"  CLIENT master: {len(client_df)} rows")
    
    # Load SASA (DATA SASA; SET SACA.SAVING DEP.UMA)
    uma = load_uma()
    sasa_list = [sa] if not sa.empty else []
    if not uma.empty:
        # Ensure UMA has same columns as SA
        if 'purpose' in uma.columns:
            uma = uma[uma['purpose'].astype(str).isin(['5', '6'])]
        sasa_list.append(uma)
    sasa = pd.concat(sasa_list, ignore_index=True) if sasa_list else pd.DataFrame()
    if not sasa.empty:
        sasa = sasa.sort_values('acctno')
        print(f"  SASA: {len(sasa)} rows")
    
    if not client_df.empty and not saca.empty:
        # DATA DEPOSIT; SET SA CA FD;
        # (Already have saca from Part 1)
        
        # IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE,.01)
        # (Already done in load functions)
        
        # PROC SORT DATA=DEPOSIT NODUPKEYS; BY ACCTNO;
        deposit = saca.drop_duplicates(subset=['acctno'])
        
        # PROC SORT DATA=DEPOSIT; BY ACCTNO;
        deposit = deposit.sort_values('acctno')
        
        # DATA DEPOSIT; MERGE DEPOSIT(IN=A) FLOAT(IN=B); BY ACCTNO;
        if not float_df.empty:
            deposit = deposit.merge(float_df, on='acctno', how='left')
        else:
            deposit['float'] = 0
        
        # AVBAL = SUM(CURBAL,(-1)*FLOAT)
        deposit['float'] = deposit['float'].fillna(0) if 'float' in deposit.columns else 0
        deposit['curbal'] = deposit['curbal'].fillna(0) if 'curbal' in deposit.columns else 0
        deposit['intpaybl'] = deposit['intpaybl'].fillna(0) if 'intpaybl' in deposit.columns else 0
        
        deposit['avbal'] = deposit['curbal'] - deposit['float']
        # AVBALTT = SUM(AVBAL,INTPAYBL)
        deposit['avbaltt'] = deposit['avbal'] + deposit['intpaybl']
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) DEPOSIT(IN=B); BY ACCTNO; IF A & B;
        client = client_df.merge(deposit, on='acctno', how='inner')
        
        # KEEP BRANCH ACCTNO NAME PRODUCT AVBAL INTPAYBL AVBALTT CURBAL FLOAT
        cols = ['branch', 'acctno', 'name', 'product', 'avbal', 'intpaybl', 'avbaltt', 'curbal', 'float']
        cols = [c for c in cols if c in client.columns]
        client = client[cols]
        
        # AVBALTT = SUM(AVBAL,INTPAYBL)
        client['avbaltt'] = client['avbal'] + client['intpaybl']
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) DEP (IN=B); BY ACCTNO; IF A & B;
        if not dep.empty:
            client = client.merge(dep, on='acctno', how='inner')
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) REMIT(DROP=NAME); BY ACCTNO;
        if not remit_df.empty:
            remit_cols = ['acctno', 'plusbal', 'unclaim']
            remit_cols = [c for c in remit_cols if c in remit_df.columns]
            if remit_cols:
                client = client.merge(remit_df[remit_cols], on='acctno', how='left')
                client['plusbal'] = client['plusbal'].fillna(0) if 'plusbal' in client.columns else 0
                client['unclaim'] = client['unclaim'].fillna(0) if 'unclaim' in client.columns else 0
            else:
                client['plusbal'] = 0
                client['unclaim'] = 0
        else:
            client['plusbal'] = 0
            client['unclaim'] = 0
        
        # AVBALTT = SUM(AVBAL,PLUSBAL,UNCLAIM,INTPAYBL)
        client['avbaltt'] = (
            client['avbal'] + 
            client['plusbal'] + 
            client['unclaim'] + 
            client['intpaybl']
        )
        
        # SI = 0; AVBALTT = SUM(AVBALTT,SI)
        client['si'] = 0
        client['avbaltt'] = client['avbaltt'] + client['si']
        
        # DATA DEPOSIT.CLIENT; MERGE DEPOSIT.CLIENT(IN=A) DEPOSIT.IBGPIDM(IN=B RENAME=(IBGAMT=IBGAMTX));
        if not ibg_df.empty:
            client = client.merge(ibg_df, on='acctno', how='left')
            client['ibgamt'] = client['ibgamt'].fillna(0) if 'ibgamt' in client.columns else 0
        else:
            client['ibgamt'] = 0
        
        # AVBALTT = SUM(AVBALTT,IBGAMT)
        client['avbaltt'] = client['avbaltt'] + client['ibgamt']
        
        # Split by threshold
        # WHERE AVBALTT > 60000
        client_high = client[client['avbaltt'] > 60000]
        # WHERE AVBALTT <= 60000
        client_low = client[client['avbaltt'] <= 60000]
        
        print(f"  Client >60k: {len(client_high)} accounts")
        print(f"  Client <=60k: {len(client_low)} accounts")
        
        # Write TEXT outputs
        def write_client_txt(df, title, filename):
            if df.empty:
                return
            lines = []
            lines.append(" ")
            lines.append(title)
            lines.append(" ")
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
            
            output_path = Path(f"{PATHS['OUTPUT']}{filename}")
            output_path.write_text('\n'.join(lines))
            print(f"  Output written to: {output_path}")
        
        write_client_txt(client_high, "CLIENT >60000", "islamic_client_high.txt")
        write_client_txt(client_low, "CLIENT <=60000", "islamic_client_low.txt")
        
        # Print reports
        if not client_high.empty and 'branch' in client_high.columns:
            print("\nCLIENT >60000 by Branch:")
            branch_summary = client_high.groupby('branch')['avbaltt'].sum().sort_index()
            for branch, total in branch_summary.items():
                print(f"  Branch {branch}: RM {total:,.2f}")
        
        if not client_low.empty and 'branch' in client_low.columns:
            print("\nCLIENT <=60000 by Branch:")
            branch_summary = client_low.groupby('branch')['avbaltt'].sum().sort_index()
            for branch, total in branch_summary.items():
                print(f"  Branch {branch}: RM {total:,.2f}")
    
    # ========== PART 3: DUPLICATE ACCOUNTS ==========
    print("\nChecking for duplicate accounts...")
    if 'trustee' in locals() and 'client' in locals() and not trustee.empty and not client.empty:
        # DATA DUPLI; SET DEPOSIX.MERGE DEPOSIT.CLIENT;
        trustee_src = trustee[['acctno']].copy()
        trustee_src['src'] = 'TRUSTEE'
        client_src = client[['acctno']].copy()
        client_src['src'] = 'CLIENT'
        
        all_acc = pd.concat([trustee_src, client_src])
        
        # PROC SORT DATA=DUPLI; BY ACCTNO;
        all_acc = all_acc.sort_values('acctno')
        
        # DATA DUPLI DUPLI2; SET DUPLI; BY ACCTNO; IF FIRST.ACCTNO THEN OUTPUT DUPLI; ELSE OUTPUT DUPLI2;
        dup = all_acc[all_acc.duplicated(subset=['acctno'], keep=False)]
        
        if not dup.empty:
            print(f"  Found {len(dup)} duplicate accounts:")
            dup_grouped = dup.groupby('acctno')['src'].apply(list).reset_index()
            for _, row in dup_grouped.iterrows():
                print(f"    {row['acctno']} appears in: {', '.join(row['src'])}")
        else:
            print("  No duplicate accounts found")
    
    # ========== SUMMARY ==========
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    if 'trustee' in locals() and not trustee.empty:
        print(f"\nTrustee Accounts:")
        print(f"  Total: RM {trustee['avbaltt'].sum():,.2f}")
        print(f"  >60k: RM {trustee_high['avbaltt'].sum():,.2f} ({len(trustee_high)} accounts)")
        print(f"  <=60k: RM {trustee_low['avbaltt'].sum():,.2f} ({len(trustee_low)} accounts)")
    
    if 'client' in locals() and not client.empty:
        print(f"\nClient Accounts:")
        print(f"  Total: RM {client['avbaltt'].sum():,.2f}")
        print(f"  >60k: RM {client_high['avbaltt'].sum():,.2f} ({len(client_high)} accounts)")
        print(f"  <=60k: RM {client_low['avbaltt'].sum():,.2f} ({len(client_low)} accounts)")
    
    print("\n" + "=" * 60)
    print("✓ EIIQINST Complete")

if __name__ == "__main__":
    main()
