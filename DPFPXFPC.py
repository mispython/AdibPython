#!/usr/bin/env python3
"""
EIBDISLM - Islamic Banking Statistics
Processes daily Islamic account balances and monthly summaries
Supports SAS7BDAT input/output and Parquet output
"""

import duckdb
import pyreadstat
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import saspy
import os
import sys

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDISLM'
OUTPUT_DIR = BASE_DIR / '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

# Initialize SAS connection for output
try:
    sas = saspy.SASsession(cfgname='default')
    print("SAS Connection established successfully")
except Exception as e:
    print(f"Error connecting to SAS: {e}")
    sys.exit(1)

# Hardcode reptdate as yesterday's date
reptdate = datetime.now() - timedelta(days=1)
reptyear, reptmon, reptday = reptdate.year, reptdate.month, reptdate.day
rdate = reptdate.strftime('%d/%m/%Y')
zdate = int(reptdate.strftime('%y%m%d'))

print(f"Islamic Banking Statistics - {rdate}")
print(f"Processing data for date: {reptdate.strftime('%Y-%m-%d')}")

# ============================================================================
# FUNCTION TO INSPECT DATASET COLUMNS
# ============================================================================

def inspect_dataset(filepath, dataset_name):
    """Inspect and return column names of a SAS dataset"""
    try:
        df, meta = pyreadstat.read_sas7bdat(filepath)
        print(f"\nColumns in {dataset_name} (first 20):")
        cols = df.columns.tolist()
        print(f"  {', '.join(cols[:20])}")
        if len(cols) > 20:
            print(f"  ... and {len(cols)-20} more columns")
        print(f"  Total columns: {len(cols)}")
        return df.columns.tolist()
    except Exception as e:
        print(f"Error reading {dataset_name}: {e}")
        return []

# ============================================================================
# INSPECT INPUT DATASETS
# ============================================================================

print("\n" + "="*80)
print("INSPECTING INPUT DATASETS")
print("="*80)

# Read a small sample to get column names
try:
    saving_sample, _ = pyreadstat.read_sas7bdat(f'{INPUT_DIR}/deposit/saving.sas7bdat')
    current_sample, _ = pyreadstat.read_sas7bdat(f'{INPUT_DIR}/deposit/current.sas7bdat')
    
    print(f"\nSAVING dataset columns (first 20):")
    saving_cols = saving_sample.columns.tolist()
    print(f"  {', '.join(saving_cols[:20])}")
    
    print(f"\nCURRENT dataset columns (first 20):")
    current_cols = current_sample.columns.tolist()
    print(f"  {', '.join(current_cols[:20])}")
    
except Exception as e:
    print(f"Error reading sample: {e}")
    sys.exit(1)

# ============================================================================
# SECTION 1: DAILY ISLAMIC BALANCE SUMMARY (DYIBU)
# ============================================================================

print("\n" + "="*80)
print("SECTION 1: DAILY ISLAMIC BALANCE SUMMARY (DYIBU)")
print("="*80)

try:
    current_df, current_meta = pyreadstat.read_sas7bdat(f'{INPUT_DIR}/deposit/current.sas7bdat')
    saving_df, saving_meta = pyreadstat.read_sas7bdat(f'{INPUT_DIR}/deposit/saving.sas7bdat')
    print(f"Loaded CURRENT: {len(current_df)} rows, {len(current_df.columns)} columns")
    print(f"Loaded SAVING: {len(saving_df)} rows, {len(saving_df.columns)} columns")
except Exception as e:
    print(f"Error loading datasets: {e}")
    sas.endsas()
    sys.exit(1)

# Determine column names (try uppercase first since we saw OPENIND)
# Map of possible column names
branch_col = None
for col in ['BRANCH', 'branch', 'Branch', 'BRANCH_NO', 'branch_no']:
    if col in current_df.columns:
        branch_col = col
        break

product_col = None
for col in ['PRODUCT', 'product', 'Product', 'PROD', 'prod']:
    if col in current_df.columns:
        product_col = col
        break

curbal_col = None
for col in ['CURBAL', 'curbal', 'CurBal', 'CURRENT_BAL', 'current_bal']:
    if col in current_df.columns:
        curbal_col = col
        break

openind_col = None
for col in ['OPENIND', 'openind', 'OPEN_IND', 'open_ind', 'OpenInd']:
    if col in current_df.columns:
        openind_col = col
        break

print(f"\nUsing columns:")
print(f"  BRANCH: {branch_col}")
print(f"  PRODUCT: {product_col}")
print(f"  CURBAL: {curbal_col}")
print(f"  OPENIND: {openind_col}")

if None in [branch_col, product_col, curbal_col]:
    print(f"\nERROR: Could not find required columns")
    print(f"Available columns in CURRENT: {current_df.columns.tolist()[:20]}")
    sas.endsas()
    sys.exit(1)

# Filter out closed accounts
if openind_col:
    current_filtered = current_df[~current_df[openind_col].isin(['B','C','P'])][[branch_col, product_col, curbal_col]].copy()
    saving_filtered = saving_df[~saving_df[openind_col].isin(['B','C','P'])][[branch_col, product_col, curbal_col]].copy()
else:
    current_filtered = current_df[[branch_col, product_col, curbal_col]].copy()
    saving_filtered = saving_df[[branch_col, product_col, curbal_col]].copy()

# Rename columns to standard names
current_filtered.columns = ['branch', 'product', 'curbal']
saving_filtered.columns = ['branch', 'product', 'curbal']

current_filtered['reptdate'] = zdate
saving_filtered['reptdate'] = zdate

dyibu_raw = pd.concat([current_filtered, saving_filtered], ignore_index=True)
print(f"Combined raw data: {len(dyibu_raw)} rows")

# Create product category flags
dyibu_raw['sai'] = np.where(dyibu_raw['product'].isin([204,207,214,215]), dyibu_raw['curbal'], 0)
dyibu_raw['saino'] = np.where(dyibu_raw['product'].isin([204,207,214,215]), 1, 0)
dyibu_raw['mbs'] = np.where(dyibu_raw['product'] == 214, dyibu_raw['curbal'], 0)
dyibu_raw['mbsno'] = np.where(dyibu_raw['product'] == 214, 1, 0)

# CAI products (excluding certain products)
cai_products = [60,61,63,64,70,71,93,94,160,161,162,163,164,166,169,66,67,168,167,182,183,184,73]
exclude_products = [96,97,61,161,63,163]
cai_condition = dyibu_raw['product'].isin(cai_products) & (dyibu_raw['curbal'] > 0) & ~dyibu_raw['product'].isin(exclude_products)
dyibu_raw['cai'] = np.where(cai_condition, dyibu_raw['curbal'], 0)
dyibu_raw['caino'] = np.where(cai_condition, 1, 0)

dyibu_raw['ca96'] = np.where(dyibu_raw['product'].isin([96,97]) & (dyibu_raw['curbal'] > 0), dyibu_raw['curbal'], 0)
dyibu_raw['cai96'] = np.where(dyibu_raw['product'].isin([96,97]) & (dyibu_raw['curbal'] > 0), 1, 0)
dyibu_raw['caig'] = np.where(dyibu_raw['product'].isin([61,161]) & (dyibu_raw['curbal'] > 0), dyibu_raw['curbal'], 0)
dyibu_raw['caigno'] = np.where(dyibu_raw['product'].isin([61,161]) & (dyibu_raw['curbal'] > 0), 1, 0)
dyibu_raw['caih'] = np.where(dyibu_raw['product'].isin([63,163]) & (dyibu_raw['curbal'] > 0), dyibu_raw['curbal'], 0)
dyibu_raw['caihno'] = np.where(dyibu_raw['product'].isin([63,163]) & (dyibu_raw['curbal'] > 0), 1, 0)

# Aggregate by branch
dyibu = dyibu_raw.groupby(['branch', 'reptdate']).agg({
    'sai': 'sum', 'saino': 'sum', 'mbs': 'sum', 'mbsno': 'sum',
    'cai': 'sum', 'caino': 'sum', 'ca96': 'sum', 'cai96': 'sum',
    'caig': 'sum', 'caigno': 'sum', 'caih': 'sum', 'caihno': 'sum'
}).reset_index()

# Save DYIBU as SAS and Parquet
try:
    dyibu_sas_path = f'{OUTPUT_DIR}/dyibu{reptmon:02d}'
    sas.sasdata(dyibu, dyibu_sas_path)
    print(f"Saved SAS dataset: {dyibu_sas_path}")
except Exception as e:
    print(f"Error saving SAS dataset: {e}")

dyibu.to_parquet(f'{OUTPUT_DIR}/dyibu{reptmon:02d}.parquet')
print(f"Saved Parquet file: {OUTPUT_DIR}/dyibu{reptmon:02d}.parquet")
print(f"Section 1: DYIBU - {len(dyibu)} branches")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def calculate_age(bdate_str, reptdate, reptmon, reptday, reptyear):
    if pd.isna(bdate_str) or bdate_str == 0 or str(bdate_str).strip() == '' or str(bdate_str) == '0':
        return 0
    try:
        bdate_str = str(bdate_str).strip()
        # Handle different date formats
        if len(bdate_str) >= 8:
            # Try to parse as MMDDYYYY or YYYYMMDD
            try:
                bdate = datetime.strptime(bdate_str[:8], '%m%d%Y')
            except:
                try:
                    bdate = datetime.strptime(bdate_str[:8], '%Y%m%d')
                except:
                    return 0
            age = reptyear - bdate.year
            if age == AGELIMIT:
                if (bdate.month == reptmon and bdate.day > reptday) or bdate.month > reptmon:
                    age = AGEBELOW
            elif age == MAXAGE:
                if (bdate.month == reptmon and bdate.day > reptday) or bdate.month > reptmon:
                    age = AGELIMIT
            elif age > MAXAGE:
                age = MAXAGE
            elif age < AGELIMIT:
                age = AGEBELOW
            else:
                age = AGELIMIT
            return age
    except:
        return 0
    return 0

def get_isa_range(curbal):
    if curbal < 501: return 500
    elif curbal < 2001: return 2000
    elif curbal < 5001: return 5000
    elif curbal < 10001: return 10000
    elif curbal < 30001: return 30000
    elif curbal < 50001: return 50000
    elif curbal < 75001: return 75000
    else: return 75001

def get_range_bucket(curbal, product):
    if product == 214:
        return get_isa_range(curbal)
    ranges = [(500, '< 500'), (1000, '< 1000'), (5000, '< 5000'), 
              (10000, '< 10000'), (50000, '< 50000'), (100000, '< 100000'),
              (500000, '< 500000'), (float('inf'), '>= 500000')]
    for limit, label in ranges:
        if curbal < limit:
            return label
    return '>= 500000'

# ============================================================================
# SECTION 2: PROCESS SAVINGS & CURRENT ACCOUNTS
# ============================================================================

print("\n" + "="*80)
print("SECTION 2: PROCESS SAVINGS & CURRENT ACCOUNTS")
print("="*80)

# Map all column names for both datasets
column_map = {
    'BRANCH': 'branch',
    'branch': 'branch',
    'PRODUCT': 'product',
    'product': 'product',
    'CURBAL': 'curbal',
    'curbal': 'curbal',
    'AVGAMT': 'avgamt',
    'avgamt': 'avgamt',
    'OPENDT': 'opendt',
    'opendt': 'opendt',
    'CLOSEDT': 'closedt',
    'closedt': 'closedt',
    'BDATE': 'bdate',
    'bdate': 'bdate',
    'CUSTCODE': 'custcode',
    'custcode': 'custcode',
    'PURPOSE': 'purpose',
    'purpose': 'purpose',
    'RACE': 'race',
    'race': 'race',
    'OPENIND': 'openind',
    'openind': 'openind'
}

try:
    # Read and filter datasets
    saving_df, _ = pyreadstat.read_sas7bdat(f'{INPUT_DIR}/deposit/saving.sas7bdat')
    current_df, _ = pyreadstat.read_sas7bdat(f'{INPUT_DIR}/deposit/current.sas7bdat')
    
    # Rename columns to standard names
    for orig, new in column_map.items():
        if orig in saving_df.columns:
            saving_df.rename(columns={orig: new}, inplace=True)
        if orig in current_df.columns:
            current_df.rename(columns={orig: new}, inplace=True)
    
    # Filter with openind if it exists
    if 'openind' in saving_df.columns:
        saving_filtered = saving_df[(~saving_df['openind'].isin(['B','C','P'])) & (~saving_df['product'].isin([297,298]))]
    else:
        saving_filtered = saving_df[~saving_df['product'].isin([297,298])]
        
    if 'openind' in current_df.columns:
        current_filtered = current_df[(~current_df['openind'].isin(['B','C','P'])) & (~current_df['product'].isin([297,298]))]
    else:
        current_filtered = current_df[~current_df['product'].isin([297,298])]
    
    # Combine datasets
    accounts_df = pd.concat([saving_filtered, current_filtered], ignore_index=True)
    print(f"Total accounts to process: {len(accounts_df)}")
    
except Exception as e:
    print(f"Error processing accounts: {e}")
    sas.endsas()
    sys.exit(1)

# Process each account
processed_data = []
print("Processing accounts...")

for idx, row in accounts_df.iterrows():
    if idx % 10000 == 0 and idx > 0:
        print(f"  Processed {idx} accounts...")
    
    product = row['product'] if pd.notna(row['product']) else 0
    curbal = row['curbal'] if pd.notna(row['curbal']) else 0
    avgamt = row['avgamt'] if pd.notna(row['avgamt']) else 0
    opendt = row['opendt'] if pd.notna(row['opendt']) else 0
    closedt = row['closedt'] if pd.notna(row['closedt']) else 0
    bdate = row['bdate'] if pd.notna(row['bdate']) else 0
    custcode = row['custcode'] if pd.notna(row['custcode']) else 0
    purpose = row['purpose'] if pd.notna(row['purpose']) and str(row['purpose']).strip() != '' else '0'
    race = row['race'] if pd.notna(row['race']) and str(row['race']).strip() != '' else '0'
    branch = row['branch'] if pd.notna(row['branch']) else 0
    
    # Calculate accytd
    accytd = 0
    if opendt != 0 and (closedt == 0 or pd.isna(closedt)):
        try:
            opendt_str = str(int(opendt))
            if len(opendt_str) >= 4:
                open_year = int(opendt_str[:4])
                if open_year == reptyear:
                    accytd = 1
        except:
            pass
    
    # Calculate age
    age = calculate_age(bdate, reptdate, reptmon, reptday, reptyear)
    
    # Get ranges
    avgrnge = get_range_bucket(avgamt, 0)
    range_val = get_range_bucket(curbal, product)
    
    processed_data.append({
        'product': product,
        'branch': branch,
        'curbal': curbal,
        'avgamt': avgamt,
        'accytd': accytd,
        'age': age,
        'purpose': str(purpose),
        'race': str(race),
        'custcode': custcode,
        'avgrnge': avgrnge,
        'range': range_val,
        'reptdate': zdate
    })

processed_df = pd.DataFrame(processed_data)
print(f"Processed {len(processed_df)} accounts")

# ============================================================================
# ALWSA: Products 204, 215 (Regular Savings)
# ============================================================================

print("\nGenerating ALWSA...")
if len(processed_df[processed_df['product'].isin([204, 215])]) > 0:
    alwsa = processed_df[processed_df['product'].isin([204, 215])].groupby(
        ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate']
    ).agg(
        noacct=('product', 'count'),
        curbal=('curbal', 'sum'),
        accytd=('accytd', 'sum'),
        avgacct=('avgamt', lambda x: (x > 0).sum()),
        avgamt=('avgamt', 'sum')
    ).reset_index()
else:
    alwsa = pd.DataFrame(columns=['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate', 
                                  'noacct', 'curbal', 'accytd', 'avgacct', 'avgamt'])

try:
    alwsa_sas_path = f'{OUTPUT_DIR}/awsa{reptmon:02d}'
    sas.sasdata(alwsa, alwsa_sas_path)
except Exception as e:
    print(f"Error saving SAS dataset: {e}")
alwsa.to_parquet(f'{OUTPUT_DIR}/awsa{reptmon:02d}.parquet')
print(f"Section 2A: ALWSA - {alwsa['noacct'].sum() if len(alwsa) > 0 else 0} accounts")

# ============================================================================
# ALWSB: Product 207 (Islamic Basic Savings)
# ============================================================================

print("Generating ALWSB...")
if len(processed_df[processed_df['product'] == 207]) > 0:
    alwsb = processed_df[processed_df['product'] == 207].groupby(
        ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate']
    ).agg(
        noacct=('product', 'count'),
        curbal=('curbal', 'sum'),
        accytd=('accytd', 'sum'),
        avgacct=('avgamt', lambda x: (x > 0).sum()),
        avgamt=('avgamt', 'sum')
    ).reset_index()
else:
    alwsb = pd.DataFrame(columns=['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate',
                                  'noacct', 'curbal', 'accytd', 'avgacct', 'avgamt'])

try:
    alwsb_sas_path = f'{OUTPUT_DIR}/awsb{reptmon:02d}'
    sas.sasdata(alwsb, alwsb_sas_path)
except Exception as e:
    print(f"Error saving SAS dataset: {e}")
alwsb.to_parquet(f'{OUTPUT_DIR}/awsb{reptmon:02d}.parquet')
print(f"Section 2B: ALWSB - {alwsb['noacct'].sum() if len(alwsb) > 0 else 0} accounts")

# ============================================================================
# ALWSC: Product 214 (Mudharabah by Age/Race)
# ============================================================================

print("Generating ALWSC...")
if len(processed_df[processed_df['product'] == 214]) > 0:
    alwsc = processed_df[processed_df['product'] == 214].groupby(
        ['product', 'range', 'race', 'age', 'reptdate']
    ).agg(
        noacct=('product', 'count'),
        curbal=('curbal', 'sum'),
        accytd=('accytd', 'sum'),
        avgamt=('avgamt', 'sum')
    ).reset_index()
else:
    alwsc = pd.DataFrame(columns=['product', 'range', 'race', 'age', 'reptdate',
                                  'noacct', 'curbal', 'accytd', 'avgamt'])

try:
    alwsc_sas_path = f'{OUTPUT_DIR}/awsc{reptmon:02d}'
    sas.sasdata(alwsc, alwsc_sas_path)
except Exception as e:
    print(f"Error saving SAS dataset: {e}")
alwsc.to_parquet(f'{OUTPUT_DIR}/awsc{reptmon:02d}.parquet')
print(f"Section 2C: ALWSC - {alwsc['noacct'].sum() if len(alwsc) > 0 else 0} accounts")

# ============================================================================
# MUDHA: Product 214 (Mudharabah by Purpose/Race/Customer)
# ============================================================================

print("Generating MUDHA...")
if len(processed_df[processed_df['product'] == 214]) > 0:
    mudha = processed_df[processed_df['product'] == 214].groupby(
        ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate']
    ).agg(
        noacct=('product', 'count'),
        curbal=('curbal', 'sum'),
        accytd=('accytd', 'sum'),
        avgamt=('avgamt', 'sum')
    ).reset_index()
else:
    mudha = pd.DataFrame(columns=['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate',
                                  'noacct', 'curbal', 'accytd', 'avgamt'])

try:
    mudha_sas_path = f'{OUTPUT_DIR}/mudh{reptmon:02d}'
    sas.sasdata(mudha, mudha_sas_path)
except Exception as e:
    print(f"Error saving SAS dataset: {e}")
mudha.to_parquet(f'{OUTPUT_DIR}/mudh{reptmon:02d}.parquet')
print(f"Section 2D: MUDHA - {mudha['noacct'].sum() if len(mudha) > 0 else 0} accounts")

# ============================================================================
# ALWCA: Products 93, 96 (Islamic Current Accounts)
# ============================================================================

print("Generating ALWCA...")
alwca_data = processed_df[(processed_df['product'].isin([93, 96])) & (processed_df['curbal'] > 0)]
if len(alwca_data) > 0:
    alwca = alwca_data.groupby(
        ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate']
    ).agg(
        noacct=('product', 'count'),
        curbal=('curbal', 'sum'),
        accytd=('accytd', 'sum'),
        avgacct=('avgamt', lambda x: (x > 0).sum()),
        avgamt=('avgamt', 'sum')
    ).reset_index()
else:
    alwca = pd.DataFrame(columns=['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate',
                                  'noacct', 'curbal', 'accytd', 'avgacct', 'avgamt'])

try:
    alwca_sas_path = f'{OUTPUT_DIR}/awca{reptmon:02d}'
    sas.sasdata(alwca, alwca_sas_path)
except Exception as e:
    print(f"Error saving SAS dataset: {e}")
alwca.to_parquet(f'{OUTPUT_DIR}/awca{reptmon:02d}.parquet')
print(f"Section 2E: ALWCA - {alwca['noacct'].sum() if len(alwca) > 0 else 0} accounts")

# ============================================================================
# ALWCB: Products 160,162,164,168,182,169 (Specific Purpose Current Accounts)
# ============================================================================

print("Generating ALWCB...")
alwcb_data = processed_df[
    (processed_df['product'].isin([160, 162, 164, 168, 182, 169])) & 
    (processed_df['curbal'] > 0) & 
    (processed_df['purpose'].isin(['1', '2', '4']))
]
if len(alwcb_data) > 0:
    alwcb = alwcb_data.groupby(
        ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate']
    ).agg(
        noacct=('product', 'count'),
        curbal=('curbal', 'sum'),
        accytd=('accytd', 'sum'),
        avgacct=('avgamt', lambda x: (x > 0).sum()),
        avgamt=('avgamt', 'sum')
    ).reset_index()
else:
    alwcb = pd.DataFrame(columns=['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate',
                                  'noacct', 'curbal', 'accytd', 'avgacct', 'avgamt'])

try:
    alwcb_sas_path = f'{OUTPUT_DIR}/awcb{reptmon:02d}'
    sas.sasdata(alwcb, alwcb_sas_path)
except Exception as e:
    print(f"Error saving SAS dataset: {e}")
alwcb.to_parquet(f'{OUTPUT_DIR}/awcb{reptmon:02d}.parquet')
print(f"Section 2F: ALWCB - {alwcb['noacct'].sum() if len(alwcb) > 0 else 0} accounts")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("ISLAMIC BANKING STATISTICS SUMMARY")
print("="*80)
print(f"""
Date: {rdate} (Yesterday)
Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Output Datasets:
1. DYIBU{reptmon:02d}  - Daily Islamic Balance Summary (SAS + Parquet)
   Fields: BRANCH, SAI, SAINO, MBS, MBSNO, CAI, CAINO, CA96, CAI96, CAIG, CAIGNO, CAIH, CAIHNO
   Records: {len(dyibu)}
   
2. AWSA{reptmon:02d}   - Products 204,215 (Regular Savings) (SAS + Parquet)
   Records: {len(alwsa)}
   
3. AWSB{reptmon:02d}   - Product 207 (Islamic Basic Savings) (SAS + Parquet)
   Records: {len(alwsb)}
   
4. AWSC{reptmon:02d}   - Product 214 (Mudharabah by Age/Race) (SAS + Parquet)
   Records: {len(alwsc)}
   
5. MUDH{reptmon:02d}   - Product 214 (Mudharabah by Purpose) (SAS + Parquet)
   Records: {len(mudha)}
   
6. AWCA{reptmon:02d}   - Products 93,96 (Islamic Current Accounts) (SAS + Parquet)
   Records: {len(alwca)}
   
7. AWCB{reptmon:02d}   - Products 160,162,164,168,182,169 (Purpose 1,2,4 only) (SAS + Parquet)
   Records: {len(alwcb)}

Total Accounts Processed: {len(processed_df)}

Product Categories:
- Savings: 204 (Regular), 207 (Basic), 214 (Mudharabah), 215 (Special)
- Current: 93,96 (Basic Islamic), 160-169,182 (Specific Purpose)

Output Formats:
- SAS7BDAT files in: {OUTPUT_DIR}
- Parquet files in: {OUTPUT_DIR}
""")

# Close SAS connection
try:
    sas.endsas()
    print("SAS Connection closed successfully")
except:
    pass

print(f"\nCompleted: {OUTPUT_DIR}")
print("Both SAS7BDAT and Parquet formats generated.")
print(f"Data processed for date: {reptdate.strftime('%Y-%m-%d')}")
