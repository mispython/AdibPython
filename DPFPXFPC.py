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
from datetime import datetime
import saspy
import os

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDISLM'
OUTPUT_DIR = BASE_DIR / '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

# Initialize SAS connection for output
sas = saspy.SASsession()

# Read reptile date from SAS dataset
reptdate_df, meta = pyreadstat.read_sas7bdat(f'{INPUT_DIR}/deposit/reptdate.sas7bdat')
reptdate = reptdate_df['reptdate'].iloc[0]
reptyear, reptmon, reptday = reptdate.year, reptdate.month, reptdate.day
rdate = reptdate.strftime('%d/%m/%Y')
zdate = int(reptdate.strftime('%y%m%d'))

print(f"Islamic Banking Statistics - {rdate}")

# ============================================================================
# SECTION 1: DAILY ISLAMIC BALANCE SUMMARY (DYIBU)
# ============================================================================

# Read current and saving datasets
current_df, _ = pyreadstat.read_sas7bdat(f'{INPUT_DIR}/deposit/current.sas7bdat')
saving_df, _ = pyreadstat.read_sas7bdat(f'{INPUT_DIR}/deposit/saving.sas7bdat')

# Filter and combine datasets
current_filtered = current_df[~current_df['openind'].isin(['B','C','P'])][['branch', 'product', 'curbal']].copy()
saving_filtered = saving_df[~saving_df['openind'].isin(['B','C','P'])][['branch', 'product', 'curbal']].copy()

current_filtered['reptdate'] = zdate
saving_filtered['reptdate'] = zdate

dyibu_raw = pd.concat([current_filtered, saving_filtered], ignore_index=True)

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
dyibu_sas_path = f'{OUTPUT_DIR}/dyibu{reptmon:02d}'
sas.sasdata(dyibu, dyibu_sas_path)
dyibu.to_parquet(f'{OUTPUT_DIR}/dyibu{reptmon:02d}.parquet')
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

# Read and filter datasets
saving_df, _ = pyreadstat.read_sas7bdat(f'{INPUT_DIR}/deposit/saving.sas7bdat')
current_df, _ = pyreadstat.read_sas7bdat(f'{INPUT_DIR}/deposit/current.sas7bdat')

saving_filtered = saving_df[(~saving_df['openind'].isin(['B','C','P'])) & (~saving_df['product'].isin([297,298]))]
current_filtered = current_df[(~current_df['openind'].isin(['B','C','P'])) & (~current_df['product'].isin([297,298]))]

# Combine datasets
accounts_df = pd.concat([saving_filtered, current_filtered], ignore_index=True)

# Process each account
processed_data = []

for idx, row in accounts_df.iterrows():
    branch = row['branch']
    product = row['product']
    curbal = row['curbal'] if pd.notna(row['curbal']) else 0
    avgamt = row['avgamt'] if pd.notna(row['avgamt']) else 0
    opendt = row['opendt'] if pd.notna(row['opendt']) else 0
    closedt = row['closedt'] if pd.notna(row['closedt']) else 0
    bdate = row['bdate'] if pd.notna(row['bdate']) else 0
    custcode = row['custcode'] if pd.notna(row['custcode']) else 0
    purpose = row['purpose'] if pd.notna(row['purpose']) and str(row['purpose']).strip() != '' else '0'
    race = row['race'] if pd.notna(row['race']) and str(row['race']).strip() != '' else '0'
    openind = row['openind']
    
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

# ============================================================================
# ALWSA: Products 204, 215 (Regular Savings)
# ============================================================================

alwsa = processed_df[processed_df['product'].isin([204, 215])].groupby(
    ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate']
).agg(
    noacct=('product', 'count'),
    curbal=('curbal', 'sum'),
    accytd=('accytd', 'sum'),
    avgacct=('avgamt', lambda x: (x > 0).sum()),
    avgamt=('avgamt', 'sum')
).reset_index()

alwsa_sas_path = f'{OUTPUT_DIR}/awsa{reptmon:02d}'
sas.sasdata(alwsa, alwsa_sas_path)
alwsa.to_parquet(f'{OUTPUT_DIR}/awsa{reptmon:02d}.parquet')
print(f"Section 2A: ALWSA - {alwsa['noacct'].sum()} accounts")

# ============================================================================
# ALWSB: Product 207 (Islamic Basic Savings)
# ============================================================================

alwsb = processed_df[processed_df['product'] == 207].groupby(
    ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate']
).agg(
    noacct=('product', 'count'),
    curbal=('curbal', 'sum'),
    accytd=('accytd', 'sum'),
    avgacct=('avgamt', lambda x: (x > 0).sum()),
    avgamt=('avgamt', 'sum')
).reset_index()

alwsb_sas_path = f'{OUTPUT_DIR}/awsb{reptmon:02d}'
sas.sasdata(alwsb, alwsb_sas_path)
alwsb.to_parquet(f'{OUTPUT_DIR}/awsb{reptmon:02d}.parquet')
print(f"Section 2B: ALWSB - {alwsb['noacct'].sum()} accounts")

# ============================================================================
# ALWSC: Product 214 (Mudharabah by Age/Race)
# ============================================================================

alwsc = processed_df[processed_df['product'] == 214].groupby(
    ['product', 'range', 'race', 'age', 'reptdate']
).agg(
    noacct=('product', 'count'),
    curbal=('curbal', 'sum'),
    accytd=('accytd', 'sum'),
    avgamt=('avgamt', 'sum')
).reset_index()

alwsc_sas_path = f'{OUTPUT_DIR}/awsc{reptmon:02d}'
sas.sasdata(alwsc, alwsc_sas_path)
alwsc.to_parquet(f'{OUTPUT_DIR}/awsc{reptmon:02d}.parquet')
print(f"Section 2C: ALWSC - {alwsc['noacct'].sum()} accounts")

# ============================================================================
# MUDHA: Product 214 (Mudharabah by Purpose/Race/Customer)
# ============================================================================

mudha = processed_df[processed_df['product'] == 214].groupby(
    ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate']
).agg(
    noacct=('product', 'count'),
    curbal=('curbal', 'sum'),
    accytd=('accytd', 'sum'),
    avgamt=('avgamt', 'sum')
).reset_index()

mudha_sas_path = f'{OUTPUT_DIR}/mudh{reptmon:02d}'
sas.sasdata(mudha, mudha_sas_path)
mudha.to_parquet(f'{OUTPUT_DIR}/mudh{reptmon:02d}.parquet')
print(f"Section 2D: MUDHA - {mudha['noacct'].sum()} accounts")

# ============================================================================
# ALWCA: Products 93, 96 (Islamic Current Accounts)
# ============================================================================

alwca = processed_df[(processed_df['product'].isin([93, 96])) & (processed_df['curbal'] > 0)].groupby(
    ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate']
).agg(
    noacct=('product', 'count'),
    curbal=('curbal', 'sum'),
    accytd=('accytd', 'sum'),
    avgacct=('avgamt', lambda x: (x > 0).sum()),
    avgamt=('avgamt', 'sum')
).reset_index()

alwca_sas_path = f'{OUTPUT_DIR}/awca{reptmon:02d}'
sas.sasdata(alwca, alwca_sas_path)
alwca.to_parquet(f'{OUTPUT_DIR}/awca{reptmon:02d}.parquet')
print(f"Section 2E: ALWCA - {alwca['noacct'].sum()} accounts")

# ============================================================================
# ALWCB: Products 160,162,164,168,182,169 (Specific Purpose Current Accounts)
# ============================================================================

alwcb = processed_df[
    (processed_df['product'].isin([160, 162, 164, 168, 182, 169])) & 
    (processed_df['curbal'] > 0) & 
    (processed_df['purpose'].isin(['1', '2', '4']))
].groupby(
    ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate']
).agg(
    noacct=('product', 'count'),
    curbal=('curbal', 'sum'),
    accytd=('accytd', 'sum'),
    avgacct=('avgamt', lambda x: (x > 0).sum()),
    avgamt=('avgamt', 'sum')
).reset_index()

alwcb_sas_path = f'{OUTPUT_DIR}/awcb{reptmon:02d}'
sas.sasdata(alwcb, alwcb_sas_path)
alwcb.to_parquet(f'{OUTPUT_DIR}/awcb{reptmon:02d}.parquet')
print(f"Section 2F: ALWCB - {alwcb['noacct'].sum()} accounts")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("ISLAMIC BANKING STATISTICS SUMMARY")
print("="*80)
print(f"""
Date: {rdate}

Output Datasets:
1. DYIBU{reptmon:02d}  - Daily Islamic Balance Summary (SAS + Parquet)
   Fields: BRANCH, SAI, SAINO, MBS, MBSNO, CAI, CAINO, CA96, CAI96, CAIG, CAIGNO, CAIH, CAIHNO
   
2. AWSA{reptmon:02d}   - Products 204,215 (Regular Savings) (SAS + Parquet)
   Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT
   
3. AWSB{reptmon:02d}   - Product 207 (Islamic Basic Savings) (SAS + Parquet)
   Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT
   
4. AWSC{reptmon:02d}   - Product 214 (Mudharabah by Age/Race) (SAS + Parquet)
   Dimensions: PRODCD × RANGE × RACE × AGE
   
5. MUDH{reptmon:02d}   - Product 214 (Mudharabah by Purpose) (SAS + Parquet)
   Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT
   
6. AWCA{reptmon:02d}   - Products 93,96 (Islamic Current Accounts) (SAS + Parquet)
   Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT
   
7. AWCB{reptmon:02d}   - Products 160,162,164,168,182,169 (Purpose 1,2,4 only) (SAS + Parquet)
   Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT

Product Categories:
- Savings: 204 (Regular), 207 (Basic), 214 (Mudharabah), 215 (Special)
- Current: 93,96 (Basic Islamic), 160-169,182 (Specific Purpose)

Metrics per Dataset:
- NOACCT: Number of accounts
- CURBAL: Total current balance
- ACCYTD: Accounts opened year-to-date
- AVGACCT: Count of accounts with average balance
- AVGAMT: Total average amount

Output Formats:
- SAS7BDAT files in: {OUTPUT_DIR}
- Parquet files in: {OUTPUT_DIR}
""")

# Close SAS connection
sas.endsas()

print(f"\nCompleted: {OUTPUT_DIR}")
print("Both SAS7BDAT and Parquet formats generated.")
