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
# FUNCTION TO SAVE SAS DATASET
# ============================================================================

def save_sas_dataset(df, dataset_name, output_dir):
    """Save DataFrame as SAS dataset using saspy"""
    try:
        # Convert column names to uppercase for SAS compatibility
        df_sas = df.copy()
        df_sas.columns = df_sas.columns.str.upper()
        
        # Convert numeric columns to appropriate types for SAS
        for col in df_sas.columns:
            if df_sas[col].dtype == 'object':
                # Convert object columns to string
                df_sas[col] = df_sas[col].astype(str)
            elif df_sas[col].dtype in ['float64', 'float32']:
                # Handle NaN values for float columns
                df_sas[col] = df_sas[col].fillna(0)
            elif df_sas[col].dtype in ['int64', 'int32']:
                # Handle NaN values for integer columns
                df_sas[col] = df_sas[col].fillna(0).astype(int)
        
        # Create SAS dataset in WORK library first
        sas_df = sas.sasdata(df_sas, dataset_name)
        
        # Save to permanent SAS dataset
        sas_df.to_file(f'{output_dir}/{dataset_name}.sas7bdat')
        print(f"  ✓ Saved SAS dataset: {dataset_name}.sas7bdat")
        return True
    except Exception as e:
        print(f"  ✗ Error saving SAS dataset {dataset_name}: {e}")
        return False

# ============================================================================
# FUNCTION TO SAVE BOTH SAS AND PARQUET
# ============================================================================

def save_datasets(df, dataset_name, output_dir):
    """Save DataFrame as both SAS7BDAT and Parquet"""
    # Save as SAS
    sas_success = save_sas_dataset(df, dataset_name, output_dir)
    
    # Save as Parquet
    try:
        df.to_parquet(f'{output_dir}/{dataset_name}.parquet')
        print(f"  ✓ Saved Parquet file: {dataset_name}.parquet")
        return True
    except Exception as e:
        print(f"  ✗ Error saving Parquet {dataset_name}: {e}")
        return False

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

# Determine column names
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

# Save DYIBU as both SAS and Parquet
dataset_name = f'dyibu{reptmon:02d}'
print(f"\nSaving {dataset_name}...")
save_datasets(dyibu, dataset_name, OUTPUT_DIR)
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
    print(f"Total accounts to process: {len(accounts_df):,}")
    
except Exception as e:
    print(f"Error processing accounts: {e}")
    sas.endsas()
    sys.exit(1)

# Process accounts using vectorized operations
print("Processing accounts using vectorized operations...")

# Pre-allocate dataframes for better performance
processed_data = []

# Process in chunks for memory efficiency
chunk_size = 100000
total_rows = len(accounts_df)

for start_idx in range(0, total_rows, chunk_size):
    end_idx = min(start_idx + chunk_size, total_rows)
    chunk = accounts_df.iloc[start_idx:end_idx].copy()
    
    # Calculate accytd using vectorized operations
    chunk['accytd'] = 0
    mask = (chunk['opendt'] != 0) & (chunk['closedt'].isna() | (chunk['closedt'] == 0))
    if mask.any():
        try:
            # Extract year from opendt
            opendt_str = chunk.loc[mask, 'opendt'].astype(str).str[:4]
            chunk.loc[mask, 'accytd'] = (opendt_str.astype(float) == reptyear).astype(int)
        except:
            pass
    
    # Calculate age
    chunk['age'] = 0
    for idx, row in chunk.iterrows():
        chunk.loc[idx, 'age'] = calculate_age(row['bdate'], reptdate, reptmon, reptday, reptyear)
    
    # Get ranges
    chunk['avgrnge'] = chunk['avgamt'].apply(lambda x: get_range_bucket(x, 0))
    chunk['range'] = chunk.apply(lambda x: get_range_bucket(x['curbal'], x['product']), axis=1)
    
    # Select and rename columns
    chunk_processed = chunk[['product', 'branch', 'curbal', 'avgamt', 'accytd', 
                            'age', 'purpose', 'race', 'custcode', 'avgrnge', 'range']].copy()
    chunk_processed['reptdate'] = zdate
    
    processed_data.append(chunk_processed)
    
    if (start_idx + chunk_size) % 500000 == 0:
        print(f"  Processed {min(end_idx, total_rows):,} accounts...")

# Combine all chunks
processed_df = pd.concat(processed_data, ignore_index=True)
print(f"✓ Processed {len(processed_df):,} accounts")

# ============================================================================
# GENERATE ALL OUTPUT DATASETS (SAS + PARQUET)
# ============================================================================

print("\n" + "="*80)
print("GENERATING OUTPUT DATASETS (SAS7BDAT + PARQUET)")
print("="*80)

def generate_dataset(df, filter_condition, groupby_cols, agg_dict, dataset_name):
    """Generic function to generate aggregated datasets"""
    if filter_condition is not None:
        filtered = df[filter_condition]
    else:
        filtered = df
    
    if len(filtered) > 0:
        result = filtered.groupby(groupby_cols).agg(agg_dict).reset_index()
    else:
        # Create empty dataframe with correct columns
        result = pd.DataFrame(columns=groupby_cols + list(agg_dict.keys()))
    return result

# Define all datasets
datasets_config = [
    {
        'name': f'awsa{reptmon:02d}',
        'description': 'Products 204,215 (Regular Savings)',
        'filter': processed_df['product'].isin([204, 215]),
        'groupby': ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate'],
        'agg': {
            'noacct': ('product', 'count'),
            'curbal': ('curbal', 'sum'),
            'accytd': ('accytd', 'sum'),
            'avgacct': ('avgamt', lambda x: (x > 0).sum()),
            'avgamt': ('avgamt', 'sum')
        }
    },
    {
        'name': f'awsb{reptmon:02d}',
        'description': 'Product 207 (Islamic Basic Savings)',
        'filter': processed_df['product'] == 207,
        'groupby': ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate'],
        'agg': {
            'noacct': ('product', 'count'),
            'curbal': ('curbal', 'sum'),
            'accytd': ('accytd', 'sum'),
            'avgacct': ('avgamt', lambda x: (x > 0).sum()),
            'avgamt': ('avgamt', 'sum')
        }
    },
    {
        'name': f'awsc{reptmon:02d}',
        'description': 'Product 214 (Mudharabah by Age/Race)',
        'filter': processed_df['product'] == 214,
        'groupby': ['product', 'range', 'race', 'age', 'reptdate'],
        'agg': {
            'noacct': ('product', 'count'),
            'curbal': ('curbal', 'sum'),
            'accytd': ('accytd', 'sum'),
            'avgamt': ('avgamt', 'sum')
        }
    },
    {
        'name': f'mudh{reptmon:02d}',
        'description': 'Product 214 (Mudharabah by Purpose)',
        'filter': processed_df['product'] == 214,
        'groupby': ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate'],
        'agg': {
            'noacct': ('product', 'count'),
            'curbal': ('curbal', 'sum'),
            'accytd': ('accytd', 'sum'),
            'avgamt': ('avgamt', 'sum')
        }
    },
    {
        'name': f'awca{reptmon:02d}',
        'description': 'Products 93,96 (Islamic Current Accounts)',
        'filter': (processed_df['product'].isin([93, 96])) & (processed_df['curbal'] > 0),
        'groupby': ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate'],
        'agg': {
            'noacct': ('product', 'count'),
            'curbal': ('curbal', 'sum'),
            'accytd': ('accytd', 'sum'),
            'avgacct': ('avgamt', lambda x: (x > 0).sum()),
            'avgamt': ('avgamt', 'sum')
        }
    },
    {
        'name': f'awcb{reptmon:02d}',
        'description': 'Products 160,162,164,168,182,169 (Purpose 1,2,4 only)',
        'filter': (processed_df['product'].isin([160, 162, 164, 168, 182, 169])) & 
                  (processed_df['curbal'] > 0) & 
                  (processed_df['purpose'].isin(['1', '2', '4'])),
        'groupby': ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate'],
        'agg': {
            'noacct': ('product', 'count'),
            'curbal': ('curbal', 'sum'),
            'accytd': ('accytd', 'sum'),
            'avgacct': ('avgamt', lambda x: (x > 0).sum()),
            'avgamt': ('avgamt', 'sum')
        }
    }
]

# Generate each dataset
datasets_results = {}
for ds_config in datasets_config:
    print(f"\nGenerating {ds_config['name']} ({ds_config['description']})...")
    result = generate_dataset(
        processed_df, 
        ds_config['filter'], 
        ds_config['groupby'], 
        ds_config['agg'], 
        ds_config['name']
    )
    datasets_results[ds_config['name']] = result
    
    # Save as both SAS and Parquet
    save_datasets(result, ds_config['name'], OUTPUT_DIR)
    
    # Print summary
    total_accts = result['noacct'].sum() if len(result) > 0 and 'noacct' in result.columns else 0
    print(f"  {ds_config['name']} - {total_accts:,.0f} accounts, {len(result)} groups")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("ISLAMIC BANKING STATISTICS - COMPLETED")
print("="*80)

# Get file counts
sas_files = list(OUTPUT_DIR.glob('*.sas7bdat'))
parquet_files = list(OUTPUT_DIR.glob('*.parquet'))

print(f"""
Processing Date: {rdate} (Yesterday)
Completion Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

OUTPUT DATASETS (SAS7BDAT + PARQUET):
1. DYIBU{reptmon:02d}  - Daily Islamic Balance Summary
   Records: {len(dyibu):,}
   Files: dyibu{reptmon:02d}.sas7bdat, dyibu{reptmon:02d}.parquet
   
2. AWSA{reptmon:02d}   - {datasets_config[0]['description']}
   Records: {len(datasets_results.get(f'awsa{reptmon:02d}', pd.DataFrame())):,}
   Files: awsa{reptmon:02d}.sas7bdat, awsa{reptmon:02d}.parquet
   
3. AWSB{reptmon:02d}   - {datasets_config[1]['description']}
   Records: {len(datasets_results.get(f'awsb{reptmon:02d}', pd.DataFrame())):,}
   Files: awsb{reptmon:02d}.sas7bdat, awsb{reptmon:02d}.parquet
   
4. AWSC{reptmon:02d}   - {datasets_config[2]['description']}
   Records: {len(datasets_results.get(f'awsc{reptmon:02d}', pd.DataFrame())):,}
   Files: awsc{reptmon:02d}.sas7bdat, awsc{reptmon:02d}.parquet
   
5. MUDH{reptmon:02d}   - {datasets_config[3]['description']}
   Records: {len(datasets_results.get(f'mudh{reptmon:02d}', pd.DataFrame())):,}
   Files: mudh{reptmon:02d}.sas7bdat, mudh{reptmon:02d}.parquet
   
6. AWCA{reptmon:02d}   - {datasets_config[4]['description']}
   Records: {len(datasets_results.get(f'awca{reptmon:02d}', pd.DataFrame())):,}
   Files: awca{reptmon:02d}.sas7bdat, awca{reptmon:02d}.parquet
   
7. AWCB{reptmon:02d}   - {datasets_config[5]['description']}
   Records: {len(datasets_results.get(f'awcb{reptmon:02d}', pd.DataFrame())):,}
   Files: awcb{reptmon:02d}.sas7bdat, awcb{reptmon:02d}.parquet

Total Accounts Processed: {len(processed_df):,}

Output Files Generated:
- SAS7BDAT files: {len(sas_files)}
- Parquet files: {len(parquet_files)}
- Output Directory: {OUTPUT_DIR}

Product Categories:
- Savings: 204 (Regular), 207 (Basic), 214 (Mudharabah), 215 (Special)
- Current: 93,96 (Basic Islamic), 160-169,182 (Specific Purpose)

Metrics per Dataset:
- NOACCT: Number of accounts
- CURBAL: Total current balance
- ACCYTD: Accounts opened year-to-date
- AVGACCT: Count of accounts with average balance
- AVGAMT: Total average amount
""")

# Close SAS connection
try:
    sas.endsas()
    print("\n✓ SAS Connection closed successfully")
except:
    pass

print(f"\n✓ Processing completed successfully!")
print(f"✓ Output directory: {OUTPUT_DIR}")
print(f"✓ Data processed for date: {reptdate.strftime('%Y-%m-%d')}")
