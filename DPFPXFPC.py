#!/usr/bin/env python3
"""
EIBDISLM - Islamic Banking Statistics
Processes daily Islamic account balances and monthly summaries
Outputs SAS7BDAT files only

Uses:
  PBMISFMT        - SAS format library for MIS codes (SAPROD, SDNAME, $RACE, $PURPOSE, AGEDESC)
  PBBDPTFMT_AII   - SAS format library for deposit products (SADENOM, SAPROD, CADENOM, CAPROD, ...)
"""

import pyreadstat
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import sys

# ---------------------------------------------------------------------------
# Import format libraries
# ---------------------------------------------------------------------------
try:
    import PBMISFMT
except ImportError:
    PBMISFMT = None
    print("WARNING: PBMISFMT module not found; using inline format logic")

try:
    import PBBDPTFMT_AII as PBBDPFMT
except ImportError:
    PBBDPFMT = None
    print("WARNING: PBBDPTFMT_AII module not found; using inline format logic")


BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDISLM'
OUTPUT_DIR = BASE_DIR / '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDISLM'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

# Hardcode reptdate as yesterday's date
reptdate = datetime.now() - timedelta(days=1)
reptyear, reptmon, reptday = reptdate.year, reptdate.month, reptdate.day
rdate = reptdate.strftime('%d/%m/%Y')
zdate = int(reptdate.strftime('%y%m%d'))

print(f"Islamic Banking Statistics - {rdate}")
print(f"Processing data for date: {reptdate.strftime('%Y-%m-%d')}")

CURRENT_FILE = INPUT_DIR / f'intg_dp_acct_current_d{reptday}.sas7bdat'
SAVING_FILE  = INPUT_DIR / f'intg_dp_acct_saving_d{reptday}.sas7bdat'

print(f"\nInput files:")
print(f"  CURRENT: {CURRENT_FILE}")
print(f"  SAVING:  {SAVING_FILE}")

missing = []
if not CURRENT_FILE.exists():
    missing.append(str(CURRENT_FILE))
if not SAVING_FILE.exists():
    missing.append(str(SAVING_FILE))
if missing:
    print("\nERROR: Missing input file(s):")
    for f in missing:
        print(f"  - {f}")
    sys.exit(1)

ENTITY_FILTER = 'PIBB'

# ============================================================================
# FORMAT WRAPPERS
# ============================================================================

def apply_saprod(product_code):
    """SAPROD: savings product -> BNM code."""
    if PBBDPFMT is not None:
        return PBBDPFMT.saprod_format(product_code)
    if PBMISFMT is not None:
        return PBMISFMT.put(product_code, 'SAPROD.')
    if product_code in (204, 214, 215):
        return '42320'
    return '42120'


def apply_sadenom(product_code):
    """SADENOM: 'I' for Islamic products, 'D' otherwise."""
    if PBBDPFMT is not None:
        return PBBDPFMT.sadenom_format(product_code)
    return 'I' if product_code in (204, 207, 214, 215, 218, 220, 221, 222, 223) else 'D'


def apply_cadenom(product_code):
    """CADENOM: 'I' for Islamic CA products, 'D' otherwise."""
    if PBBDPFMT is not None:
        return PBBDPFMT.cadenom_format(product_code)
    return 'D'


def apply_caprod(product_code):
    """CAPROD: current account product -> BNM code."""
    if PBBDPFMT is not None:
        return PBBDPFMT.caprod_format(product_code)
    return '42110'


def apply_race(race_code):
    """$RACE: '0'-'3' pass through, else '0'."""
    if PBBDPFMT is not None:
        return PBBDPFMT.race_format(race_code)
    if PBMISFMT is not None:
        return PBMISFMT.put(race_code, '$RACE.')
    code = str(race_code).rstrip() if race_code is not None else ''
    return code if code in ('0', '1', '2', '3') else '0'


def apply_purpose(purpose_code):
    """$PURPOSE: normalize purpose code."""
    if PBMISFMT is not None:
        return PBMISFMT.put(purpose_code, '$PURPOSE.')
    return str(purpose_code) if purpose_code not in (None, '') else '0'


def apply_agedesc(age_code):
    """AGEDESC: 0=WITHOUT BIRTHDATE, 11=BELOW 12, 12=12 TO BELOW 18, 18=18+."""
    if PBMISFMT is not None:
        return PBMISFMT.put(age_code, 'AGEDESC.')
    return str(age_code)


# ============================================================================
# FUNCTION TO SAVE SAS DATASET
# ============================================================================

def save_sas_dataset(df, dataset_name, output_dir):
    """Save DataFrame as SAS7BDAT using saspy"""
    try:
        import saspy
        sas = saspy.SASsession(cfgname='default')

        df_sas = df.copy()
        df_sas.columns = df_sas.columns.str.upper()

        for col in df_sas.columns:
            if df_sas[col].dtype == 'object':
                df_sas[col] = df_sas[col].astype(str)
            elif df_sas[col].dtype in ['float64', 'float32']:
                df_sas[col] = df_sas[col].fillna(0)
            elif df_sas[col].dtype in ['int64', 'int32']:
                df_sas[col] = df_sas[col].fillna(0).astype(int)

        sas.dataframe2sasdata(df_sas, table=dataset_name, libref='work')
        sas.saslib('outlib', path=str(output_dir), engine='base')
        sas.submit(f"""
            data outlib.{dataset_name};
                set work.{dataset_name};
            run;
        """)

        print(f"  ✓ Saved SAS dataset: {dataset_name}.sas7bdat")
        sas.endsas()
        return True
    except Exception as e:
        print(f"  ✗ Error saving SAS dataset {dataset_name}: {e}")
        return False

# ============================================================================
# INSPECT INPUT DATASETS
# ============================================================================

print("\n" + "="*80)
print("INSPECTING INPUT DATASETS")
print("="*80)

try:
    saving_sample, _ = pyreadstat.read_sas7bdat(str(SAVING_FILE))
    current_sample, _ = pyreadstat.read_sas7bdat(str(CURRENT_FILE))

    print(f"\nSAVING dataset columns (first 20):")
    print(f"  {', '.join(saving_sample.columns.tolist()[:20])}")

    print(f"\nCURRENT dataset columns (first 20):")
    print(f"  {', '.join(current_sample.columns.tolist()[:20])}")

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
    current_df, _ = pyreadstat.read_sas7bdat(str(CURRENT_FILE))
    saving_df, _ = pyreadstat.read_sas7bdat(str(SAVING_FILE))
    print(f"Loaded CURRENT: {len(current_df)} rows, {len(current_df.columns)} columns")
    print(f"Loaded SAVING:  {len(saving_df)} rows, {len(saving_df.columns)} columns")
except Exception as e:
    print(f"Error loading datasets: {e}")
    sys.exit(1)

entity_col_c = next((c for c in ['ENTITY_CD', 'entity_cd', 'Entity_cd'] if c in current_df.columns), None)
entity_col_s = next((c for c in ['ENTITY_CD', 'entity_cd', 'Entity_cd'] if c in saving_df.columns), None)

if entity_col_c is None or entity_col_s is None:
    print("\nERROR: ENTITY_CD column not found in one of the datasets")
    sys.exit(1)

current_df = current_df[current_df[entity_col_c] == ENTITY_FILTER].copy()
saving_df  = saving_df[saving_df[entity_col_s] == ENTITY_FILTER].copy()

print(f"\nAfter ENTITY_CD = '{ENTITY_FILTER}' filter:")
print(f"  CURRENT: {len(current_df)} rows")
print(f"  SAVING:  {len(saving_df)} rows")

def find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

branch_col = find_col(current_df, ['BRANCH', 'branch', 'Branch', 'BRANCH_NO', 'branch_no'])
product_col = find_col(current_df, ['PRODUCT', 'product', 'Product', 'PROD', 'prod'])
curbal_col = find_col(current_df, ['CURBAL', 'curbal', 'CurBal', 'CURRENT_BAL', 'current_bal'])
openind_col = find_col(current_df, ['OPENIND', 'openind', 'OPEN_IND', 'open_ind', 'OpenInd'])

print(f"\nUsing columns:")
print(f"  BRANCH:  {branch_col}")
print(f"  PRODUCT: {product_col}")
print(f"  CURBAL:  {curbal_col}")
print(f"  OPENIND: {openind_col}")

if None in [branch_col, product_col, curbal_col]:
    print("\nERROR: Could not find required columns")
    sys.exit(1)

if openind_col:
    current_filtered = current_df[~current_df[openind_col].isin(['B', 'C', 'P'])][
        [branch_col, product_col, curbal_col]].copy()
    saving_filtered = saving_df[~saving_df[openind_col].isin(['B', 'C', 'P'])][
        [branch_col, product_col, curbal_col]].copy()
else:
    current_filtered = current_df[[branch_col, product_col, curbal_col]].copy()
    saving_filtered = saving_df[[branch_col, product_col, curbal_col]].copy()

current_filtered.columns = ['branch', 'product', 'curbal']
saving_filtered.columns = ['branch', 'product', 'curbal']

current_filtered['reptdate'] = zdate
saving_filtered['reptdate'] = zdate

dyibu_raw = pd.concat([current_filtered, saving_filtered], ignore_index=True)
print(f"Combined raw data: {len(dyibu_raw)} rows")

dyibu_raw['sai'] = np.where(dyibu_raw['product'].isin([204, 207, 214, 215]), dyibu_raw['curbal'], 0)
dyibu_raw['saino'] = np.where(dyibu_raw['product'].isin([204, 207, 214, 215]), 1, 0)
dyibu_raw['mbs'] = np.where(dyibu_raw['product'] == 214, dyibu_raw['curbal'], 0)
dyibu_raw['mbsno'] = np.where(dyibu_raw['product'] == 214, 1, 0)

cai_products = [60, 61, 63, 64, 70, 71, 93, 94, 160, 161, 162, 163, 164, 166,
                169, 66, 67, 168, 167, 182, 183, 184, 73]
exclude_products = [96, 97, 61, 161, 63, 163]
cai_condition = (dyibu_raw['product'].isin(cai_products)
                 & (dyibu_raw['curbal'] > 0)
                 & ~dyibu_raw['product'].isin(exclude_products))
dyibu_raw['cai'] = np.where(cai_condition, dyibu_raw['curbal'], 0)
dyibu_raw['caino'] = np.where(cai_condition, 1, 0)

dyibu_raw['ca96'] = np.where(dyibu_raw['product'].isin([96, 97]) & (dyibu_raw['curbal'] > 0), dyibu_raw['curbal'], 0)
dyibu_raw['cai96'] = np.where(dyibu_raw['product'].isin([96, 97]) & (dyibu_raw['curbal'] > 0), 1, 0)
dyibu_raw['caig'] = np.where(dyibu_raw['product'].isin([61, 161]) & (dyibu_raw['curbal'] > 0), dyibu_raw['curbal'], 0)
dyibu_raw['caigno'] = np.where(dyibu_raw['product'].isin([61, 161]) & (dyibu_raw['curbal'] > 0), 1, 0)
dyibu_raw['caih'] = np.where(dyibu_raw['product'].isin([63, 163]) & (dyibu_raw['curbal'] > 0), dyibu_raw['curbal'], 0)
dyibu_raw['caihno'] = np.where(dyibu_raw['product'].isin([63, 163]) & (dyibu_raw['curbal'] > 0), 1, 0)

dyibu = dyibu_raw.groupby(['branch', 'reptdate']).agg({
    'sai': 'sum', 'saino': 'sum', 'mbs': 'sum', 'mbsno': 'sum',
    'cai': 'sum', 'caino': 'sum', 'ca96': 'sum', 'cai96': 'sum',
    'caig': 'sum', 'caigno': 'sum', 'caih': 'sum', 'caihno': 'sum'
}).reset_index()

dataset_name = f'dyibu{reptmon:02d}'
save_sas_dataset(dyibu, dataset_name, OUTPUT_DIR)
print(f"Section 1: DYIBU - {len(dyibu)} branches")

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def calculate_age(bdate_str, reptdate, reptmon, reptday, reptyear):
    if pd.isna(bdate_str) or bdate_str == 0 or str(bdate_str).strip() in ('', '0'):
        return 0
    try:
        bdate_str = str(bdate_str).strip()
        if len(bdate_str) >= 8:
            try:
                bdate = datetime.strptime(bdate_str[:8], '%m%d%Y')
            except Exception:
                try:
                    bdate = datetime.strptime(bdate_str[:8], '%Y%m%d')
                except Exception:
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
    except Exception:
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

column_map = {
    'BRANCH': 'branch', 'branch': 'branch',
    'PRODUCT': 'product', 'product': 'product',
    'CURBAL': 'curbal', 'curbal': 'curbal',
    'AVGAMT': 'avgamt', 'avgamt': 'avgamt',
    'OPENDT': 'opendt', 'opendt': 'opendt',
    'CLOSEDT': 'closedt', 'closedt': 'closedt',
    'BDATE': 'bdate', 'bdate': 'bdate',
    'CUSTCODE': 'custcode', 'custcode': 'custcode',
    'PURPOSE': 'purpose', 'purpose': 'purpose',
    'RACE': 'race', 'race': 'race',
    'OPENIND': 'openind', 'openind': 'openind',
}

try:
    saving_df, _ = pyreadstat.read_sas7bdat(str(SAVING_FILE))
    current_df, _ = pyreadstat.read_sas7bdat(str(CURRENT_FILE))

    entity_col_s = next((c for c in ['ENTITY_CD', 'entity_cd'] if c in saving_df.columns), None)
    entity_col_c = next((c for c in ['ENTITY_CD', 'entity_cd'] if c in current_df.columns), None)
    if entity_col_s is None or entity_col_c is None:
        print("ERROR: ENTITY_CD column not found")
        sys.exit(1)

    saving_df = saving_df[saving_df[entity_col_s] == ENTITY_FILTER].copy()
    current_df = current_df[current_df[entity_col_c] == ENTITY_FILTER].copy()

    print(f"After ENTITY_CD = '{ENTITY_FILTER}' filter:")
    print(f"  SAVING:  {len(saving_df):,} rows")
    print(f"  CURRENT: {len(current_df):,} rows")

    for orig, new in column_map.items():
        if orig in saving_df.columns:
            saving_df.rename(columns={orig: new}, inplace=True)
        if orig in current_df.columns:
            current_df.rename(columns={orig: new}, inplace=True)

    if 'openind' in saving_df.columns:
        saving_filtered = saving_df[(~saving_df['openind'].isin(['B', 'C', 'P']))
                                    & (~saving_df['product'].isin([297, 298]))]
    else:
        saving_filtered = saving_df[~saving_df['product'].isin([297, 298])]

    if 'openind' in current_df.columns:
        current_filtered = current_df[(~current_df['openind'].isin(['B', 'C', 'P']))
                                      & (~current_df['product'].isin([297, 298]))]
    else:
        current_filtered = current_df[~current_df['product'].isin([297, 298])]

    accounts_df = pd.concat([saving_filtered, current_filtered], ignore_index=True)
    print(f"Total accounts to process: {len(accounts_df):,}")

except Exception as e:
    print(f"Error processing accounts: {e}")
    sys.exit(1)

print("Processing accounts using vectorized operations...")

processed_data = []
chunk_size = 100000
total_rows = len(accounts_df)

for start_idx in range(0, total_rows, chunk_size):
    end_idx = min(start_idx + chunk_size, total_rows)
    chunk = accounts_df.iloc[start_idx:end_idx].copy()

    chunk['accytd'] = 0
    mask = (chunk['opendt'] != 0) & (chunk['closedt'].isna() | (chunk['closedt'] == 0))
    if mask.any():
        try:
            opendt_str = chunk.loc[mask, 'opendt'].astype(str).str[:4]
            chunk.loc[mask, 'accytd'] = (opendt_str.astype(float) == reptyear).astype(int)
        except Exception:
            pass

    chunk['age'] = 0
    for idx, row in chunk.iterrows():
        chunk.loc[idx, 'age'] = calculate_age(row['bdate'], reptdate, reptmon, reptday, reptyear)

    chunk['avgrnge'] = chunk['avgamt'].apply(lambda x: get_range_bucket(x, 0))
    chunk['range'] = chunk.apply(lambda x: get_range_bucket(x['curbal'], x['product']), axis=1)

    # Apply SAS formats via PBMISFMT / PBBDPTFMT_AII
    chunk['sadenom'] = chunk['product'].apply(apply_sadenom)
    chunk['cadenom'] = chunk['product'].apply(apply_cadenom)
    chunk['saprod'] = chunk['product'].apply(apply_saprod)
    chunk['caprod'] = chunk['product'].apply(apply_caprod)
    chunk['race_fmt'] = chunk['race'].apply(apply_race)

    chunk_processed = chunk[['product', 'branch', 'curbal', 'avgamt', 'accytd',
                             'age', 'purpose', 'race', 'race_fmt', 'custcode',
                             'avgrnge', 'range', 'sadenom', 'cadenom',
                             'saprod', 'caprod']].copy()
    chunk_processed['reptdate'] = zdate

    processed_data.append(chunk_processed)

    if (start_idx + chunk_size) % 500000 == 0:
        print(f"  Processed {min(end_idx, total_rows):,} accounts...")

processed_df = pd.concat(processed_data, ignore_index=True)
print(f"✓ Processed {len(processed_df):,} accounts")

# ============================================================================
# GENERATE ALL OUTPUT DATASETS (SAS7BDAT ONLY)
# ============================================================================

print("\n" + "="*80)
print("GENERATING OUTPUT DATASETS (SAS7BDAT)")
print("="*80)

def generate_dataset(df, filter_condition, groupby_cols, agg_dict):
    if filter_condition is not None:
        filtered = df[filter_condition]
    else:
        filtered = df

    if len(filtered) == 0:
        return pd.DataFrame(columns=groupby_cols + list(agg_dict.keys()))

    base_agg = {
        'noacct': ('product', 'count'),
        'curbal': ('curbal', 'sum'),
        'accytd': ('accytd', 'sum'),
        'avgamt': ('avgamt', 'sum'),
    }

    result = filtered.groupby(groupby_cols).agg(**base_agg).reset_index()

    if 'avgacct' in agg_dict:
        cond = (
            filtered.assign(_avg_pos=(filtered['avgamt'] > 0).astype(int))
                    .groupby(groupby_cols)['_avg_pos']
                    .sum()
                    .reset_index()
                    .rename(columns={'_avg_pos': 'avgacct'})
        )
        result = result.merge(cond, on=groupby_cols, how='left')

    return result

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
            'avgamt': ('avgamt', 'sum'),
        },
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
            'avgamt': ('avgamt', 'sum'),
        },
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
            'avgamt': ('avgamt', 'sum'),
        },
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
            'avgamt': ('avgamt', 'sum'),
        },
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
            'avgamt': ('avgamt', 'sum'),
        },
    },
    {
        'name': f'awcb{reptmon:02d}',
        'description': 'Products 160,162,164,168,182,169 (Purpose 1,2,4 only)',
        'filter': (processed_df['product'].isin([160, 162, 164, 168, 182, 169]))
                  & (processed_df['curbal'] > 0)
                  & (processed_df['purpose'].isin(['1', '2', '4'])),
        'groupby': ['purpose', 'race', 'custcode', 'avgrnge', 'range', 'product', 'reptdate'],
        'agg': {
            'noacct': ('product', 'count'),
            'curbal': ('curbal', 'sum'),
            'accytd': ('accytd', 'sum'),
            'avgacct': ('avgamt', lambda x: (x > 0).sum()),
            'avgamt': ('avgamt', 'sum'),
        },
    },
]

datasets_results = {}
for ds_config in datasets_config:
    print(f"\nGenerating {ds_config['name']} ({ds_config['description']})...")
    result = generate_dataset(
        processed_df,
        ds_config['filter'],
        ds_config['groupby'],
        ds_config['agg'],
    )
    datasets_results[ds_config['name']] = result

    save_sas_dataset(result, ds_config['name'], OUTPUT_DIR)

    total_accts = result['noacct'].sum() if len(result) > 0 and 'noacct' in result.columns else 0
    print(f"  {ds_config['name']} - {total_accts:,.0f} accounts, {len(result)} groups")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("ISLAMIC BANKING STATISTICS - COMPLETED")
print("="*80)

sas_files = list(OUTPUT_DIR.glob('*.sas7bdat'))

print(f"""
Processing Date: {rdate} (Yesterday)
Completion Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Input Files:
  CURRENT: {CURRENT_FILE}
  SAVING:  {SAVING_FILE}
  Entity filter: ENTITY_CD = '{ENTITY_FILTER}'

Format Modules Used:
  PBMISFMT:        {'✓ loaded' if PBMISFMT else '✗ not available (using inline logic)'}
  PBBDPTFMT_AII:   {'✓ loaded' if PBBDPFMT else '✗ not available (using inline logic)'}

OUTPUT DATASETS (SAS7BDAT only):
1. DYIBU{reptmon:02d}  - Daily Islamic Balance Summary
   Records: {len(dyibu):,}
   
2. AWSA{reptmon:02d}   - {datasets_config[0]['description']}
   Records: {len(datasets_results.get(f'awsa{reptmon:02d}', pd.DataFrame())):,}
   
3. AWSB{reptmon:02d}   - {datasets_config[1]['description']}
   Records: {len(datasets_results.get(f'awsb{reptmon:02d}', pd.DataFrame())):,}
   
4. AWSC{reptmon:02d}   - {datasets_config[2]['description']}
   Records: {len(datasets_results.get(f'awsc{reptmon:02d}', pd.DataFrame())):,}
   
5. MUDH{reptmon:02d}   - {datasets_config[3]['description']}
   Records: {len(datasets_results.get(f'mudh{reptmon:02d}', pd.DataFrame())):,}
   
6. AWCA{reptmon:02d}   - {datasets_config[4]['description']}
   Records: {len(datasets_results.get(f'awca{reptmon:02d}', pd.DataFrame())):,}
   
7. AWCB{reptmon:02d}   - {datasets_config[5]['description']}
   Records: {len(datasets_results.get(f'awcb{reptmon:02d}', pd.DataFrame())):,}

Total Accounts Processed: {len(processed_df):,}

Output Files Generated:
- SAS7BDAT files: {len(sas_files)}
- Output Directory: {OUTPUT_DIR}
""")

print(f"\n✓ Processing completed successfully!")
print(f"✓ Output directory: {OUTPUT_DIR}")
print(f"✓ Data processed for date: {reptdate.strftime('%Y-%m-%d')}")
