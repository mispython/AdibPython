"""
DALWPBBD - PBB Deposit Manipulation for Weekly Processing

Purpose: Process deposit extracts for weekly reporting
- Apply format mappings (customer, product, state, currency)
- Split Savings and Current accounts
- Handle ACE and FCY (Foreign Currency) accounts
- Summarize at branch level for RDAL

Invoked by: EIBWEEK1-3
Input: DEPOSIT.SAVING, DEPOSIT.CURRENT
Output: BNM.SAVG, BNM.CURN, BNM.FCY, BNM.DEPT

Format Mappings (from PBBDPFMT):
- SACUSTCD: Savings customer code
- DDCUSTCD: Demand deposit customer code
- STATECD: Branch to state code
- SAPROD/CAPROD: Product codes
- SADENOM/CADENOM: Currency denomination
"""

import polars as pl
import os

# Directories
DEPOSIT_DIR = 'data/deposit/'
CISDP_DIR = 'data/cisdp/'
BNM_DIR = 'data/bnm/'

os.makedirs(BNM_DIR, exist_ok=True)

print("DALWPBBD - PBB Deposit Manipulation")
print("=" * 60)

# Get environment variables
reptmon = os.environ.get('REPTMON', '12')
nowk = os.environ.get('NOWK', '4')

print(f"Period: {reptmon}/{nowk}")

# Format mappings (PBBDPFMT equivalents)
# Customer code formats
SACUSTCD = {  # Savings customer codes
    range(1, 76): '01',     # Individual
    77: '77', 78: '78',     # Staff
    79: '79',               # Corporate
    range(80, 95): '80',    # Others
    95: '95', 96: '96'      # Special
}

DDCUSTCD = {  # Demand deposit customer codes
    range(1, 76): '01',
    77: '77', 78: '78',
    79: '79',
    range(80, 95): '80',
    95: '95', 96: '96'
}

# State codes (example - adjust based on actual mapping)
STATECD = {
    range(1, 100): '01',     # Selangor
    range(100, 200): '02',   # KL
    range(200, 300): '03',   # Penang
    # Add more as needed
}

# Product codes (simplified - adjust based on actual)
SAPROD = {  # Savings products
    100: '11000', 101: '11001', 102: '11002',
    # Add mappings
}

CAPROD = {  # Current products
    1: '10001', 2: '10002', 3: '10003',
    # Add mappings
}

# Denomination (currency)
SADENOM = {100: 'L', 101: 'L', 102: 'L'}  # L=Local, F=Foreign
CADENOM = {1: 'L', 2: 'L', 3: 'L', 400: 'F', 403: 'F'}

# ACE products
ACE_PRODUCTS = [177, 178, 179]  # Example ACE product codes

# Helper function for format lookup
def apply_format(value, format_dict):
    """Apply format mapping"""
    if value in format_dict:
        return format_dict[value]
    for key in format_dict:
        if isinstance(key, range) and value in key:
            return format_dict[key]
    return '99'  # Default

print("=" * 60)

# Process SAVINGS
print("\n1. Processing Savings Accounts...")
try:
    df_saving = pl.read_parquet(f'{DEPOSIT_DIR}SAVING.parquet')
    
    # Filter active accounts
    df_saving = df_saving.filter(
        (~pl.col('OPENIND').is_in(['B','C','P'])) &
        (pl.col('CURBAL') >= 0)
    )
    
    # Apply formats
    df_saving = df_saving.with_columns([
        pl.col('CUSTCODE').map_elements(lambda x: apply_format(x, SACUSTCD), return_dtype=pl.Utf8).alias('CUSTCD'),
        pl.col('BRANCH').map_elements(lambda x: apply_format(x, STATECD), return_dtype=pl.Utf8).alias('STATECD'),
        pl.col('PRODUCT').map_elements(lambda x: apply_format(x, SAPROD), return_dtype=pl.Utf8).alias('PRODCD'),
        pl.col('PRODUCT').map_elements(lambda x: apply_format(x, SADENOM), return_dtype=pl.Utf8).alias('AMTIND')
    ])
    
    # Select columns
    savg_cols = ['BRANCH', 'PRODUCT', 'CUSTCD', 'STATECD', 'PRODCD', 'NAME', 
                 'ACCTNO', 'CURBAL', 'INTPAYBL', 'AMTIND', 'COSTCTR', 
                 'DNBFISME', 'CURCODE']
    
    df_saving = df_saving.select([col for col in savg_cols if col in df_saving.columns])
    
    # Save
    df_saving.write_parquet(f'{BNM_DIR}SAVG{reptmon}{nowk}.parquet')
    
    print(f"  ✓ Savings: {len(df_saving):,} accounts")

except Exception as e:
    print(f"  ✗ Savings: {e}")
    df_saving = pl.DataFrame([])

# Process CURRENT ACCOUNTS
print("\n2. Processing Current Accounts...")
try:
    df_current = pl.read_parquet(f'{DEPOSIT_DIR}CURRENT.parquet')
    
    # Filter active accounts
    df_current = df_current.filter(
        (~pl.col('OPENIND').is_in(['B','C','P'])) &
        (pl.col('CURBAL') >= 0)
    )
    
    # Apply formats
    df_current = df_current.with_columns([
        pl.col('BRANCH').map_elements(lambda x: apply_format(x, STATECD), return_dtype=pl.Utf8).alias('STATECD'),
        pl.col('PRODUCT').map_elements(lambda x: apply_format(x, CAPROD), return_dtype=pl.Utf8).alias('PRODCD'),
        pl.col('PRODUCT').map_elements(lambda x: apply_format(x, CADENOM), return_dtype=pl.Utf8).alias('AMTIND')
    ])
    
    # Handle Vostro accounts
    df_current = df_current.with_columns([
        pl.when(pl.col('PRODUCT') == 104).then(pl.lit('02'))  # Vostro Local
          .when(pl.col('PRODUCT') == 105).then(pl.lit('81'))  # Vostro Foreign
          .otherwise(
              pl.col('CUSTCODE').map_elements(lambda x: apply_format(x, DDCUSTCD), return_dtype=pl.Utf8)
          )
          .alias('CUSTCD')
    ])
    
    # Split: ACE, FCY, Regular
    df_ace = df_current.filter(pl.col('PRODUCT').is_in(ACE_PRODUCTS))
    df_fcy = df_current.filter(
        ((pl.col('PRODUCT') >= 400) & (pl.col('PRODUCT') <= 444)) |
        ((pl.col('PRODUCT') >= 450) & (pl.col('PRODUCT') <= 454))
    )
    df_regular = df_current.filter(
        ~pl.col('PRODUCT').is_in(ACE_PRODUCTS) &
        ~(((pl.col('PRODUCT') >= 400) & (pl.col('PRODUCT') <= 444)) |
          ((pl.col('PRODUCT') >= 450) & (pl.col('PRODUCT') <= 454)))
    )
    
    # ACE: Clear interest payable
    if len(df_ace) > 0:
        df_ace = df_ace.with_columns([pl.lit(0).alias('INTPAYBL')])
    
    # FCY: Adjust SECTOR
    if len(df_fcy) > 0:
        df_fcy = df_fcy.with_columns([
            pl.when(pl.col('CUSTCD').is_in(['77','78','95']))
              .then(
                  pl.when(pl.col('SECTOR').is_in([4,5])).then(pl.lit(1))
                  .when(pl.col('SECTOR').is_in([1,2,3])).then(pl.col('SECTOR'))
                  .otherwise(pl.lit(1))
              )
              .otherwise(
                  pl.when(pl.col('SECTOR').is_in([1,2,3])).then(pl.lit(4))
                  .when(pl.col('SECTOR').is_in([4,5])).then(pl.col('SECTOR'))
                  .otherwise(pl.lit(4))
              )
              .alias('SECTOR')
        ])
    
    # Combine for CURN
    df_curn = pl.concat([df for df in [df_ace, df_regular] if len(df) > 0]) if df_ace is not None or df_regular is not None else pl.DataFrame([])
    
    # Save CURN
    if len(df_curn) > 0:
        df_curn.write_parquet(f'{BNM_DIR}CURN{reptmon}{nowk}.parquet')
        print(f"  ✓ Current: {len(df_curn):,} accounts")
    
    # Save FCY
    if len(df_fcy) > 0:
        df_fcy.write_parquet(f'{BNM_DIR}FCY{reptmon}{nowk}.parquet')
        print(f"  ✓ FCY: {len(df_fcy):,} accounts")
        
        # Merge FCY with customer info and append to CURN
        try:
            df_cisdp = pl.read_parquet(f'{CISDP_DIR}DEPOSIT.parquet')
            df_cisdp = df_cisdp.select(['ACCTNO', 'CUSTNO']).unique()
            
            df_fcy = df_fcy.join(df_cisdp, on='ACCTNO', how='left')
            df_fcy = df_fcy.drop('CUSTNO')
            
            # Append to CURN
            df_curn = pl.concat([df_curn, df_fcy])
            df_curn.write_parquet(f'{BNM_DIR}CURN{reptmon}{nowk}.parquet')
            
            print(f"  ✓ Combined CURN: {len(df_curn):,} accounts")
        except:
            pass

except Exception as e:
    print(f"  ✗ Current: {e}")
    df_curn = pl.DataFrame([])

# Summarize for RDAL (DEPT)
print("\n3. Summarizing for RDAL...")

# Savings summary
if len(df_saving) > 0:
    df_dept_savg = df_saving.group_by(['BRANCH', 'STATECD', 'PRODCD', 'CUSTCD', 'AMTIND']).agg([
        pl.col('CURBAL').sum(),
        pl.col('INTPAYBL').sum()
    ])
    print(f"  ✓ Savings summary: {len(df_dept_savg):,} records")
else:
    df_dept_savg = pl.DataFrame([])

# Current summary
if len(df_curn) > 0:
    # Ensure SECTOR column exists
    if 'SECTOR' not in df_curn.columns:
        df_curn = df_curn.with_columns([pl.lit(None).alias('SECTOR')])
    
    df_dept_curn = df_curn.group_by(['BRANCH', 'STATECD', 'PRODCD', 'CUSTCD', 'SECTOR', 'AMTIND']).agg([
        pl.col('CURBAL').sum(),
        pl.col('INTPAYBL').sum()
    ])
    print(f"  ✓ Current summary: {len(df_dept_curn):,} records")
else:
    df_dept_curn = pl.DataFrame([])

# Combine and save DEPT
if len(df_dept_savg) > 0 or len(df_dept_curn) > 0:
    df_dept = pl.concat([df for df in [df_dept_savg, df_dept_curn] if len(df) > 0])
    df_dept.write_parquet(f'{BNM_DIR}DEPT{reptmon}{nowk}.parquet')
    print(f"  ✓ DEPT summary: {len(df_dept):,} total records")

print(f"\n{'='*60}")
print("✓ DALWPBBD Complete")
print(f"{'='*60}")
print(f"\nOutputs Generated:")
print(f"  BNM.SAVG{reptmon}{nowk} - Savings accounts")
print(f"  BNM.CURN{reptmon}{nowk} - Current accounts")
print(f"  BNM.FCY{reptmon}{nowk} - Foreign currency accounts")
print(f"  BNM.DEPT{reptmon}{nowk} - Branch-level summary")
print(f"\nFormat Mappings Applied:")
print(f"  Customer codes (SACUSTCD, DDCUSTCD)")
print(f"  State codes (STATECD)")
print(f"  Product codes (SAPROD, CAPROD)")
print(f"  Currency denomination (SADENOM, CADENOM)")
print(f"\nSpecial Handling:")
print(f"  Vostro accounts (104=CB '02', 105=CB '81')")
print(f"  ACE products (INTPAYBL=0)")
print(f"  FCY accounts (SECTOR adjustment)")
