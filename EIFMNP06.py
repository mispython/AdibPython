"""
EIFMNP06 - NPL Specific Provision (SP) Report

Purpose: Track movements of Specific Provision for NPL accounts
- SP (Specific Provision) calculation by risk category
- Market value assessment and depreciation
- Compare with previous month (existing vs current NPL)

Provision Rates:
- Bad (>364 days or written off): 100%
- Doubtful (274-364 days): 50%
- Substandard-2 (183-273 days): 20%
- Substandard-1 (<183 days): 20%

Formula:
  NETEXP = OSPRIN + OTHERFEE - MARKETVL
  SP = NETEXP × Provision Rate
"""

import polars as pl
from datetime import datetime
import os

# Directories
NPL_DIR = 'data/npl/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIFMNP06 - NPL Specific Provision Report")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{NPL_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    reptmon = f'{reptdate.month:02d}'
    prevmon = f'{(reptdate.month-1 if reptdate.month > 1 else 12):02d}'
    rdate = reptdate.strftime('%d %B %Y')
    
    print(f"Report Date: {rdate}")
    print(f"Period: {reptmon}, Previous: {prevmon}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Loan type mapping
LNTYP = {
    128: 'HPD AITAB', 130: 'HPD AITAB', 983: 'HPD AITAB',
    700: 'HPD CONVENTIONAL', 705: 'HPD CONVENTIONAL',
    380: 'HPD CONVENTIONAL', 381: 'HPD CONVENTIONAL',
    993: 'HPD CONVENTIONAL', 996: 'HPD CONVENTIONAL',
    720: 'HPD CONVENTIONAL', 725: 'HPD CONVENTIONAL',
}

for i in range(200, 300):
    LNTYP[i] = 'HOUSING LOANS'

def get_loantyp(loantype):
    return LNTYP.get(loantype, 'OTHERS')

# Process LOANS
print("\n1. Processing NPL Loans...")
try:
    df_loan = pl.read_parquet(f'{NPL_DIR}LOAN{reptmon}.parquet')
    
    # Read write-off accounts
    try:
        df_woff = pl.read_parquet(f'{NPL_DIR}WSP2.parquet')
        df_loan = df_loan.join(df_woff.select(['ACCTNO']).with_columns([
            pl.lit('Y').alias('WRITEOFF')
        ]), on='ACCTNO', how='left')
    except:
        df_loan = df_loan.with_columns([pl.lit('N').alias('WRITEOFF')])
    
    # Read IIS from IIS report
    try:
        df_iis = pl.read_parquet(f'{NPL_DIR}IIS{reptmon}.parquet')
        df_loan = df_loan.join(df_iis.select(['ACCTNO','IIS']), on='ACCTNO', how='left')
    except:
        df_loan = df_loan.with_columns([pl.lit(0.0).alias('IIS')])
    
    # Fill nulls
    df_loan = df_loan.with_columns([
        pl.col('WRITEOFF').fill_null('N'),
        pl.col('IIS').fill_null(0),
        pl.col('SPP2').fill_null(0),
        pl.when(pl.col('EARNTERM').is_in([0, None]))
          .then(pl.col('NOTETERM'))
          .otherwise(pl.col('EARNTERM'))
          .alias('EARNTERM')
    ])
    
    # Update borrower status
    df_loan = df_loan.with_columns([
        pl.when((pl.col('WRITEOFF') == 'Y') & (pl.col('WDOWNIND') != 'Y'))
          .then(pl.lit('W'))
          .otherwise(pl.col('BORSTAT'))
          .alias('BORSTAT')
    ])
    
    print(f"  ✓ NPL Loans: {len(df_loan):,} accounts")

except Exception as e:
    print(f"  ✗ Loans: {e}")
    import sys
    sys.exit(1)

# Calculate SP for existing NPL
print("\n2. Calculating SP for existing NPL...")
df_existing = df_loan.filter(pl.col('EXIST') == 'Y')

# Calculate UHC (Unearned Hiring Charges)
df_existing = df_existing.with_columns([pl.lit(0.0).alias('UHC')])

# Calculate NETBAL and OSPRIN
df_existing = df_existing.with_columns([
    (pl.col('CURBAL') - pl.col('UHC')).alias('NETBAL'),
    (pl.col('CURBAL') - pl.col('UHC') - pl.col('IIS')).alias('OSPRIN')
])

# Calculate OTHER FEES
df_existing = df_existing.with_columns([
    pl.when(pl.col('LOANTYPE').is_in([380, 381]))
      .then(pl.col('FEEAMT') - pl.col('FEETOT2'))
      .otherwise(
          pl.col('FEEAMT8') - pl.col('FEETOT2') + 
          pl.col('FEEAMTA') - pl.col('FEEAMT5')
      )
      .alias('OTHERFEE')
])

df_existing = df_existing.with_columns([
    pl.when(pl.col('OTHERFEE') < 0).then(0).otherwise(pl.col('OTHERFEE')).alias('OTHERFEE')
])

# Zero out OTHERFEE for specific loan types
df_existing = df_existing.with_columns([
    pl.when(pl.col('LOANTYPE').is_in([983, 993]))
      .then(0)
      .otherwise(pl.col('OTHERFEE'))
      .alias('OTHERFEE')
])

# Calculate Market Value with depreciation
df_existing = df_existing.with_columns([
    ((reptdate.year - pl.col('ISSDTE').dt.year()) + 
     (reptdate.month - pl.col('ISSDTE').dt.month()) / 12).alias('AGE')
])

df_existing = df_existing.with_columns([
    pl.when(
        (pl.col('APPVALUE') > 0) &
        (pl.col('LOANTYPE').is_in([705,128,700,130,380,381,720,725]) | 
         (pl.col('CENSUS7') == '9')) &
        ((pl.col('DAYS') > 89) | (pl.col('USER5') == 'N')) &
        (~pl.col('BORSTAT').is_in(['F','R','I','Y','W'])) &
        (~pl.col('LOANTYPE').is_in([983,993]))
    )
    .then(
        pl.when(pl.col('CENSUS7') != '9')
          .then(pl.col('APPVALUE') - pl.col('APPVALUE') * pl.col('AGE') * 0.2)
          .otherwise(pl.col('APPVALUE'))
    )
    .when(pl.col('BORSTAT').is_in(['R']))
      .then(pl.col('APPVALUE'))
    .otherwise(0)
    .alias('MARKETVL')
])

# Handle hardcoded market values
df_existing = df_existing.with_columns([
    pl.when(pl.col('HARDCODE') == 'Y')
      .then(pl.col('WREALVL'))
      .otherwise(pl.col('MARKETVL'))
      .alias('MARKETVL')
])

# Ensure non-negative
df_existing = df_existing.with_columns([
    pl.when(pl.col('MARKETVL') < 0).then(0).otherwise(pl.col('MARKETVL')).alias('MARKETVL')
])

# Calculate NET EXPOSURE
df_existing = df_existing.with_columns([
    pl.when(pl.col('DAYS') > 273)
      .then(pl.col('OSPRIN') + pl.col('OTHERFEE'))
      .otherwise(pl.col('OSPRIN') + pl.col('OTHERFEE') - pl.col('MARKETVL'))
      .alias('NETEXP')
])

# Calculate SP based on days overdue
df_existing = df_existing.with_columns([
    pl.when(pl.col('DAYS') > 364)
      .then(pl.col('NETEXP'))  # 100%
    .when(pl.col('DAYS') > 273)
      .then(pl.col('NETEXP') / 2)  # 50%
    .when(pl.col('DAYS') > 89)
      .then(pl.col('NETEXP') * 0.2)  # 20%
    .when((pl.col('DAYS') < 90) & (pl.col('USER5') == 'N'))
      .then(pl.col('NETEXP') * 0.2)  # 20%
    .when(pl.col('BORSTAT').is_in(['F','R','I','W']))
      .then(pl.col('NETEXP'))  # 100%
    .when((pl.col('DAYS') > 89) & (pl.col('BORSTAT') == 'Y'))
      .then(pl.col('NETEXP') / 5)  # 20%
    .otherwise(0)
    .alias('SP')
])

# Ensure non-negative
df_existing = df_existing.with_columns([
    pl.when(pl.col('SP') < 0).then(0).otherwise(pl.col('SP')).alias('SP')
])

# Calculate movements
df_existing = df_existing.with_columns([
    (pl.col('SP') - pl.col('SPP2')).alias('SPPL'),
    pl.lit(0.0).alias('RECOVER'),
    pl.lit(0.0).alias('SPPW')
])

# Adjust SPPL if negative
df_existing = df_existing.with_columns([
    pl.when(pl.col('SPPL') < 0).then(0).otherwise(pl.col('SPPL')).alias('SPPL')
])

# Handle written-off accounts
df_existing = df_existing.with_columns([
    pl.when(pl.col('BORSTAT') == 'W')
      .then(pl.col('SPP2'))
      .otherwise(pl.col('SPPW'))
      .alias('SPPW'),
    pl.when(pl.col('BORSTAT') == 'W')
      .then(0)
      .otherwise(pl.col('SP'))
      .alias('SP'),
    pl.when(pl.col('BORSTAT') == 'W')
      .then(0)
      .otherwise(pl.col('MARKETVL'))
      .alias('MARKETVL'),
    pl.when(pl.col('BORSTAT') != 'W')
      .then(pl.col('SPP2') - pl.col('SP'))
      .otherwise(0)
      .alias('RECOVER')
])

# Ensure RECOVER non-negative
df_existing = df_existing.with_columns([
    pl.when(pl.col('RECOVER') < 0).then(0).otherwise(pl.col('RECOVER')).alias('RECOVER')
])

print(f"  ✓ Existing NPL: {len(df_existing):,} accounts")

# Calculate SP for current NPL
print("\n3. Calculating SP for current NPL...")
df_current = df_loan.filter(pl.col('EXIST') != 'Y')

# Same calculations as existing
df_current = df_current.with_columns([pl.lit(0.0).alias('UHC')])

df_current = df_current.with_columns([
    (pl.col('CURBAL') - pl.col('UHC')).alias('NETBAL'),
    (pl.col('CURBAL') - pl.col('UHC') - pl.col('IIS')).alias('OSPRIN')
])

df_current = df_current.with_columns([
    pl.when(pl.col('LOANTYPE').is_in([380, 381]))
      .then(pl.col('FEEAMT') - pl.col('FEETOT2'))
      .otherwise(
          pl.col('FEEAMT8') - pl.col('FEETOT2') + 
          pl.col('FEEAMTA') - pl.col('FEEAMT5')
      )
      .alias('OTHERFEE')
])

df_current = df_current.with_columns([
    pl.when(pl.col('OTHERFEE') < 0).then(0).otherwise(pl.col('OTHERFEE')).alias('OTHERFEE'),
    pl.when(pl.col('LOANTYPE').is_in([983, 993]))
      .then(0)
      .otherwise(pl.col('OTHERFEE'))
      .alias('OTHERFEE')
])

# Market value calculation (same as existing)
df_current = df_current.with_columns([
    ((reptdate.year - pl.col('ISSDTE').dt.year()) + 
     (reptdate.month - pl.col('ISSDTE').dt.month()) / 12).alias('AGE')
])

df_current = df_current.with_columns([
    pl.when(
        (pl.col('APPVALUE') > 0) &
        (pl.col('LOANTYPE').is_in([705,130,700,128,380,381,720,725]) | 
         (pl.col('CENSUS7') == '9')) &
        ((pl.col('DAYS') > 89) | (pl.col('USER5') == 'N')) &
        (~pl.col('BORSTAT').is_in(['F','R','I','Y','W'])) &
        (~pl.col('LOANTYPE').is_in([983,993]))
    )
    .then(
        pl.when(pl.col('CENSUS7') != '9')
          .then(pl.col('APPVALUE') - pl.col('APPVALUE') * pl.col('AGE') * 0.2)
          .otherwise(pl.col('APPVALUE'))
    )
    .when(pl.col('BORSTAT').is_in(['R']))
      .then(pl.col('APPVALUE'))
    .otherwise(0)
    .alias('MARKETVL')
])

df_current = df_current.with_columns([
    pl.when(pl.col('HARDCODE') == 'Y')
      .then(pl.col('WREALVL'))
      .otherwise(pl.col('MARKETVL'))
      .alias('MARKETVL'),
    pl.when(pl.col('MARKETVL') < 0).then(0).otherwise(pl.col('MARKETVL')).alias('MARKETVL')
])

df_current = df_current.with_columns([
    pl.when(pl.col('DAYS') > 273)
      .then(pl.col('OSPRIN') + pl.col('OTHERFEE'))
      .otherwise(pl.col('OSPRIN') + pl.col('OTHERFEE') - pl.col('MARKETVL'))
      .alias('NETEXP')
])

df_current = df_current.with_columns([
    pl.when(pl.col('DAYS') > 364)
      .then(pl.col('NETEXP'))
    .when(pl.col('DAYS') > 273)
      .then(pl.col('NETEXP') / 2)
    .when(pl.col('DAYS') > 89)
      .then(pl.col('NETEXP') * 0.2)
    .when((pl.col('DAYS') < 90) & (pl.col('USER5') == 'N'))
      .then(pl.col('NETEXP') * 0.2)
    .when(pl.col('BORSTAT').is_in(['F','R','I','W']))
      .then(pl.col('NETEXP'))
    .when((pl.col('DAYS') > 89) & (pl.col('BORSTAT') == 'Y'))
      .then(pl.col('NETEXP') / 5)
    .otherwise(0)
    .alias('SP')
])

df_current = df_current.with_columns([
    pl.when(pl.col('SP') < 0).then(0).otherwise(pl.col('SP')).alias('SP'),
    pl.col('SP').alias('SPPL'),  # For new NPL, SPPL = SP
    pl.lit(0.0).alias('RECOVER'),
    pl.lit(0.0).alias('SPPW')
])

print(f"  ✓ Current NPL: {len(df_current):,} accounts")

# Combine existing and current
df_combined = pl.concat([df_existing, df_current])

# Risk classification
df_combined = df_combined.with_columns([
    pl.when((pl.col('DAYS') > 364) | (pl.col('BORSTAT') == 'W'))
      .then(pl.lit('BAD'))
    .when(pl.col('DAYS') > 273)
      .then(pl.lit('DOUBTFUL'))
    .when(pl.col('DAYS') > 182)
      .then(pl.lit('SUBSTANDARD 2'))
    .when((pl.col('DAYS') < 90) & (pl.col('USER5') == 'N'))
      .then(pl.lit('SUBSTANDARD-1'))
    .otherwise(pl.lit('SUBSTANDARD-1'))
    .alias('RISK')
])

# Filter out Islamic and specific cost centers
df_combined = df_combined.filter(
    ((pl.col('COSTCTR') < 3000) | (pl.col('COSTCTR') > 3999)) &
    (~pl.col('COSTCTR').is_in([4043, 4048])) &
    (pl.col('COSTCTR').is_not_null())
)

# Add loan type description
df_combined = df_combined.with_columns([
    pl.col('LOANTYPE').map_elements(get_loantyp, return_dtype=pl.Utf8).alias('LOANTYP')
])

print(f"\n  ✓ Total NPL: {len(df_combined):,} accounts")

# Save output
df_combined.write_csv(f'{OUTPUT_DIR}NPL_SP.csv')
print(f"  ✓ Saved: NPL_SP.csv")

# Summary by risk
print(f"\n4. Summary by Risk Category:")
df_risk = df_combined.group_by('RISK').agg([
    pl.count().alias('COUNT'),
    pl.col('CURBAL').sum(),
    pl.col('NETEXP').sum(),
    pl.col('SP').sum()
])

for row in df_risk.sort('RISK').iter_rows(named=True):
    coverage = (row['SP'] / row['NETEXP'] * 100) if row['NETEXP'] > 0 else 0
    print(f"  {row['RISK']}: {row['COUNT']:,} accounts, "
          f"Exposure: RM {row['NETEXP']:,.2f}, "
          f"SP: RM {row['SP']:,.2f} ({coverage:.1f}%)")

print(f"\n{'='*60}")
print("✓ EIFMNP06 Complete")
print(f"{'='*60}")
print(f"\nPUBLIC BANK - (NPL FROM 3 MONTHS & ABOVE)")
print(f"MOVEMENTS OF SPECIFIC PROVISION FOR THE MONTH ENDING {rdate}")
print(f"\nFormula:")
print(f"  NETEXP = OSPRIN + OTHERFEE - MARKETVL")
print(f"  SP = NETEXP × Provision Rate")
print(f"\nProvision Rates:")
print(f"  Bad (>364 days): 100%")
print(f"  Doubtful (274-364 days): 50%")
print(f"  Substandard-2 (183-273 days): 20%")
print(f"  Substandard-1 (<183 days): 20%")
print(f"\nMarket Value Depreciation:")
print(f"  20% per year from issue date")
print(f"  MARKETVL = APPVALUE - (APPVALUE × AGE × 0.2)")
print(f"\nOutput: NPL_SP.csv")
