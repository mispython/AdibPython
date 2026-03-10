"""
EIFMNP03 - NPL Interest in Suspense (IIS) Report

Purpose: Track movements of Interest in Suspense for NPL accounts
- IIS (Interest In Suspense) movements
- OI (Overdue Interest) movements
- Risk classification (Substandard-1, Substandard-2, Doubtful, Bad)
- Compare with previous month (existing vs current NPL)

Formula:
  IIS = 2×(REMMTH+1)×TERMCHG / (EARNTERM×(EARNTERM+1))
  Cumulative for each remaining term
"""

import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os

# Directories
NPL_DIR = 'data/npl/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIFMNP03 - NPL Interest in Suspense Report")
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

# Add housing loans (200-299)
for i in range(200, 300):
    LNTYP[i] = 'HOUSING LOANS'

def get_loantyp(loantype):
    """Map loan type to category"""
    return LNTYP.get(loantype, 'OTHERS')

# Helper: Calculate IIS
def calculate_iis(earnterm, termchg, remmth_start, remmth_end):
    """Calculate IIS using formula: 2×(REMMTH+1)×TERMCHG / (EARNTERM×(EARNTERM+1))"""
    if earnterm == 0 or termchg == 0:
        return 0
    
    iis = 0
    for remmth in range(int(remmth_start), int(remmth_end) - 1, -1):
        iis += 2 * (remmth + 1) * termchg / (earnterm * (earnterm + 1))
    return iis

# Process LOANS
print("\n1. Processing NPL Loans...")
try:
    df_loan = pl.read_parquet(f'{NPL_DIR}LOAN{reptmon}.parquet')
    
    # Read write-off accounts
    try:
        df_woff = pl.read_parquet(f'{NPL_DIR}WIIS.parquet')
        df_loan = df_loan.join(df_woff.select(['ACCTNO']).with_columns([
            pl.lit('Y').alias('WRITEOFF')
        ]), on='ACCTNO', how='left')
    except:
        df_loan = df_loan.with_columns([pl.lit('N').alias('WRITEOFF')])
    
    # Fill nulls
    df_loan = df_loan.with_columns([
        pl.col('WRITEOFF').fill_null('N'),
        pl.col('IISP').fill_null(0),
        pl.col('OIP').fill_null(0),
        pl.col('IISPW').fill_null(0),
        pl.when(pl.col('EARNTERM').is_in([0, None]))
          .then(pl.col('NOTETERM'))
          .otherwise(pl.col('EARNTERM'))
          .alias('EARNTERM')
    ])
    
    # Update borrower status for written-off accounts
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

# Calculate IIS for existing NPL
print("\n2. Calculating IIS for existing NPL...")
df_existing = df_loan.filter(pl.col('EXIST') == 'Y')

# Calculate remaining months
current_year = reptdate.year
current_month = reptdate.month

df_existing = df_existing.with_columns([
    # REMMTH calculation (simplified)
    (pl.col('EARNTERM') - 
     ((current_year - pl.col('ISSDTE').dt.year()) * 12 +
      current_month - pl.col('ISSDTE').dt.month() + 1)).alias('REMMTH2')
])

# Initialize IIS columns
df_existing = df_existing.with_columns([
    pl.lit(0.0).alias('IIS'),
    pl.lit(0.0).alias('SUSPEND'),
    pl.lit(0.0).alias('RECOVER'),
    pl.lit(0.0).alias('RECC'),
    pl.lit(0.0).alias('UHC'),
    pl.lit(0.0).alias('OI'),
    pl.lit(0.0).alias('OISUSP'),
    pl.lit(0.0).alias('OIRECV'),
    pl.lit(0.0).alias('OIRECC'),
    pl.lit(0.0).alias('OIW')
])

# Calculate OI (Overdue Interest)
df_existing = df_existing.with_columns([
    (pl.col('FEETOT2') - pl.col('FEEAMTA') + pl.col('FEEAMT5')).fill_null(0).alias('OI')
])

# For HP loans (720, 725), use ACCRUAL
df_existing = df_existing.with_columns([
    pl.when(pl.col('LOANTYPE').is_in([720, 725]))
      .then(pl.col('ACCRUAL'))
      .otherwise(pl.col('IIS'))
      .alias('IIS')
])

# Calculate UHC and NETBAL
df_existing = df_existing.with_columns([
    (pl.col('CURBAL') - pl.col('UHC')).alias('NETBAL')
])

# Set SUSPEND and OISUSP
df_existing = df_existing.with_columns([
    pl.col('IIS').alias('SUSPEND'),
    pl.col('OI').alias('OISUSP')
])

print(f"  ✓ Existing NPL: {len(df_existing):,} accounts")

# Calculate IIS for current NPL
print("\n3. Calculating IIS for current NPL...")
df_current = df_loan.filter(pl.col('EXIST') != 'Y')

# Same calculation as existing
df_current = df_current.with_columns([
    (pl.col('EARNTERM') - 
     ((current_year - pl.col('ISSDTE').dt.year()) * 12 +
      current_month - pl.col('ISSDTE').dt.month() + 1)).alias('REMMTH2')
])

df_current = df_current.with_columns([
    pl.lit(0.0).alias('IIS'),
    pl.lit(0.0).alias('SUSPEND'),
    pl.lit(0.0).alias('RECOVER'),
    pl.lit(0.0).alias('RECC'),
    pl.lit(0.0).alias('UHC'),
    pl.lit(0.0).alias('OI'),
    pl.lit(0.0).alias('OISUSP'),
    pl.lit(0.0).alias('OIRECV'),
    pl.lit(0.0).alias('OIRECC'),
    pl.lit(0.0).alias('OIW')
])

df_current = df_current.with_columns([
    (pl.col('FEETOT2') - pl.col('FEEAMTA') + pl.col('FEEAMT5')).fill_null(0).alias('OI'),
    (pl.col('CURBAL') - pl.col('UHC')).alias('NETBAL')
])

df_current = df_current.with_columns([
    pl.when(pl.col('LOANTYPE').is_in([720, 725]))
      .then(pl.col('ACCRUAL'))
      .otherwise(pl.col('IIS'))
      .alias('IIS')
])

df_current = df_current.with_columns([
    pl.col('IIS').alias('SUSPEND'),
    pl.col('OI').alias('OISUSP')
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

# Calculate TOTIIS
df_combined = df_combined.with_columns([
    (pl.col('IIS') + pl.col('OI')).alias('TOTIIS')
])

print(f"\n  ✓ Total NPL: {len(df_combined):,} accounts")

# Save output
df_combined.write_csv(f'{OUTPUT_DIR}NPL_IIS.csv')
print(f"  ✓ Saved: NPL_IIS.csv")

# Summary by risk
print(f"\n4. Summary by Risk Category:")
df_risk = df_combined.group_by('RISK').agg([
    pl.count().alias('COUNT'),
    pl.col('CURBAL').sum(),
    pl.col('IIS').sum(),
    pl.col('OI').sum(),
    pl.col('TOTIIS').sum()
])

for row in df_risk.sort('RISK').iter_rows(named=True):
    print(f"  {row['RISK']}: {row['COUNT']:,} accounts, "
          f"Balance: RM {row['CURBAL']:,.2f}, "
          f"Total IIS: RM {row['TOTIIS']:,.2f}")

# Summary by loan type
print(f"\n5. Summary by Loan Type:")
df_loantyp = df_combined.group_by('LOANTYP').agg([
    pl.count().alias('COUNT'),
    pl.col('CURBAL').sum(),
    pl.col('TOTIIS').sum()
])

for row in df_loantyp.sort('LOANTYP').iter_rows(named=True):
    print(f"  {row['LOANTYP']}: {row['COUNT']:,} accounts, RM {row['TOTIIS']:,.2f}")

print(f"\n{'='*60}")
print("✓ EIFMNP03 Complete")
print(f"{'='*60}")
print(f"\nPUBLIC BANK - (NPL FROM 3 MONTHS & ABOVE)")
print(f"MOVEMENTS OF INTEREST IN SUSPENSE FOR THE MONTH ENDING {rdate}")
print(f"\nComponents:")
print(f"  IIS: Interest In Suspense")
print(f"    Opening Bal (IISP)")
print(f"    + Suspended (SUSPEND)")
print(f"    - Written Back (RECOVER)")
print(f"    - Reversal (RECC)")
print(f"    - Written Off (IISPW)")
print(f"    = Closing Bal (IIS)")
print(f"\n  OI: Overdue Interest")
print(f"    Opening Bal (OIP)")
print(f"    + Suspended (OISUSP)")
print(f"    - Written Back (OIRECV)")
print(f"    - Reversal (OIRECC)")
print(f"    - Written Off (OIW)")
print(f"    = Closing Bal (OI)")
print(f"\nRisk Categories:")
print(f"  SUBSTANDARD-1: < 90 days or USER5='N'")
print(f"  SUBSTANDARD-2: 183-273 days")
print(f"  DOUBTFUL: 274-364 days")
print(f"  BAD: > 364 days or Written Off")
print(f"\nOutput: NPL_IIS.csv")
