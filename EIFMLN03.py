"""
EIFMLN03 - HPD Weighted Average Lending Rate Report

Purpose: Calculate weighted average lending rate for HPD (Hire Purchase Dealer)
- RDIR II (Revised Disclosure & Information Requirements)
- APR (Annual Percentage Rate) calculation
- Product 34111 only (specific HPD product)
- By branch analysis

Formula:
  APR = TRATE × (300×TERM + TRATE) / ((NOTETERM×TRATE) + (150×TERM×(NOTETERM+1)))
  Where: TRATE = NOTETERM × INTRATE
         TERM = min(NOTETERM, 12)
"""

import polars as pl
from datetime import datetime
import os

# Directories
BNM_DIR = 'data/bnm/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIFMLN03 - HPD Weighted Average Lending Rate Report")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{BNM_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    nowk = '4'  # Always week 4 for month-end
    reptmon = f'{reptdate.month:02d}'
    rdate = reptdate.strftime('%d %B %Y')
    
    print(f"Report Date: {rdate}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

# Read description
try:
    df_desc = pl.read_parquet(f'{BNM_DIR}SDESC.parquet')
    sdesc = df_desc['SDESC'][0] if len(df_desc) > 0 else 'PUBLIC BANK BERHAD'
except:
    sdesc = 'PUBLIC BANK BERHAD'

print(f"Entity: {sdesc}")
print("=" * 60)

# Branch code format (PBBELF - simplified)
BRCHCD = {
    # Example branch codes - adjust based on actual mapping
    1: 'HEAD OFFICE',
    2: 'MAIN BRANCH',
    # Add more as needed
}

def get_branch_name(branch_code):
    """Map branch code to name"""
    return BRCHCD.get(branch_code, f'BRANCH {branch_code:03d}')

# Process HPD LOANS
print("\n1. Processing HPD Loans (Product 34111)...")
try:
    df_loan = pl.read_parquet(f'{BNM_DIR}LOAN{reptmon}{nowk}.parquet')
    
    # Filter: Product 34111, Normal status only
    df_loan = df_loan.filter(
        (pl.col('PRODCD') == '34111') &
        (pl.col('LOANSTAT') == 1)
    )
    
    print(f"  ✓ HPD Loans: {len(df_loan):,} accounts")
    
    if len(df_loan) == 0:
        print("  ⚠ No HPD loans found")
        import sys
        sys.exit(0)

except Exception as e:
    print(f"  ✗ Loans: {e}")
    import sys
    sys.exit(1)

# Calculate APR
print("\n2. Calculating APR...")

# TERM = min(NOTETERM, 12)
df_loan = df_loan.with_columns([
    pl.when(pl.col('NOTETERM') > 12)
      .then(pl.lit(12))
      .otherwise(pl.col('NOTETERM'))
      .alias('TERM')
])

# TRATE = NOTETERM × INTRATE
df_loan = df_loan.with_columns([
    (pl.col('NOTETERM') * pl.col('INTRATE')).alias('TRATE')
])

# APR Formula:
# APR = TRATE × (300×TERM + TRATE) / ((NOTETERM×TRATE) + (150×TERM×(NOTETERM+1)))
df_loan = df_loan.with_columns([
    (
        pl.col('TRATE') * 
        ((300 * pl.col('TERM')) + pl.col('TRATE'))
    ) / (
        (pl.col('NOTETERM') * pl.col('TRATE')) +
        (150 * pl.col('TERM') * (pl.col('NOTETERM') + 1))
    )
    .alias('APR')
])

# Weighted Amount = BALANCE × APR
df_loan = df_loan.with_columns([
    (pl.col('BALANCE') * pl.col('APR')).alias('WAMT'),
    pl.col('BRANCH').alias('BRHNO')
])

print(f"  ✓ APR calculated for {len(df_loan):,} accounts")

# Summarize by branch
print("\n3. Summarizing by branch...")
df_summary = df_loan.group_by('BRANCH').agg([
    pl.col('BALANCE').sum(),
    pl.col('WAMT').sum()
])

# Calculate weighted average rate
df_summary = df_summary.with_columns([
    (pl.col('WAMT') / pl.col('BALANCE')).alias('WAVRATE')
])

df_summary = df_summary.sort('BRANCH')

# Add branch names
df_summary = df_summary.with_columns([
    pl.col('BRANCH').map_elements(get_branch_name, return_dtype=pl.Utf8).alias('BRCH')
])

# Save output
df_summary.write_csv(f'{OUTPUT_DIR}HPD_WALR.csv')

print(f"  ✓ Branches: {len(df_summary):,}")

# Calculate totals
total_balance = df_summary['BALANCE'].sum()
total_wamt = df_summary['WAMT'].sum()
overall_wavrate = total_wamt / total_balance if total_balance > 0 else 0

print(f"\n  Total Balance: RM {total_balance:,.2f}")
print(f"  Total Weighted Amount: RM {total_wamt:,.2f}")
print(f"  Overall WA Rate: {overall_wavrate:.8f}")

# Display by branch
print(f"\n  Breakdown by Branch:")
for row in df_summary.iter_rows(named=True):
    branch = row['BRANCH']
    balance = row['BALANCE']
    wamt = row['WAMT']
    wavrate = row['WAVRATE']
    brch = row['BRCH']
    print(f"    {branch:03d} ({brch}): RM {balance:,.2f}, WA Rate: {wavrate:.8f}")

print(f"\n{'='*60}")
print("✓ EIFMLN03 Complete")
print(f"{'='*60}")
print(f"\n{sdesc} REPORT AS AT {rdate}")
print(f"WEIGHTED AVERAGE LENDING RATE ON HPD (RDIR II)")
print(f"\nProduct: 34111 (HPD - Hire Purchase Dealer)")
print(f"Status: Normal accounts only (LOANSTAT = 1)")
print(f"\nAPR Formula:")
print(f"  TRATE = NOTETERM × INTRATE")
print(f"  TERM = min(NOTETERM, 12)")
print(f"  APR = TRATE × (300×TERM + TRATE) /")
print(f"        ((NOTETERM×TRATE) + (150×TERM×(NOTETERM+1)))")
print(f"\nWeighted Average Rate:")
print(f"  WAVRATE = Σ(BALANCE × APR) / Σ(BALANCE)")
print(f"  Overall: {overall_wavrate:.8f}")
print(f"\nOutput: HPD_WALR.csv")
