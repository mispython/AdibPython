"""
EIFMNP22 - HP Fee Receivable Report

Purpose: Outstanding balance for PC/Fee Receivable for HP loans
- FEEAMT3: PC/Fee Receivable
- FEEAMT4: Current Fees (Amount Access Current)
- Focus on specific HP loan types (110, 115, 700, 705)
- Separate reports for NPL (6+ months) and All HP Loans
"""

import polars as pl
from datetime import datetime
import struct
import os

# Directories
NPL_DIR = 'data/npl/'
LOAN_DIR = 'data/loan/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIFMNP22 - HP Fee Receivable Report")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{NPL_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    reptmon = f'{reptdate.month:02d}'
    rdate = reptdate.strftime('%d %B %Y')
    
    print(f"Report Date: {rdate}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Loan type mapping
LNTYP = {
    110: 'HPD AITAB',
    115: 'HPD AITAB',
    983: 'HPD AITAB',
    700: 'HPD CONVENTIONAL',
    705: 'HPD CONVENTIONAL',
    993: 'HPD CONVENTIONAL',
}

# Add housing loans
for i in range(200, 300):
    LNTYP[i] = 'HOUSING LOANS'

# Add fixed loans
for i in range(300, 500):
    LNTYP[i] = 'FIXED LOANS'
for i in range(504, 551):
    LNTYP[i] = 'FIXED LOANS'
for i in range(900, 981):
    LNTYP[i] = 'FIXED LOANS'

def get_loantyp(loantype):
    return LNTYP.get(loantype, 'OTHERS')

# Read LNNOTE
print("\n1. Reading LNNOTE...")
try:
    df_lnnote = pl.read_parquet(f'{LOAN_DIR}LNNOTE.parquet')
    
    # Filter
    df_lnnote = df_lnnote.filter(
        (pl.col('REVERSED') != 'Y') &
        (pl.col('NOTENO').is_not_null()) &
        (pl.col('NTBRCH').is_not_null()) &
        (pl.col('LOANTYPE').is_in([110, 115, 700, 705]))
    )
    
    print(f"  ✓ LNNOTE: {len(df_lnnote):,} records")

except Exception as e:
    print(f"  ✗ LNNOTE: {e}")
    import sys
    sys.exit(1)

# Read FEPLAN from packed decimal file
print("\n2. Reading FEPLAN file...")
try:
    # Parse FEEFILE (packed decimal format)
    feefile_path = f'{LOAN_DIR}FEEFILE.dat'
    
    # Note: This is a placeholder for packed decimal parsing
    # In production, you'd need proper packed decimal parsing
    # For now, we'll create from existing FEEAMT columns if available
    
    # Try to read from parquet if converted
    try:
        df_feplan = pl.read_parquet(f'{LOAN_DIR}FEPLAN.parquet')
        df_feplan = df_feplan.filter(
            (pl.col('LOANTYPE').is_in([110, 115, 700, 705])) &
            (pl.col('FEEPLN').is_in(['DC']))
        )
        print(f"  ✓ FEPLAN: {len(df_feplan):,} records")
        
    except:
        print(f"  ⚠ FEPLAN not found, using LNNOTE fees directly")
        df_feplan = None

except Exception as e:
    print(f"  ✗ FEPLAN: {e}")
    df_feplan = None

# Aggregate FEPLAN if available
if df_feplan is not None:
    print("\n3. Aggregating FEPLAN...")
    df_feepo = df_feplan.group_by(['ACCTNO', 'NOTENO']).agg([
        pl.col('FEEAMT').sum().alias('FEEAMT3A'),
        pl.col('FEEAMT4').sum().alias('FEEAMT4A')
    ])
    
    # Merge with LNNOTE
    df_lnnote = df_lnnote.join(df_feepo, on=['ACCTNO', 'NOTENO'], how='left')
    
    # Add to existing fees
    df_lnnote = df_lnnote.with_columns([
        (pl.col('FEEAMT3').fill_null(0) + pl.col('FEEAMT3A').fill_null(0)).alias('FEEAMT3'),
        (pl.col('FEEAMT4').fill_null(0) + pl.col('FEEAMT4A').fill_null(0)).alias('FEEAMT4')
    ])
    
    print(f"  ✓ Merged FEPLAN with LNNOTE")
else:
    # Ensure FEEAMT3 and FEEAMT4 columns exist
    if 'FEEAMT3' not in df_lnnote.columns:
        df_lnnote = df_lnnote.with_columns([pl.lit(0.0).alias('FEEAMT3')])
    if 'FEEAMT4' not in df_lnnote.columns:
        df_lnnote = df_lnnote.with_columns([pl.lit(0.0).alias('FEEAMT4')])

# Read NPL LOAN data
print("\n4. Reading NPL LOAN data...")
try:
    df_loan = pl.read_parquet(f'{NPL_DIR}LOAN{reptmon}.parquet')
    
    # Filter HP loans
    df_loan = df_loan.filter(
        pl.col('LOANTYPE').is_in([110, 115, 700, 705])
    )
    
    print(f"  ✓ NPL Loans: {len(df_loan):,} records")

except Exception as e:
    print(f"  ✗ NPL Loans: {e}")
    import sys
    sys.exit(1)

# Risk classification
df_loan = df_loan.with_columns([
    pl.when(pl.col('DAYS') > 364)
      .then(pl.lit('BAD'))
    .when(pl.col('DAYS') > 273)
      .then(pl.lit('DOUBTFUL'))
    .when(pl.col('DAYS') > 182)
      .then(pl.lit('SUBSTANDARD 2'))
    .otherwise(pl.lit('SUBSTANDARD-1'))
    .alias('RISK')
])

# Add branch and loan type
df_loan = df_loan.with_columns([
    pl.col('LOANTYPE').map_elements(get_loantyp, return_dtype=pl.Utf8).alias('LOANTYP')
])

# Fill null fees
df_loan = df_loan.with_columns([
    pl.col('FEEAMT3').fill_null(0),
    pl.col('FEEAMT4').fill_null(0)
])

# Report 1: NPL 6+ Months
print("\n5. Generating NPL Report (6+ Months)...")
df_npl_6m = df_loan.filter(pl.col('DAYS') > 182)

df_npl_summary = df_npl_6m.group_by(['LOANTYP', 'RISK']).agg([
    pl.col('FEEAMT3').sum(),
    pl.col('FEEAMT4').sum()
])

df_npl_summary.write_csv(f'{OUTPUT_DIR}HP_FEE_NPL_6M.csv')
print(f"  ✓ Saved: HP_FEE_NPL_6M.csv")

# Display summary
print(f"\n  NPL Fee Receivable (6+ Months):")
total_feeamt3_npl = 0
total_feeamt4_npl = 0

for row in df_npl_summary.sort(['LOANTYP', 'RISK']).iter_rows(named=True):
    print(f"    {row['LOANTYP']} / {row['RISK']}:")
    print(f"      PC/Fee Rec: RM {row['FEEAMT3']:,.2f}")
    print(f"      Current Fees: RM {row['FEEAMT4']:,.2f}")
    total_feeamt3_npl += row['FEEAMT3']
    total_feeamt4_npl += row['FEEAMT4']

print(f"\n  Total NPL (6+ months):")
print(f"    PC/Fee Receivable: RM {total_feeamt3_npl:,.2f}")
print(f"    Current Fees: RM {total_feeamt4_npl:,.2f}")

# Report 2: All HP Loans
print("\n6. Generating All HP Loans Report...")

# Filter LNNOTE for all HP loans
df_all_hp = df_lnnote.filter(
    (pl.col('REVERSED') != 'Y') &
    (pl.col('NOTENO').is_not_null()) &
    (pl.col('NTBRCH').is_not_null()) &
    (pl.col('LOANTYPE').is_in([110, 115, 700, 705, 709, 710, 750, 752, 760, 770, 775, 799]))
)

# Aggregate
total_feeamt3_all = df_all_hp['FEEAMT3'].sum()
total_feeamt4_all = df_all_hp['FEEAMT4'].sum()

# Save summary
df_all_summary = pl.DataFrame({
    'CATEGORY': ['ALL HP LOANS'],
    'FEEAMT3': [total_feeamt3_all],
    'FEEAMT4': [total_feeamt4_all]
})

df_all_summary.write_csv(f'{OUTPUT_DIR}HP_FEE_ALL.csv')
print(f"  ✓ Saved: HP_FEE_ALL.csv")

print(f"\n  All HP Loans:")
print(f"    PC/Fee Receivable: RM {total_feeamt3_all:,.2f}")
print(f"    Amt Access Curr: RM {total_feeamt4_all:,.2f}")

print(f"\n{'='*60}")
print("✓ EIFMNP22 Complete")
print(f"{'='*60}")
print(f"\nPUBLIC BANK BERHAD")
print(f"OUTSTANDING BALANCE FOR PC/FEE RECEIVABLE AS AT {rdate}")
print(f"\nReport 1: NPL FROM 6 MONTHS & ABOVE")
print(f"  HP Loan Types: 110, 115, 700, 705")
print(f"  Criteria: DAYS > 182")
print(f"  PC/Fee Receivable: RM {total_feeamt3_npl:,.2f}")
print(f"  Current Fees: RM {total_feeamt4_npl:,.2f}")
print(f"\nReport 2: ALL HP LOANS")
print(f"  HP Loan Types: 110, 115, 700, 705, 709, 710, 750, 752, 760, 770, 775, 799")
print(f"  PC/Fee Receivable: RM {total_feeamt3_all:,.2f}")
print(f"  Amt Access Curr: RM {total_feeamt4_all:,.2f}")
print(f"\nFee Components:")
print(f"  FEEAMT3: PC/Fee Receivable")
print(f"  FEEAMT4: Current Fees (Amount Access Current)")
print(f"  FEEPLN: 'DC' (Specific fee plan)")
print(f"\nOutputs:")
print(f"  - HP_FEE_NPL_6M.csv (NPL 6+ months)")
print(f"  - HP_FEE_ALL.csv (All HP loans)")
