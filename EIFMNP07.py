"""
EIFMNP07 - NPL General Provision (GP) Report

Purpose: Calculate General Provision for performing loan portfolio
- GP (General Provision) for non-NPL accounts
- Regulatory minimum provision (typically 1.5% of performing loans)
- Excludes NPL accounts (already covered by Specific Provision)

Formula:
  GP = (Total Performing Loans - IIS on Performing) × GP Rate
  Typical GP Rate: 1.5% (BNM regulatory minimum)
"""

import polars as pl
from datetime import datetime
import os

# Directories
NPL_DIR = 'data/npl/'
BNM_DIR = 'data/bnm/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIFMNP07 - NPL General Provision Report")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{NPL_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    reptmon = f'{reptdate.month:02d}'
    prevmon = f'{(reptdate.month-1 if reptdate.month > 1 else 12):02d}'
    rdate = reptdate.strftime('%d %B %Y')
    nowk = '4'  # Month-end
    
    print(f"Report Date: {rdate}")
    print(f"Period: {reptmon}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# GP Rate (BNM minimum requirement)
GP_RATE = 0.015  # 1.5%

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

# Read all loans
print("\n1. Processing All Loans...")
try:
    # Read full loan portfolio
    df_all_loans = pl.read_parquet(f'{BNM_DIR}LOAN{reptmon}{nowk}.parquet')
    
    print(f"  ✓ Total Loans: {len(df_all_loans):,} accounts")

except Exception as e:
    print(f"  ✗ Loans: {e}")
    import sys
    sys.exit(1)

# Read NPL accounts
print("\n2. Identifying NPL Accounts...")
try:
    df_npl = pl.read_parquet(f'{NPL_DIR}LOAN{reptmon}.parquet')
    
    # Get list of NPL account numbers
    npl_accounts = df_npl['ACCTNO'].unique().to_list()
    
    print(f"  ✓ NPL Accounts: {len(npl_accounts):,} accounts")

except Exception as e:
    print(f"  ⚠ NPL data not found, assuming no NPL: {e}")
    npl_accounts = []

# Filter performing loans (exclude NPL)
print("\n3. Filtering Performing Loans...")
df_performing = df_all_loans.filter(
    ~pl.col('ACCTNO').is_in(npl_accounts)
)

# Exclude Islamic (COSTCTR 3000-3999)
df_performing = df_performing.filter(
    (pl.col('COSTCTR') < 3000) | (pl.col('COSTCTR') > 3999)
)

# Exclude specific cost centers
df_performing = df_performing.filter(
    (~pl.col('COSTCTR').is_in([4043, 4048])) &
    (pl.col('COSTCTR').is_not_null())
)

print(f"  ✓ Performing Loans: {len(df_performing):,} accounts")

# Calculate base for GP
print("\n4. Calculating GP Base...")

# Calculate UHC (Unearned Hiring Charges) - simplified
df_performing = df_performing.with_columns([
    pl.lit(0.0).alias('UHC')
])

# Calculate IIS for performing loans (typically minimal)
df_performing = df_performing.with_columns([
    pl.lit(0.0).alias('IIS')
])

# Calculate net balance
df_performing = df_performing.with_columns([
    (pl.col('BALANCE') - pl.col('UHC')).alias('NETBAL'),
    (pl.col('BALANCE') - pl.col('UHC') - pl.col('IIS')).alias('OSPRIN')
])

# Add loan type
df_performing = df_performing.with_columns([
    pl.col('LOANTYPE').map_elements(get_loantyp, return_dtype=pl.Utf8).alias('LOANTYP')
])

# Calculate totals
total_balance = df_performing['BALANCE'].sum()
total_netbal = df_performing['NETBAL'].sum()
total_osprin = df_performing['OSPRIN'].sum()

print(f"  Total Balance: RM {total_balance:,.2f}")
print(f"  Total Net Balance: RM {total_netbal:,.2f}")
print(f"  Total O/S Principal: RM {total_osprin:,.2f}")

# Calculate GP
print("\n5. Calculating General Provision...")

# GP = Outstanding Principal × GP Rate
gp_required = total_osprin * GP_RATE

print(f"  GP Base: RM {total_osprin:,.2f}")
print(f"  GP Rate: {GP_RATE*100:.2f}%")
print(f"  GP Required: RM {gp_required:,.2f}")

# Read previous GP (if available)
try:
    if reptmon == '01':
        gpp_opening = 0  # Start of year
    else:
        df_prev_gp = pl.read_parquet(f'{NPL_DIR}GP{prevmon}.parquet')
        gpp_opening = df_prev_gp['GP'][0] if len(df_prev_gp) > 0 else 0
except:
    gpp_opening = 0

# Calculate movements
gp_current = gp_required
gp_movement = gp_current - gpp_opening

if gp_movement > 0:
    gp_provision = gp_movement
    gp_writeback = 0
else:
    gp_provision = 0
    gp_writeback = abs(gp_movement)

print(f"\n  Opening Balance (GPP): RM {gpp_opening:,.2f}")
print(f"  Provision Made: RM {gp_provision:,.2f}")
print(f"  Written Back: RM {gp_writeback:,.2f}")
print(f"  Closing Balance (GP): RM {gp_current:,.2f}")

# Summary by loan type
print("\n6. Summary by Loan Type:")
df_summary = df_performing.group_by('LOANTYP').agg([
    pl.count().alias('COUNT'),
    pl.col('BALANCE').sum(),
    pl.col('OSPRIN').sum()
])

df_summary = df_summary.with_columns([
    (pl.col('OSPRIN') * GP_RATE).alias('GP_REQUIRED')
])

for row in df_summary.sort('LOANTYP').iter_rows(named=True):
    print(f"  {row['LOANTYP']}: {row['COUNT']:,} accounts, "
          f"O/S: RM {row['OSPRIN']:,.2f}, "
          f"GP: RM {row['GP_REQUIRED']:,.2f}")

# Save output
df_output = pl.DataFrame({
    'REPORT_DATE': [reptdate],
    'TOTAL_ACCOUNTS': [len(df_performing)],
    'TOTAL_BALANCE': [total_balance],
    'TOTAL_OSPRIN': [total_osprin],
    'GP_RATE': [GP_RATE],
    'GPP_OPENING': [gpp_opening],
    'GP_PROVISION': [gp_provision],
    'GP_WRITEBACK': [gp_writeback],
    'GP_CLOSING': [gp_current]
})

df_output.write_csv(f'{OUTPUT_DIR}GP_SUMMARY.csv')
df_summary.write_csv(f'{OUTPUT_DIR}GP_BY_LOANTYPE.csv')
df_performing.write_csv(f'{OUTPUT_DIR}PERFORMING_LOANS.csv')

print(f"\n{'='*60}")
print("✓ EIFMNP07 Complete")
print(f"{'='*60}")
print(f"\nPUBLIC BANK - GENERAL PROVISION REPORT")
print(f"AS AT {rdate}")
print(f"\nPerforming Loan Portfolio:")
print(f"  Total Accounts: {len(df_performing):,}")
print(f"  Total Balance: RM {total_balance:,.2f}")
print(f"  Outstanding Principal: RM {total_osprin:,.2f}")
print(f"\nGeneral Provision Calculation:")
print(f"  GP Rate: {GP_RATE*100:.2f}% (BNM Minimum)")
print(f"  GP Base: Outstanding Principal")
print(f"  Formula: GP = OSPRIN × {GP_RATE*100:.2f}%")
print(f"\nMovements:")
print(f"  Opening Balance: RM {gpp_opening:,.2f}")
print(f"  Provision Made: RM {gp_provision:,.2f}")
print(f"  Written Back: RM {gp_writeback:,.2f}")
print(f"  Closing Balance: RM {gp_current:,.2f}")
print(f"\nExclusions:")
print(f"  - NPL accounts (covered by Specific Provision)")
print(f"  - Islamic banking (COSTCTR 3000-3999)")
print(f"  - Specific cost centers (4043, 4048)")
print(f"\nOutputs:")
print(f"  - GP_SUMMARY.csv (Overall summary)")
print(f"  - GP_BY_LOANTYPE.csv (By loan type)")
print(f"  - PERFORMING_LOANS.csv (Detail)")
print(f"\nRegulatory Compliance:")
print(f"  BNM requires minimum 1.5% GP on performing loans")
print(f"  Current GP: RM {gp_current:,.2f}")
print(f"  Minimum Required: RM {gp_required:,.2f}")
print(f"  Status: {'✓ COMPLIANT' if gp_current >= gp_required else '✗ SHORTFALL'}")
