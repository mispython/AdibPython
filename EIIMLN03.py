"""
EIIMLN03 - Islamic Weighted Average Lending Rate Report

Purpose: Calculate weighted average lending rates for Islamic loans
- Prescribed rates (housing & BNM funded)
- Non-prescribed rates (housing & other loans)
- Exclude staff loans, penalty rates, litigation accounts
- Output to flat file for SRS (Statistical Reporting System)

Loan Types:
P1: Prescribed Rate (Housing Loans)
P2: Prescribed Rate (BNM Funded Loans)
P3: Non-Prescribed Rate (Housing Loans)
P4: Non-Prescribed Rate (Other Loans)
"""

import polars as pl
from datetime import datetime
import os

# Directories
BNM_DIR = 'data/bnm/'
ODGP3_DIR = 'data/odgp3/'
OUTPUT_DIR = 'data/output/'
M4_DIR = 'data/m4/'

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(M4_DIR, exist_ok=True)

print("EIIMLN03 - Islamic Weighted Average Lending Rate")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{BNM_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    day = reptdate.day
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'
    
    reptyear = str(reptdate.year)
    reptmon = f'{reptdate.month:02d}'
    reptday = f'{day:02d}'
    rdate = reptdate.strftime('%d%m%y')
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Week: {nowk}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

# Read description
try:
    df_desc = pl.read_parquet(f'{BNM_DIR}SDESC.parquet')
    sdesc = df_desc['SDESC'][0] if len(df_desc) > 0 else 'PUBLIC ISLAMIC BANK BERHAD'
except:
    sdesc = 'PUBLIC ISLAMIC BANK BERHAD'

print(f"Entity: {sdesc}")
print("=" * 60)

# Loan type labels
LOANTYP_DESC = {
    'P1': 'PRESCRIBED RATE (HOUSING LOANS)',
    'P2': 'PRESCRIBED RATE (BNM FUNDED LOANS)',
    'P3': 'NON-PRESCRIBED RATE (HOUSING LOANS)',
    'P4': 'NON-PRESCRIBED RATE (OTHER LOANS)'
}

# Process LOANS
print("\n1. Processing Islamic Loans...")
try:
    df_loan = pl.read_parquet(f'{BNM_DIR}LOAN{reptmon}{nowk}.parquet')
    
    # Exclude staff loans (products 124, 145 with PRODCD='54120')
    df_loan = df_loan.filter(
        ~((pl.col('PRODUCT').is_in([124,145])) & (pl.col('PRODCD') == '54120'))
    )
    
    # Filter Islamic loans
    df_loan = df_loan.filter(
        (pl.col('PRODCD').str.slice(0,2) == '34') &
        (pl.col('PRODCD') != '34111')  # Exclude specific product
    )
    
    print(f"  ✓ Islamic Loans: {len(df_loan):,} accounts")

except Exception as e:
    print(f"  ✗ Loans: {e}")
    import sys
    sys.exit(1)

# Read GP3 risk data
try:
    df_gp3 = pl.read_parquet(f'{ODGP3_DIR}GP3.parquet')
    df_gp3 = df_gp3.with_columns([
        pl.col('RISKCODE').cast(pl.Int64).alias('RISKRTE')
    ])
    df_gp3 = df_gp3.with_columns([
        pl.when(pl.col('RISKRTE') < 1)
          .then(pl.lit(1))
          .otherwise(pl.col('RISKRTE'))
          .alias('LOANSTAT')
    ])
    
    # Merge with loans
    df_loan = df_loan.join(df_gp3.select(['ACCTNO','LOANSTAT']), on='ACCTNO', how='left')
    
except:
    print("  ⚠ GP3 data not found, using loan LOANSTAT")

# Classify loan types
print("\n2. Classifying loan types...")

# Initialize LOANTYP
df_loan = df_loan.with_columns([pl.lit('P4').alias('LOANTYP')])

# Overdrafts
df_loan = df_loan.with_columns([
    pl.when(pl.col('ACCTYPE') == 'OD')
      .then(
          pl.when(pl.col('PRODUCT') == 119)
            .then(pl.lit('P1'))
          .when(pl.col('PRODUCT').is_in([120,137,138,154,155,192,193,194,195]))
            .then(pl.lit('P3'))
          .when(pl.col('PRODUCT').is_in([73,187,188,47,48,49,17,14]))
            .then(pl.lit('P2'))
          .otherwise(pl.lit('P4'))
      )
      .otherwise(pl.col('LOANTYP'))
      .alias('LOANTYP')
])

# Exclude specific OD products
df_loan = df_loan.filter(
    ~((pl.col('ACCTYPE') == 'OD') & pl.col('PRODUCT').is_in([93,162]))
)

# Term Loans
df_loan = df_loan.with_columns([
    pl.when(pl.col('ACCTYPE') == 'LN')
      .then(
          pl.when(pl.col('PRODUCT').is_in([225,226]))
            .then(
                pl.when((pl.col('INTRATE') <= 9) | (pl.col('SPREAD') <= 1.75))
                  .then(pl.lit('P1'))
                  .otherwise(pl.lit('P3'))
            )
          .when(
              (pl.col('PRODUCT') == 169) &
              (pl.col('CENSUS').is_in([169.01, 169.02, 169.03, 169.04]))
          )
            .then(pl.lit('P2'))
          .otherwise(pl.col('LOANTYP'))
      )
      .otherwise(pl.col('LOANTYP'))
      .alias('LOANTYP')
])

# Exclude specific LN products
df_loan = df_loan.filter(
    ~pl.col('PRODUCT').is_in([668,669,670,672,673,674,675,690] + 
                             list(range(851,861)) + 
                             [671,676,677] + 
                             list(range(691,696)))
)

# Add branch number
df_loan = df_loan.with_columns([
    pl.col('BRANCH').alias('BRHNO')
])

print(f"  ✓ Classified: {len(df_loan):,} loans")

# Summary by loan type
df_summary = df_loan.group_by('LOANTYP').agg([
    pl.col('BALANCE').sum()
])

for row in df_summary.iter_rows(named=True):
    loantyp = row['LOANTYP']
    balance = row['BALANCE']
    desc = LOANTYP_DESC.get(loantyp, loantyp)
    print(f"    {loantyp} ({desc}): RM {balance:,.2f}")

# Report 1: Exclude penalty & litigation (LOANSTAT = 1)
print("\n3. Generating Report 1 (Normal Accounts)...")
df_normal = df_loan.filter(pl.col('LOANSTAT') == 1)

df_rpt1 = df_normal.group_by(['LOANTYP','INTRATE']).agg([
    pl.col('BALANCE').sum()
])

df_rpt1 = df_rpt1.with_columns([
    (pl.col('INTRATE') * pl.col('BALANCE')).alias('PRODUCT')
])

df_rpt1 = df_rpt1.sort(['LOANTYP','INTRATE'])

# Calculate weighted average
df_wa1 = df_rpt1.group_by('LOANTYP').agg([
    pl.col('BALANCE').sum(),
    pl.col('PRODUCT').sum()
])

df_wa1 = df_wa1.with_columns([
    (pl.col('PRODUCT') / pl.col('BALANCE')).alias('WA_RATE')
])

print(f"\n  Weighted Average Rates (Excluding Penalty & Litigation):")
for row in df_wa1.iter_rows(named=True):
    loantyp = row['LOANTYP']
    wa_rate = row['WA_RATE']
    balance = row['BALANCE']
    desc = LOANTYP_DESC.get(loantyp, loantyp)
    print(f"    {desc}: {wa_rate:.4f}% (RM {balance:,.2f})")

df_rpt1.write_csv(f'{OUTPUT_DIR}WALR_NORMAL.csv')

# Report 2: Prescribed only, include all (penalty & litigation)
print("\n4. Generating Report 2 (Prescribed - All Accounts)...")
df_prescribed = df_loan.filter(pl.col('LOANTYP').is_in(['P1','P2']))

df_rpt2 = df_prescribed.group_by(['LOANTYP','INTRATE']).agg([
    pl.col('BALANCE').sum()
])

df_rpt2 = df_rpt2.with_columns([
    (pl.col('INTRATE') * pl.col('BALANCE')).alias('PRODUCT')
])

df_rpt2 = df_rpt2.sort(['LOANTYP','INTRATE'])

# Weighted average for prescribed
df_wa2 = df_rpt2.group_by('LOANTYP').agg([
    pl.col('BALANCE').sum(),
    pl.col('PRODUCT').sum()
])

df_wa2 = df_wa2.with_columns([
    (pl.col('PRODUCT') / pl.col('BALANCE')).alias('WA_RATE')
])

print(f"\n  Weighted Average Rates (Prescribed - Including Penalty & Litigation):")
for row in df_wa2.iter_rows(named=True):
    loantyp = row['LOANTYP']
    wa_rate = row['WA_RATE']
    balance = row['BALANCE']
    desc = LOANTYP_DESC.get(loantyp, loantyp)
    print(f"    {desc}: {wa_rate:.4f}% (RM {balance:,.2f})")

df_rpt2.write_csv(f'{OUTPUT_DIR}WALR_PRESCRIBED.csv')

# Overall prescribed total
df_prescribed_total = df_rpt2.group_by('INTRATE').agg([
    pl.col('BALANCE').sum(),
    pl.col('PRODUCT').sum()
])

total_balance = df_prescribed_total['BALANCE'].sum()
total_product = df_prescribed_total['PRODUCT'].sum()
overall_wa = total_product / total_balance if total_balance > 0 else 0

print(f"\n  Overall Prescribed WA Rate: {overall_wa:.4f}% (RM {total_balance:,.2f})")

# Generate SRS Flat File
print("\n5. Generating SRS Flat File...")

# Type 1: Prescribed (normal accounts only)
df_srs1 = df_normal.filter(
    pl.col('LOANTYP').is_in(['P1','P2'])
).group_by('INTRATE').agg([
    pl.col('BALANCE').sum()
])

# Type 4: All (normal accounts only)
df_srs4 = df_normal.group_by('INTRATE').agg([
    pl.col('BALANCE').sum()
])

# Type 9: By branch
df_srs9 = df_loan.group_by(['BRHNO','INTRATE']).agg([
    pl.col('BALANCE').sum()
])

# Write flat file
with open(f'{M4_DIR}M4LOAN.txt', 'w') as f:
    # Header
    f.write(f"{reptyear}{reptmon}{reptday}\n")
    
    # Type 01 (Prescribed)
    for row in df_srs1.iter_rows(named=True):
        intrate = int(round(row['INTRATE'] * 100))
        balance = int(round(row['BALANCE'] * 100))
        f.write(f"001M4 000001{intrate:04d}{balance:015d}\n")
    
    # Type 04 (All)
    for row in df_srs4.iter_rows(named=True):
        intrate = int(round(row['INTRATE'] * 100))
        balance = int(round(row['BALANCE'] * 100))
        f.write(f"001M4 000004{intrate:04d}{balance:015d}\n")
    
    # Type 09 (By Branch)
    for row in df_srs9.iter_rows(named=True):
        brhno = int(row['BRHNO'])
        intrate = int(round(row['INTRATE'] * 100))
        balance = int(round(row['BALANCE'] * 100))
        f.write(f"{brhno:03d}M4 000009{intrate:04d}{balance:015d}\n")
    
    # EOF marker
    f.write("EOF\n")

print(f"  ✓ SRS file: M4LOAN.txt")
print(f"    Type 01: {len(df_srs1):,} records (Prescribed)")
print(f"    Type 04: {len(df_srs4):,} records (All)")
print(f"    Type 09: {len(df_srs9):,} records (By Branch)")

print(f"\n{'='*60}")
print("✓ EIIMLN03 Complete")
print(f"{'='*60}")
print(f"\nIslamic Weighted Average Lending Rate Report")
print(f"Entity: {sdesc}")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"\nLoan Classifications:")
print(f"  P1: Prescribed Rate (Housing Loans)")
print(f"  P2: Prescribed Rate (BNM Funded Loans)")
print(f"  P3: Non-Prescribed Rate (Housing Loans)")
print(f"  P4: Non-Prescribed Rate (Other Loans)")
print(f"\nExclusions:")
print(f"  - Staff loans (Products 124,145 with PRODCD='54120')")
print(f"  - Product 34111")
print(f"  - Specific OD/LN products")
print(f"\nOutputs:")
print(f"  - WALR_NORMAL.csv (Normal accounts)")
print(f"  - WALR_PRESCRIBED.csv (Prescribed with penalty)")
print(f"  - M4LOAN.txt (SRS flat file)")
