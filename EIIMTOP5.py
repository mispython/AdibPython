"""
EIIMTOP5 - Top 50 Islamic Depositors Report (Production Ready)

Purpose:
- Identify top 50 largest depositors by total balance
- Separate individual and corporate customers
- Support liquidity framework reporting
- Track PB subsidiaries

Report Sections:
1. Top 50 Individual Depositors (FD + CA)
   - Personal customers (CUSTCODE 77, 78, 95, 96)
   - Sorted by total balance
   - Account-level details

2. Top 50 Corporate Depositors (FD + CA)
   - Corporate customers (INDORG = 'O')
   - Sorted by total balance
   - Account-level details

3. PB Subsidiaries Report
   - Specific customers: 53227, 169990, 170108, 3562038, 3721354
   - Subsidiary accounts under top corporate depositors

Account Types:
- CA: Current Accounts (3000000000-3999999999)
- FD: Fixed Deposits (1000000000-1999999999, 7000000000-7999999999)

Exclusions:
- PURPOSE = '2' (specific purpose accounts)
- CA Products: 400-410
- FD Products: 350-357

Week Assignment:
- Day 8:  Week 1
- Day 15: Week 2
- Day 22: Week 3
- Day 29+: Week 4
"""

import polars as pl
from datetime import datetime
import os

# Directories
DEPOSIT_DIR = 'data/deposit/'
CISCA_DIR = 'data/cisca/'
CISFD_DIR = 'data/cisfd/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIIMTOP5 - Top 50 Islamic Depositors Report")
print("=" * 60)

# Read REPTDATE and determine week
try:
    df_reptdate = pl.read_parquet(f'{DEPOSIT_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    day = reptdate.day
    month = reptdate.month
    year = reptdate.year
    
    # Determine week
    if day == 8:
        nowk = '1'
    elif day == 15:
        nowk = '2'
    elif day == 22:
        nowk = '3'
    else:
        nowk = '4'
    
    reptyear = year
    reptmon = f'{month:02d}'
    reptday = f'{day:02d}'
    rdate = reptdate.strftime('%d%m%y')
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')} (Week {nowk})")

except Exception as e:
    print(f"  ⚠ REPTDATE: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Read CIS data for CA and FD
print("\nReading CIS customer data...")

# CA Customer data
try:
    df_cisca = pl.read_parquet(f'{CISCA_DIR}DEPOSIT.parquet')
    df_cisca = df_cisca.select(['CUSTNO', 'ACCTNO', 'CUSTNAME', 'NEWIC', 'OLDIC', 'INDORG'])
    df_cisca = df_cisca.with_columns([
        pl.when(pl.col('NEWIC') != '')
          .then(pl.col('NEWIC'))
          .otherwise(pl.col('OLDIC'))
          .alias('ICNO')
    ])
    df_cisca = df_cisca.filter(
        (pl.col('ACCTNO') >= 3000000000) & (pl.col('ACCTNO') <= 3999999999)
    )
    print(f"  ✓ CA customers: {len(df_cisca):,}")
except Exception as e:
    print(f"  ⚠ CISCA: {e}")
    df_cisca = pl.DataFrame([])

# FD Customer data
try:
    df_cisfd = pl.read_parquet(f'{CISFD_DIR}DEPOSIT.parquet')
    df_cisfd = df_cisfd.select(['CUSTNO', 'ACCTNO', 'CUSTNAME', 'NEWIC', 'OLDIC', 'INDORG'])
    df_cisfd = df_cisfd.with_columns([
        pl.when(pl.col('NEWIC') != '')
          .then(pl.col('NEWIC'))
          .otherwise(pl.col('OLDIC'))
          .alias('ICNO')
    ])
    df_cisfd = df_cisfd.filter(
        ((pl.col('ACCTNO') >= 1000000000) & (pl.col('ACCTNO') <= 1999999999)) |
        ((pl.col('ACCTNO') >= 7000000000) & (pl.col('ACCTNO') <= 7999999999))
    )
    print(f"  ✓ FD customers: {len(df_cisfd):,}")
except Exception as e:
    print(f"  ⚠ CISFD: {e}")
    df_cisfd = pl.DataFrame([])

# Read deposit balances
print("\nReading deposit balances...")

# CA balances
try:
    df_ca = pl.read_parquet(f'{DEPOSIT_DIR}CURRENT.parquet')
    df_ca = df_ca.filter(pl.col('CURBAL') > 0)
    df_ca = df_ca.filter(
        (pl.col('ACCTNO') >= 3000000000) & (pl.col('ACCTNO') <= 3999999999)
    )
    print(f"  ✓ CA accounts: {len(df_ca):,}")
except Exception as e:
    print(f"  ⚠ CA: {e}")
    df_ca = pl.DataFrame([])

# FD balances
try:
    df_fd = pl.read_parquet(f'{DEPOSIT_DIR}FD.parquet')
    df_fd = df_fd.filter(pl.col('CURBAL') > 0)
    df_fd = df_fd.filter(
        ((pl.col('ACCTNO') >= 1000000000) & (pl.col('ACCTNO') <= 1999999999)) |
        ((pl.col('ACCTNO') >= 7000000000) & (pl.col('ACCTNO') <= 7999999999))
    )
    print(f"  ✓ FD accounts: {len(df_fd):,}")
except Exception as e:
    print(f"  ⚠ FD: {e}")
    df_fd = pl.DataFrame([])

# Merge CA with CIS data
print("\nProcessing CA accounts...")
if len(df_ca) > 0 and len(df_cisca) > 0:
    df_ca_merged = df_ca.join(df_cisca, on='ACCTNO', how='inner')
    
    # Filter exclusions
    df_ca_merged = df_ca_merged.filter(
        (pl.col('PURPOSE') != '2') &
        (~pl.col('PRODUCT').is_in([400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410]))
    )
    
    df_ca_merged = df_ca_merged.with_columns([
        pl.col('CURBAL').alias('CABAL')
    ])
    
    # Split individual and corporate
    df_caind = df_ca_merged.filter(pl.col('CUSTCODE').is_in([77, 78, 95, 96]))
    df_caorg = df_ca_merged.filter(pl.col('INDORG') == 'O')
    
    print(f"  ✓ Individual CA: {len(df_caind):,}")
    print(f"  ✓ Corporate CA: {len(df_caorg):,}")
else:
    df_caind = pl.DataFrame([])
    df_caorg = pl.DataFrame([])

# Merge FD with CIS data
print("\nProcessing FD accounts...")
if len(df_fd) > 0 and len(df_cisfd) > 0:
    df_fd_merged = df_fd.join(df_cisfd, on='ACCTNO', how='inner')
    
    # Filter exclusions
    df_fd_merged = df_fd_merged.filter(
        (pl.col('PURPOSE') != '2') &
        (~pl.col('PRODUCT').is_in([350, 351, 352, 353, 354, 355, 356, 357]))
    )
    
    df_fd_merged = df_fd_merged.with_columns([
        pl.col('CURBAL').alias('FDBAL')
    ])
    
    # Split individual and corporate
    df_fdind = df_fd_merged.filter(pl.col('CUSTCODE').is_in([77, 78, 95, 96]))
    df_fdorg = df_fd_merged.filter(pl.col('INDORG') == 'O')
    
    print(f"  ✓ Individual FD: {len(df_fdind):,}")
    print(f"  ✓ Corporate FD: {len(df_fdorg):,}")
else:
    df_fdind = pl.DataFrame([])
    df_fdorg = pl.DataFrame([])

# Generate Top 50 Individual Report
print("\nGenerating Top 50 Individual Depositors...")
df_ind_data = []
if len(df_fdind) > 0:
    df_ind_data.append(df_fdind.with_columns([
        pl.lit(0).alias('CABAL'),
        pl.col('FDBAL').alias('CURBAL')
    ]))
if len(df_caind) > 0:
    df_ind_data.append(df_caind.with_columns([
        pl.lit(0).alias('FDBAL'),
        pl.col('CABAL').alias('CURBAL')
    ]))

if df_ind_data:
    df_ind = pl.concat(df_ind_data)
    
    # Replace blank ICNO with 'XX'
    df_ind = df_ind.with_columns([
        pl.when(pl.col('ICNO').str.strip_chars() == '')
          .then(pl.lit('XX'))
          .otherwise(pl.col('ICNO'))
          .alias('ICNO')
    ])
    
    # Summarize by ICNO and CUSTNAME
    df_ind_summary = df_ind.group_by(['ICNO', 'CUSTNAME']).agg([
        pl.col('CURBAL').sum().alias('CURBAL'),
        pl.col('FDBAL').sum().alias('FDBAL'),
        pl.col('CABAL').sum().alias('CABAL')
    ]).sort('CURBAL', descending=True).head(50)
    
    print(f"  ✓ Top 50 individuals identified")
    
    # Save summary
    df_ind_summary.write_csv(f'{OUTPUT_DIR}TOP50_INDIVIDUAL_{reptmon}{reptyear}.csv')
    
    # Get account details for top 50
    df_ind_detail = df_ind.join(
        df_ind_summary.select(['ICNO', 'CUSTNAME']),
        on=['ICNO', 'CUSTNAME'],
        how='inner'
    ).sort(['ICNO', 'CUSTNAME'])
    
    df_ind_detail.write_csv(f'{OUTPUT_DIR}TOP50_INDIVIDUAL_DETAIL_{reptmon}{reptyear}.csv')

# Generate Top 50 Corporate Report
print("\nGenerating Top 50 Corporate Depositors...")
df_org_data = []
if len(df_fdorg) > 0:
    df_org_data.append(df_fdorg.with_columns([
        pl.lit(0).alias('CABAL'),
        pl.col('FDBAL').alias('CURBAL')
    ]))
if len(df_caorg) > 0:
    df_org_data.append(df_caorg.with_columns([
        pl.lit(0).alias('FDBAL'),
        pl.col('CABAL').alias('CURBAL')
    ]))

if df_org_data:
    df_org = pl.concat(df_org_data)
    
    # Replace blank ICNO
    df_org = df_org.with_columns([
        pl.when(pl.col('ICNO').str.strip_chars() == '')
          .then(pl.lit('XX'))
          .otherwise(pl.col('ICNO'))
          .alias('ICNO')
    ])
    
    # Summarize
    df_org_summary = df_org.group_by(['ICNO', 'CUSTNAME']).agg([
        pl.col('CURBAL').sum().alias('CURBAL'),
        pl.col('FDBAL').sum().alias('FDBAL'),
        pl.col('CABAL').sum().alias('CABAL')
    ]).sort('CURBAL', descending=True).head(50)
    
    print(f"  ✓ Top 50 corporates identified")
    
    # Save summary
    df_org_summary.write_csv(f'{OUTPUT_DIR}TOP50_CORPORATE_{reptmon}{reptyear}.csv')
    
    # Get account details
    df_org_detail = df_org.join(
        df_org_summary.select(['ICNO', 'CUSTNAME']),
        on=['ICNO', 'CUSTNAME'],
        how='inner'
    ).sort(['ICNO', 'CUSTNAME'])
    
    df_org_detail.write_csv(f'{OUTPUT_DIR}TOP50_CORPORATE_DETAIL_{reptmon}{reptyear}.csv')

# Generate PB Subsidiaries Report
print("\nGenerating PB Subsidiaries Report...")
pb_subs = [53227, 169990, 170108, 3562038, 3721354]

if df_org_data:
    df_subs = df_org.filter(pl.col('CUSTNO').is_in(pb_subs)).sort(['CUSTNO', 'ACCTNO'])
    
    if len(df_subs) > 0:
        print(f"  ✓ PB subsidiaries: {len(df_subs):,} accounts")
        df_subs.write_csv(f'{OUTPUT_DIR}PB_SUBSIDIARIES_{reptmon}{reptyear}.csv')

print(f"\n{'='*60}")
print(f"✓ EIIMTOP5 Complete!")
print(f"{'='*60}")
print(f"\nTop 50 Islamic Depositors Report Summary:")
print(f"  Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"  Week: {nowk}")
print(f"\nReports Generated:")
print(f"  1. Top 50 Individual Depositors (FD + CA)")
print(f"     - Summary and detail reports")
print(f"  2. Top 50 Corporate Depositors (FD + CA)")
print(f"     - Summary and detail reports")
print(f"  3. PB Subsidiaries Report")
print(f"     - Customers: {', '.join(map(str, pb_subs))}")
print(f"\nAccount Types:")
print(f"  CA: Current Accounts (3000000000-3999999999)")
print(f"  FD: Fixed Deposits (1000000000-1999999999, 7000000000-7999999999)")
print(f"\nExclusions:")
print(f"  PURPOSE = '2' (specific purpose)")
print(f"  CA Products: 400-410")
print(f"  FD Products: 350-357")
