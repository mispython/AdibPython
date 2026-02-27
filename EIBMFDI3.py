"""
EIBMFDI3 - FD Rate Exception Report (Individual)

Purpose: Report individual FD accounts > 1M with rate exceptions

Input: FD1M.parquet (large accounts from EIBDFD1M)
Filter: Individual accounts only (PP != 'C')
Output: CSV report with rate comparisons

Columns:
- Customer IC, name, account details
- Offered rate vs counter rate
- Balance breakdown (standard vs special)
"""

import polars as pl
import os

# Directories
FD_DIR = 'data/fd/'
FD1M_DIR = 'data/fd1m/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBMFDI3 - FD Rate Exception Report (Individual)")

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{FD_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    reptdt = reptdate.strftime('%d/%m/%Y')
except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

# Read FD1M data
try:
    df_fdr = pl.read_parquet(f'{FD1M_DIR}FD1M.parquet')
    
    # Filter: Individual only (not Corporate)
    df_fdr = df_fdr.filter(pl.col('PP') != 'C')
    
    # Add NAME field
    df_fdr = df_fdr.with_columns([
        pl.col('CUSTNAME').alias('NAME')
    ])
    
    # Sort by branch, name, account, maturity date
    df_fdr = df_fdr.sort(['BRANCH', 'NAME', 'ACCTNO', 'MATDTE'])
    
    print(f"✓ Found {len(df_fdr):,} individual accounts")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

# Generate report
if len(df_fdr) > 0:
    # Select report columns
    report_cols = [
        'BRANCH', 'NAME', 'NEWIC', 'JI', 'ACCTNO', 'CDNO',
        'CURBAL', 'TERM', 'DEPDTE', 'MATDTE', 
        'RATE', 'NR', 'CURBALY', 'CURBALN'
    ]
    
    # Keep only available columns
    available_cols = [col for col in report_cols if col in df_fdr.columns]
    df_report = df_fdr.select(available_cols)
    
    # Calculate summaries by account
    df_acct_summary = df_report.group_by(['BRANCH', 'NAME', 'NEWIC', 'ACCTNO']).agg([
        pl.col('CURBAL').sum().alias('ACCT_TOTAL'),
        pl.col('CURBALY').sum().alias('ACCT_Y') if 'CURBALY' in available_cols else pl.lit(0).alias('ACCT_Y'),
        pl.col('CURBALN').sum().alias('ACCT_N') if 'CURBALN' in available_cols else pl.lit(0).alias('ACCT_N')
    ])
    
    # Calculate summaries by customer
    df_cust_summary = df_report.group_by(['BRANCH', 'NAME', 'NEWIC']).agg([
        pl.col('CURBAL').sum().alias('NAME_TOTAL'),
        pl.col('CURBALY').sum().alias('NAME_Y') if 'CURBALY' in available_cols else pl.lit(0).alias('NAME_Y'),
        pl.col('CURBALN').sum().alias('NAME_N') if 'CURBALN' in available_cols else pl.lit(0).alias('NAME_N')
    ])
    
    # Save detailed report
    df_report.write_csv(f'{OUTPUT_DIR}FDI3_DETAIL.csv')
    print(f"✓ Saved FDI3_DETAIL.csv")
    
    # Save account summary
    df_acct_summary.write_csv(f'{OUTPUT_DIR}FDI3_ACCT_SUMMARY.csv')
    print(f"✓ Saved FDI3_ACCT_SUMMARY.csv")
    
    # Save customer summary
    df_cust_summary.write_csv(f'{OUTPUT_DIR}FDI3_CUST_SUMMARY.csv')
    print(f"✓ Saved FDI3_CUST_SUMMARY.csv")
    
    # Print summary statistics
    total_accounts = df_acct_summary.shape[0]
    total_customers = df_cust_summary.shape[0]
    total_balance = df_report['CURBAL'].sum()
    
    print(f"\nReport Summary:")
    print(f"  Date: {reptdt}")
    print(f"  Customers: {total_customers:,}")
    print(f"  Accounts: {total_accounts:,}")
    print(f"  Total Balance: RM {total_balance:,.2f}")
    
    # Rate exception summary
    if 'RATE' in df_report.columns and 'NR' in df_report.columns:
        df_exceptions = df_report.filter(pl.col('RATE') != pl.col('NR'))
        if len(df_exceptions) > 0:
            print(f"  Rate Exceptions: {len(df_exceptions):,}")
            print(f"  Exception Amount: RM {df_exceptions['CURBAL'].sum():,.2f}")

else:
    print("⚠ No individual accounts found")

print("✓ EIBMFDI3 Complete")
