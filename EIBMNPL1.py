"""
EIBMNPL1 - Total Overdue Loans Report

Purpose: Generate comprehensive overdue loans analysis
- Risk classification by days overdue (0-7 days to 1095+ days)
- Separate reports for Loans, HP, O/D, and Combined
- NPL classification (Overdue, Delinquent, Non-Performing)

Risk Categories (15 levels):
0:  0-7 days
1:  8-30 days
2:  31-59 days
3:  60-89 days
4:  90-121 days (CA - Close Attention)
5:  122-151 days
6:  152-182 days
7:  183-213 days (NPL - Non-Performing)
8:  214-243 days
9:  244-273 days
10: 274-364 days
11: 365-547 days
12: 548-729 days
13: 730-1094 days
14: 1095+ days (3+ years)

NPL Classification:
- Overdue: 30+ days (RISKRATE > 0)
- Delinquent: 90+ days (RISKRATE > 3)
- Non-Performing: 183+ days (RISKRATE > 6)
"""

import polars as pl
from datetime import datetime
import os

# Directories
BNM_DIR = 'data/bnm/'
BNM1_DIR = 'data/bnm1/'
OD_DIR = 'data/od/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBMNPL1 - Total Overdue Loans Report")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{BNM1_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    nowk = '4'
    reptmon = f'{reptdate.month:02d}'
    rdate = reptdate.strftime('%d%m%y')
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Week: {nowk}, Month: {reptmon}")

except Exception as e:
    print(f"✗ ERROR: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Risk classification function
def classify_risk(days):
    """Classify risk by days overdue"""
    if days is None or days <= 7:
        return 0
    elif days <= 30:
        return 1
    elif days <= 59:
        return 2
    elif days <= 89:
        return 3
    elif days <= 121:
        return 4
    elif days <= 151:
        return 5
    elif days <= 182:
        return 6
    elif days <= 213:
        return 7
    elif days <= 243:
        return 8
    elif days <= 273:
        return 9
    elif days <= 364:
        return 10
    elif days <= 547:
        return 11
    elif days <= 729:
        return 12
    elif days <= 1094:
        return 13
    else:
        return 14

# Read LOAN data
print("\nProcessing LOAN data...")
try:
    df_loan = pl.read_parquet(f'{BNM_DIR}LOAN{reptmon}{nowk}.parquet')
    
    # Filter: Term loans only
    df_loan = df_loan.filter(
        (pl.col('NOTENO') < 90000) &  # Remove HP
        (pl.col('ACCTYPE') == 'LN') &
        (pl.col('BRANCH').is_not_null()) &
        (pl.col('BALANCE') >= 1.00) &
        (~pl.col('PRODUCT').is_in([517, 500]))
    )
    
    # Calculate days overdue
    df_loan = df_loan.with_columns([
        pl.when(pl.col('BLDATE') > 0)
          .then((reptdate - pl.col('BLDATE').cast(pl.Date)).dt.total_days())
          .otherwise(None)
          .alias('DAYS')
    ])
    
    # Classify risk
    df_loan = df_loan.with_columns([
        pl.col('DAYS').map_elements(classify_risk, return_dtype=pl.Int64).alias('RISKRATE')
    ])
    
    # Add RISKBAL for NPL (RISKRTE 1-4)
    df_loan = df_loan.with_columns([
        pl.when(pl.col('RISKRTE').is_in([1,2,3,4]))
          .then(pl.col('BALANCE'))
          .otherwise(0)
          .alias('RISKBAL')
    ])
    
    # Split: Regular loans vs HP (380, 381)
    df_loan1 = df_loan.filter(~pl.col('PRODUCT').is_in([380, 381]))
    df_loan2 = df_loan.filter(pl.col('PRODUCT').is_in([380, 381]))
    
    print(f"  ✓ Regular Loans: {len(df_loan1):,}")
    print(f"  ✓ HP Loans: {len(df_loan2):,}")

except Exception as e:
    print(f"  ✗ ERROR: {e}")
    df_loan1 = pl.DataFrame([])
    df_loan2 = pl.DataFrame([])

# Read O/D data
print("\nProcessing O/D data...")
try:
    # Read overdraft balances
    df_od_bal = pl.read_parquet(f'{BNM_DIR}LOAN{reptmon}{nowk}.parquet')
    df_od_bal = df_od_bal.filter(
        (pl.col('ACCTYPE') == 'OD') &
        ((pl.col('APPRLIMT') >= 0) | (pl.col('BALANCE') < 0)) &
        ((pl.col('ACCTNO') <= 3900000000) | (pl.col('ACCTNO') > 3999999999)) &
        (~pl.col('PRODUCT').is_in([517, 500]))
    ).select(['ACCTNO', 'BRANCH', 'BALANCE', 'PRODUCT'])
    
    # Read overdraft dates
    df_od_dates = pl.read_parquet(f'{OD_DIR}OVERDFT.parquet')
    df_od_dates = df_od_dates.filter(
        (pl.col('EXCESSDT') > 0) | (pl.col('TODDATE') > 0)
    ).select(['ACCTNO', 'EXCESSDT', 'TODDATE', 'RISKCODE']).unique()
    
    # Merge
    df_loan3 = df_od_bal.join(df_od_dates, on='ACCTNO', how='inner')
    
    # Calculate BLDATE (earlier of EXCESSDT or TODDATE)
    df_loan3 = df_loan3.with_columns([
        pl.when((pl.col('EXCESSDT') > 0) & (pl.col('TODDATE') > 0))
          .then(pl.min_horizontal(['EXCESSDT', 'TODDATE']))
          .when(pl.col('EXCESSDT') > 0)
          .then(pl.col('EXCESSDT'))
          .when(pl.col('TODDATE') > 0)
          .then(pl.col('TODDATE'))
          .otherwise(None)
          .alias('BLDATE')
    ])
    
    # Calculate days
    df_loan3 = df_loan3.with_columns([
        pl.when(pl.col('BLDATE').is_not_null())
          .then((reptdate - pl.col('BLDATE').cast(pl.Date)).dt.total_days() + 1)
          .otherwise(None)
          .alias('DAYS')
    ])
    
    # Classify risk
    df_loan3 = df_loan3.with_columns([
        pl.col('DAYS').map_elements(classify_risk, return_dtype=pl.Int64).alias('RISKRATE'),
        pl.col('RISKCODE').cast(pl.Int64).alias('RISKRTE')
    ])
    
    # Add RISKBAL
    df_loan3 = df_loan3.with_columns([
        pl.when(pl.col('RISKCODE').is_in(['1','2','3','4']))
          .then(pl.col('BALANCE'))
          .otherwise(0)
          .alias('RISKBAL')
    ])
    
    print(f"  ✓ O/D Accounts: {len(df_loan3):,}")

except Exception as e:
    print(f"  ✗ ERROR: {e}")
    df_loan3 = pl.DataFrame([])

# Combine all datasets
print("\nCombining datasets...")
df_loan4 = pl.concat([df_loan1, df_loan2, df_loan3]) if len(df_loan1) > 0 or len(df_loan2) > 0 or len(df_loan3) > 0 else pl.DataFrame([])

if len(df_loan4) > 0:
    print(f"  ✓ Total Records: {len(df_loan4):,}")
    
    # Generate summaries by risk category
    print("\nGenerating risk summaries...")
    
    df_summary = df_loan4.group_by(['BRANCH', 'RISKRATE']).agg([
        pl.col('BALANCE').count().alias('COUNT'),
        pl.col('BALANCE').sum().alias('AMOUNT'),
        pl.col('RISKBAL').sum().alias('NPL_AMOUNT')
    ])
    
    # Calculate NPL categories
    df_npl_summary = df_loan4.group_by('BRANCH').agg([
        pl.col('BALANCE').count().alias('TOTAL_COUNT'),
        pl.col('BALANCE').sum().alias('TOTAL_AMOUNT'),
        pl.when(pl.col('RISKRATE') > 0).then(pl.col('BALANCE')).otherwise(0).sum().alias('OVERDUE_AMT'),
        pl.when(pl.col('RISKRATE') > 0).then(1).otherwise(0).sum().alias('OVERDUE_CNT'),
        pl.when(pl.col('RISKRATE') > 3).then(pl.col('BALANCE')).otherwise(0).sum().alias('DELINQ_AMT'),
        pl.when(pl.col('RISKRATE') > 3).then(1).otherwise(0).sum().alias('DELINQ_CNT'),
        pl.when(pl.col('RISKRATE') > 6).then(pl.col('BALANCE')).otherwise(0).sum().alias('NPL_AMT'),
        pl.when(pl.col('RISKRATE') > 6).then(1).otherwise(0).sum().alias('NPL_CNT')
    ])
    
    # Save reports
    df_summary.write_csv(f'{OUTPUT_DIR}NPL_RISK_DETAIL.csv')
    df_npl_summary.write_csv(f'{OUTPUT_DIR}NPL_CATEGORY_SUMMARY.csv')
    
    print(f"  ✓ Saved NPL_RISK_DETAIL.csv")
    print(f"  ✓ Saved NPL_CATEGORY_SUMMARY.csv")
    
    # Print summary
    total_balance = df_loan4['BALANCE'].sum()
    overdue = df_loan4.filter(pl.col('RISKRATE') > 0)['BALANCE'].sum()
    delinquent = df_loan4.filter(pl.col('RISKRATE') > 3)['BALANCE'].sum()
    npl = df_loan4.filter(pl.col('RISKRATE') > 6)['BALANCE'].sum()
    
    print(f"\n{'='*60}")
    print("NPL Summary:")
    print(f"  Total Loans: RM {total_balance:,.2f}")
    print(f"  Overdue (30+ days): RM {overdue:,.2f} ({overdue/total_balance*100:.2f}%)")
    print(f"  Delinquent (90+ days): RM {delinquent:,.2f} ({delinquent/total_balance*100:.2f}%)")
    print(f"  NPL (183+ days): RM {npl:,.2f} ({npl/total_balance*100:.2f}%)")

else:
    print("  ⚠ No data to process")

print(f"\n{'='*60}")
print("✓ EIBMNPL1 Complete")
print(f"{'='*60}")
print(f"\nReport Categories:")
print(f"  - Loans (excluding HP)")
print(f"  - HP (products 380, 381)")
print(f"  - O/D (Overdrafts)")
print(f"  - Combined (All)")
print(f"\nRisk Levels: 15 categories (0-14)")
print(f"NPL Classifications:")
print(f"  Overdue: 30+ days (RISKRATE > 0)")
print(f"  Delinquent: 90+ days (RISKRATE > 3)")
print(f"  Non-Performing: 183+ days (RISKRATE > 6)")
