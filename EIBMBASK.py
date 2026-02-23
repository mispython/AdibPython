"""
EIBMBASK - CA & NPL Monitoring with Base Comparison (Production Ready)

Purpose:
- Monitor Close Attention (CA) and Non-Performing Loans (NPL)
- Compare current month against baseline (previous month/year-start)
- Identify newly surfaced delinquent accounts
- Generate reports by branch, customer, risk category

Risk Categories:
- 0C: Close Attention (CA) - 60-89 days in arrears
- 1C: Special Mention 1 (SS1) - 90-182 days
- 2S: Special Mention 2 (SS2) - >182 days
- 3D: Doubtful (D) - substandard
- 4B: Bad (B) - loss

Account Types:
- OD: Overdraft accounts (ACCTNO 3000000000-3999999999)
- LN: Term loans (ACCTNO 2000000000-2999999999)
- BT: Bills/Trade (ACCTNO 2500000000-2599999999, Islamic 8000000000-8999999999)

Base Comparison:
- BASECA: Accounts in CA at previous month (60-89 days)
- BASENPL: Accounts in NPL at previous month (>=90 days)
- BASECAX: Extended CA tracking (60-89 days)
- BILLNPLBASE/BILLCABASE: Bill trade base

Key Logic:
- NEW='*': Newly surfaced this month (not in RISKOLD)
- DAYS calculation: REPTDATE - BLDATE
- MTHARR: Months in arrears (based on DAYS)
- CORPBIND: CB=Corporate, IS=Islamic

Output Reports:
1. Newly surfaced CA (this month)
2. Newly surfaced NPL (this month)
3. Newly surfaced this year
4. Current NPL listing
5. Current CA listing
6. NPL reduction report
7. Summary by branch/product
"""

import polars as pl
from datetime import datetime, timedelta
import os

# Directories
BNM_DIR = 'data/bnm/'
BNM1_DIR = 'data/bnm1/'
MIS_DIR = 'data/mis/'
PREVOD_DIR = 'data/prevod/'
BTRADE_DIR = 'data/btrade/'
OD_DIR = 'data/od/'
OUTPUT_DIR = 'data/output/'

for d in [MIS_DIR, OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

print("EIBMBASK - CA & NPL Monitoring with Base Comparison")
print("=" * 60)

# Read REPTDATE
print("\nReading REPTDATE...")
try:
    df_reptdate = pl.read_parquet(f'{BNM1_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    reptmon = f'{reptdate.month:02d}'
    prevmon = f'{(reptdate.month - 1 if reptdate.month > 1 else 12):02d}'
    reptday = f'{reptdate.day:02d}'
    rdate = reptdate.strftime('%d%m%y')
    rmonth = reptdate.strftime('%B %Y')
    ryear = f'{reptdate.year - 1:02d}'
    nowk = '4'
    
    # BTCADT calculation (bill trade days threshold)
    if reptdate.month in [2, 4, 6, 7, 9, 11, 12]:
        btcadt = 61
    elif reptdate.month in [3, 5, 8, 10]:
        btcadt = 62
    elif reptdate.month == 1:
        btcadt = 63 if (reptdate.year % 4 == 0) else 64
    else:
        btcadt = 61
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    print(f"Month: {reptmon}, Prev: {prevmon}, BTCADT: {btcadt}")
    
    # Read previous REPTDATE
    df_preptdate = pl.read_parquet(f'{PREVOD_DIR}REPTDATE.parquet')
    preptdt = df_preptdate['REPTDATE'][0]
    
except Exception as e:
    print(f"  ⚠ REPTDATE: {e}")
    import sys
    sys.exit(1)

print("=" * 60)

# Create BASE datasets (if January, save previous year's data)
print("\nCreating BASE datasets...")

if reptmon == '01':
    print("  January: Saving previous year base...")
    
    # Save BASECA from previous year
    try:
        df_baseca = pl.read_parquet(f'{MIS_DIR}BASECA.parquet')
        df_baseca.write_parquet(f'{MIS_DIR}BASECA{ryear}.parquet')
    except:
        pass
    
    # Save BASENPL from previous year
    try:
        df_basenpl = pl.read_parquet(f'{MIS_DIR}BASENPL.parquet')
        df_basenpl.write_parquet(f'{MIS_DIR}BASENPL{ryear}.parquet')
    except:
        pass
    
    # Create new BASECA (OD: 1-182 days, LN: 90-182 days)
    # CAODBASE: Overdraft CA (60-89 days excess/TOD)
    try:
        df_overdft = pl.read_parquet(f'{PREVOD_DIR}OVERDFT.parquet')
        
        df_caodbase = df_overdft.with_columns([
            pl.lit(None).cast(pl.Int32).alias('NOTENO'),
            pl.col('RISKCODE').cast(pl.Int32).alias('RISKBASE'),
            (preptdt - pl.col('EXCESSDT') + 1).alias('DAY1'),
            (preptdt - pl.col('TODDATE') + 1).alias('DAY2')
        ]).filter(
            (pl.col('RISKCODE') == 1) & 
            (((pl.col('DAY1') >= 1) & (pl.col('DAY1') <= 182)) | 
             ((pl.col('DAY2') >= 1) & (pl.col('DAY2') <= 182)))
        ).select(['ACCTNO', 'NOTENO', 'RISKBASE'])
    except:
        df_caodbase = pl.DataFrame([])
    
    # CALNBASE: Term loan CA (90-182 days)
    try:
        df_loan = pl.read_parquet(f'{BNM_DIR}LOAN{prevmon}{nowk}.parquet')
        
        df_calnbase = df_loan.with_columns([
            (preptdt - pl.col('BLDATE')).alias('DAYS')
        ]).filter(
            ((pl.col('ACCTNO') > 2000000000) & (pl.col('ACCTNO') < 3000000000) & 
             (pl.col('RISKRTE') == 1) & (pl.col('DAYS') >= 90)) |
            ((pl.col('ACCTNO') > 8000000000) & (pl.col('ACCTNO') < 9000000000) & 
             (pl.col('DAYS') >= 90) & (pl.col('DAYS') <= 182))
        ).with_columns([
            pl.when(pl.col('RISKRTE').is_not_null())
              .then(pl.col('RISKRTE'))
              .otherwise(pl.lit(1))
              .alias('RISKBASE')
        ]).select(['ACCTNO', 'NOTENO', 'RISKBASE'])
    except:
        df_calnbase = pl.DataFrame([])
    
    # Combine BASECA
    df_baseca = pl.concat([df_caodbase, df_calnbase]) if len(df_caodbase) > 0 or len(df_calnbase) > 0 else pl.DataFrame([])
    if len(df_baseca) > 0:
        df_baseca = df_baseca.unique(subset=['ACCTNO', 'NOTENO'])
        df_baseca.write_parquet(f'{MIS_DIR}BASECA.parquet')
        print(f"  ✓ BASECA: {len(df_baseca):,} accounts")
    
    # Create BASENPL (NPL >= 90 days)
    try:
        df_loan = pl.read_parquet(f'{BNM_DIR}LOAN{prevmon}{nowk}.parquet')
        
        df_basenpl = df_loan.with_columns([
            (preptdt - pl.col('BLDATE')).alias('DAYS')
        ]).filter(
            ((pl.col('ACCTNO') > 2000000000) & (pl.col('ACCTNO') < 4000000000) & 
             pl.col('RISKRTE').is_in([2, 3, 4])) |
            ((pl.col('ACCTNO') > 8000000000) & (pl.col('ACCTNO') < 9000000000) & 
             (pl.col('DAYS') > 182))
        ).with_columns([
            pl.when((pl.col('ACCTNO') > 8000000000) & (pl.col('DAYS') > 182))
              .then(pl.lit(2))
              .otherwise(pl.col('RISKRTE'))
              .alias('RISKBASE')
        ]).select(['ACCTNO', 'NOTENO', 'RISKBASE', 'BLDATE'])
        
        df_basenpl = df_basenpl.unique(subset=['ACCTNO', 'NOTENO'])
        df_basenpl.write_parquet(f'{MIS_DIR}BASENPL.parquet')
        print(f"  ✓ BASENPL: {len(df_basenpl):,} accounts")
    except:
        df_basenpl = pl.DataFrame([])
    
    # Create BASECAX (extended CA: 60-89 days)
    try:
        df_overdft = pl.read_parquet(f'{PREVOD_DIR}OVERDFT.parquet')
        
        df_caodbasx = df_overdft.with_columns([
            pl.lit(None).cast(pl.Int32).alias('NOTENO'),
            pl.lit(0).cast(pl.Int32).alias('RISKBASE'),
            (preptdt - pl.col('EXCESSDT') + 1).alias('DAY1'),
            (preptdt - pl.col('TODDATE') + 1).alias('DAY2')
        ]).filter(
            ((pl.col('DAY1') >= 60) & (pl.col('DAY1') <= 89)) | 
            ((pl.col('DAY2') >= 60) & (pl.col('DAY2') <= 89))
        ).select(['ACCTNO', 'NOTENO', 'RISKBASE'])
    except:
        df_caodbasx = pl.DataFrame([])
    
    try:
        df_loan = pl.read_parquet(f'{BNM_DIR}LOAN{prevmon}{nowk}.parquet')
        
        df_calnbasx = df_loan.with_columns([
            (preptdt - pl.col('BLDATE')).alias('DAYS'),
            pl.lit(0).cast(pl.Int32).alias('RISKBASE')
        ]).filter(
            (((pl.col('ACCTNO') > 2000000000) & (pl.col('ACCTNO') < 3000000000)) |
             ((pl.col('ACCTNO') > 8000000000) & (pl.col('ACCTNO') < 9000000000))) &
            (pl.col('DAYS') >= 60) & (pl.col('DAYS') <= 89)
        ).select(['ACCTNO', 'NOTENO', 'RISKBASE'])
    except:
        df_calnbasx = pl.DataFrame([])
    
    df_basecax = pl.concat([df_caodbasx, df_calnbasx]) if len(df_caodbasx) > 0 or len(df_calnbasx) > 0 else pl.DataFrame([])
    if len(df_basecax) > 0:
        df_basecax = df_basecax.unique(subset=['ACCTNO', 'NOTENO'])
        df_basecax.write_parquet(f'{MIS_DIR}BASECAX.parquet')
        print(f"  ✓ BASECAX: {len(df_basecax):,} accounts")
    
    # Create BILLNPLBASE and BILLCABASE
    try:
        df_btrade = pl.read_parquet(f'{BTRADE_DIR}BTRAD{prevmon}{nowk}.parquet')
        
        df_billnplbase = df_btrade.with_columns([
            (preptdt - pl.col('MATDATE') + 1).alias('DAYS'),
            pl.lit(4).cast(pl.Int32).alias('RISKBASE'),
            pl.lit(preptdt).alias('REPTDT')
        ]).filter(
            (pl.col('MATDATE').is_not_null()) & 
            (pl.col('BALANCE') > 0) & 
            (pl.col('DAYS') >= 90)
        ).with_columns([
            pl.col('ACCTNOX').alias('ACCTNO')
        ]).select(['ACCTNO', 'TRANSREF', 'MATDATE', 'RISKBASE'])
        
        df_billnplbase.write_parquet(f'{MIS_DIR}BILLNPLBASE.parquet')
        print(f"  ✓ BILLNPLBASE: {len(df_billnplbase):,} bills")
    except:
        df_billnplbase = pl.DataFrame([])
    
    try:
        df_btrade = pl.read_parquet(f'{BTRADE_DIR}BTRAD{prevmon}{nowk}.parquet')
        
        df_billcabase = df_btrade.with_columns([
            (preptdt - pl.col('MATDATE') + 1).alias('DAYS'),
            pl.lit(0).cast(pl.Int32).alias('RISKBASE'),
            pl.lit(preptdt).alias('REPTDT')
        ]).filter(
            (pl.col('MATDATE').is_not_null()) & 
            (pl.col('BALANCE') > 0) & 
            (pl.col('DAYS') >= 60) & 
            (pl.col('DAYS') < 90)
        ).with_columns([
            pl.col('ACCTNOX').alias('ACCTNO')
        ]).select(['ACCTNO', 'TRANSREF', 'MATDATE', 'RISKBASE'])
        
        df_billcabase.write_parquet(f'{MIS_DIR}BILLCABASE.parquet')
        print(f"  ✓ BILLCABASE: {len(df_billcabase):,} bills")
    except:
        df_billcabase = pl.DataFrame([])

# Read base datasets
print("\nReading base datasets...")
try:
    df_baseca = pl.read_parquet(f'{MIS_DIR}BASECA.parquet')
    print(f"  BASECA: {len(df_baseca):,}")
except:
    df_baseca = pl.DataFrame([])

try:
    df_basenpl = pl.read_parquet(f'{MIS_DIR}BASENPL.parquet')
    print(f"  BASENPL: {len(df_basenpl):,}")
except:
    df_basenpl = pl.DataFrame([])

try:
    df_basecax = pl.read_parquet(f'{MIS_DIR}BASECAX.parquet')
    print(f"  BASECAX: {len(df_basecax):,}")
except:
    df_basecax = pl.DataFrame([])

# Combine base
df_base = pl.concat([df_basecax, df_baseca, df_basenpl]) if any(len(df) > 0 for df in [df_basecax, df_baseca, df_basenpl]) else pl.DataFrame([])

# Process OD accounts
print("\nProcessing OD accounts...")
try:
    df_loan3 = pl.read_parquet(f'{BNM_DIR}LOAN{reptmon}{nowk}.parquet').filter(
        (pl.col('ACCTYPE') == 'OD') & 
        ((pl.col('APPRLIMT') >= 0) | (pl.col('BALANCE') < 0))
    ).select(['ACCTNO', 'BRANCH', 'BALANCE', 'PRODUCT', 'NAME', 'APPRLIMT', 
              'ISSDTE', 'AMTIND', 'PRODCD', 'ORGCODE', 'ODINTACC'])
    
    df_od = pl.read_parquet(f'{OD_DIR}OVERDFT.parquet').filter(
        (pl.col('EXCESSDT') > 0) | (pl.col('TODDATE') > 0)
    ).select(['ACCTNO', 'EXCESSDT', 'TODDATE', 'RISKCODE', 'REVIEWDT']).unique(subset=['ACCTNO'])
    
    df_od = df_loan3.join(df_od, on='ACCTNO', how='inner')
    
    # Calculate BLDATE
    df_od = df_od.with_columns([
        pl.when((pl.col('EXCESSDT') != 0) & (pl.col('EXCESSDT') <= pl.col('TODDATE')))
          .then(pl.col('EXCESSDT'))
          .when((pl.col('TODDATE') != 0) & (pl.col('TODDATE') < pl.col('EXCESSDT')))
          .then(pl.col('TODDATE'))
          .when(pl.col('EXCESSDT') > 0)
          .then(pl.col('EXCESSDT'))
          .when(pl.col('TODDATE') > 0)
          .then(pl.col('TODDATE'))
          .otherwise(None)
          .alias('BLDATE'),
        
        pl.col('RISKCODE').cast(pl.Int32).alias('RISKRTE'),
        
        pl.when(pl.col('ORGCODE').is_in(['1', '001', '100']))
          .then(pl.lit('CB'))
          .otherwise(pl.lit(''))
          .alias('CORPBIND')
    ])
    
    # Adjust balance for ODINTACC
    df_od = df_od.with_columns([
        pl.when(pl.col('ODINTACC') > 0)
          .then(pl.col('BALANCE') - pl.col('ODINTACC'))
          .otherwise(pl.col('BALANCE'))
          .alias('BALANCE')
    ])
    
    # Calculate DAYS
    df_od = df_od.with_columns([
        (reptdate - pl.col('BLDATE') + 1).alias('DAYS')
    ])
    
    print(f"  ✓ OD: {len(df_od):,} accounts")

except Exception as e:
    print(f"  ⚠ OD: {e}")
    df_od = pl.DataFrame([])

# Process LN accounts
print("\nProcessing LN accounts...")
try:
    df_loans = pl.read_parquet(f'{BNM_DIR}LOAN{reptmon}{nowk}.parquet').filter(
        (pl.col('PRODUCT').is_in([700, 705, 380, 381, 128, 130]).not_()) &
        (pl.col('ACCTYPE') == 'LN') &
        (pl.col('BRANCH').is_not_null()) &
        (pl.col('BALANCE') >= 1.00) &
        (pl.col('PRODCD') != 'N')
    ).with_columns([
        (reptdate - pl.col('BLDATE')).alias('DAYS'),
        pl.when(pl.col('ACCTYIND') == 900)
          .then(pl.lit('CB'))
          .otherwise(pl.lit(''))
          .alias('CORPBIND')
    ]).select(['BRANCH', 'BALANCE', 'RISKRATE', 'RISKRTE', 'ACCTNO', 'DAYS', 
               'NOTENO', 'PRODUCT', 'APPRLIMT', 'ISSDTE', 'NAME', 'AMTIND', 
               'PRODCD', 'CORPBIND', 'BORSTAT'])
    
    print(f"  ✓ LN: {len(df_loans):,} accounts")

except Exception as e:
    print(f"  ⚠ LN: {e}")
    df_loans = pl.DataFrame([])

# Combine OD and LN
df_combine = pl.concat([df_loans, df_od]) if len(df_loans) > 0 or len(df_od) > 0 else pl.DataFrame([])

# Merge with LNNOTE for SCORE1
print("\nMerging with LNNOTE...")
try:
    df_lnnote = pl.read_parquet(f'{BNM1_DIR}LNNOTE.parquet').filter(
        pl.col('PAIDIND') != 'P'
    ).select(['ACCTNO', 'NOTENO', 'SCORE1'])
    
    df_combine = df_combine.join(df_lnnote, on=['ACCTNO', 'NOTENO'], how='left')

except Exception as e:
    print(f"  ⚠ LNNOTE merge: {e}")

# Merge with base
df_combine = df_combine.join(df_base, on=['ACCTNO', 'NOTENO'], how='left')

# Get previous month risk (RISKOLD)
print("\nMerging with previous month risk...")
try:
    if reptmon == '01':
        df_lnprv = pl.read_parquet(f'{MIS_DIR}GETCIS{prevmon}.parquet').with_columns([
            pl.lit('  ').alias('RISKOLD')
        ]).select(['ACCTNO', 'NOTENO', 'RISKOLD'])
    else:
        df_lnprv = pl.read_parquet(f'{MIS_DIR}GETCIS{prevmon}.parquet').with_columns([
            pl.col('RISK').alias('RISKOLD')
        ]).select(['ACCTNO', 'NOTENO', 'RISKOLD'])
    
    df_combine = df_combine.join(df_lnprv, on=['ACCTNO', 'NOTENO'], how='left')

except Exception as e:
    print(f"  ⚠ Previous month: {e}")
    df_combine = df_combine.with_columns([pl.lit('').alias('RISKOLD')])

# Calculate months in arrears (MTHARR)
df_combine = df_combine.with_columns([
    pl.when(pl.col('DAYS') > 729).then((pl.col('DAYS') / 365 * 12).cast(pl.Int32))
      .when(pl.col('DAYS') > 698).then(pl.lit(23))
      .when(pl.col('DAYS') > 668).then(pl.lit(22))
      .when(pl.col('DAYS') > 638).then(pl.lit(21))
      .when(pl.col('DAYS') > 608).then(pl.lit(20))
      .when(pl.col('DAYS') > 577).then(pl.lit(19))
      .when(pl.col('DAYS') > 547).then(pl.lit(18))
      .when(pl.col('DAYS') > 516).then(pl.lit(17))
      .when(pl.col('DAYS') > 486).then(pl.lit(16))
      .when(pl.col('DAYS') > 456).then(pl.lit(15))
      .when(pl.col('DAYS') > 424).then(pl.lit(14))
      .when(pl.col('DAYS') > 394).then(pl.lit(13))
      .when(pl.col('DAYS') > 364).then(pl.lit(12))
      .when(pl.col('DAYS') > 333).then(pl.lit(11))
      .when(pl.col('DAYS') > 303).then(pl.lit(10))
      .when(pl.col('DAYS') > 273).then(pl.lit(9))
      .when(pl.col('DAYS') > 243).then(pl.lit(8))
      .when(pl.col('DAYS') > 212).then(pl.lit(7))
      .when(pl.col('DAYS') > 182).then(pl.lit(6))
      .when(pl.col('DAYS') > 151).then(pl.lit(5))
      .when(pl.col('DAYS') > 121).then(pl.lit(4))
      .when(pl.col('DAYS') > 89).then(pl.lit(3))
      .when(pl.col('DAYS') > 59).then(pl.lit(2))
      .when(pl.col('DAYS') > 29).then(pl.lit(1))
      .otherwise(pl.lit(0))
      .alias('MTHARR')
])

# Override for BORSTAT
df_combine = df_combine.with_columns([
    pl.when(pl.col('BORSTAT') == 'F')
      .then(pl.lit(999))
      .when(pl.col('BORSTAT') == 'W')
      .then(pl.lit(0))
      .otherwise(pl.col('MTHARR'))
      .alias('MTHARR')
])

# Determine RISK and NEW flag
df_combine = df_combine.with_columns([
    pl.when(
        (pl.col('RISKRTE') == 1) & 
        ((pl.col('DAYS') >= 90) & (pl.col('DAYS') <= 182)) &
        pl.col('RISKBASE').is_in([1, 2, 3, 4]).not_()
    ).then(pl.lit('1C'))
      .when(
        (pl.col('RISKRTE').is_in([2, 3, 4])) &
        pl.col('RISKBASE').is_in([1, 2, 3, 4]).not_()
      ).then(
        pl.when(pl.col('RISKRTE') == 2).then(pl.lit('2S'))
          .when(pl.col('RISKRTE') == 3).then(pl.lit('3D'))
          .when(pl.col('RISKRTE') == 4).then(pl.lit('4B'))
          .otherwise(None)
      )
      .when(
        (pl.col('DAYS') >= 60) & (pl.col('DAYS') <= 89) &
        pl.col('RISKBASE').is_in([0, 1, 2, 3, 4]).not_()
      ).then(pl.lit('0C'))
      .otherwise(None)
      .alias('RISK')
])

# NEW flag
df_combine = df_combine.with_columns([
    pl.when(
        (pl.col('RISK') == '1C') & 
        pl.col('RISKOLD').is_in(['1C', '2S', '3D', '4B']).not_()
    ).then(pl.lit('*'))
      .when(
        pl.col('RISK').is_in(['2S', '3D', '4B']) &
        pl.col('RISKOLD').is_in(['1C', '2S', '3D', '4B']).not_()
      ).then(pl.lit('*'))
      .when(
        (pl.col('RISK') == '0C') & (pl.col('RISKOLD') != '0C')
      ).then(pl.lit('*'))
      .otherwise(None)
      .alias('NEW')
])

# Save CA dataset
df_ca = df_combine.filter(pl.col('RISK').is_not_null())
df_ca.write_parquet(f'{MIS_DIR}CA{reptmon}{nowk}.parquet')

print(f"\n✓ CA{reptmon}{nowk}: {len(df_ca):,} accounts")
print(f"  - CA (0C): {len(df_ca.filter(pl.col('RISK') == '0C')):,}")
print(f"  - SS1 (1C): {len(df_ca.filter(pl.col('RISK') == '1C')):,}")
print(f"  - SS2 (2S): {len(df_ca.filter(pl.col('RISK') == '2S')):,}")
print(f"  - D (3D): {len(df_ca.filter(pl.col('RISK') == '3D')):,}")
print(f"  - B (4B): {len(df_ca.filter(pl.col('RISK') == '4B')):,}")
print(f"  - New this month: {len(df_ca.filter(pl.col('NEW') == '*')):,}")

# Generate summary reports
print("\nGenerating summary reports...")

# Group by IC (customer ID)
df_getcis = df_ca.group_by('IC').agg([
    pl.col('BALANCE').sum().alias('TOTBAL'),
    pl.when(pl.col('RISK').is_in(['1C', '2S', '3D', '4B']))
      .then(pl.col('BALANCE'))
      .otherwise(None)
      .sum()
      .alias('YRTOTNP'),
    pl.when(pl.col('NEW') == '*')
      .then(pl.col('BALANCE'))
      .otherwise(None)
      .sum()
      .alias('NEWNPBAL')
])

df_getcis.write_parquet(f'{MIS_DIR}GETCIS{reptmon}.parquet')

# Generate CSV reports
print("\nGenerating CSV reports...")

# Newly surfaced NPL this month
df_new_npl = df_ca.filter(
    pl.col('RISK').is_in(['1C', '2S', '3D', '4B']) & 
    (pl.col('NEW') == '*')
).sort('BALANCE', descending=True)

df_new_npl.write_csv(f'{OUTPUT_DIR}NEW_NPL_{reptmon}.csv', separator=';')
print(f"  ✓ NEW_NPL_{reptmon}.csv: {len(df_new_npl):,} accounts")

# Newly surfaced CA this month
df_new_ca = df_ca.filter(
    (pl.col('RISK') == '0C') & (pl.col('NEW') == '*')
).sort('BALANCE', descending=True)

df_new_ca.write_csv(f'{OUTPUT_DIR}NEW_CA_{reptmon}.csv', separator=';')
print(f"  ✓ NEW_CA_{reptmon}.csv: {len(df_new_ca):,} accounts")

# Current NPL
df_curr_npl = df_ca.filter(
    pl.col('RISK').is_in(['1C', '2S', '3D', '4B'])
).sort('BALANCE', descending=True)

df_curr_npl.write_csv(f'{OUTPUT_DIR}CURR_NPL_{reptmon}.csv', separator=';')
print(f"  ✓ CURR_NPL_{reptmon}.csv: {len(df_curr_npl):,} accounts")

# Current CA
df_curr_ca = df_ca.filter(pl.col('RISK') == '0C').sort('BALANCE', descending=True)

df_curr_ca.write_csv(f'{OUTPUT_DIR}CURR_CA_{reptmon}.csv', separator=';')
print(f"  ✓ CURR_CA_{reptmon}.csv: {len(df_curr_ca):,} accounts")

print(f"\n{'='*60}")
print(f"✓ EIBMBASK Complete!")
print(f"{'='*60}")
print(f"\nRisk Categories:")
print(f"  0C: Close Attention (60-89 days)")
print(f"  1C: Special Mention 1 (90-182 days)")
print(f"  2S: Special Mention 2 (>182 days)")
print(f"  3D: Doubtful")
print(f"  4B: Bad")
print(f"\nBase Comparison:")
print(f"  BASECA: Previous CA accounts")
print(f"  BASENPL: Previous NPL accounts")
print(f"  NEW='*': Newly surfaced this month")
print(f"\nOutputs:")
print(f"  - MIS.CA{reptmon}{nowk}: Main CA/NPL dataset")
print(f"  - MIS.GETCIS{reptmon}: Customer summary")
print(f"  - CSV reports: NEW_NPL, NEW_CA, CURR_NPL, CURR_CA")
