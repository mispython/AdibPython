"""
EIBMCOSX - Transaction Cost Analysis Report (Production Ready)

Purpose:
- Analyze transaction volumes by delivery channel
- Calculate cost per transaction for different channels
- Track deposit, loan, and BTR transactions

Transaction Categories:
1. DEP-BDS (Line 6):  Deposit - Branch Delivery System
2. LN-BDS (Line 11):  Loan - Branch Delivery System
3. LN-OTC (Line 14):  Loan - Over The Counter
4. DEP-BAT (Line 22): Deposit - Batch
5. LN-BAT (Line 25):  Loan - Batch
6. BTR-361 (Line 31): BTR Channel 361
7. BTR-SYS (Line 38): BTR System
8. CDM-515 (Line 46): Cash Deposit Machine

Channel Filters:
- SRC (Source channels): 511-513, 521-523, 526-527
- SRX (Exclusion): Various channels to exclude from batch
- BTR: 361
- CDM: 515

Loan BDS Adjustment:
- Add 5500 transactions per day to LN-BDS
- Adjustment = 5500 * REPTDAY
"""

import polars as pl
from datetime import datetime
import os

# Directories
DEP_DIR = 'data/dep/'
DEP1_DIR = 'data/dep1/'
CRD_DIR = 'data/crd/'
CRL_DIR = 'data/crl/'
BTRSA_DIR = 'data/btrsa/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("EIBMCOSX - Transaction Cost Analysis Report")
print("=" * 60)

# Read REPTDATE
try:
    df_reptdate = pl.read_parquet(f'{DEP_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    day = reptdate.day
    month = reptdate.month
    year = reptdate.year
    
    edate = f'{year % 100:02d}{month:02d}{day:02d}'
    rdate = reptdate.strftime('%d%m%y')
    reptday = f'{day:02d}'
    reptmon = f'{month:02d}'
    reptyear = f'{year % 100:02d}'
    
    # Week numbers (1-4 for all 4 weeks)
    wk1, wk2, wk3, wk4 = '01', '02', '03', '04'
    
    print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")

except Exception as e:
    print(f"  ⚠ REPTDATE: {e}")
    import sys
    sys.exit(1)

# Read start date
try:
    df_sdate = pl.read_parquet(f'{DEP1_DIR}REPTDATE.parquet')
    sdate_val = df_sdate['REPTDATE'][0]
    sdate = f'{sdate_val.year % 100:02d}{sdate_val.month:02d}{sdate_val.day:02d}'
except:
    sdate = edate

# Channel definitions
src_channels = [511, 512, 513, 521, 522, 523, 526, 527]
srx_channels = [0, 511, 512, 513, 521, 522, 523, 526, 527, 5, 251, 321, 323, 
                399, 325, 331, 333, 225, 226, 341, 342, 343, 220, 361]

print("=" * 60)

# Process deposit transactions
print("\nProcessing deposit transactions...")
dep_data = []
try:
    for wk in [wk1, wk2, wk3, wk4]:
        try:
            df = pl.read_parquet(f'{CRD_DIR}DPBTRAN{reptyear}{reptmon}{wk}.parquet')
            dep_data.append(df)
        except:
            pass
    
    if dep_data:
        df_dep = pl.concat(dep_data)
        
        # Categorize transactions
        df_dep = df_dep.with_columns([
            pl.lit(1).alias('NOACC'),
            pl.when(pl.col('CHANNEL').is_in(src_channels))
              .then(pl.lit('DEP-BDS(LINE  6)'))
              .when(~pl.col('CHANNEL').is_in(srx_channels))
              .then(pl.lit('DEP-BAT(LINE 22)'))
              .when(pl.col('CHANNEL') == 361)
              .then(pl.lit('BTR-361(LINE 31)'))
              .when(pl.col('CHANNEL') == 515)
              .then(pl.lit('CDM-515(LINE 46)'))
              .otherwise(None)
              .alias('CAT')
        ])
        
        # Summarize
        df_dep_summary = df_dep.filter(pl.col('CAT').is_not_null()).group_by('CAT').agg([
            pl.col('NOACC').sum().alias('NOACC')
        ])
        
        print(f"  ✓ Deposit transactions: {len(df_dep):,}")
    else:
        df_dep_summary = pl.DataFrame([])

except Exception as e:
    print(f"  ⚠ Deposits: {e}")
    df_dep_summary = pl.DataFrame([])

# Process loan transactions
print("\nProcessing loan transactions...")
lns_data = []
try:
    for wk in [wk1, wk2, wk3, wk4]:
        try:
            df = pl.read_parquet(f'{CRL_DIR}LNBTRAN{reptyear}{reptmon}{wk}.parquet')
            lns_data.append(df)
        except:
            pass
    
    if lns_data:
        df_lns = pl.concat(lns_data)
        
        # Categorize transactions
        lns_cats = []
        
        # LN-OTC (TRANCODE = 610)
        df_otc = df_lns.filter(pl.col('TRANCODE') == 610).with_columns([
            pl.lit(1).alias('NOACC'),
            pl.lit('LN-OTC (LINE 14)').alias('CAT')
        ])
        if len(df_otc) > 0:
            lns_cats.append(df_otc)
        
        # LN-BAT (TRANCODE = 619)
        df_bat = df_lns.filter(pl.col('TRANCODE') == 619).with_columns([
            pl.lit(1).alias('NOACC'),
            pl.lit('LN-BAT (LINE 25)').alias('CAT')
        ])
        if len(df_bat) > 0:
            lns_cats.append(df_bat)
        
        # LN-BDS (Other channels)
        df_bds = df_lns.filter(
            (pl.col('TRANCODE') != 610) & 
            (pl.col('TRANCODE') != 619) &
            (~pl.col('CHANNEL').is_in([501, 502, 503, 504, 505, 506, 510]))
        ).with_columns([
            pl.lit(1).alias('NOACC'),
            pl.lit('LN-BDS (LINE 11)').alias('CAT')
        ])
        if len(df_bds) > 0:
            lns_cats.append(df_bds)
        
        # Combine
        if lns_cats:
            df_lns_cat = pl.concat(lns_cats)
            df_lns_summary = df_lns_cat.group_by('CAT').agg([
                pl.col('NOACC').sum().alias('NOACC')
            ])
            
            # Add adjustment to LN-BDS (5500 per day)
            df_lns_summary = df_lns_summary.with_columns([
                pl.when(pl.col('CAT').str.starts_with('LN-BDS'))
                  .then(pl.col('NOACC') + (5500 * day))
                  .otherwise(pl.col('NOACC'))
                  .alias('NOACC')
            ])
            
            print(f"  ✓ Loan transactions: {len(df_lns):,}")
        else:
            df_lns_summary = pl.DataFrame([])
    else:
        df_lns_summary = pl.DataFrame([])

except Exception as e:
    print(f"  ⚠ Loans: {e}")
    df_lns_summary = pl.DataFrame([])

# Process BTR system transactions
print("\nProcessing BTR system transactions...")
try:
    df_btr = pl.read_parquet(f'{BTRSA_DIR}COMM{reptday}{reptmon}.parquet')
    
    # Filter conditions
    df_btr = df_btr.with_columns([
        pl.when(pl.col('CREATDS').is_not_null() & (pl.col('CREATDS') != 0))
          .then(pl.col('CREATDS'))
          .otherwise(None)
          .alias('ISSDTE')
    ])
    
    # Excluded GL codes
    excluded_gl = ['0062', '0056', '0053', '0064', '0066', '0012', '0014', 
                   '0016', '0059', '0047', '0049', '0044', '0042', '0034']
    
    df_btr = df_btr.filter(
        pl.col('GLMNEMO').str.slice(0, 2) == '00' &
        (pl.col('ISSDTE').is_between(int(sdate), int(edate))) &
        (~pl.col('GLMNEMO').is_in(excluded_gl))
    ).with_columns([
        pl.lit(1).alias('NOACC'),
        pl.lit('BTR-SYS(LINE 38)').alias('CAT')
    ])
    
    df_btr_summary = df_btr.group_by('CAT').agg([
        pl.col('NOACC').sum().alias('NOACC')
    ])
    
    print(f"  ✓ BTR transactions: {len(df_btr):,}")

except Exception as e:
    print(f"  ⚠ BTR: {e}")
    df_btr_summary = pl.DataFrame([])

# Combine all summaries
print("\nCombining transaction summaries...")
summaries = [df for df in [df_dep_summary, df_lns_summary, df_btr_summary] if len(df) > 0]

if summaries:
    df_ooo = pl.concat(summaries).sort('CAT')
    
    # Generate report
    print(f"\nTransaction Cost Analysis:")
    print(f"{'='*60}")
    print(f"{'Category':<25} {'Transactions':>15}")
    print(f"{'-'*60}")
    
    for row in df_ooo.iter_rows(named=True):
        print(f"{row['CAT']:<25} {row['NOACC']:>15,}")
    
    # Write to CSV
    df_ooo.write_csv(f'{OUTPUT_DIR}COSTX_{reptmon}{reptyear}.csv')
    
    # Write formatted text report
    with open(f'{OUTPUT_DIR}COSTX_{reptmon}{reptyear}.txt', 'w') as f:
        f.write(f"---COST PER TRX--- {rdate}\n")
        for row in df_ooo.iter_rows(named=True):
            f.write(f"{row['CAT']:<15} {row['NOACC']:>12,}\n")

else:
    print(f"  ⚠ No transaction data found")

print(f"\n{'='*60}")
print(f"✓ EIBMCOSX Complete!")
print(f"{'='*60}")
print(f"\nTransaction Categories:")
print(f"  DEP-BDS (Line 6):  Deposit - Branch Delivery System")
print(f"  LN-BDS (Line 11):  Loan - Branch Delivery (+ 5500/day)")
print(f"  LN-OTC (Line 14):  Loan - Over The Counter")
print(f"  DEP-BAT (Line 22): Deposit - Batch")
print(f"  LN-BAT (Line 25):  Loan - Batch")
print(f"  BTR-361 (Line 31): BTR Channel 361")
print(f"  BTR-SYS (Line 38): BTR System")
print(f"  CDM-515 (Line 46): Cash Deposit Machine")
