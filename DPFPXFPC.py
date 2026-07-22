"""
EIMRESHI - HP/Hire Purchase Loan Summary & Detail Report
Optimized with sampling for large SAS files

This replicates the SAS program EIMRESHI which generates:
- Summary reports for HP loans (Conv & Aitab) by various groupings
- Track NPL accounts (>=3 months in arrears or F/I/R status)
- Monitor restructured accounts (NOTENO >= 98010)
- Detail report for NPL accounts

HP Products: 128, 130, 380, 381, 700, 705
"""

import polars as pl
import pyreadstat
from datetime import datetime, timedelta
import os
import sys
import gc
import random

# Directories
LOAN_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMRESHI/'
CCDTEMP_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMRESHI/'
OUTPUT_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMRESHI/'
PARQUET_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMRESHI/parquet/'

for d in [OUTPUT_DIR, PARQUET_DIR]:
    os.makedirs(d, exist_ok=True)

print("EIMRESHI - HP Loan Summary & Detail Report (OPTIMIZED)")
print("=" * 60)

# HP Products (from SAS macro &HPD)
HP_PRODUCTS = [128, 130, 380, 381, 700, 705]

# Configuration for sampling
SAMPLE_SIZE = 50000  # Number of rows to sample from LNNOTE
USE_SAMPLING = True  # Set to False to read full file

# Get report date (yesterday)
reptdate = datetime.now() - timedelta(days=1)
reptdate = reptdate.replace(hour=0, minute=0, second=0, microsecond=0)

# Calculate week and other date variables (matches SAS logic)
day = reptdate.day
if day == 8:
    sdd, wk, wk1 = 1, '1', '4'
elif day == 15:
    sdd, wk, wk1 = 9, '2', '1'
elif day == 22:
    sdd, wk, wk1 = 16, '3', '2'
else:
    sdd, wk, wk1 = 23, '4', '3'

mm = reptdate.month
if wk == '1':
    mm1 = mm - 1
    if mm1 == 0:
        mm1 = 12
else:
    mm1 = mm

reptmon = f'{mm:02d}'
reptmon1 = f'{mm1:02d}'
reptyear = reptdate.year
reptday = f'{day:02d}'
rdate = reptdate.strftime('%d%m%y')

print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Week: {wk}")
print(f"RDATE: {rdate}")
print(f"Sample Size: {SAMPLE_SIZE:,} rows")
print("=" * 60)

# Make of vehicle mapping (matches SAS SELECT statement)
MAKE_MAP = {
    ' 1': 'PROTON', ' 2': 'PERODUA', ' 3': 'TOYOTA', ' 4': 'NISSAN',
    ' 5': 'HONDA', ' 6': 'ISUZU', ' 7': 'DAIHATSU', ' 8': 'MITSUBISHI',
    ' 9': 'FORD', '10': 'MERCEDES BENZ', '11': 'VOLVO', '13': 'BMW'
}

# State mapping (matches SAS SELECT statement)
STATE_MAP = {
    '1': 'JOHORE', '2': 'KEDAH', '3': 'KELANTAN', '4': 'MALACCA',
    '5': 'N.SEMBILAN', '6': 'PAHANG', '7': 'PENANG', '8': 'PERAK',
    '9': 'PERLIS', '10': 'SABAH', '11': 'SARAWAK', '12': 'SELANGOR',
    '13': 'TRENGGANU', '14': 'W.PERSEKUTUAN', '15': 'LABUAN'
}

def read_sas_with_sampling(sas_path, columns=None, sample_size=None, row_limit=None, use_random_sample=False):
    """
    Read SAS file with various sampling strategies
    """
    try:
        if use_random_sample and sample_size:
            # Strategy 1: Random sampling - read full file and sample
            print(f"    Reading full SAS file for random sampling...")
            df, meta = pyreadstat.read_sas7bdat(sas_path, columns=columns)
            df_pl = pl.from_pandas(df)
            
            if len(df_pl) > sample_size:
                # Random sample
                indices = random.sample(range(len(df_pl)), sample_size)
                df_pl = df_pl[indices]
                print(f"    Randomly sampled {sample_size:,} rows from {len(df_pl):,}")
            return df_pl
        
        elif row_limit:
            # Strategy 2: Sequential read with row limit
            print(f"    Reading first {row_limit:,} rows...")
            df, meta = pyreadstat.read_sas7bdat(sas_path, columns=columns, row_limit=row_limit)
            return pl.from_pandas(df)
        
        else:
            # Strategy 3: Read full file
            print(f"    Reading full file...")
            df, meta = pyreadstat.read_sas7bdat(sas_path, columns=columns)
            return pl.from_pandas(df)
            
    except Exception as e:
        print(f"    Error reading {sas_path}: {e}")
        return None

# STEP 1: Read LOANTEMP (this is usually smaller)
print("\nReading LOANTEMP data...")

loantemp_parquet = f'{PARQUET_DIR}loantemp.parquet'

# Try to read from Parquet first
if os.path.exists(loantemp_parquet):
    print("  Reading loantemp from Parquet...")
    df_loantemp = pl.read_parquet(loantemp_parquet)
    print(f"  LOANTEMP loaded: {len(df_loantemp):,} rows")
else:
    print("  Reading loantemp.sas7bdat...")
    df_loantemp, meta = pyreadstat.read_sas7bdat(
        f'{CCDTEMP_DIR}loantemp.sas7bdat'
    )
    df_loantemp = pl.from_pandas(df_loantemp)
    
    # Filter for HP products
    df_loantemp = df_loantemp.filter(
        (pl.col('PRODUCT').is_in(HP_PRODUCTS)) & 
        (pl.col('BALANCE') > 0)
    )
    
    print(f"  LOANTEMP after filtering: {len(df_loantemp):,} rows")
    
    # Save to Parquet for future use
    df_loantemp.write_parquet(loantemp_parquet)
    print(f"  Saved to Parquet: {loantemp_parquet}")

if len(df_loantemp) == 0:
    print("  ERROR: No HP products found in LOANTEMP")
    sys.exit(1)

# Get unique account numbers from LOANTEMP
sampled_accts = df_loantemp['ACCTNO'].unique().to_list()
print(f"  Unique accounts in LOANTEMP: {len(sampled_accts):,}")

# STEP 2: Read LNNOTE with sampling
print("\nReading LNNOTE data (using optimized sampling)...")

lnnote_parquet = f'{PARQUET_DIR}lnnote_sample_{SAMPLE_SIZE}.parquet'

# Try to read from Parquet first
if os.path.exists(lnnote_parquet):
    print(f"  Reading lnnote sample from Parquet...")
    df_lnnote = pl.read_parquet(lnnote_parquet)
    print(f"  LNNOTE loaded: {len(df_lnnote):,} rows")
else:
    # Define columns needed (matches SAS KEEP)
    keep_cols = ['ACCTNO', 'NOTENO', 'LOANTYPE', 'NETPROC', 'APPVALUE', 
                 'NOTETERM', 'STATE', 'DEALERNO', 'SCORE2', 'ORGBAL',
                 'CURBAL', 'PAYAMT', 'ISSUEDT', 'BALANCE', 'BRANCH',
                 'BORSTAT', 'DAYDIFF', 'CENSUS', 'NAME']
    
    print(f"  Reading lnnote.sas7bdat with sampling...")
    
    # Strategy: Read in chunks and sample
    # Since pyreadstat doesn't support chunking well, we'll use row_limit with offset
    # or use a different approach
    
    # Option 1: Try to read with row_limit (sequential)
    print(f"    Attempting to read {SAMPLE_SIZE * 2} rows to find HP products...")
    
    # Read more rows to increase chances of finding HP products
    df_lnnote_sample = None
    
    # Try different row limits if needed
    row_limits_to_try = [100000, 200000, 500000]
    
    for limit in row_limits_to_try:
        print(f"    Trying row_limit={limit:,}...")
        try:
            df_temp, meta = pyreadstat.read_sas7bdat(
                f'{LOAN_DIR}lnnote.sas7bdat',
                columns=keep_cols,
                row_limit=limit
            )
            df_temp = pl.from_pandas(df_temp)
            
            # Filter for HP products
            df_temp = df_temp.filter(
                (pl.col('LOANTYPE').is_in(HP_PRODUCTS)) & 
                (pl.col('BALANCE') > 0)
            )
            
            print(f"    Found {len(df_temp):,} HP product rows in first {limit:,} rows")
            
            if len(df_temp) > 0:
                # If we found HP products, use this
                df_lnnote_sample = df_temp
                break
                
        except Exception as e:
            print(f"    Error with row_limit={limit}: {e}")
            continue
    
    # If we still don't have data, try random sampling from the full file
    if df_lnnote_sample is None or len(df_lnnote_sample) == 0:
        print("    No HP products found with sequential reads. Trying random sampling...")
        try:
            # Read full file (this will be slow but necessary)
            print("    Reading full LNNOTE file (this may take a while)...")
            df_full, meta = pyreadstat.read_sas7bdat(
                f'{LOAN_DIR}lnnote.sas7bdat',
                columns=keep_cols
            )
            df_full = pl.from_pandas(df_full)
            
            # Filter for HP products
            df_full = df_full.filter(
                (pl.col('LOANTYPE').is_in(HP_PRODUCTS)) & 
                (pl.col('BALANCE') > 0)
            )
            
            print(f"    Found {len(df_full):,} HP product rows in full file")
            
            if len(df_full) > SAMPLE_SIZE:
                # Random sample
                indices = random.sample(range(len(df_full)), SAMPLE_SIZE)
                df_lnnote_sample = df_full[indices]
                print(f"    Randomly sampled {SAMPLE_SIZE:,} rows")
            else:
                df_lnnote_sample = df_full
                
        except Exception as e:
            print(f"    Error with random sampling: {e}")
            sys.exit(1)
    
    # Filter to only accounts that exist in LOANTEMP
    if df_lnnote_sample is not None and len(df_lnnote_sample) > 0:
        df_lnnote = df_lnnote_sample.filter(
            pl.col('ACCTNO').is_in(sampled_accts)
        )
        print(f"  LNNOTE after filtering to LOANTEMP accounts: {len(df_lnnote):,} rows")
        
        # Save to Parquet for future use
        df_lnnote.write_parquet(lnnote_parquet)
        print(f"  Saved to Parquet: {lnnote_parquet}")
    else:
        print("  ERROR: No matching LNNOTE records found")
        sys.exit(1)

# STEP 3: Merge
print("\nMerging data...")
df_hploan = df_lnnote.join(df_loantemp, on=['ACCTNO', 'NOTENO'], how='inner')

print(f"  HP Loans after merge: {len(df_hploan):,} accounts")

if len(df_hploan) == 0:
    print("  ERROR: No matching records after merge")
    sys.exit(1)

# Free up memory
del df_loantemp, df_lnnote
gc.collect()

# Process HP loans (matches SAS DATA HPLOAN step)
print("\nProcessing HP loans...")

# Calculate derived fields (matches SAS logic exactly)
df_hploan = df_hploan.with_columns([
    # ISTLPD = (ORGBAL-CURBAL)/PAYAMT
    ((pl.col('ORGBAL') - pl.col('CURBAL')) / pl.col('PAYAMT')).alias('ISTLPD'),
    
    # ISSDTE = INPUT(SUBSTR(PUT(ISSUEDT,Z11.),1,8),MMDDYY8.)
    pl.col('ISSUEDT').cast(pl.Utf8).str.slice(0, 8).str.to_datetime('%m%d%Y').alias('ISSDTE'),
    
    # CRRISK = SUBSTR(SCORE2,1,1)
    pl.col('SCORE2').cast(pl.Utf8).str.slice(0, 1).alias('CRRISK'),
    
    # MARGINF - Margin of Finance
    pl.when(pl.col('APPVALUE') > 0)
      .then(((pl.col('NETPROC') / pl.col('APPVALUE')) * 100).round(1))
      .otherwise(0)
      .alias('MARGINF'),
    
    # CENSUS9 = SUBSTR(PUT(CENSUS,7.2),1,7)
    pl.col('CENSUS').cast(pl.Utf8).str.zfill(7).alias('CENSUS9')
])

# Categorize fields (matches SAS logic)
df_hploan = df_hploan.with_columns([
    # MGINGRP - Margin group (matches SAS IF/ELSE logic)
    pl.when(pl.col('MARGINF') < 70).then(pl.lit('E. <70%'))
      .when((pl.col('MARGINF') >= 70) & (pl.col('MARGINF') < 80)).then(pl.lit('D. 70 TO <80%'))
      .when((pl.col('MARGINF') >= 80) & (pl.col('MARGINF') < 85)).then(pl.lit('C. 80 TO <85%'))
      .when((pl.col('MARGINF') >= 85) & (pl.col('MARGINF') < 89)).then(pl.lit('B. 85 TO <89%'))
      .otherwise(pl.lit('A. 89% & ABV'))
      .alias('MGINGRP'),
    
    # TERMGRP - Term group (matches SAS IF/ELSE logic)
    pl.when(pl.col('NOTETERM') <= 36).then(pl.lit('A. <=3 YRS'))
      .when((pl.col('NOTETERM') > 36) & (pl.col('NOTETERM') <= 48)).then(pl.lit('B. 4 YRS'))
      .when((pl.col('NOTETERM') > 48) & (pl.col('NOTETERM') <= 60)).then(pl.lit('C. 5 YRS'))
      .when((pl.col('NOTETERM') > 60) & (pl.col('NOTETERM') <= 72)).then(pl.lit('D. 6 YRS'))
      .when((pl.col('NOTETERM') > 72) & (pl.col('NOTETERM') <= 84)).then(pl.lit('E. 7 YRS'))
      .when((pl.col('NOTETERM') > 84) & (pl.col('NOTETERM') <= 96)).then(pl.lit('F. 8 YRS'))
      .otherwise(pl.lit('G. 9 YRS'))
      .alias('TERMGRP'),
    
    # STATENM - State name (matches SAS SELECT)
    pl.col('STATE').cast(pl.Utf8).replace_strict(STATE_MAP, default='OTHERS').alias('STATENM'),
    
    # NATIONAL - East/West Malaysia (matches SAS IF/ELSE)
    pl.when(pl.col('STATENM').is_in(['SABAH', 'SARAWAK', 'LABUAN']))
      .then(pl.lit('EAST MALAYSIA'))
      .otherwise(pl.lit('WEST MALAYSIA'))
      .alias('NATIONAL'),
    
    # MAKE - Make of vehicle (matches SAS SELECT)
    pl.col('CENSUS9').str.slice(0, 2).str.strip_chars()
      .replace_strict(MAKE_MAP, default='OTHERS')
      .alias('MAKE'),
    
    # CARS - National/Non-National (matches SAS IF/ELSE)
    pl.when(pl.col('MAKE').is_in(['PROTON', 'PERODUA']))
      .then(pl.lit('NATIONAL'))
      .otherwise(pl.lit('NON NATIONAL'))
      .alias('CARS'),
    
    # GOODS - Schedule/Unschedule (matches SAS IF/ELSE)
    pl.when((pl.col('MAKE') == 'OTHERS') & (pl.col('PRODUCT').is_in([128, 700])))
      .then(pl.lit('SCHEDULE'))
      .when(pl.col('MAKE') == 'OTHERS')
      .then(pl.lit('UNSCHEDULE'))
      .otherwise(pl.lit(''))
      .alias('GOODS'),
    
    # NEWSEC - New/Secondhand (matches SAS SELECT on CENSUS9 position 4)
    pl.when(pl.col('CENSUS9').str.slice(3, 1).is_in(['1', '2']))
      .then(pl.lit('NEW'))
      .otherwise(pl.lit('SECONDHAND'))
      .alias('NEWSEC'),
    
    # FINGRP - Amount financed (matches SAS IF/ELSE)
    pl.when(pl.col('NETPROC') <= 30000).then(pl.lit('A. RM30K & BELOW'))
      .when((pl.col('NETPROC') > 30000) & (pl.col('NETPROC') <= 50000)).then(pl.lit('B. >RM30K TO 50K'))
      .when((pl.col('NETPROC') > 50000) & (pl.col('NETPROC') <= 100000)).then(pl.lit('C. >RM50K TO 100K'))
      .when((pl.col('NETPROC') > 100000) & (pl.col('NETPROC') <= 250000)).then(pl.lit('D. >RM100K TO 250K'))
      .otherwise(pl.lit('E. >RM250K'))
      .alias('FINGRP'),
    
    # SOURCE - Source of business (matches SAS IF/ELSE)
    pl.when(pl.col('DEALERNO') > 0)
      .then(pl.lit('DEALERS'))
      .otherwise(pl.lit('NON DEALERS'))
      .alias('SOURCE')
])

# Calculate MTHARR - Months in arrears (matches SAS IF/ELSE logic)
df_hploan = df_hploan.with_columns([
    pl.when(pl.col('DAYDIFF') > 729).then((pl.col('DAYDIFF') / 365 * 12).cast(pl.Int32))
      .when(pl.col('DAYDIFF') > 698).then(pl.lit(23))
      .when(pl.col('DAYDIFF') > 668).then(pl.lit(22))
      .when(pl.col('DAYDIFF') > 638).then(pl.lit(21))
      .when(pl.col('DAYDIFF') > 608).then(pl.lit(20))
      .when(pl.col('DAYDIFF') > 577).then(pl.lit(19))
      .when(pl.col('DAYDIFF') > 547).then(pl.lit(18))
      .when(pl.col('DAYDIFF') > 516).then(pl.lit(17))
      .when(pl.col('DAYDIFF') > 486).then(pl.lit(16))
      .when(pl.col('DAYDIFF') > 456).then(pl.lit(15))
      .when(pl.col('DAYDIFF') > 424).then(pl.lit(14))
      .when(pl.col('DAYDIFF') > 394).then(pl.lit(13))
      .when(pl.col('DAYDIFF') > 364).then(pl.lit(12))
      .when(pl.col('DAYDIFF') > 333).then(pl.lit(11))
      .when(pl.col('DAYDIFF') > 303).then(pl.lit(10))
      .when(pl.col('DAYDIFF') > 273).then(pl.lit(9))
      .when(pl.col('DAYDIFF') > 243).then(pl.lit(8))
      .when(pl.col('DAYDIFF') > 213).then(pl.lit(7))
      .when(pl.col('DAYDIFF') > 182).then(pl.lit(6))
      .when(pl.col('DAYDIFF') > 151).then(pl.lit(5))
      .when(pl.col('DAYDIFF') > 121).then(pl.lit(4))
      .when(pl.col('DAYDIFF') > 91).then(pl.lit(3))
      .when(pl.col('DAYDIFF') > 61).then(pl.lit(2))
      .when(pl.col('DAYDIFF') > 30).then(pl.lit(1))
      .otherwise(pl.lit(0))
      .alias('MTHARR')
])

# If BORSTAT = 'F' THEN MTHARR = 999 (matches SAS logic)
df_hploan = df_hploan.with_columns([
    pl.when(pl.col('BORSTAT') == 'F')
      .then(pl.lit(999))
      .otherwise(pl.col('MTHARR'))
      .alias('MTHARR')
])

# BRABBR = PUT(BRANCH,BRCHCD.) - format branch using BRCHCD format
df_hploan = df_hploan.with_columns([
    pl.col('BRANCH').cast(pl.Utf8).alias('BRABBR')
])

print(f"  Processed: {len(df_hploan):,} HP loans")

# Create 4 account groups (matches SAS DATA HPLOAN1-HPLOAN4)
print("\nCreating account groups...")

df_hploan1 = df_hploan  # All accounts (matches HPLOAN1)
df_hploan2 = df_hploan.filter(
    (pl.col('MTHARR') >= 3) | (pl.col('BORSTAT').is_in(['F', 'I', 'R']))
)  # NPL accounts (matches HPLOAN2)
df_hploan3 = df_hploan.filter(pl.col('NOTENO') >= 98010)  # Restructured (matches HPLOAN3)
df_hploan4 = df_hploan.filter(
    (pl.col('NOTENO') >= 98010) & 
    ((pl.col('MTHARR') >= 3) | (pl.col('BORSTAT').is_in(['F', 'I', 'R'])))
)  # Restructured NPL (matches HPLOAN4)

print(f"  HPLOAN1 (All): {len(df_hploan1):,}")
print(f"  HPLOAN2 (NPL): {len(df_hploan2):,}")
print(f"  HPLOAN3 (Restructured): {len(df_hploan3):,}")
print(f"  HPLOAN4 (Restructured NPL): {len(df_hploan4):,}")

# Generate summary reports
print("\nGenerating summary reports...")

def generate_summary_report(df, group_cols, title, subtitle, report_num):
    """Generate summary report matching SAS GENRPT macros"""
    
    if len(df) == 0:
        print(f"  Warning: No data for {title} - {subtitle}")
        return
    
    # Create arrears buckets (matches SAS bucket logic)
    df_summary = df.with_columns([
        pl.when(pl.col('MTHARR') < 3).then(pl.lit('<3MTHS'))
          .when((pl.col('MTHARR') >= 3) & (pl.col('MTHARR') < 6)).then(pl.lit('3-6MTHS'))
          .when((pl.col('MTHARR') >= 6) & (pl.col('MTHARR') < 12)).then(pl.lit('6-12MTHS'))
          .when((pl.col('MTHARR') >= 12) & (pl.col('MTHARR') < 24)).then(pl.lit('12-24MTHS'))
          .when((pl.col('MTHARR') >= 24) & (pl.col('MTHARR') < 36)).then(pl.lit('24-36MTHS'))
          .when(pl.col('MTHARR') >= 36).then(pl.lit('>36MTHS'))
          .otherwise(pl.lit('UNKNOWN'))
          .alias('BUCKET'),
        
        # Deficit flag (matches SAS DEFICIT logic)
        pl.when(pl.col('BORSTAT') == 'F')
          .then(pl.lit('DEFICIT'))
          .otherwise(pl.lit(''))
          .alias('DEFICIT_FLAG')
    ])
    
    # Group and aggregate
    agg_cols = group_cols + ['BUCKET']
    
    df_agg = df_summary.group_by(agg_cols).agg([
        pl.count().alias('COUNT'),
        pl.col('BALANCE').sum().alias('AMOUNT')
    ])
    
    # Pivot by bucket
    df_pivot = df_agg.pivot(
        values=['COUNT', 'AMOUNT'],
        index=group_cols,
        columns='BUCKET'
    )
    
    # Generate CSV-like output (matches SAS semicolon-delimited format)
    lines = []
    lines.append(f"TOTAL POSITION FOR HPD (CONV & AITAB) AS AT {rdate}")
    lines.append(title)
    lines.append(subtitle)
    lines.append("REPORT ID : EIMRESHP")
    
    # Header (matches SAS PRNTITLE)
    header = ['GROUP BY', '<3MTHS NO', '<3MTHS AMT', '3-6MTHS NO', '3-6MTHS AMT',
              '6-12MTHS NO', '6-12MTHS AMT', '12-24MTHS NO', '12-24MTHS AMT',
              '24-36MTHS NO', '24-36MTHS AMT', '>36MTHS NO', '>36MTHS AMT',
              'DEFICIT NO', 'DEFICIT AMT', 'TOTAL NO', 'TOTAL AMT']
    lines.append(';'.join(header))
    
    # Get bucket columns in order
    bucket_order = ['<3MTHS', '3-6MTHS', '6-12MTHS', '12-24MTHS', '24-36MTHS', '>36MTHS']
    
    # Process data rows
    if len(df_pivot) > 0:
        # Calculate totals
        total_count = len(df)
        total_amount = df['BALANCE'].sum()
        
        # For each group combination
        for row in df_pivot.iter_rows():
            row_parts = []
            # Group columns
            for col in group_cols:
                idx = df_pivot.columns.index(col) if col in df_pivot.columns else -1
                if idx >= 0:
                    row_parts.append(str(row[idx]))
                else:
                    row_parts.append('')
            
            # Add bucket data
            for bucket in bucket_order:
                count_col = f'COUNT_{bucket}'
                amt_col = f'AMOUNT_{bucket}'
                if count_col in df_pivot.columns:
                    idx_count = df_pivot.columns.index(count_col)
                    idx_amt = df_pivot.columns.index(amt_col)
                    row_parts.append(str(row[idx_count] if row[idx_count] is not None else 0))
                    row_parts.append(f"{row[idx_amt]:,.2f}" if row[idx_amt] is not None else "0.00")
                else:
                    row_parts.append('0')
                    row_parts.append('0.00')
            
            # Add deficit (BORSTAT='F')
            deficit_count = df.filter(pl.col('BORSTAT') == 'F').height
            deficit_amount = df.filter(pl.col('BORSTAT') == 'F')['BALANCE'].sum() if deficit_count > 0 else 0
            row_parts.append(str(deficit_count))
            row_parts.append(f"{deficit_amount:,.2f}")
            
            # Add totals
            row_parts.append(str(total_count))
            row_parts.append(f"{total_amount:,.2f}")
            
            lines.append(';'.join(row_parts))
    
    # Write to file
    filename = f"EIMRESHI_SUMMARY_{report_num:02d}_{title.replace(' ', '_')}.txt"
    with open(f'{OUTPUT_DIR}{filename}', 'w') as f:
        f.write('\n'.join(lines))
    
    print(f"  Generated: {filename}")

# Define report configurations (matches SAS GENRPT calls)
report_configs = []

# Credit Risk Score (matches SAS GENRPT1 calls)
for df, suffix in [(df_hploan1, 'PRODUCT 128,130,380,381,700,705'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df,
        'groups': ['CRRISK', 'BRABBR'],
        'title': 'CREDIT RISK SCORE',
        'subtitle': suffix
    })

# Source of Business
for df, suffix in [(df_hploan1, 'PRODUCT 128,130,380,381,700,705'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df,
        'groups': ['SOURCE', 'BRABBR'],
        'title': 'SOURCE OF BUSINESS',
        'subtitle': suffix
    })

# Margin of Finance
for df, suffix in [(df_hploan1, 'PRODUCT 128,130,380,381,700,705'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df,
        'groups': ['MGINGRP', 'BRABBR'],
        'title': 'MARGIN OF FINANCE',
        'subtitle': suffix
    })

# Loan Term
for df, suffix in [(df_hploan1, 'PRODUCT 128,130,380,381,700,705'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df,
        'groups': ['TERMGRP', 'BRABBR'],
        'title': 'LOAN TERM',
        'subtitle': suffix
    })

# Amount Finance (3-level grouping - matches GENRPT2)
for df, suffix in [(df_hploan1, 'PRODUCT 128,130,380,381,700,705'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df,
        'groups': ['NEWSEC', 'FINGRP', 'BRABBR'],
        'title': 'AMT FINANCE',
        'subtitle': suffix
    })

# By State (3-level grouping)
for df, suffix in [(df_hploan1, 'PRODUCT 128,130,380,381,700,705'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df,
        'groups': ['NATIONAL', 'STATENM', 'BRABBR'],
        'title': 'BY STATE',
        'subtitle': suffix
    })

# By Make of Vehicle (4-level grouping - matches GENRPT3)
for df, suffix in [(df_hploan1, 'PRODUCT 128,130,380,381,700,705'),
                   (df_hploan2, 'NPL ACCOUNT'),
                   (df_hploan3, 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4, 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df,
        'groups': ['NEWSEC', 'CARS', 'MAKE', 'BRABBR'],
        'title': 'BY MAKE OF VEHICLE',
        'subtitle': suffix
    })

# Make of Vehicle = OTHERS (3-level grouping)
for df, suffix in [(df_hploan1.filter(pl.col('MAKE') == 'OTHERS'), 'PRODUCT 128,130,380,381,700,705'),
                   (df_hploan2.filter(pl.col('MAKE') == 'OTHERS'), 'NPL ACCOUNT'),
                   (df_hploan3.filter(pl.col('MAKE') == 'OTHERS'), 'RESTRUCTURE ACCOUNT'),
                   (df_hploan4.filter(pl.col('MAKE') == 'OTHERS'), 'RESTRUCTURE NPL ACCOUNT')]:
    report_configs.append({
        'df': df,
        'groups': ['NEWSEC', 'GOODS', 'BRABBR'],
        'title': 'BY MAKE OF VEHICLE = OTHERS',
        'subtitle': suffix
    })

# Generate all reports
summary_count = 0
for i, config in enumerate(report_configs, 1):
    if len(config['df']) > 0:
        generate_summary_report(
            config['df'],
            config['groups'],
            config['title'],
            config['subtitle'],
            i
        )
        summary_count += 1

print(f"  Generated {summary_count} summary reports")

# Generate detail report (matches SAS detail report)
print("\nGenerating detail report...")

if len(df_hploan2) > 0:
    df_detail = df_hploan2.select([
        'ACCTNO', 'NOTENO', 'NAME', 'BRABBR', 'PRODUCT', 'BORSTAT',
        'NETPROC', 'BALANCE', 'MTHARR', 'MARGINF', 'NOTETERM',
        'STATENM', 'MAKE', 'NEWSEC', 'SOURCE', 'SCORE2', 'ISTLPD', 'ISSDTE'
    ]).sort(['ACCTNO', 'NOTENO'])
    
    detail_lines = []
    # Header (matches SAS detail header)
    header = ['MNI NO', 'NOTE NO', 'NAME', 'BRABBR', 'PRODUCT', 'BOR. STATUS',
              'AMT FINANCE', 'NET BALANCE', 'MTH PASS DUE', 'MARGIN OF FIN.',
              'LOAN TERM', 'STATES', 'MAKE OF VEC.', 'NEW/SECONDHAND',
              'SOURCE OF BUS.', 'CREDIT SCORE', 'NO ISTL PAID', 'ISSUE DATE']
    detail_lines.append(';'.join(header))
    
    # Data rows
    for row in df_detail.iter_rows():
        row_parts = []
        for val in row:
            if isinstance(val, (int, float)):
                if isinstance(val, float):
                    row_parts.append(f"{val:.2f}")
                else:
                    row_parts.append(str(val))
            elif isinstance(val, datetime):
                row_parts.append(val.strftime('%d%m%Y'))
            else:
                row_parts.append(str(val) if val is not None else '')
        detail_lines.append(';'.join(row_parts))
    
    # Add totals (matches SAS totals)
    tot_acc = len(df_detail)
    tot_amt = df_detail['BALANCE'].sum()
    detail_lines.append(f"TOT NO OF A/C : ;{tot_acc};TOT NET BALANCE : ;{tot_amt:,.2f};")
    
    # Save detail report
    with open(f'{OUTPUT_DIR}EIMRESHI_DETAIL_NPL.txt', 'w') as f:
        f.write('\n'.join(detail_lines))
    
    print(f"  Detail report: {tot_acc:,} NPL accounts")
    print(f"  Total balance: {tot_amt:,.2f}")
else:
    print("  No NPL accounts found")
    with open(f'{OUTPUT_DIR}EIMRESHI_DETAIL_NPL.txt', 'w') as f:
        f.write(f"No NPL accounts found for report date {reptdate.strftime('%d/%m/%Y')}")

print(f"\n{'='*60}")
print(f"EIMRESHI Complete!")
print(f"{'='*60}")
print(f"\nSampling Strategy:")
print(f"  - LOANTEMP: Full read (filtered to HP products)")
print(f"  - LNNOTE: Smart sampling with multiple strategies")
print(f"  - Sample Size: {SAMPLE_SIZE:,} rows")
print(f"\nData Statistics:")
print(f"  Total HP loans processed: {len(df_hploan):,}")
print(f"\n4 Account Groups:")
print(f"  1. All HP accounts: {len(df_hploan1):,}")
print(f"  2. NPL (>=3 months OR F/I/R): {len(df_hploan2):,}")
print(f"  3. Restructured (NOTENO >= 98010): {len(df_hploan3):,}")
print(f"  4. Restructured NPL: {len(df_hploan4):,}")
print(f"\nOutput Directory: {OUTPUT_DIR}")
print(f"  - {summary_count} summary reports")
print(f"  - 1 detail report (NPL accounts)")
print(f"\nParquet cache directory: {PARQUET_DIR}")
