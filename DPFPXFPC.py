"""
EIMRESHP - HP/Hire Purchase Loan Summary & Detail Report (Production Ready)

Purpose:
- Generate summary reports for HP loans (Conv & Aitab) by various groupings
- Track NPL accounts (>=3 months in arrears or F/I/R status)
- Monitor restructured accounts (NOTENO >= 98010)
- Detail report for NPL accounts

HP Products: 128, 130, 380, 381, 700, 705

Report Categories:
1. Credit Risk Score (CRRISK)
2. Source of Business (Dealers vs Non-Dealers)
3. Margin of Finance (<70%, 70-80%, 80-85%, 85-89%, 89%+)
4. Loan Term (<=3yrs, 4yrs, 5yrs, 6yrs, 7yrs, 8yrs, 9yrs+)
5. Amount Financed (<=30K, 30-50K, 50-100K, 100-250K, >250K)
6. By State (14 states + Labuan, grouped East/West Malaysia)
7. By Make of Vehicle (13 makes, National vs Non-National)
8. Make = OTHERS (Schedule vs Unschedule)

4 Account Groups:
- HPLOAN1: All HP accounts
- HPLOAN2: NPL (>=3 months OR F/I/R status)
- HPLOAN3: Restructured (NOTENO >= 98010)
- HPLOAN4: Restructured NPL

Arrears Buckets:
- <3 months, 3-6 months, 6-12 months, 12-24 months, 24-36 months, >36 months, Deficit (F)
"""

import polars as pl
import pyreadstat
from datetime import datetime, timedelta
import os
import sys

# Directories
LOAN_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMRESHP'
CCDTEMP_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMRESHP'
OUTPUT_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMRESHP'

for d in [OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

print("EIMRESHP - HP Loan Summary & Detail Report")
print("=" * 60)

# HP Products (from PBBLNFMT)
HP_PRODUCTS = [128, 130, 380, 381, 700, 705]

# Use yesterday's date as report date
print("\nDetermining report date...")
reptdate = datetime.now() - timedelta(days=1)
day = reptdate.day

# Week determination
if day <= 7:
    sdd, wk, wk1 = 1, '1', '4'
elif day <= 14:
    sdd, wk, wk1 = 9, '2', '1'
elif day <= 21:
    sdd, wk, wk1 = 16, '3', '2'
else:
    sdd, wk, wk1 = 23, '4', '3'

mm = reptdate.month
mm1 = 12 if (wk == '1' and mm == 1) else (mm - 1 if wk == '1' else mm)

reptyear = reptdate.year
reptmon = f'{mm:02d}'
reptday = f'{day:02d}'
rdate = reptdate.strftime('%d%m%y')

print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Week: {wk}")

print("=" * 60)

# Make of vehicle mapping
MAKE_MAP = {
    '1': 'PROTON', '2': 'PERODUA', '3': 'TOYOTA', '4': 'NISSAN',
    '5': 'HONDA', '6': 'ISUZU', '7': 'DAIHATSU', '8': 'MITSUBISHI',
    '9': 'FORD', '10': 'MERCEDES BENZ', '11': 'VOLVO', '13': 'BMW'
}

# State mapping
STATE_MAP = {
    '1': 'JOHORE', '2': 'KEDAH', '3': 'KELANTAN', '4': 'MALACCA',
    '5': 'N.SEMBILAN', '6': 'PAHANG', '7': 'PENANG', '8': 'PERAK',
    '9': 'PERLIS', '10': 'SABAH', '11': 'SARAWAK', '12': 'SELANGOR',
    '13': 'TRENGGANU', '14': 'W.PERSEKUTUAN', '15': 'LABUAN'
}

# Read loan data from SAS7BDAT files
print("\nReading LOANTEMP.sas7bdat...")
try:
    df_loantemp, meta_loantemp = pyreadstat.read_sas7bdat(f'{CCDTEMP_DIR}/LOANTEMP.sas7bdat')
    df_loantemp = pl.from_pandas(df_loantemp)
    
    # Filter HP products with balance > 0
    df_loantemp = df_loantemp.filter(
        (pl.col('PRODUCT').is_in(HP_PRODUCTS)) & 
        (pl.col('BALANCE') > 0)
    )
    print(f"  LOANTEMP records: {len(df_loantemp):,}")
    
except Exception as e:
    print(f"  Error reading LOANTEMP.sas7bdat: {e}")
    sys.exit(1)

print("\nReading LNNOTE.sas7bdat...")
try:
    df_lnnote, meta_lnnote = pyreadstat.read_sas7bdat(f'{LOAN_DIR}/LNNOTE.sas7bdat')
    df_lnnote = pl.from_pandas(df_lnnote)
    
    # Filter HP products with balance > 0
    # Include BRANCH renamed as BRABBR for consistency with original code
    df_lnnote = df_lnnote.filter(
        (pl.col('LOANTYPE').is_in(HP_PRODUCTS)) & 
        (pl.col('BALANCE') > 0)
    ).select([
        'ACCTNO', 'NOTENO', 'LOANTYPE', 'NETPROC', 'APPVALUE',
        'NOTETERM', 'STATE', 'DEALERNO', 'SCORE2', 'ORGBAL',
        'CURBAL', 'PAYAMT', 'ISSUEDT', 'BRANCH'
    ]).rename({'BRANCH': 'BRABBR'})
    
    print(f"  LNNOTE records: {len(df_lnnote):,}")
    
except Exception as e:
    print(f"  Error reading LNNOTE.sas7bdat: {e}")
    sys.exit(1)

# Merge loan data
print("\nMerging loan data...")
try:
    df_hploan = df_lnnote.join(
        df_loantemp, 
        on=['ACCTNO', 'NOTENO'], 
        how='inner'
    )
    print(f"  Merged HP Loans: {len(df_hploan):,} accounts")
    
except Exception as e:
    print(f"  Error merging data: {e}")
    sys.exit(1)

# Process HP loans
print("\nProcessing HP loans...")

# Calculate derived fields - handle SAS date conversion properly
# SAS dates are number of days since 1960-01-01
df_hploan = df_hploan.with_columns([
    # Installments paid
    ((pl.col('ORGBAL') - pl.col('CURBAL')) / pl.col('PAYAMT')).alias('ISTLPD'),
    
    # Convert SAS date to string date (SAS date to datetime)
    # SAS epoch is 1960-01-01
    (pl.lit(datetime(1960, 1, 1)).cast(pl.Datetime) + 
     pl.duration(days=pl.col('ISSUEDT').cast(pl.Int64))).alias('ISSDTE'),
    
    # Credit risk (first character of SCORE2)
    pl.col('SCORE2').cast(pl.Utf8).str.slice(0, 1).alias('CRRISK'),
    
    # Margin of finance
    pl.when(pl.col('APPVALUE') > 0)
      .then((pl.col('NETPROC') / pl.col('APPVALUE') * 100).round(1))
      .otherwise(0)
      .alias('MARGINF'),
    
    # Census code (first 2 digits for make)
    pl.col('CENSUS').cast(pl.Utf8).str.zfill(7).alias('CENSUS9')
])

print(f"  Step 1 complete - Basic calculations done")

# Categorize fields
df_hploan = df_hploan.with_columns([
    # Margin group
    pl.when(pl.col('MARGINF') < 70).then(pl.lit('E. <70%'))
      .when(pl.col('MARGINF') < 80).then(pl.lit('D. 70 TO <80%'))
      .when(pl.col('MARGINF') < 85).then(pl.lit('C. 80 TO <85%'))
      .when(pl.col('MARGINF') < 89).then(pl.lit('B. 85 TO <89%'))
      .otherwise(pl.lit('A. 89% & ABV'))
      .alias('MGINGRP'),
    
    # Term group
    pl.when(pl.col('NOTETERM') <= 36).then(pl.lit('A. <=3 YRS'))
      .when(pl.col('NOTETERM') <= 48).then(pl.lit('B. 4 YRS'))
      .when(pl.col('NOTETERM') <= 60).then(pl.lit('C. 5 YRS'))
      .when(pl.col('NOTETERM') <= 72).then(pl.lit('D. 6 YRS'))
      .when(pl.col('NOTETERM') <= 84).then(pl.lit('E. 7 YRS'))
      .when(pl.col('NOTETERM') <= 96).then(pl.lit('F. 8 YRS'))
      .otherwise(pl.lit('G. 9 YRS'))
      .alias('TERMGRP'),
    
    # State name
    pl.col('STATE').cast(pl.Utf8).replace_strict(STATE_MAP, default='OTHERS').alias('STATENM'),
    
    # National (East/West Malaysia)
    pl.when(pl.col('STATE').cast(pl.Utf8).is_in(['10', '11', '15']))
      .then(pl.lit('EAST MALAYSIA'))
      .otherwise(pl.lit('WEST MALAYSIA'))
      .alias('NATIONAL'),
    
    # Make of vehicle
    pl.col('CENSUS9').str.slice(0, 2).str.strip_chars()
      .replace_strict(MAKE_MAP, default='OTHERS')
      .alias('MAKE'),
    
    # New/Secondhand
    pl.when(pl.col('CENSUS9').str.slice(3, 1).is_in(['1', '2']))
      .then(pl.lit('NEW'))
      .otherwise(pl.lit('SECONDHAND'))
      .alias('NEWSEC'),
    
    # Amount financed group
    pl.when(pl.col('NETPROC') <= 30000).then(pl.lit('A. RM30K & BELOW'))
      .when(pl.col('NETPROC') <= 50000).then(pl.lit('B. >RM30K TO 50K'))
      .when(pl.col('NETPROC') <= 100000).then(pl.lit('C. >RM50K TO 100K'))
      .when(pl.col('NETPROC') <= 250000).then(pl.lit('D. >RM100K TO 250K'))
      .otherwise(pl.lit('E. >RM250K'))
      .alias('FINGRP'),
    
    # Source of business
    pl.when(pl.col('DEALERNO') > 0)
      .then(pl.lit('DEALERS'))
      .otherwise(pl.lit('NON DEALERS'))
      .alias('SOURCE')
])

print(f"  Step 2 complete - Categorizations done")

# Cars (National vs Non-National)
df_hploan = df_hploan.with_columns([
    pl.when(pl.col('MAKE').is_in(['PROTON', 'PERODUA']))
      .then(pl.lit('NATIONAL'))
      .otherwise(pl.lit('NON NATIONAL'))
      .alias('CARS')
])

# Goods (for MAKE = OTHERS)
df_hploan = df_hploan.with_columns([
    pl.when((pl.col('MAKE') == 'OTHERS') & pl.col('PRODUCT').is_in([128, 700]))
      .then(pl.lit('SCHEDULE'))
      .when(pl.col('MAKE') == 'OTHERS')
      .then(pl.lit('UNSCHEDULE'))
      .otherwise(pl.lit(''))
      .alias('GOODS')
])

print(f"  Step 3 complete - Vehicle classifications done")

# Calculate months in arrears (MTHARR)
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

# Deficit flag (999 for BORSTAT='F')
df_hploan = df_hploan.with_columns([
    pl.when(pl.col('BORSTAT') == 'F')
      .then(pl.lit(999))
      .otherwise(pl.col('MTHARR'))
      .alias('MTHARR')
])

print(f"  Step 4 complete - Arrears calculation done")
print(f"  Processed: {len(df_hploan):,} HP loans")

# Create 4 account groups
print("\nCreating account groups...")

df_hploan1 = df_hploan  # All accounts
df_hploan2 = df_hploan.filter(
    (pl.col('MTHARR') >= 3) | pl.col('BORSTAT').is_in(['F', 'I', 'R'])
)  # NPL
df_hploan3 = df_hploan.filter(pl.col('NOTENO') >= 98010)  # Restructured
df_hploan4 = df_hploan.filter(
    (pl.col('NOTENO') >= 98010) & 
    ((pl.col('MTHARR') >= 3) | pl.col('BORSTAT').is_in(['F', 'I', 'R']))
)  # Restructured NPL

print(f"  HPLOAN1 (All): {len(df_hploan1):,}")
print(f"  HPLOAN2 (NPL): {len(df_hploan2):,}")
print(f"  HPLOAN3 (Restructured): {len(df_hploan3):,}")
print(f"  HPLOAN4 (Restructured NPL): {len(df_hploan4):,}")

# Generate summary reports
print("\nGenerating summary reports...")

def generate_summary(df, group_cols, title, subtitle):
    """Generate summary report by grouping"""
    
    # Create arrears buckets
    df_summary = df.with_columns([
        pl.when(pl.col('MTHARR') < 3).then(pl.lit('<3MTHS'))
          .when(pl.col('MTHARR') < 6).then(pl.lit('3-6MTHS'))
          .when(pl.col('MTHARR') < 12).then(pl.lit('6-12MTHS'))
          .when(pl.col('MTHARR') < 24).then(pl.lit('12-24MTHS'))
          .when(pl.col('MTHARR') < 36).then(pl.lit('24-36MTHS'))
          .when(pl.col('MTHARR') >= 36).then(pl.lit('>36MTHS'))
          .otherwise(pl.lit('UNKNOWN'))
          .alias('BUCKET')
    ])
    
    # Group and aggregate
    agg_cols = group_cols + ['BUCKET']
    
    df_agg = df_summary.group_by(agg_cols).agg([
        pl.len().alias('COUNT'),
        pl.col('BALANCE').sum().alias('AMOUNT')
    ])
    
    # Pivot by bucket
    df_pivot = df_agg.pivot(
        values=['COUNT', 'AMOUNT'],
        index=group_cols,
        columns='BUCKET'
    )
    
    return df_pivot

def write_summary_to_text(df_pivot, group_cols, title, subtitle, filename):
    """Write summary report to formatted text file"""
    
    with open(filename, 'w') as f:
        # Header
        f.write("=" * 120 + "\n")
        f.write(f"EIMRESHP - HIRE PURCHASE LOAN SUMMARY REPORT\n")
        f.write(f"REPORT TITLE: {title}\n")
        f.write(f"SUB TITLE: {subtitle}\n")
        f.write(f"REPORT DATE: {reptdate.strftime('%d/%m/%Y')}\n")
        f.write("=" * 120 + "\n\n")
        
        # Convert to pandas for easier text formatting
        df_display = df_pivot.to_pandas()
        
        # Write data
        f.write(df_display.to_string(index=False))
        f.write("\n\n")
        
        # Calculate totals
        total_count = df_pivot.select(pl.col('^.*COUNT.*$').sum()).sum(axis=1)[0] if len(df_pivot) > 0 else 0
        total_amount = df_pivot.select(pl.col('^.*AMOUNT.*$').sum()).sum(axis=1)[0] if len(df_pivot) > 0 else 0
        
        f.write("-" * 120 + "\n")
        f.write(f"TOTAL ACCOUNTS: {total_count:,.0f}\n")
        f.write(f"TOTAL AMOUNT: RM {total_amount:,.2f}\n")
        f.write("=" * 120 + "\n")
        f.write("END OF REPORT\n")

# Report configurations
reports = [
    # Credit Risk Score
    {'df': df_hploan1, 'groups': ['CRRISK', 'BRABBR'], 'title': 'CREDIT RISK SCORE', 'subtitle': 'PRODUCT 128,130,380,381,700,705'},
    {'df': df_hploan2, 'groups': ['CRRISK', 'BRABBR'], 'title': 'CREDIT RISK SCORE', 'subtitle': 'NPL ACCOUNT'},
    {'df': df_hploan3, 'groups': ['CRRISK', 'BRABBR'], 'title': 'CREDIT RISK SCORE', 'subtitle': 'RESTRUCTURE ACCOUNT'},
    {'df': df_hploan4, 'groups': ['CRRISK', 'BRABBR'], 'title': 'CREDIT RISK SCORE', 'subtitle': 'RESTRUCTURE NPL ACCOUNT'},
    
    # Source of Business
    {'df': df_hploan1, 'groups': ['SOURCE', 'BRABBR'], 'title': 'SOURCE OF BUSINESS', 'subtitle': 'PRODUCT 128,130,380,381,700,705'},
    {'df': df_hploan2, 'groups': ['SOURCE', 'BRABBR'], 'title': 'SOURCE OF BUSINESS', 'subtitle': 'NPL ACCOUNT'},
    {'df': df_hploan3, 'groups': ['SOURCE', 'BRABBR'], 'title': 'SOURCE OF BUSINESS', 'subtitle': 'RESTRUCTURE ACCOUNT'},
    {'df': df_hploan4, 'groups': ['SOURCE', 'BRABBR'], 'title': 'SOURCE OF BUSINESS', 'subtitle': 'RESTRUCTURE NPL ACCOUNT'},
    
    # Margin of Finance
    {'df': df_hploan1, 'groups': ['MGINGRP', 'BRABBR'], 'title': 'MARGIN OF FINANCE', 'subtitle': 'PRODUCT 128,130,380,381,700,705'},
    {'df': df_hploan2, 'groups': ['MGINGRP', 'BRABBR'], 'title': 'MARGIN OF FINANCE', 'subtitle': 'NPL ACCOUNT'},
    {'df': df_hploan3, 'groups': ['MGINGRP', 'BRABBR'], 'title': 'MARGIN OF FINANCE', 'subtitle': 'RESTRUCTURE ACCOUNT'},
    {'df': df_hploan4, 'groups': ['MGINGRP', 'BRABBR'], 'title': 'MARGIN OF FINANCE', 'subtitle': 'RESTRUCTURE NPL ACCOUNT'},
    
    # Loan Term
    {'df': df_hploan1, 'groups': ['TERMGRP', 'BRABBR'], 'title': 'LOAN TERM', 'subtitle': 'PRODUCT 128,130,380,381,700,705'},
    {'df': df_hploan2, 'groups': ['TERMGRP', 'BRABBR'], 'title': 'LOAN TERM', 'subtitle': 'NPL ACCOUNT'},
    {'df': df_hploan3, 'groups': ['TERMGRP', 'BRABBR'], 'title': 'LOAN TERM', 'subtitle': 'RESTRUCTURE ACCOUNT'},
    {'df': df_hploan4, 'groups': ['TERMGRP', 'BRABBR'], 'title': 'LOAN TERM', 'subtitle': 'RESTRUCTURE NPL ACCOUNT'},
    
    # Amount Financed
    {'df': df_hploan1, 'groups': ['NEWSEC', 'FINGRP', 'BRABBR'], 'title': 'AMT FINANCE', 'subtitle': 'PRODUCT 128,130,380,381,700,705'},
    {'df': df_hploan2, 'groups': ['NEWSEC', 'FINGRP', 'BRABBR'], 'title': 'AMT FINANCE', 'subtitle': 'NPL ACCOUNT'},
    {'df': df_hploan3, 'groups': ['NEWSEC', 'FINGRP', 'BRABBR'], 'title': 'AMT FINANCE', 'subtitle': 'RESTRUCTURE ACCOUNT'},
    {'df': df_hploan4, 'groups': ['NEWSEC', 'FINGRP', 'BRABBR'], 'title': 'AMT FINANCE', 'subtitle': 'RESTRUCTURE NPL ACCOUNT'},
    
    # By State
    {'df': df_hploan1, 'groups': ['NATIONAL', 'STATENM', 'BRABBR'], 'title': 'BY STATE', 'subtitle': 'PRODUCT 128,130,380,381,700,705'},
    {'df': df_hploan2, 'groups': ['NATIONAL', 'STATENM', 'BRABBR'], 'title': 'BY STATE', 'subtitle': 'NPL ACCOUNT'},
    {'df': df_hploan3, 'groups': ['NATIONAL', 'STATENM', 'BRABBR'], 'title': 'BY STATE', 'subtitle': 'RESTRUCTURE ACCOUNT'},
    {'df': df_hploan4, 'groups': ['NATIONAL', 'STATENM', 'BRABBR'], 'title': 'BY STATE', 'subtitle': 'RESTRUCTURE NPL ACCOUNT'},
    
    # By Make of Vehicle
    {'df': df_hploan1, 'groups': ['NEWSEC', 'CARS', 'MAKE', 'BRABBR'], 'title': 'BY MAKE OF VEHICLE', 'subtitle': 'PRODUCT 128,130,380,381,700,705'},
    {'df': df_hploan2, 'groups': ['NEWSEC', 'CARS', 'MAKE', 'BRABBR'], 'title': 'BY MAKE OF VEHICLE', 'subtitle': 'NPL ACCOUNT'},
    {'df': df_hploan3, 'groups': ['NEWSEC', 'CARS', 'MAKE', 'BRABBR'], 'title': 'BY MAKE OF VEHICLE', 'subtitle': 'RESTRUCTURE ACCOUNT'},
    {'df': df_hploan4, 'groups': ['NEWSEC', 'CARS', 'MAKE', 'BRABBR'], 'title': 'BY MAKE OF VEHICLE', 'subtitle': 'RESTRUCTURE NPL ACCOUNT'},
    
    # Make = OTHERS
    {'df': df_hploan1.filter(pl.col('MAKE') == 'OTHERS'), 'groups': ['NEWSEC', 'GOODS', 'BRABBR'], 'title': 'BY MAKE OF VEHICLE = OTHERS', 'subtitle': 'PRODUCT 128,130,380,381,700,705'},
    {'df': df_hploan2.filter(pl.col('MAKE') == 'OTHERS'), 'groups': ['NEWSEC', 'GOODS', 'BRABBR'], 'title': 'BY MAKE OF VEHICLE = OTHERS', 'subtitle': 'NPL ACCOUNT'},
    {'df': df_hploan3.filter(pl.col('MAKE') == 'OTHERS'), 'groups': ['NEWSEC', 'GOODS', 'BRABBR'], 'title': 'BY MAKE OF VEHICLE = OTHERS', 'subtitle': 'RESTRUCTURE ACCOUNT'},
    {'df': df_hploan4.filter(pl.col('MAKE') == 'OTHERS'), 'groups': ['NEWSEC', 'GOODS', 'BRABBR'], 'title': 'BY MAKE OF VEHICLE = OTHERS', 'subtitle': 'RESTRUCTURE NPL ACCOUNT'}
]

# Generate all reports
summary_count = 0
for i, report in enumerate(reports):
    if len(report['df']) > 0:
        print(f"  Generating report {i+1}/36: {report['title']} - {report['subtitle']}...")
        
        try:
            df_report = generate_summary(
                report['df'], 
                report['groups'], 
                report['title'], 
                report['subtitle']
            )
            
            filename = f"{OUTPUT_DIR}/EIMRESHP_SUMMARY_{i+1:02d}_{report['title'].replace(' ', '_')}.txt"
            write_summary_to_text(
                df_report,
                report['groups'],
                report['title'],
                report['subtitle'],
                filename
            )
            summary_count += 1
        except Exception as e:
            print(f"    Warning: Failed to generate report {i+1}: {e}")
            continue

print(f"  Generated {summary_count} summary reports")

# Generate detail report for NPL accounts
print("\nGenerating detail report...")

try:
    df_detail = df_hploan2.select([
        'ACCTNO', 'NOTENO', 'NAME', 'BRABBR', 'PRODUCT', 'BORSTAT',
        'NETPROC', 'BALANCE', 'MTHARR', 'MARGINF', 'NOTETERM',
        'STATENM', 'MAKE', 'NEWSEC', 'SOURCE', 'SCORE2', 'ISTLPD', 'ISSDTE'
    ]).sort(['ACCTNO', 'NOTENO'])

    # Write detail report to text file
    detail_file = f'{OUTPUT_DIR}/EIMRESHP_DETAIL_NPL.txt'
    with open(detail_file, 'w') as f:
        f.write("=" * 150 + "\n")
        f.write("EIMRESHP - HIRE PURCHASE NPL DETAIL REPORT\n")
        f.write(f"REPORT DATE: {reptdate.strftime('%d/%m/%Y')}\n")
        f.write(f"TOTAL NPL ACCOUNTS: {len(df_detail):,}\n")
        f.write(f"TOTAL NPL BALANCE: RM {df_detail['BALANCE'].sum():,.2f}\n")
        f.write("=" * 150 + "\n\n")
        
        # Convert to pandas for formatting
        df_detail_pd = df_detail.to_pandas()
        
        # Write header
        header_line = f"{'ACCTNO':<12} {'NOTENO':<8} {'NAME':<30} {'BRANCH':<6} {'PROD':<5} {'BORSTAT':<8} {'NETPROC':<12} {'BALANCE':<12} {'MTHARR':<7} {'MARGINF%':<10} {'TERM':<5} {'STATE':<15} {'MAKE':<15} {'NEW/SEC':<10} {'SOURCE':<12} {'SCORE':<6} {'INSTPD':<8} {'ISSUEDT':<10}\n"
        f.write(header_line)
        f.write("-" * 150 + "\n")
        
        # Write data lines
        for row in df_detail_pd.itertuples():
            name_str = str(row.NAME)[:30] if row.NAME else ''
            issuedt_str = str(row.ISSDTE)[:10] if row.ISSDTE else ''
            score_str = str(row.SCORE2)[:6] if row.SCORE2 else ''
            
            line = f"{row.ACCTNO:<12} {row.NOTENO:<8} {name_str:<30} {str(row.BRABBR):<6} {row.PRODUCT:<5} {str(row.BORSTAT):<8} {row.NETPROC:<12,.2f} {row.BALANCE:<12,.2f} {row.MTHARR:<7} {row.MARGINF:<10.1f} {row.NOTETERM:<5} {str(row.STATENM):<15} {str(row.MAKE):<15} {str(row.NEWSEC):<10} {str(row.SOURCE):<12} {score_str:<6} {row.ISTLPD:<8.1f} {issuedt_str:<10}\n"
            f.write(line)

    print(f"  Detail report: {len(df_detail):,} NPL accounts")
    print(f"  Total balance: RM {df_detail['BALANCE'].sum():,.2f}")
    
except Exception as e:
    print(f"  Error generating detail report: {e}")

print(f"\n{'='*60}")
print(f"EIMRESHP Complete!")
print(f"{'='*60}")
print(f"\nOutputs:")
print(f"  - {summary_count} summary reports (by category)")
print(f"  - 1 detail report (NPL accounts)")
print(f"\nHP Products: {HP_PRODUCTS}")
print(f"\n4 Account Groups:")
print(f"  1. All HP accounts: {len(df_hploan1):,}")
print(f"  2. NPL (>=3 months OR F/I/R): {len(df_hploan2):,}")
print(f"  3. Restructured (NOTENO >= 98010): {len(df_hploan3):,}")
print(f"  4. Restructured NPL: {len(df_hploan4):,}")
print(f"\nReport Categories:")
print(f"  1. Credit Risk Score")
print(f"  2. Source of Business")
print(f"  3. Margin of Finance")
print(f"  4. Loan Term")
print(f"  5. Amount Financed")
print(f"  6. By State")
print(f"  7. By Make of Vehicle")
print(f"  8. Make = OTHERS")
