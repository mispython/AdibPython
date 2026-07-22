"""
EIMRESHI - HP/Hire Purchase Loan Summary & Detail Report (Production Ready)

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

# Directories
LOAN_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMRESHI/'
CCDTEMP_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMRESHI/'
OUTPUT_DIR = '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMRESHI/'

for d in [OUTPUT_DIR]:
    os.makedirs(d, exist_ok=True)

print("EIMRESHI - HP Loan Summary & Detail Report")
print("=" * 60)

# HP Products (from PBBLNFMT)
HP_PRODUCTS = [128, 130, 380, 381, 700, 705]

# Use yesterday's date instead of REPTDATE
reptdate = datetime.now() - timedelta(days=1)
reptdate = reptdate.replace(hour=0, minute=0, second=0, microsecond=0)

day = reptdate.day

# Week determination
if day == 8:
    sdd, wk, wk1 = 1, '1', '4'
elif day == 15:
    sdd, wk, wk1 = 9, '2', '1'
elif day == 22:
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

# Read loan data from SAS files
print("\nReading loan data from SAS files...")
try:
    # Read LOANTEMP.sas7bdat
    df_loantemp, meta = pyreadstat.read_sas7bdat(f'{CCDTEMP_DIR}LOANTEMP.sas7bdat')
    df_loantemp = pl.from_pandas(df_loantemp)
    df_loantemp = df_loantemp.filter(
        (pl.col('PRODUCT').is_in(HP_PRODUCTS)) & 
        (pl.col('BALANCE') > 0)
    )
    
    # Read LNNOTE.sas7bdat
    df_lnnote, meta = pyreadstat.read_sas7bdat(f'{LOAN_DIR}LNNOTE.sas7bdat')
    df_lnnote = pl.from_pandas(df_lnnote)
    df_lnnote = df_lnnote.filter(
        (pl.col('LOANTYPE').is_in(HP_PRODUCTS)) & 
        (pl.col('BALANCE') > 0)
    ).select([
        'ACCTNO', 'NOTENO', 'LOANTYPE', 'NETPROC', 'APPVALUE',
        'NOTETERM', 'STATE', 'DEALERNO', 'SCORE2', 'ORGBAL',
        'CURBAL', 'PAYAMT', 'ISSUEDT'
    ])
    
    # Merge
    df_hploan = df_lnnote.join(df_loantemp, on=['ACCTNO', 'NOTENO'], how='inner')
    
    print(f"  HP Loans: {len(df_hploan):,} accounts")
    
except Exception as e:
    print(f"  Error: {e}")
    import sys
    sys.exit(1)

# Process HP loans
print("\nProcessing HP loans...")

# Calculate derived fields
df_hploan = df_hploan.with_columns([
    # Installments paid
    ((pl.col('ORGBAL') - pl.col('CURBAL')) / pl.col('PAYAMT')).alias('ISTLPD'),
    
    # Issue date (assuming ISSUEDT is in numeric format like MMDDYYYY)
    pl.col('ISSUEDT').cast(pl.Utf8).str.slice(0, 8).str.to_datetime('%m%d%Y').alias('ISSDTE'),
    
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
    pl.col('STATE').cast(pl.Utf8).replace(STATE_MAP, default='OTHERS').alias('STATENM'),
    
    # National (East/West Malaysia)
    pl.when(pl.col('STATE').cast(pl.Utf8).is_in(['10', '11', '15']))
      .then(pl.lit('EAST MALAYSIA'))
      .otherwise(pl.lit('WEST MALAYSIA'))
      .alias('NATIONAL'),
    
    # Make of vehicle
    pl.col('CENSUS9').str.slice(0, 2).str.strip_chars()
      .replace(MAKE_MAP, default='OTHERS')
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

# Generate summary reports as text files
print("\nGenerating summary reports...")

def generate_summary_text(df, group_cols, title, subtitle, report_num):
    """Generate summary report as formatted text"""
    
    # Create arrears buckets
    df_summary = df.with_columns([
        pl.when(pl.col('MTHARR') < 3).then(pl.lit('<3MTHS'))
          .when(pl.col('MTHARR') < 6).then(pl.lit('3-6MTHS'))
          .when(pl.col('MTHARR') < 12).then(pl.lit('6-12MTHS'))
          .when(pl.col('MTHARR') < 24).then(pl.lit('12-24MTHS'))
          .when(pl.col('MTHARR') < 36).then(pl.lit('24-36MTHS'))
          .when(pl.col('MTHARR') >= 36).then(pl.lit('>36MTHS'))
          .otherwise(pl.lit('UNKNOWN'))
          .alias('BUCKET'),
        
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
    
    # Generate text report
    report_lines = []
    report_lines.append("=" * 100)
    report_lines.append(f"EIMRESHI SUMMARY REPORT - {title}")
    report_lines.append("=" * 100)
    report_lines.append(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
    report_lines.append(f"Week: {wk}")
    report_lines.append(f"Subtitle: {subtitle}")
    report_lines.append("=" * 100)
    report_lines.append("")
    
    # Add group descriptions
    group_desc = {
        'CRRISK': 'Credit Risk Score',
        'SOURCE': 'Source of Business',
        'MGINGRP': 'Margin of Finance',
        'TERMGRP': 'Loan Term',
        'FINGRP': 'Amount Financed',
        'STATENM': 'State',
        'NATIONAL': 'Region',
        'MAKE': 'Make of Vehicle',
        'CARS': 'Car Category',
        'NEWSEC': 'New/Secondhand',
        'GOODS': 'Goods Type',
        'BRABBR': 'Branch'
    }
    
    for col in group_cols:
        if col in group_desc:
            report_lines.append(f"Group By: {group_desc[col]}")
    
    report_lines.append("")
    
    # Define bucket order
    bucket_order = ['<3MTHS', '3-6MTHS', '6-12MTHS', '12-24MTHS', '24-36MTHS', '>36MTHS', 'UNKNOWN']
    
    # Format the report
    if len(df_pivot) > 0:
        # Extract columns
        count_cols = [f'COUNT_{b}' for b in bucket_order if f'COUNT_{b}' in df_pivot.columns]
        amount_cols = [f'AMOUNT_{b}' for b in bucket_order if f'AMOUNT_{b}' in df_pivot.columns]
        
        # Calculate total count and amount
        total_count = 0
        total_amount = 0
        for col in count_cols:
            total_count += df_pivot[col].sum() if df_pivot[col].dtype in [pl.Int64, pl.Float64] else 0
        for col in amount_cols:
            total_amount += df_pivot[col].sum() if df_pivot[col].dtype in [pl.Int64, pl.Float64] else 0
        
        # Header
        header_parts = []
        for col in group_cols:
            if col in group_desc:
                header_parts.append(group_desc[col])
            else:
                header_parts.append(col)
        header_parts.append('TOTAL COUNT')
        header_parts.append('TOTAL AMOUNT')
        
        # Add bucket headers
        for bucket in bucket_order:
            if f'COUNT_{bucket}' in df_pivot.columns:
                header_parts.append(f'COUNT_{bucket}')
                header_parts.append(f'AMT_{bucket}')
        
        report_lines.append(" | ".join(header_parts))
        report_lines.append("-" * len(" | ".join(header_parts)))
        
        # Data rows
        for row in df_pivot.iter_rows():
            row_parts = []
            for col in group_cols:
                val = row[df_pivot.columns.index(col)] if col in df_pivot.columns else ''
                row_parts.append(str(val))
            
            # Add totals
            row_count = 0
            row_amount = 0
            for col in count_cols:
                idx = df_pivot.columns.index(col)
                row_count += row[idx] if isinstance(row[idx], (int, float)) else 0
            for col in amount_cols:
                idx = df_pivot.columns.index(col)
                row_amount += row[idx] if isinstance(row[idx], (int, float)) else 0
            
            row_parts.append(f"{row_count:,}")
            row_parts.append(f"{row_amount:,.2f}")
            
            # Add bucket data
            for bucket in bucket_order:
                if f'COUNT_{bucket}' in df_pivot.columns:
                    count_idx = df_pivot.columns.index(f'COUNT_{bucket}')
                    amt_idx = df_pivot.columns.index(f'AMOUNT_{bucket}')
                    row_parts.append(f"{row[count_idx]:,}")
                    row_parts.append(f"{row[amt_idx]:,.2f}")
            
            report_lines.append(" | ".join(row_parts))
        
        report_lines.append("-" * len(" | ".join(header_parts)))
        
        # Add totals row
        total_parts = ['TOTAL'] + [''] * (len(group_cols) - 1)
        total_parts.append(f"{total_count:,}")
        total_parts.append(f"{total_amount:,.2f}")
        
        for bucket in bucket_order:
            if f'COUNT_{bucket}' in df_pivot.columns:
                count_idx = df_pivot.columns.index(f'COUNT_{bucket}')
                amt_idx = df_pivot.columns.index(f'AMOUNT_{bucket}')
                total_parts.append(f"{df_pivot[count_idx].sum():,}")
                total_parts.append(f"{df_pivot[amt_idx].sum():,.2f}")
        
        report_lines.append(" | ".join(total_parts))
    
    report_lines.append("")
    report_lines.append(f"Total Accounts: {len(df):,}")
    report_lines.append(f"Total Balance: {df['BALANCE'].sum():,.2f}")
    report_lines.append("=" * 100)
    
    return "\n".join(report_lines)

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

# Generate all reports as text files
summary_count = 0
for i, report in enumerate(reports):
    if len(report['df']) > 0:
        report_text = generate_summary_text(
            report['df'], 
            report['groups'], 
            report['title'], 
            report['subtitle'],
            i + 1
        )
        
        filename = f"EIMRESHI_SUMMARY_{i+1:02d}_{report['title'].replace(' ', '_')}.txt"
        with open(f'{OUTPUT_DIR}{filename}', 'w') as f:
            f.write(report_text)
        summary_count += 1

print(f"  Generated {summary_count} summary reports")

# Generate detail report for NPL accounts
print("\nGenerating detail report...")

df_detail = df_hploan2.select([
    'ACCTNO', 'NOTENO', 'NAME', 'BRABBR', 'PRODUCT', 'BORSTAT',
    'NETPROC', 'BALANCE', 'MTHARR', 'MARGINF', 'NOTETERM',
    'STATENM', 'MAKE', 'NEWSEC', 'SOURCE', 'SCORE2', 'ISTLPD', 'ISSDTE'
]).sort(['ACCTNO', 'NOTENO'])

# Generate detail report as text
detail_lines = []
detail_lines.append("=" * 100)
detail_lines.append("EIMRESHI DETAIL REPORT - NPL ACCOUNTS")
detail_lines.append("=" * 100)
detail_lines.append(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
detail_lines.append(f"Week: {wk}")
detail_lines.append("=" * 100)
detail_lines.append("")

# Headers
headers = ['ACCTNO', 'NOTENO', 'NAME', 'BRABBR', 'PRODUCT', 'BORSTAT', 
           'NETPROC', 'BALANCE', 'MTHARR', 'MARGINF', 'NOTETERM',
           'STATENM', 'MAKE', 'NEWSEC', 'SOURCE', 'SCORE2', 'ISTLPD', 'ISSDTE']
detail_lines.append(" | ".join(headers))
detail_lines.append("-" * 150)

# Data rows
for row in df_detail.iter_rows():
    row_parts = []
    for i, val in enumerate(row):
        if isinstance(val, (int, float)):
            if headers[i] in ['NETPROC', 'BALANCE', 'MARGINF']:
                row_parts.append(f"{val:,.2f}")
            elif headers[i] in ['ISTLPD']:
                row_parts.append(f"{val:.2f}")
            else:
                row_parts.append(str(val))
        else:
            row_parts.append(str(val) if val is not None else '')
    detail_lines.append(" | ".join(row_parts))

detail_lines.append("-" * 150)
tot_acc = len(df_detail)
tot_amt = df_detail['BALANCE'].sum()
detail_lines.append(f"Total Accounts: {tot_acc:,}")
detail_lines.append(f"Total Balance: {tot_amt:,.2f}")
detail_lines.append("=" * 100)

# Save detail report
with open(f'{OUTPUT_DIR}EIMRESHI_DETAIL_NPL.txt', 'w') as f:
    f.write("\n".join(detail_lines))

print(f"  Detail report: {tot_acc:,} NPL accounts")
print(f"  Total balance: {tot_amt:,.2f}")

print(f"\n{'='*60}")
print(f"EIMRESHI Complete!")
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
print(f"\nOutput Directory: {OUTPUT_DIR}")
