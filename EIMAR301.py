import polars as pl
from datetime import datetime
from dateutil.relativedelta import relativedelta

BNM_REPTDATE = 'data/bnm/reptdate.parquet'
BNM_LOANTEMP = 'data/bnm/loantemp.parquet'
BRHFILE = 'data/brhfile.parquet'
OUTPUT_A = 'data/eimar301a.txt'
OUTPUT_B = 'data/eimar301b.txt'
OUTPUT_C = 'data/eimar301c.txt'
OUTPUT_D = 'data/eimar301d.txt'

HPD_PRODUCTS = [500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510]

df_reptdate = pl.read_parquet(BNM_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

if reptdate.month == 1:
    pmth = 12
    pyear = reptdate.year - 1
else:
    pmth = reptdate.month - 1
    pyear = reptdate.year

pdate = datetime(pyear, pmth, 1).date()
preptdte = pdate

rdate = reptdate.strftime('%d%m%y')
reptyear = reptdate.strftime('%Y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')

df_lntemp = pl.read_parquet(BNM_LOANTEMP).filter(
    (pl.col('BALANCE') > 0) &
    (pl.col('BORSTAT') != 'Z') &
    pl.col('PRODUCT').is_in(HPD_PRODUCTS)
).sort('BRANCH')

df_brhdata = pl.read_parquet(BRHFILE).select(['BRANCH', 'BRABBR']).rename({'BRABBR': 'BRHCODE'}).sort('BRANCH')

df_lntemp = df_lntemp.join(df_brhdata, on='BRANCH', how='inner')

df_loan = df_lntemp.filter(
    (pl.col('ARREAR2') >= 3) |
    pl.col('BORSTAT').is_in(['R', 'I', 'F', 'Y']) |
    ((pl.col('ISSDTE') >= pdate) & (pl.col('DAYDIFF') >= 8))
)

records = []
for row in df_loan.iter_rows(named=True):
    arrear2_val = 15 if row['BORSTAT'] == 'F' else row['ARREAR2']
    
    arrears_map = {
        1: '< 1 MTH', 2: '1-2 MTHS', 3: '2-3 MTHS', 4: '3-4 MTHS',
        5: '4-5 MTHS', 6: '5-6 MTHS', 7: '6-7 MTHS', 8: '7-8 MTHS',
        9: '8-9 MTHS', 10: '9-12 MTHS', 11: '12-18 MTHS', 12: '18-24 MTHS',
        13: '24-36 MTHS', 14: '>36 MTHS', 15: 'DEFICIT'
    }
    arrears = arrears_map.get(arrear2_val, 'UNKNOWN')
    
    cacbr = '000'
    
    base_row = {
        **row,
        'ARREAR2': arrear2_val,
        'ARREARS': arrears,
        'CACBR': cacbr
    }
    
    if row['PRODUCT'] in [380, 381, 700, 705, 720, 725]:
        records.append({**base_row, 'CAT': 'A', 'TYPE': 'HP DIRECT(CONV) '})
    
    if row['PRODUCT'] in [128, 130, 131, 132]:
        records.append({**base_row, 'CAT': 'C', 'TYPE': 'AITAB '})
    
    if row['PRODUCT'] in [380, 381]:
        records.append({**base_row, 'CAT': 'B', 'TYPE': 'HP (380,381) '})

df_loan1 = pl.DataFrame(records).sort(['CAT', 'BRANCH', 'ARREAR2'], descending=[False, False, False])

def write_report_a(df):
    df_filtered = df.filter(pl.col('CACBR') == '000').sort(['CAT', 'BRANCH', 'ARREAR2', 'BALANCE'], descending=[False, False, False, True])
    
    lines = []
    page_count = 0
    line_count = 6
    
    current_cat = None
    current_branch = None
    current_arrear = None
    
    total = 0
    totac = 0
    brhamt = 0
    brhac = 0
    brharr = 0
    brharrac = 0
    
    for row in df_filtered.iter_rows(named=True):
        if current_cat != row['CAT']:
            if current_cat is not None:
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append(f"     GRAND TOTAL                                  NO OF A/C : {totac:>12,}                                                    {total:>17,.2f}")
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append('')
            
            current_cat = row['CAT']
            page_count = 0
            total = 0
            totac = 0
        
        if current_branch != row['BRANCH']:
            if current_branch is not None:
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append(f"     BRANCH TOTAL                                 NO OF A/C : {brhac:>12,}                                                    {brhamt:>17,.2f}")
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append('')
            
            current_branch = row['BRANCH']
            brhamt = 0
            brhac = 0
            page_count += 1
            line_count = 6
            
            lines.append(f"PROGRAM-ID:EIMAR301-A - BRANCH : {row['BRANCH']:03d}       P U B L I C   B A N K   B E R H A D                                             PAGE NO.: {page_count}")
            lines.append(f"                        {row['TYPE']:30}2 MTHS & ABOVE AND A/C PAID 2 ISTL AND BELOW AS AT {rdate}")
            lines.append(' ')
            lines.append('BRH NAME                     NOTENO     ISSUE DT             LST TR DT        ISTL AMT         NO ISTL PD BORSTAT ARREARS                  BALANCE')
            lines.append('     ACC NO                   PRODUCT    MATURE DT            LST TR AMT                                 DAYS ARR                 DELQ REASON CODE')
            lines.append('     COLLATERAL DESC')
            lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        
        if current_arrear != row['ARREAR2']:
            if current_arrear is not None:
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append(f"     SUBTOTAL                                     NO OF A/C : {brharrac:>12,}                                                    {brharr:>17,.2f}")
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append('')
                line_count += 4
            
            current_arrear = row['ARREAR2']
            brharr = 0
            brharrac = 0
        
        if line_count > 56:
            page_count += 1
            line_count = 6
            lines.append(f"PROGRAM-ID:EIMAR301-A - BRANCH : {row['BRANCH']:03d}       P U B L I C   B A N K   B E R H A D                                             PAGE NO.: {page_count}")
        
        issdte_str = row['ISSDTE'].strftime('%d%m%Y') if row['ISSDTE'] else '        '
        lastran_str = row['LASTRAN'].strftime('%d%m%Y') if row['LASTRAN'] else '        '
        maturdt_str = row['MATURDT'].strftime('%d%m%Y') if row['MATURDT'] else '        '
        
        lines.append(f"{row['BRHCODE']:3} {row['NAME']:<20} {row['NOTENO']:<9} {issdte_str:<18} {lastran_str:<9} {row['PAYAMT']:>15,.2f} {row['NOISTLPD']:>8,} {row['BORSTAT']:<8} {row['ARREARS']:<19} {row['BALANCE']:>17,.2f}")
        lines.append(f"     {row['ACCTNO']:<20} {row['PRODUCT']:<9} {maturdt_str:<13} {row['LSTTRNAM']:>15,.2f}                                 {row['DAYDIFF']:>8,}                 {row['DELQCD']}")
        lines.append(f"     {row['COLLDESC']}")
        lines.append(' ')
        line_count += 4
        
        brharr += row['BALANCE']
        brharrac += 1
        brhamt += row['BALANCE']
        brhac += 1
        total += row['BALANCE']
        totac += 1
    
    if current_arrear is not None:
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        lines.append(f"     SUBTOTAL                                     NO OF A/C : {brharrac:>12,}                                                    {brharr:>17,.2f}")
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        lines.append('')
    
    if current_branch is not None:
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        lines.append(f"     BRANCH TOTAL                                 NO OF A/C : {brhac:>12,}                                                    {brhamt:>17,.2f}")
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        lines.append('')
    
    if current_cat is not None:
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        lines.append(f"     GRAND TOTAL                                  NO OF A/C : {totac:>12,}                                                    {total:>17,.2f}")
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
    
    with open(OUTPUT_A, 'w') as f:
        for line in lines:
            f.write(line + '\n')

write_report_a(df_loan1)

def write_report_b(df):
    df_filtered = df.filter(
        (pl.col('ARREAR2') >= 4) & (pl.col('ARREAR2') < 10) &
        ~pl.col('BORSTAT').is_in(['F', 'I', 'R'])
    ).sort(['CAT', 'BRANCH', 'ARREAR2', 'BALANCE'], descending=[False, False, False, True])
    
    lines = []
    page_count = 0
    line_count = 6
    
    current_cat = None
    current_branch = None
    current_arrear = None
    
    total = 0
    totac = 0
    brhamt = 0
    brhac = 0
    brharr = 0
    brharrac = 0
    
    for row in df_filtered.iter_rows(named=True):
        if current_cat != row['CAT']:
            if current_cat is not None:
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append(f"     GRAND TOTAL                                  NO OF A/C : {totac:>12,}                      {total:>17,.2f}")
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append('')
            
            current_cat = row['CAT']
            total = 0
            totac = 0
            page_count = 0
        
        if current_branch != row['BRANCH']:
            if current_branch is not None:
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append(f"     BRANCH TOTAL                                 NO OF A/C : {brhac:>12,}                      {brhamt:>17,.2f}")
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append('')
            
            current_branch = row['BRANCH']
            page_count = 0
            brhamt = 0
            brhac = 0
            page_count += 1
            line_count = 6
            
            lines.append(f"PROGRAM-ID:EIMAR301-B - BRANCH : {row['BRANCH']:03d}       P U B L I C   B A N K   B E R H A D                                      PAGE NO.: {page_count}")
            lines.append(f"                        {row['TYPE']:30}ACCOUNT WITH 3 - 8 MONTH IN ARREAR AS AT {rdate}")
            lines.append(' ')
            lines.append('BRH ACCTNO               NAME                            NOTENO     PRODUCT BORSTAT ISSUE DT     DAYS ARREARS                         BALANCE NO ISTL PAID')
            lines.append('     LST TR DT        MAT. DATE           LST TR AMT   ISTL AMT   COLLATERAL DESCRIPTION')
            lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        
        if current_arrear != row['ARREAR2']:
            if current_arrear is not None:
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append(f"     SUBTOTAL                                     NO OF A/C : {brharrac:>12,}                      {brharr:>17,.2f}")
                lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
                lines.append('')
                line_count += 4
            
            current_arrear = row['ARREAR2']
            brharr = 0
            brharrac = 0
        
        if line_count > 56:
            page_count += 1
            line_count = 6
        
        issdte_str = row['ISSDTE'].strftime('%d%m%Y') if row['ISSDTE'] else '        '
        lastran_str = row['LASTRAN'].strftime('%d%m%Y') if row['LASTRAN'] else '        '
        maturdt_str = row['MATURDT'].strftime('%d%m%Y') if row['MATURDT'] else '        '
        
        lines.append(f"{row['BRHCODE']:3} {row['ACCTNO']:<11} {row['NAME']:<25} {row['NOTENO']:<14} {row['PRODUCT']:<5} {row['BORSTAT']:<9} {issdte_str:<11} {row['DAYDIFF']:<4} {row['ARREARS']:<16} {row['BALANCE']:>17,.2f} {row['NOISTLPD']:>10,}")
        lines.append(f"     {lastran_str:<11} {maturdt_str:<13} {row['LSTTRNAM']:>17,.2f} {row['PAYAMT']:>11,.2f} {row['COLLDESC']}")
        lines.append(' ')
        line_count += 3
        
        brharr += row['BALANCE']
        brharrac += 1
        brhamt += row['BALANCE']
        brhac += 1
        total += row['BALANCE']
        totac += 1
    
    if current_arrear is not None:
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        lines.append(f"     SUBTOTAL                                     NO OF A/C : {brharrac:>12,}                      {brharr:>17,.2f}")
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        lines.append('')
    
    if current_branch is not None:
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        lines.append(f"     BRANCH TOTAL                                 NO OF A/C : {brhac:>12,}                      {brhamt:>17,.2f}")
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        lines.append('')
    
    if current_cat is not None:
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
        lines.append(f"     GRAND TOTAL                                  NO OF A/C : {totac:>12,}                      {total:>17,.2f}")
        lines.append('-' * 40 + '-' * 40 + '-' * 40 + '----------')
    
    with open(OUTPUT_B, 'w') as f:
        for line in lines:
            f.write(line + '\n')

write_report_b(df_loan1)

df_newrel = df_lntemp.filter(
    (pl.col('ISSDTE') >= pdate) & (pl.col('DAYDIFF') >= 8)
).with_columns([
    pl.when(pl.col('NOISTLPD') < 1)
      .then(pl.lit('NO PAYMENT'))
      .when((pl.col('NOISTLPD') >= 1) & (pl.col('NOISTLPD') < 2))
      .then(pl.lit('PAID 1 ISTL'))
      .otherwise(pl.lit('PAID 2 ISTL'))
      .alias('PAYDESC')
])

df_newrel_summary = df_newrel.group_by(['BRHCODE', 'PAYDESC']).agg([
    pl.len().alias('NOACCT'),
    pl.col('BALANCE').sum()
])

with open(OUTPUT_C, 'w') as f:
    f.write('PROGRAM ID : EIMAR301-C\n')
    f.write('PUBLIC BANK BERHAD\n')
    f.write(f'SUMMARY ON AC WITH PAYMENT OF 2 ISTL & BELOW AS AT {rdate}\n\n')
    f.write('BRANCH;NO PAYMENT;NO PAYMENT;PAID 1 ISTL;PAID 1 ISTL;PAID 2 ISTL;PAID 2 ISTL;TOTAL;TOTAL\n')
    f.write('      ;NO OF A/C;O/S BALANCE;NO OF A/C;O/S BALANCE;NO OF A/C;O/S BALANCE;NO OF A/C;O/S BALANCE\n')
    
    for brhcode in df_newrel_summary['BRHCODE'].unique().sort():
        df_brh = df_newrel_summary.filter(pl.col('BRHCODE') == brhcode)
        
        no_pmt = df_brh.filter(pl.col('PAYDESC') == 'NO PAYMENT')
        paid1 = df_brh.filter(pl.col('PAYDESC') == 'PAID 1 ISTL')
        paid2 = df_brh.filter(pl.col('PAYDESC') == 'PAID 2 ISTL')
        
        no_pmt_cnt = no_pmt['NOACCT'][0] if len(no_pmt) > 0 else 0
        no_pmt_bal = no_pmt['BALANCE'][0] if len(no_pmt) > 0 else 0
        paid1_cnt = paid1['NOACCT'][0] if len(paid1) > 0 else 0
        paid1_bal = paid1['BALANCE'][0] if len(paid1) > 0 else 0
        paid2_cnt = paid2['NOACCT'][0] if len(paid2) > 0 else 0
        paid2_bal = paid2['BALANCE'][0] if len(paid2) > 0 else 0
        
        total_cnt = no_pmt_cnt + paid1_cnt + paid2_cnt
        total_bal = no_pmt_bal + paid1_bal + paid2_bal
        
        f.write(f"{brhcode};{no_pmt_cnt:>8,};{no_pmt_bal:>15,.2f};{paid1_cnt:>8,};{paid1_bal:>15,.2f};{paid2_cnt:>8,};{paid2_bal:>15,.2f};{total_cnt:>8,};{total_bal:>15,.2f}\n")

df_accarr = df_lntemp.filter(
    (pl.col('NOISTLPD') >= 2) & (pl.col('NOISTLPD') < 3) & (pl.col('DAYDIFF') >= 8)
).with_columns([
    pl.lit('PAID 2 ISTL').alias('PAYDESC')
])

df_accarr_summary = df_accarr.group_by(['BRHCODE', 'PAYDESC']).agg([
    pl.len().alias('NOACCT'),
    pl.col('BALANCE').sum()
])

with open(OUTPUT_D, 'w') as f:
    f.write('PROGRAM ID : EIMAR301-D\n')
    f.write('PUBLIC BANK BERHAD\n')
    f.write(f'SUMMARY ON A/C IN ARREAR WITH 2 ISTL PAID ONLY AS AT {rdate}\n\n')
    f.write('BRANCH;PAID 2 ISTL;PAID 2 ISTL\n')
    f.write('      ;NO OF A/C;O/S BALANCE\n')
    
    for row in df_accarr_summary.iter_rows(named=True):
        f.write(f"{row['BRHCODE']};{row['NOACCT']:>8,};{row['BALANCE']:>15,.2f}\n")

print(f"Reports generated:")
print(f"  {OUTPUT_A}")
print(f"  {OUTPUT_B}")
print(f"  {OUTPUT_C}")
print(f"  {OUTPUT_D}")
