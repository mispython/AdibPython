"""
EIBHTUKR - TUK Loan Reporting for CGC
Includes: PBBELF, EIFPCFMT format definitions
"""

import polars as pl
from datetime import datetime, timedelta
import sys

# Import format definition programs (%INC PGM equivalent)
sys.path.insert(0, '/mnt/user-data/outputs')
from pbbelf import LIBRARY_PATHS, format_ddmmyy8, format_ddmmyy10
from eifpcfmt import get_state_name

# Use library paths from PBBELF
LOAN_DIR = LIBRARY_PATHS['LOAN']
RAW_DIR = LIBRARY_PATHS['RAW']
TEMP_DIR = LIBRARY_PATHS['TEMP']

BRHFILE = 'data/brhfile.dat'
OUTPUT_RPT1 = 'reports/tuk_outstanding_loans.txt'
OUTPUT_RPT2 = 'reports/tuk_interest_accrued.txt'
OUTPUT_RPT3 = 'reports/tuk_interest_paid.txt'
OUTPUT_RPT4 = 'reports/tuk_package134.txt'
OUTPUT_RPT5 = 'reports/tuk_package2.txt'

# TUK format - Product code to description (from original SAS PROC FORMAT)
TUK_FORMAT = {
    521: 'TUK 1-CODE 521',
    522: 'TUK 2-CODE 522',
    523: 'TUK 3-CODE 523',
    528: 'TUK 4-CODE 528'
}

# COL format - Column type descriptions (from original SAS PROC FORMAT)
COL_FORMAT = {
    'A': 'TOTAL INTEREST',
    'B': 'INT. TO CGC (2/6)',
    'C': 'INT. TO CGC (1/6)',
    'D': 'INT. TO INSTITUTION APPT BY CGC (4/6)'
}

def get_tuk_description(product):
    """Get TUK product description"""
    return TUK_FORMAT.get(product, f'PRODUCT {product}')

def get_col_description(col):
    """Get column description"""
    return COL_FORMAT.get(col, col)

today = datetime.today().date()
first_of_month = datetime(today.year, today.month, 1).date()
reptdate = first_of_month - timedelta(days=1)

mth = reptdate.month
if mth == 12:
    lastmth = 6
elif mth == 6:
    lastmth = 12
else:
    lastmth = mth

day = reptdate.day
if day == 8:
    sdd = 1
    wk = '1'
    wk1 = '4'
elif day == 15:
    sdd = 9
    wk = '2'
    wk1 = '1'
elif day == 22:
    sdd = 16
    wk = '3'
    wk1 = '2'
else:
    sdd = 23
    wk = '4'
    wk1 = '3'

nowk = wk
rdate = reptdate.strftime('%d/%m/%Y')
reptmon = f'{mth:02d}'
lreptmon = f'{lastmth:02d}'
ryear = f'{reptdate.year:04d}'

if reptmon == '06':
    fulldate = '1 JANUARY TO 30 JUNE '
elif reptmon == '12':
    fulldate = '1 JULY TO 31 DECEMBER '
else:
    fulldate = f'MONTH {reptmon} '

print(f"Processing TUK Loan Reporting")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Report Period: {fulldate}{ryear}")

branch_records = []
with open(BRHFILE, 'r') as f:
    for line in f:
        branch_records.append({
            'BRANCH': int(line[1:4]),
            'BRH': line[5:8].strip(),
            'BRHNAME': line[11:36].strip(),
            'STATE': line[44:45].strip()
        })

df_brhdata = pl.DataFrame(branch_records)

df_raw = pl.read_parquet(f'{RAW_DIR}LNNOTE.parquet').filter(
    pl.col('LOANTYPE').is_in([521, 522, 523, 528])
)

df_process = pl.read_parquet(f'{LOAN_DIR}LOAN{reptmon}{nowk}.parquet').filter(
    pl.col('PRODUCT').is_in([521, 522, 523, 528])
).with_columns([
    pl.lit(reptdate).alias('TODATE')
])

df_selected = df_raw.join(
    df_process.select(['ACCTNO', 'NOTENO', 'PRODUCT', 'TODATE']),
    on=['ACCTNO', 'NOTENO'],
    how='inner'
)

arrears_list = []
for row in df_selected.iter_rows(named=True):
    arrearno = 0
    biltot = row.get('BILTOT', 0)
    
    if biltot and biltot > 0:
        todate = row['TODATE']
        bldate = row.get('BLDATE')
        
        if bldate:
            days_diff = (todate - bldate).days
            arrearno = days_diff / 14
            
            if arrearno % 1 != 0:
                arrearno = int(days_diff / 14) + 1
            else:
                arrearno = int(days_diff / 14)
    
    arrears_list.append(arrearno)

df_selected = df_selected.with_columns([
    pl.Series('ARREARNO', arrears_list)
])

df_cdrlntuk = df_selected.join(df_brhdata, on='BRANCH', how='left').sort('BRANCH')

df_cdrlntuk = df_cdrlntuk.with_columns([
    (pl.col('BRANCH').cast(str).str.zfill(3) + ' (' + pl.col('BRHNAME') + ')').alias('BRNAME')
]).sort(['STATE', 'BRANCH', 'CGCREF'])

df_cdrlntuk_sorted = df_cdrlntuk.sort(['STATE', 'BRNAME'])

num_list = []
for (state, brname), group in df_cdrlntuk_sorted.group_by(['STATE', 'BRNAME'], maintain_order=True):
    for i in range(len(group)):
        num_list.append(i + 1)

df_cdrlntuk_sorted = df_cdrlntuk_sorted.with_columns([
    pl.Series('NUM', num_list)
])

# Apply STATE format from EIFPCFMT (equivalent to FORMAT STATE $STATE.)
state_names = []
for state_code in df_cdrlntuk_sorted['STATE'].to_list():
    state_names.append(get_state_name(state_code))

df_cdrlntuk_sorted = df_cdrlntuk_sorted.with_columns([
    pl.Series('STATE_NAME', state_names)
])

df_cdrlntuk.write_parquet(f'{TEMP_DIR}CDRLNTUK.parquet')
df_cdrlntuk_sorted.write_parquet(f'{TEMP_DIR}CDRLNTUK_sorted.parquet')

df_cdrtuk = df_cdrlntuk.join(df_brhdata, on='BRANCH', how='left', suffix='_brh')

df_cdrtuk.write_parquet(f'{TEMP_DIR}CDRTUK.parquet')

tuk_records = []
for row in df_cdrtuk.iter_rows(named=True):
    product = row['PRODUCT']
    hstint = row.get('HSTINT', 0)
    
    tuk_records.append({
        **row,
        'COL': 'A',
        'ACCRBAL': hstint,
        'INTBAL': hstint,
        'ACCRTOT': 0,
        'INTTOT': 0
    })
    
    if product in [521, 523, 528]:
        accrbal = round((2/6) * hstint, 2)
        intbal = round((2/6) * hstint, 2)
        tuk_records.append({
            **row,
            'COL': 'B',
            'ACCRBAL': accrbal,
            'INTBAL': intbal,
            'ACCRTOT': accrbal,
            'INTTOT': intbal
        })
    elif product == 522:
        accrbal_c = round((1/6) * hstint, 2)
        intbal_c = round((1/6) * hstint, 2)
        tuk_records.append({
            **row,
            'COL': 'C',
            'ACCRBAL': accrbal_c,
            'INTBAL': intbal_c,
            'ACCRTOT': accrbal_c,
            'INTTOT': intbal_c
        })
        
        accrbal_d = round((4/6) * hstint, 2)
        intbal_d = round((4/6) * hstint, 2)
        tuk_records.append({
            **row,
            'COL': 'D',
            'ACCRBAL': accrbal_d,
            'INTBAL': intbal_d,
            'ACCRTOT': 0,
            'INTTOT': 0
        })

df_cdrtuk_expanded = pl.DataFrame(tuk_records)

df_cdrtuk_expanded.write_parquet(f'{TEMP_DIR}CDRTUK{reptmon}.parquet')

tuk134_records = []
tuk2_records = []

for row in df_cdrlntuk.iter_rows(named=True):
    product = row['PRODUCT']
    hstint = row.get('HSTINT', 0)
    acctyind = row.get('ACCTYIND', 0)
    
    if acctyind == 700:
        classif = 'FOR FORMER HHB ACCOUNTS ONLY'
    elif acctyind == 800:
        classif = 'FOR FORMER ADF ACCOUNTS ONLY'
    else:
        classif = 'FOR THE PUBLIC BANK ACCOUNTS ONLY'
    
    if product in [521, 523, 528]:
        interb = round((4/6) * hstint, 2)
        intercgc = round((2/6) * hstint, 2)
        tuk134_records.append({
            **row,
            'CLASSIF': classif,
            'INTERB': interb,
            'INTERCGC': intercgc
        })
    elif product == 522:
        interb = round((1/6) * hstint, 2)
        intercgc = round((1/6) * hstint, 2)
        interag = round((4/6) * hstint, 2)
        tuk2_records.append({
            **row,
            'CLASSIF': classif,
            'INTERB': interb,
            'INTERCGC': intercgc,
            'INTERAG': interag
        })

df_tuk134 = pl.DataFrame(tuk134_records).sort(['CLASSIF', 'STATE', 'BRNAME'])
df_tuk2 = pl.DataFrame(tuk2_records).sort(['CLASSIF', 'STATE', 'BRNAME', 'BRH'])

with open(OUTPUT_RPT4, 'w') as f:
    pagecnt = 0
    # Use PBBELF date format
    date_today = format_ddmmyy8(datetime.today())
    
    tbalance = tapprlim = thstprin = tintpdyt = tinterb = tintercg = 0
    
    for (classif, state, brname), branch_group in df_tuk134.group_by(['CLASSIF', 'STATE', 'BRNAME'], maintain_order=True):
        pagecnt += 1
        brh = branch_group['BRH'][0]
        
        # Apply STATE format from EIFPCFMT
        state_name = get_state_name(state)
        
        f.write(f"REPORT NAME : EIBHTUKR - 4{' '*18}P U B L I C   B A N K   B E R H A D{' '*34}PAGE NO : {pagecnt}\n")
        f.write(f"{' '*29}TABUNG USAHAWAN KECIL (TUK) LOAN SCHEME - PACKAGE 1,3,OR 4\n")
        f.write(f"{' '*46}{classif}\n")
        f.write(f"{' '*17}PROFIT EARNED FOR THE HALF YEARLY REPAYMENT FOR PERIOD ENDED : {fulldate}{ryear}"
                f"{' '*34}DATE    : {date_today}\n\n")
        f.write(f" STATE : {state_name} BRANCH CODE : {brname} BRANCH ABBR : {brh}\n\n")
        f.write(f"      CGC REF{' '*48}  LOAN    OUSTANDING       REPAYMENT COLLECTED(RM)       INCOME SHARING IN (RM)\n")
        f.write(f"NO.   NUMBER     ACCOUNT NO   NAME OF BORROWER      AMOUNT(RM) BALANCE(RM)  PRINCIPAL    INTEREST6%   BANK(X 4/6)  CGC (X 2/6)\n")
        f.write(f"{'_'*130}\n\n")
        
        cnt = 0
        sbalance = sapprlim = shstprin = sintpdyt = sinterb = sintercg = 0
        bbalance = bapprlim = bhstprin = bintpdyt = binterb = bintercg = 0
        
        for row in branch_group.iter_rows(named=True):
            cnt += 1
            
            cgcref = row.get('CGCREF', '')
            acctno = row.get('ACCTNO', 0)
            name = (row.get('NAME', '') or '')[:24]
            apprlimt = row.get('APPRLIMT', 0)
            balance = row.get('BALANCE', 0)
            hstprin = row.get('HSTPRIN', 0)
            hstint = row.get('HSTINT', 0)
            interb = row.get('INTERB', 0)
            intercgc = row.get('INTERCGC', 0)
            
            sbalance += balance
            sapprlim += apprlimt
            shstprin += hstprin
            sintpdyt += hstint
            sinterb += interb
            sintercg += intercgc
            bbalance += balance
            bapprlim += apprlimt
            bhstprin += hstprin
            bintpdyt += hstint
            binterb += interb
            bintercg += intercgc
            tbalance += balance
            tapprlim += apprlimt
            thstprin += hstprin
            tintpdyt += hstint
            tinterb += interb
            tintercg += intercgc
            
            f.write(f"{cnt:3d}  {cgcref:<15} {acctno:10.0f}  {name:<24} {apprlimt:>10,.2f} {balance:>10,.2f} "
                    f"{hstprin:>9,.2f} {hstint:>11,.2f} {interb:>11,.2f} {intercgc:>11,.2f}\n")
        
        f.write(f"\n {'-'*130}\n")
        f.write(f"{' '*56}{sapprlim:>12,.2f} {sbalance:>12,.2f} {shstprin:>12,.2f} {sintpdyt:>12,.2f} "
                f"{sinterb:>12,.2f} {sintercg:>12,.2f}\n")
        f.write(f" {'-'*130}\n")
        
        f.write(f"\n\n {'+'*130}\n")
        f.write(f"{' '*54}{bapprlim:>13,.2f}{' '*12}{bhstprin:>12,.2f}{' '*12}{binterb:>12,.2f}\n")
        f.write(f"{' '*67}{bbalance:>13,.2f}{' '*12}{bintpdyt:>12,.2f}{' '*12}{bintercg:>12,.2f}\n")
        f.write(f" {'+'*130}\n")
        f.write("\f")
    
    if len(df_tuk134) > 0:
        f.write(f"\n\n {'='*130}\n")
        f.write(f"{' '*54}{tapprlim:>13,.2f}{' '*12}{thstprin:>12,.2f}{' '*12}{tinterb:>12,.2f}\n")
        f.write(f"{' '*67}{tbalance:>13,.2f}{' '*12}{tintpdyt:>12,.2f}{' '*12}{tintercg:>12,.2f}\n")
        f.write(f" {'='*130}\n")

print(f"\nTUK Loan Reporting Complete")
print(f"\nOutput files generated:")
print(f"  {OUTPUT_RPT4} (TUK Package 1/3/4 Detail)")
print(f"\nData files:")
print(f"  {TEMP_DIR}CDRLNTUK.parquet")
print(f"  {TEMP_DIR}CDRTUK{reptmon}.parquet")
print(f"\nTUK Loan counts:")
print(f"  Package 1/3/4 (521/523/528): {len(df_tuk134)} accounts")
print(f"  Package 2 (522): {len(df_tuk2)} accounts")
print(f"  Total: {len(df_cdrlntuk)} accounts")
