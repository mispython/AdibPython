import polars as pl
from datetime import datetime

MNITB_REPTDATE = 'data/mnitb/reptdate.parquet'
BRHFILE = 'data/brhfile.parquet'
MIS_DIR = 'data/mis/'
ISR01 = 'data/isr01.txt'

df_reptdate = pl.read_parquet(MNITB_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

reptyear = reptdate.strftime('%Y')
reptyr = reptdate.strftime('%y')
reptmon = reptdate.strftime('%m')
reptday = reptdate.strftime('%d')
rdate = int(reptdate.strftime('%y%j'))
reptdt = reptdate.strftime('%d/%m/%Y')

df_brhdata = pl.read_parquet(BRHFILE)
df_brhdata = df_brhdata.with_columns([
    pl.when(pl.col('BRANCH').is_in([101, 279]))
      .then(pl.lit('C'))
      .otherwise(pl.col('STATUS'))
      .alias('STATUS')
]).sort(['BRANCH', 'BRABBR'])

df_sabal = pl.read_parquet(f'{MIS_DIR}DYPOSXBR{reptmon}.parquet')
df_sabal = df_sabal.filter(pl.col('REPTDATE') == rdate).sort('BRANCH')

df_merged = df_sabal.join(df_brhdata, on='BRANCH', how='left')

df_merged = df_merged.with_columns([
    pl.col('TOTSAVG').fill_null(0),
    pl.col('TOTSAVGI').fill_null(0)
])

df_merged = df_merged.with_columns([
    (pl.col('TOTSAVG') + pl.col('TOTSAVGI')).alias('TOTSA'),
    (pl.col('SACNT') + pl.col('ISACNT')).alias('TOTCNT')
])

df_merged = df_merged.with_columns([
    ((pl.col('TOTSA') / 1000).round(0)).cast(pl.Int64).alias('TOTSAML')
])

df_allsa = df_merged
df_sabal_open = df_merged.filter(pl.col('STATUS') == 'O')
df_excep = df_merged.filter(pl.col('STATUS') != 'O')

def format_number(val, decimals=0):
    if val is None:
        return '0' if decimals == 0 else '0.00'
    if decimals == 0:
        return f'{int(val):,}'
    else:
        return f'{float(val):,.2f}'

lines = []

lines.append('REPORT ID : EIMISR01')
lines.append(f"MONTHLY SAVINGS DEPOSITS OUTSTANDING BALANCE & ACCOUNTS BY BRANCH @ {reptdt}")
lines.append('RETAIL FINANCIAL SERVICES - SALES ADMINISTRATION & SUPPORT')
lines.append(' ')

header1 = (
    f"{'':>2};{'':>12};{'':>12};{'':>21};"
    f"{'OUTSTANDING BALANCE':>20};{'':>21};{'':>21};{'':>21};"
    f"{'NO. OF OUSTANDING ACCOUNTS':>41};;"
)
lines.append(header1)

header2 = (
    f"{'':>2};{'':>12};{'':>12};{'':>21};"
    f"{'(RM)':>20};{'':>21};{'':>21};{'':>21};"
    f"{'(ALL BALANCES)':>20};{'':>21};"
)
lines.append(header2)

header3 = (
    f"{'NO':>14};{'BRANCH ABBV.':>12};{'BRANCH CODE':>11};"
    f"{'PBB':>20};{'PIBB':>20};{'TOTAL':>20};{'TOTAL (RM \\'000)':>20};"
    f"{'PBB':>20};{'PIBB':>20};{'TOTAL':>20};"
)
lines.append(header3)

header4 = (
    f"{'':>2};{'':>12};{'':>12};"
    f"{'(A)':>20};{'(B)':>20};{'(C)=(A)+(B)':>20};{'':>21};"
    f"{'(D)':>20};{'(E)':>20};{'(F)=(D)+(E)':>20};"
)
lines.append(header4)

xtotsavg = 0
xtotsavgi = 0
xtotsa = 0
xtotsaml = 0
xsacnt = 0
xisacnt = 0
xtotcnt = 0

for idx, row in enumerate(df_sabal_open.iter_rows(named=True), 1):
    line = (
        f"{idx:>3};{row['BRABBR']:>12};{row['BRANCH']:03d};"
        f"{format_number(row['TOTSAVG'], 2):>20};"
        f"{format_number(row['TOTSAVGI'], 2):>20};"
        f"{format_number(row['TOTSA'], 2):>20};"
        f"{format_number(row['TOTSAML'], 0):>20};"
        f"{format_number(row['SACNT'], 0):>20};"
        f"{format_number(row['ISACNT'], 0):>20};"
        f"{format_number(row['TOTCNT'], 0):>20};"
    )
    lines.append(line)
    
    xtotsavg += row['TOTSAVG']
    xtotsavgi += row['TOTSAVGI']
    xtotsa += row['TOTSA']
    xtotsaml += row['TOTSAML']
    xsacnt += row['SACNT']
    xisacnt += row['ISACNT']
    xtotcnt += row['TOTCNT']

total_a_line = (
    f"{'':>2};{'TOTAL (A)':>12};{'':>12};"
    f"{format_number(xtotsavg, 2):>20};"
    f"{format_number(xtotsavgi, 2):>20};"
    f"{format_number(xtotsa, 2):>20};"
    f"{format_number(xtotsaml, 0):>20};"
    f"{format_number(xsacnt, 0):>20};"
    f"{format_number(xisacnt, 0):>20};"
    f"{format_number(xtotcnt, 0):>20};"
)
lines.append(total_a_line)

if len(df_excep) == 0:
    lines.append(f"{'':>2};EXCEPTIONAL REPORT")
    lines.append(f"{'':>2};(CLOSED/MERGED BRANCHES)")
    lines.append(f"{'':>2};TOTAL (B)")
else:
    lines.append(f"{'':>2};EXCEPTIONAL REPORT")
    lines.append(f"{'':>2};(CLOSED/MERGED BRANCHES)")
    
    extotsavg = 0
    extotsavgi = 0
    extotsa = 0
    extotsaml = 0
    exsacnt = 0
    exisacnt = 0
    extotcnt = 0
    
    for idx, row in enumerate(df_excep.iter_rows(named=True), 1):
        line = (
            f"{idx:>3};{row['BRABBR']:>12};{row['BRANCH']:03d};"
            f"{format_number(row['TOTSAVG'], 2):>20};"
            f"{format_number(row['TOTSAVGI'], 2):>20};"
            f"{format_number(row['TOTSA'], 2):>20};"
            f"{format_number(row['TOTSAML'], 0):>20};"
            f"{format_number(row['SACNT'], 0):>20};"
            f"{format_number(row['ISACNT'], 0):>20};"
            f"{format_number(row['TOTCNT'], 0):>20};"
        )
        lines.append(line)
        
        extotsavg += row['TOTSAVG']
        extotsavgi += row['TOTSAVGI']
        extotsa += row['TOTSA']
        extotsaml += row['TOTSAML']
        exsacnt += row['SACNT']
        exisacnt += row['ISACNT']
        extotcnt += row['TOTCNT']
    
    total_b_line = (
        f"{'':>2};{'TOTAL (B)':>12};{'':>12};"
        f"{format_number(extotsavg, 2):>20};"
        f"{format_number(extotsavgi, 2):>20};"
        f"{format_number(extotsa, 2):>20};"
        f"{format_number(extotsaml, 0):>20};"
        f"{format_number(exsacnt, 0):>20};"
        f"{format_number(exisacnt, 0):>20};"
        f"{format_number(extotcnt, 0):>20};"
    )
    lines.append(total_b_line)

df_allsa_sum = df_allsa.select([
    pl.col('TOTSAVG').sum().alias('TOTSAVG'),
    pl.col('TOTSAVGI').sum().alias('TOTSAVGI'),
    pl.col('TOTSA').sum().alias('TOTSA'),
    pl.col('TOTSAML').sum().alias('TOTSAML'),
    pl.col('SACNT').sum().alias('SACNT'),
    pl.col('ISACNT').sum().alias('ISACNT'),
    pl.col('TOTCNT').sum().alias('TOTCNT')
])

grand_total = df_allsa_sum.to_dicts()[0]
grand_total_line = (
    f"{'':>2};{'GRAND TOTAL (A)+(B)':>23};;"
    f"{format_number(grand_total['TOTSAVG'], 2):>20};"
    f"{format_number(grand_total['TOTSAVGI'], 2):>20};"
    f"{format_number(grand_total['TOTSA'], 2):>20};"
    f"{format_number(grand_total['TOTSAML'], 0):>20};"
    f"{format_number(grand_total['SACNT'], 0):>20};"
    f"{format_number(grand_total['ISACNT'], 0):>20};"
    f"{format_number(grand_total['TOTCNT'], 0):>20};"
)
lines.append(grand_total_line)

with open(ISR01, 'w') as f:
    for line in lines:
        f.write(line + '\n')

print(f"Report generated: {ISR01}")
