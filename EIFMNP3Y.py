import polars as pl
from datetime import datetime, timedelta

LOAN_DIR = 'data/loan/'
CAP_DIR = 'data/cap/'
NPL6_DIR = 'data/npl6/'
NPLX_DIR = 'data/nplx/'
OUTPUT_FILE = 'reports/npl_3month.txt'
OUTPUT_RPS = 'reports/npl_3month_rps.txt'

HPD = [110, 115, 132, 135, 700, 705, 380, 381, 720, 725]
OTHER_LOANS = [15, 20, 71, 72]

def lntyp_format(loantype):
    """Format loan type"""
    if loantype in [110, 115, 132, 135]:
        return 'HPD AITAB'
    elif loantype in [700, 705, 380, 381, 720, 725]:
        return 'HPD CONVENTIONAL'
    else:
        return 'OTHERS'

def ndays_format(days):
    """Convert days to months in arrears"""
    if days < 30:
        return 0
    elif days < 60:
        return 1
    elif days < 90:
        return 2
    elif days < 120:
        return 3
    elif days < 150:
        return 4
    elif days < 180:
        return 5
    elif days < 210:
        return 6
    elif days < 240:
        return 7
    elif days < 270:
        return 8
    elif days < 300:
        return 9
    elif days < 330:
        return 10
    elif days < 360:
        return 11
    elif days < 720:
        return int(days / 30)
    else:
        return 24

df_reptdate = pl.read_parquet(f'{LOAN_DIR}REPTDATE.parquet')
reptdate = df_reptdate['REPTDATE'][0]

mm = reptdate.month
mm1 = mm - 1 if mm > 1 else 12

prevdate = datetime(reptdate.year, reptdate.month, 1).date() - timedelta(days=1)
prevmon = f'{prevdate.month:02d}'
prevyear = f'{prevdate.year % 100:02d}'
reptmon = f'{reptdate.month:02d}'
reptmon1 = f'{mm1:02d}'
rdate = reptdate.strftime('%d%m%y')

print(f"Processing 3 Month NPL Report")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Previous Month: {prevmon}/{prevyear}")

df_hpcomp = pl.read_parquet(f'{LOAN_DIR}HPCOMP.parquet').select([
    'DEALERNO', 'COMPNAME'
]).sort('DEALERNO')

df_lnnote = pl.read_parquet(f'{LOAN_DIR}LNNOTE.parquet').sort('DEALERNO')

df_lnnote = df_lnnote.join(df_hpcomp, on='DEALERNO', how='left')

df_cap = pl.read_parquet(f'{CAP_DIR}CAP{prevmon}{prevyear}.parquet').select([
    'ACCTNO', 'NOTENO', 'CAP'
]).sort(['ACCTNO', 'NOTENO'])

df_lnnote = df_lnnote.join(df_cap, on=['ACCTNO', 'NOTENO'], how='left')

df_loancomm = pl.read_parquet(f'{LOAN_DIR}LNCOMM.parquet').select([
    'ACCTNO', 'COMMNO', 'CORGAMT'
]).sort(['ACCTNO', 'COMMNO'])

all_loan_types = HPD + OTHER_LOANS

df_lnnote = df_lnnote.filter(
    (pl.col('REVERSED') != 'Y') &
    pl.col('NOTENO').is_not_null() &
    pl.col('NTBRCH').is_not_null() &
    (pl.col('PAIDIND') != 'P') &
    pl.col('LOANTYPE').is_in(all_loan_types)
).unique(subset=['ACCTNO', 'COMMNO'])

def calculate_next_bldate(bldate, issdte):
    """Calculate next billing date"""
    dd = issdte.day
    mm = bldate.month + 1
    yy = bldate.year
    
    if mm > 12:
        mm = 1
        yy += 1
    
    if mm == 2:
        if yy % 4 == 0:
            max_day = 29
        else:
            max_day = 28
    elif mm in [4, 6, 9, 11]:
        max_day = 30
    else:
        max_day = 31
    
    if dd > max_day:
        dd = max_day
    
    return datetime(yy, mm, dd).date()

processed = []
for row in df_lnnote.iter_rows(named=True):
    new_row = row.copy()
    
    if row.get('BORSTAT') in ['W', 'Z']:
        continue
    
    if row.get('FLAG1') == 'F':
        apprlimt = row.get('NETPROC', 0)
    else:
        commno = row['COMMNO']
        corgamt = df_loancomm.filter(
            (pl.col('ACCTNO') == row['ACCTNO']) & 
            (pl.col('COMMNO') == commno)
        )
        apprlimt = corgamt['CORGAMT'][0] if len(corgamt) > 0 else 0
    
    new_row['APPRLIMT'] = apprlimt
    
    if row.get('ISSUEDT'):
        issuedt_str = str(int(row['ISSUEDT'])).zfill(11)[:8]
        try:
            issdte = datetime.strptime(issuedt_str, '%m%d%Y').date()
            new_row['ISSDTE'] = issdte
        except:
            new_row['ISSDTE'] = None
    else:
        new_row['ISSDTE'] = None
    
    census_str = str(row.get('CENSUS', 0)).zfill(8)
    new_row['CENSUS9'] = census_str[6:7] if len(census_str) >= 7 else ''
    
    colldesc = row.get('COLLDESC', '')
    new_row['MAKE'] = colldesc[:14] if colldesc else ''
    new_row['MODEL'] = colldesc[14:35] if len(colldesc) > 14 else ''
    new_row['REGNO'] = colldesc[39:52] if len(colldesc) > 39 else ''
    
    bldate = row.get('BLDATE')
    issdte = new_row.get('ISSDTE')
    
    if bldate and bldate > 0:
        if row.get('BILPAY', 0) > 0:
            biltot = row.get('BILTOT', 0) or 0
            if biltot / row['BILPAY'] <= 0.01:
                if issdte:
                    bldate = calculate_next_bldate(bldate, issdte)
                    new_row['BLDATE'] = bldate
        
        days = (reptdate - bldate).days
        
        if row.get('OLDNOTEDAYARR', 0) > 0 and 98000 <= row.get('NOTENO', 0) <= 98999:
            if days < 0:
                days = 0
            days = days + row['OLDNOTEDAYARR']
        
        new_row['DAYS'] = days
    else:
        new_row['DAYS'] = 0
    
    if row.get('INTAMT', 0) > 0.1:
        new_row['TERMCHG'] = row['INTAMT'] + row.get('INTEARN2', 0)
    else:
        new_row['TERMCHG'] = 0
    
    new_row['NETBAL'] = row.get('BALANCE', 0)
    new_row['LOANTYP'] = lntyp_format(row['LOANTYPE'])
    
    prmoffhp = row.get('PRMOFFHP', 0)
    if prmoffhp:
        prmoffhp_str = str(int(prmoffhp)).zfill(5)
        new_row['PRIMOFHP'] = prmoffhp_str[2:5]
    else:
        new_row['PRIMOFHP'] = ''
    
    if (new_row.get('DAYS', 0) > 89 or 
        row.get('BORSTAT') in ['F', 'R', 'I'] or 
        row.get('USER5') == 'N'):
        processed.append(new_row)

df_loan3m = pl.DataFrame(processed).sort(['ACCTNO', 'NOTENO'])

df_sp2 = pl.read_parquet(f'{NPLX_DIR}SP2.parquet').drop('NETBAL').with_columns([
    pl.col('RECOVER').alias('RECOVER_SP2')
]).sort(['ACCTNO', 'NOTENO'])

df_iis = pl.read_parquet(f'{NPLX_DIR}IIS.parquet').select([
    'ACCTNO', 'NOTENO', 'TOTIIS', 'OISUSP', 'SUSPEND', 
    'RECOVER', 'RECC', 'OIRECV', 'OIRECC'
]).sort(['ACCTNO', 'NOTENO'])

df_loan3m = df_loan3m.join(df_sp2, on=['ACCTNO', 'NOTENO'], how='left')
df_loan3m = df_loan3m.join(df_iis, on=['ACCTNO', 'NOTENO'], how='left')

df_loan3m = df_loan3m.with_columns([
    (pl.col('SUSPEND').fill_null(0) + pl.col('OISUSP').fill_null(0)).alias('IISCHRG'),
    (pl.col('RECOVER').fill_null(0) + pl.col('RECC').fill_null(0) + 
     pl.col('OIRECV').fill_null(0) + pl.col('OIRECC').fill_null(0)).alias('IISWRTB'),
    pl.col('RECOVER_SP2').fill_null(0).alias('SAWRTB')
])

arrears_list = []
for days in df_loan3m['DAYS'].to_list():
    arrears = ndays_format(days)
    if arrears == 24:
        arrears = round(days / 30)
    arrears_list.append(arrears)

df_loan3m = df_loan3m.with_columns([
    pl.Series('ARREARS', arrears_list)
])

writeoff_loantypes = [981, 982, 983, 991, 992, 993, 984, 985, 994, 995, 678, 679, 698, 699]

df_loan3m = df_loan3m.with_columns([
    pl.when(pl.col('LOANTYPE').is_in(writeoff_loantypes))
    .then(999)
    .otherwise(pl.col('ARREARS'))
    .alias('ARREARS'),
    pl.when(pl.col('LOANTYPE').is_in(writeoff_loantypes))
    .then(0)
    .otherwise(pl.col('OSPRIN'))
    .alias('OSPRIN')
])

df_loan3m.write_parquet(f'{NPL6_DIR}LOAN3M{reptmon}.parquet')

df_loan3m = df_loan3m.sort(['GUAREND', 'NETBAL'], descending=[False, True])

agg_fields = ['NETBAL', 'NETPROC', 'TOTIIS', 'OSPRIN', 'MARKETVL', 'SP', 'SPPL', 'IISCHRG', 'IISWRTB', 'SAWRTB']

df_totname = df_loan3m.group_by('GUAREND').agg([
    pl.col(field).sum().alias(f'{field}T') for field in agg_fields
] + [pl.len().alias('_FREQ_')]).with_columns([
    pl.lit('SUB-TOTAL  ').alias('SUBTOT')
] + [
    pl.col(f'{field}T').alias(field) for field in agg_fields
])

grand_totals = {field: df_totname[f'{field}T'].sum() for field in agg_fields}
df_grand = pl.DataFrame({
    'SUBTOT': ['GRAND TOTAL'],
    **{field: [grand_totals[field]] for field in agg_fields}
})

df_loanname = df_loan3m.join(
    df_totname.drop(['SUBTOT'] + agg_fields), 
    on='GUAREND', 
    how='left'
)

df_totname_out = pl.concat([
    df_loanname,
    df_totname.filter(pl.col('_FREQ_') > 1)
]).sort(['NETBALT', 'GUAREND', 'SUBTOT'], descending=[True, False, False])

df_totnm = pl.concat([df_totname_out, df_grand])

df_totnm.write_parquet(f'{NPL6_DIR}TOTNM{reptmon}.parquet')

with open(OUTPUT_FILE, 'w') as f:
    f.write(" \n")
    f.write(f"PUBLIC FINANCE - (NPL IN 3 MONTHS) {rdate}\n")
    f.write(" \n")
    f.write(f"  {'ACCOUNT NO':<16}{'NOTE NO':<10}{'CUSTOMER NAME':<31}{'      ':<14}{'BRANCH':<11}{'DAYS':<6}"
            f"{'BORSTAT':<9}{'ISSUE DATE':<15}{'NET PROCEED':>24}{'NET BAL':>26}{'IIS':>18}"
            f"{'IIS CHARGE':>12}{'IIS WRITTEN BACK':>20}{'PRINCIPAL':>16}{'REALISABLE VALUE':>24}"
            f"{'SP MADE':>18}{'SP CHARGE':>15}{'SA WRITTEN BACK':>17}{'CLASS':>6}{'MTH PST DUE':>16}"
            f"{'SECTOR CODE':>13}{'MAKE':>15}{'MODEL':>17}{'CAC NAME':>23}{'REGION OFF':>15}"
            f"{'REG NO':>13}{'DEALER NO':>13}{'DEALER NAME':>29}{'ORI BRANCH':>11}"
            f"{'CUSTFISS':>9}{'MODELDES':>9}{'USER5':>7}{'CAPCLOSE':>17}{'SCORE2'}\n")
    f.write(" \n")
    
    for row in df_totnm.iter_rows(named=True):
        acctno = f"{row.get('ACCTNO', 0):.0f}" if row.get('ACCTNO') else ' ' * 10
        noteno = f"{row.get('NOTENO', 0):.0f}" if row.get('NOTENO') else ' ' * 5
        name = (row.get('NAME', '') or '')[:25]
        subtot = row.get('SUBTOT', '')
        branch = row.get('BRANCH', '')
        days = f"{row.get('DAYS', 0):.0f}" if row.get('DAYS') else ''
        borstat = row.get('BORSTAT', '')
        issdte = row.get('ISSDTE', '')
        if isinstance(issdte, datetime):
            issdte = issdte.strftime('%d/%m/%Y')
        
        netproc = f"{row.get('NETPROC', 0):13,.2f}"
        netbal = f"{row.get('NETBAL', 0):13,.2f}"
        totiis = f"{row.get('TOTIIS', 0):13,.2f}"
        iischrg = f"{row.get('IISCHRG', 0):13,.2f}"
        iiswrtb = f"{row.get('IISWRTB', 0):13,.2f}"
        osprin = f"{row.get('OSPRIN', 0):13,.2f}"
        marketvl = f"{row.get('MARKETVL', 0):13,.2f}"
        sp = f"{row.get('SP', 0):13,.2f}"
        sppl = f"{row.get('SPPL', 0):13,.2f}"
        sawrtb = f"{row.get('SAWRTB', 0):13,.2f}"
        risk = row.get('RISK', '')
        arrears = f"{row.get('ARREARS', 0):.0f}" if row.get('ARREARS') else ''
        sectorcd = row.get('SECTORCD', '')
        make = (row.get('MAKE', '') or '')[:15]
        model = (row.get('MODEL', '') or '')[:17]
        cacname = row.get('CACNAME', '')
        region = row.get('REGION', '')
        regno = (row.get('REGNO', '') or '')[:13]
        dealerno = row.get('DEALERNO', '')
        compname = (row.get('COMPNAME', '') or '')[:29]
        primofhp = row.get('PRIMOFHP', '')
        custfiss = row.get('CUSTFISS', '')
        modeldes = row.get('MODELDES', '')
        user5 = row.get('USER5', '')
        cap = f"{row.get('CAP', 0):16,.2f}" if row.get('CAP') else ' ' * 16
        score2 = row.get('SCORE2', '')
        
        f.write(f"  {acctno:<16}{noteno:<10}{name:<31}{subtot:<14}{branch:<11}{days:<6}"
                f"{borstat:<9}{issdte:<15}{netproc:>24}{netbal:>26}{totiis:>18}"
                f"{iischrg:>12}{iiswrtb:>20}{osprin:>16}{marketvl:>24}"
                f"{sp:>18}{sppl:>15}{sawrtb:>17}{risk:>6}{arrears:>16}"
                f"{sectorcd:>13}{make:>15}{model:>17}{cacname:>23}{region:>15}"
                f"{regno:>13}{dealerno:>13}{compname:>29}{primofhp:>11}"
                f"{custfiss:>9}{modeldes:>9}{user5:>7}{cap:>17}{score2}\n")

print(f"\n3 Month NPL Report Complete")
print(f"\nOutput files generated:")
print(f"  {OUTPUT_FILE} (Master Report)")
print(f"  {NPL6_DIR}LOAN3M{reptmon}.parquet")
print(f"  {NPL6_DIR}TOTNM{reptmon}.parquet")
print(f"\nNPL accounts (>89 days): {len(df_loan3m)}")
print(f"Total exposure: RM {df_loan3m['NETBAL'].sum():,.2f}")
print(f"\nFiltering criteria:")
print(f"  - Days in arrears: >89 days OR")
print(f"  - Borrower status: F/R/I OR")
print(f"  - USER5: N")
print(f"  - Loan types: HP/AITAB + Other ({', '.join(map(str, all_loan_types))})")
print(f"  - Excluded: W/Z borrower status")
