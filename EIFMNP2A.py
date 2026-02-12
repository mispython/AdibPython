import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

LOAN_DIR = 'data/loan/'
CAP_DIR = 'data/cap/'
NPL6_DIR = 'data/npl6/'
OUTPUT_FILE = 'reports/npl_2month_closeattention.txt'
OUTPUT_RPS = 'reports/npl_2month_rps.txt'

HPD = [128, 130, 131, 132, 700, 705, 380, 381, 720, 725]

def lntyp_format(loantype):
    """Format loan type"""
    if loantype in [128, 130, 131, 132]:
        return 'HPD AITAB'
    elif loantype in [700, 705, 380, 381, 720, 725]:
        return 'HPD CONVENTIONAL'
    else:
        return 'OTHERS'

df_reptdate = pl.read_parquet(f'{LOAN_DIR}REPTDATE.parquet')
reptdate = df_reptdate['REPTDATE'][0]

prevdate = datetime(reptdate.year, reptdate.month, 1).date() - timedelta(days=1)
prevmon = f'{prevdate.month:02d}'
prevyear = f'{prevdate.year % 100:02d}'
reptmon = f'{reptdate.month:02d}'
rdate = reptdate.strftime('%d%m%y')

print(f"Processing 2 Month Close Attention NPL Report")
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

df_lnnote = df_lnnote.filter(
    (pl.col('REVERSED') != 'Y') &
    pl.col('NOTENO').is_not_null() &
    pl.col('NTBRCH').is_not_null() &
    (pl.col('PAIDIND') != 'P') &
    ~pl.col('BORSTAT').is_in(['F', 'R', 'I', 'Z']) &
    pl.col('LOANTYPE').is_in(HPD)
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
    
    colldesc = row.get('COLLDESC', '')
    new_row['MAKE'] = colldesc[:14] if colldesc else ''
    new_row['MODEL'] = colldesc[14:35] if len(colldesc) > 14 else ''
    
    bldate = row.get('BLDATE')
    issdte = new_row.get('ISSDTE')
    
    if bldate:
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
    
    if not row.get('BORSTAT') in ['F', 'I', 'R'] and 60 <= new_row.get('DAYS', 0) <= 89:
        processed.append(new_row)

df_loan2m = pl.DataFrame(processed)

df_loan2m.write_parquet(f'{NPL6_DIR}LOAN2M{reptmon}.parquet')

df_loan2m = df_loan2m.sort(['GUAREND', 'NETBAL'], descending=[False, True])

df_totname = df_loan2m.group_by('GUAREND').agg([
    pl.col('NETBAL').sum().alias('TOTNET'),
    pl.len().alias('_FREQ_')
]).with_columns([
    pl.lit('SUB-TOTAL').alias('SUBTOT')
])

grand_total = df_totname['TOTNET'].sum()
df_grand = pl.DataFrame({
    'SUBTOT': ['GRAND TOTAL'],
    'NETBAL': [grand_total],
    'GRANDTOT': [grand_total]
})

df_loanname = df_loan2m.join(
    df_totname.drop(['SUBTOT', 'NETBAL']), 
    on='GUAREND', 
    how='left'
)

df_totname_out = pl.concat([
    df_loanname,
    df_totname.filter(pl.col('_FREQ_') > 1)
]).sort(['TOTNET', 'GUAREND', 'SUBTOT'], descending=[True, False, False])

df_totname_out = pl.concat([df_totname_out, df_grand]).with_columns([
    pl.lit(rdate).alias('REPTDATE')
])

df_totname_out.write_parquet(f'{NPL6_DIR}TOTNAME.parquet')

with open(OUTPUT_FILE, 'w') as f:
    f.write(f"PUBLIC BANK - (CA IN 2 MONTHS) AS AT {rdate}\n")
    f.write(f"{'A/C NO.':<18}{'NOTE NO.':<10}{'NAME':<36}{' - ':<10}{'BRANCH':<9}{'DAYS':<6}"
            f"{'BORSTAT':<11}{'ISSUE DATE':<14}{'NETPROC':<20}{'NETBAL':<20}{'CAC NAME':<25}"
            f"{'REGION OFF':<20}{'MAKE':<15}{'MODEL':<40}{'DEALER NO':<10}{'DEALER NAME':<40}"
            f"{'ORI BRANCH':<11}{'CUSTFISS':<10}{'CAPCLOSE':<17}{'SCORE2'}\n")
    
    for row in df_totname_out.iter_rows(named=True):
        acctno = f"{row.get('ACCTNO', 0):12.0f}" if row.get('ACCTNO') else ' ' * 12
        noteno = f"{row.get('NOTENO', 0):8.0f}" if row.get('NOTENO') else ' ' * 8
        name = (row.get('NAME', '') or '')[:36]
        subtot = row.get('SUBTOT', '')
        branch = row.get('BRANCH', '')
        days = f"{row.get('DAYS', 0):4.0f}" if row.get('DAYS') else '    '
        borstat = row.get('BORSTAT', '')
        issdte = row.get('ISSDTE', '')
        if isinstance(issdte, datetime):
            issdte = issdte.strftime('%d/%m/%Y')
        netproc = f"{row.get('NETPROC', 0):13,.2f}" if row.get('NETPROC') else ' ' * 13
        netbal = f"{row.get('NETBAL', 0):13,.2f}"
        cacname = row.get('CACNAME', '')
        region = row.get('REGION', '')
        make = (row.get('MAKE', '') or '')[:15]
        model = (row.get('MODEL', '') or '')[:40]
        dealerno = row.get('DEALERNO', '')
        compname = (row.get('COMPNAME', '') or '')[:40]
        primofhp = row.get('PRIMOFHP', '')
        custfiss = row.get('CUSTFISS', '')
        cap = f"{row.get('CAP', 0):16,.2f}" if row.get('CAP') else ' ' * 16
        score2 = row.get('SCORE2', '')
        
        f.write(f"{acctno:<18}{noteno:<10}{name:<36}{subtot:<10}{branch:<9}{days:<6}"
                f"{borstat:<11}{issdte:<14}{netproc:>20}{netbal:>20}{cacname:<25}"
                f"{region:<20}{make:<15}{model:<40}{dealerno:<10}{compname:<40}"
                f"{primofhp:<11}{custfiss:<10}{cap:>17}{score2}\n")

df_rps2m = df_loan2m.with_columns([
    pl.lit(rdate).alias('REPTDATE')
]).sort(['BRANCH', 'NETBAL'], descending=[False, True])

with open(OUTPUT_RPS, 'w') as f:
    pagecnt = 0
    linecnt = 0
    brchamt = 0
    bnmamt = 0
    prev_branch = None
    
    for row in df_rps2m.iter_rows(named=True):
        branch = row['BRANCH']
        
        if branch != prev_branch:
            if prev_branch is not None:
                pass
            
            pagecnt = 1
            brchamt = 0
            linecnt = 7
            
            f.write(f"BRANCH    :  {branch:40}P U B L I C   B A N K   B E R H A D{' ' * 44}PAGE NO : {pagecnt}\n")
            f.write(f"REPORT NO :  2MTHNPL                  CLOSE ATTENTION IN 2 MONTHS {rdate}\n")
            f.write("\n\n")
            f.write(f"         {'A/C NO.':<12}{'NOTE NO.':<10}{'NAME':<36}{'BRANCH':<9}{'DAYS':<9}"
                    f"{'BORSTAT':<11}{'ISSUE DATE':<24}{'NETBAL':<65}{'MAKE':<14}{'MODEL'}\n")
            f.write(f"    {'─'*130}\n\n\n")
            
            prev_branch = branch
        
        netbal_val = row.get('NETBAL', 0)
        brchamt += netbal_val
        bnmamt += netbal_val
        
        acctno = f"{row.get('ACCTNO', 0):12.0f}"
        noteno = f"{row.get('NOTENO', 0):8.0f}"
        name = (row.get('NAME', '') or '')[:36]
        days = f"{row.get('DAYS', 0):4.0f}"
        borstat = row.get('BORSTAT', '')
        issdte = row.get('ISSDTE', '')
        if isinstance(issdte, datetime):
            issdte = issdte.strftime('%d/%m/%Y')
        netbal = f"{netbal_val:13,.2f}"
        
        f.write(f"    {acctno} {noteno} {name:<36} {branch:<9} {days:>4} {borstat:>7} {issdte:>10} {netbal:>39}\n")
        
        linecnt += 1
        
        if linecnt > 55:
            pagecnt += 1
            linecnt = 7
            f.write(f"\f")
            f.write(f"BRANCH    :  {branch:40}P U B L I C   B A N K   B E R H A D{' ' * 44}PAGE NO : {pagecnt}\n")
            f.write(f"REPORT NO :  2MTHNPL                  CLOSE ATTENTION IN 2 MONTHS {rdate}\n")
            f.write("\n\n")
            f.write(f"         {'A/C NO.':<12}{'NOTE NO.':<10}{'NAME':<36}{'BRANCH':<9}{'DAYS':<9}"
                    f"{'BORSTAT':<11}{'ISSUE DATE':<24}{'NETBAL':<65}{'MAKE':<14}{'MODEL'}\n")
            f.write(f"    {'─'*130}\n\n\n")

print(f"\n2 Month Close Attention NPL Report Complete")
print(f"\nOutput files generated:")
print(f"  {OUTPUT_FILE} (Master Report)")
print(f"  {OUTPUT_RPS} (RPS Branch Report)")
print(f"\nAccounts in 2-month close attention: {len(df_loan2m)}")
print(f"Total exposure: RM {df_loan2m['NETBAL'].sum():,.2f}")
print(f"\nFiltering criteria:")
print(f"  - Days in arrears: 60-89 days")
print(f"  - Loan types: HP/AITAB (128,130,131,132,700,705,380,381,720,725)")
print(f"  - Borrower status: Not F/R/I/Z")
print(f"  - Account status: Active (not paid)")
