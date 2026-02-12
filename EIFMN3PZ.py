import polars as pl
from datetime import datetime

LOAN_DIR = 'data/loan/'
NPL6_DIR = 'data/npl6/'
NPLX_DIR = 'data/nplx/'
OUTPUT_FILE = 'reports/npl_newly_surfaced.txt'
OUTPUT_RPS = 'reports/npl_newly_surfaced_rps.txt'

def lntyp_format(loantype):
    """Format loan type"""
    if loantype in [128, 130, 131, 132]:
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
reptmon = f'{reptdate.month:02d}'
reptmon1 = f'{mm1:02d}'
rdate = reptdate.strftime('%d%m%y')

print(f"Processing Newly Surfaced NPL Report")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Comparing: Month {reptmon} vs Month {reptmon1}")

df_thismth = pl.read_parquet(f'{NPL6_DIR}LOAN3M{reptmon}.parquet').sort('ACCTNO')
df_lastmth = pl.read_parquet(f'{NPL6_DIR}LOAN3M{reptmon1}.parquet').sort('ACCTNO')

df_newca = df_thismth.join(
    df_lastmth.select(['ACCTNO']), 
    on='ACCTNO', 
    how='anti'
)

df_newca = df_newca.sort(['ACCTNO', 'NOTENO'])

df_sp2 = pl.read_parquet(f'{NPLX_DIR}SP2.parquet').drop('NETBAL').sort(['ACCTNO', 'NOTENO'])

df_iis = pl.read_parquet(f'{NPLX_DIR}IIS.parquet').select([
    'ACCTNO', 'NOTENO', 'TOTIIS', 'OISUSP', 'SUSPEND'
]).sort(['ACCTNO', 'NOTENO'])

df_newca = df_newca.join(df_sp2, on=['ACCTNO', 'NOTENO'], how='left')
df_newca = df_newca.join(df_iis, on=['ACCTNO', 'NOTENO'], how='left')

df_newca = df_newca.with_columns([
    (pl.col('SUSPEND').fill_null(0) + pl.col('OISUSP').fill_null(0)).alias('IISCHRG')
])

arrears_list = []
for row in df_newca.iter_rows(named=True):
    days = row.get('DAYS', 0)
    arrears = ndays_format(days)
    
    if arrears == 24:
        arrears = round(days / 30) if days else 0
    
    loantype = row.get('LOANTYPE', 0)
    if loantype in [981, 982, 983, 991, 992, 993, 984, 985, 994, 995, 678, 679, 698, 699]:
        arrears = 999
    
    arrears_list.append(arrears)

df_newca = df_newca.with_columns([
    pl.Series('ARREARS', arrears_list)
])

writeoff_loantypes = [981, 982, 983, 991, 992, 993, 984, 985, 994, 995, 678, 679, 698, 699]

df_newca = df_newca.with_columns([
    pl.when(pl.col('LOANTYPE').is_in(writeoff_loantypes))
    .then(0)
    .otherwise(pl.col('OSPRIN'))
    .alias('OSPRIN')
])

df_newca = df_newca.sort('GUAREND')

agg_fields = ['NETBAL', 'SPPL', 'IISCHRG']

df_totname = df_newca.group_by('GUAREND').agg([
    pl.col('NETBAL').sum().alias('TOTNET'),
    pl.col('SPPL').sum().alias('SPPLT'),
    pl.col('IISCHRG').sum().alias('IISCHRGT'),
    pl.len().alias('_FREQ_')
]).with_columns([
    pl.lit('SUB-TOTAL  ').alias('SUBTOT'),
    pl.col('TOTNET').alias('NETBAL'),
    pl.col('SPPLT').alias('SPPL'),
    pl.col('IISCHRGT').alias('IISCHRG')
])

grand_total = df_totname['TOTNET'].sum()
grand_sppl = df_totname['SPPLT'].sum()
grand_iischrg = df_totname['IISCHRGT'].sum()

df_grand = pl.DataFrame({
    'SUBTOT': ['GRAND TOTAL'],
    'NETBAL': [grand_total],
    'SPPL': [grand_sppl],
    'IISCHRG': [grand_iischrg],
    'GRANDTOT': [grand_total],
    'SPPLG': [grand_sppl],
    'IISCHRGG': [grand_iischrg]
})

df_loanname = df_newca.join(
    df_totname.drop(['SUBTOT', 'NETBAL', 'SPPL', 'IISCHRG']), 
    on='GUAREND', 
    how='left'
)

df_loanname = df_loanname.with_columns([
    pl.when(pl.col('ISSDTE').is_not_null())
    .then(pl.col('ISSDTE').dt.strftime('%d/%m/%Y'))
    .otherwise(None)
    .alias('ISSDTE_STR')
])

df_totname_out = pl.concat([
    df_loanname,
    df_totname.filter(pl.col('_FREQ_') > 1)
]).sort(['TOTNET', 'GUAREND', 'SUBTOT'], descending=[True, False, False])

df_totnmn = pl.concat([df_totname_out, df_grand])

df_totnmn.write_parquet(f'{NPL6_DIR}TOTNMN{reptmon}.parquet')

with open(OUTPUT_FILE, 'w') as f:
    f.write(" \n")
    f.write(f"PUBLIC BANK - (NEWLY SURFACE NPL) AS AT {rdate}\n")
    f.write(" \n")
    f.write(f"  {'ACCOUNT NO':<16}{'NOTE NO':<10}{'CUSTOMER NAME':<31}{'      ':<14}{'BRANCH':<11}{'DAYS':<6}"
            f"{'BORSTAT':<9}{'ISSUE DATE':<15}{'NET PROCEED':>24}{'NET BAL':>26}{'IIS':>18}"
            f"{'IIS CHARGE':>16}{'PRINCIPAL':>16}{'REALISABLE VALUE':>24}{'SP MADE':>18}"
            f"{'SP CHARGE':>16}{'CLASS':>14}{'MTH PST DUE':>14}{'SECTOR CODE':>15}{'MAKE':>16}"
            f"{'MODEL':>23}{'CAC NAME':>20}{'REGION OFF':>15}{'REG NO':>15}{'DEALER NO':>13}"
            f"{'DEALER NAME':>29}{'ORI BRANCH':>11}{'CUSTFISS':>9}{'MODELDES':>9}{'USER5':>7}"
            f"{'CAPCLOSE':>17}{'SCORE2'}\n")
    f.write(" \n")
    
    for row in df_totnmn.iter_rows(named=True):
        acctno = f"{row.get('ACCTNO', 0):.0f}" if row.get('ACCTNO') else ' ' * 10
        noteno = f"{row.get('NOTENO', 0):.0f}" if row.get('NOTENO') else ' ' * 5
        name = (row.get('NAME', '') or '')[:25]
        subtot = row.get('SUBTOT', '')
        branch = row.get('BRANCH', '')
        days = f"{row.get('DAYS', 0):.0f}" if row.get('DAYS') else ''
        borstat = row.get('BORSTAT', '')
        issdte = row.get('ISSDTE_STR', '') or row.get('ISSDTE', '')
        if isinstance(issdte, datetime):
            issdte = issdte.strftime('%d/%m/%Y')
        
        netproc = f"{row.get('NETPROC', 0):13,.2f}"
        netbal = f"{row.get('NETBAL', 0):13,.2f}"
        totiis = f"{row.get('TOTIIS', 0):13,.2f}"
        iischrg = f"{row.get('IISCHRG', 0):13,.2f}"
        osprin = f"{row.get('OSPRIN', 0):13,.2f}"
        marketvl = f"{row.get('MARKETVL', 0):13,.2f}"
        sp = f"{row.get('SP', 0):13,.2f}"
        sppl = f"{row.get('SPPL', 0):13,.2f}"
        risk = row.get('RISK', '')
        arrears = f"{row.get('ARREARS', 0):.0f}" if row.get('ARREARS') else ''
        sectorcd = row.get('SECTORCD', '')
        make = (row.get('MAKE', '') or '')[:16]
        model = (row.get('MODEL', '') or '')[:23]
        cacname = row.get('CACNAME', '')
        region = row.get('REGION', '')
        regno = (row.get('REGNO', '') or '')[:15]
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
                f"{iischrg:>16}{osprin:>16}{marketvl:>24}{sp:>18}"
                f"{sppl:>16}{risk:>14}{arrears:>14}{sectorcd:>15}{make:>16}"
                f"{model:>23}{cacname:>20}{region:>15}{regno:>15}{dealerno:>13}"
                f"{compname:>29}{primofhp:>11}{custfiss:>9}{modeldes:>9}{user5:>7}"
                f"{cap:>17}{score2}\n")

df_rps3m = df_newca.with_columns([
    pl.when(pl.col('ISSDTE').is_not_null())
    .then(pl.col('ISSDTE').dt.strftime('%d/%m/%Y'))
    .otherwise(None)
    .alias('ISSDTE_STR')
]).sort(['BRANCH', 'NETBAL'], descending=[False, True])

with open(OUTPUT_RPS, 'w') as f:
    f.write(f"PUBLIC BANK - (NEWLY SURFACE NPL) AS AT {rdate}\n\n")
    f.write(f"{'BRANCH':<10}{'NAME':<25}{'A/C NO.':<12}{'NOTE NO':<10}{'DAYS':<7}{'BORSTAT':<9}"
            f"{'ISSUE DATE':<15}{'NET BALANCE':>18}{'NET PROCEED':>18}{'TOTAL IIS':>16}"
            f"{'IIS CHARGE':>16}{'BALANCE':>18}{'MARKET VALUE':>18}{'SP':>16}"
            f"{'SA CHARGE':>16}{'RISK':<8}{'ARREARS':<10}{'SECTOR':<8}\n")
    f.write("─" * 200 + "\n\n")
    
    current_branch = None
    branch_netbal = 0
    branch_netproc = 0
    branch_totiis = 0
    branch_iischrg = 0
    branch_osprin = 0
    branch_marketvl = 0
    branch_sp = 0
    branch_sppl = 0
    
    for row in df_rps3m.iter_rows(named=True):
        branch = row.get('BRANCH', '')
        
        if branch != current_branch:
            if current_branch is not None:
                f.write(f"\n{'─'*200}\n")
                f.write(f"{'SUBTOTAL':<35}{' '*35}{branch_netbal:>18,.2f}{branch_netproc:>18,.2f}"
                        f"{branch_totiis:>16,.2f}{branch_iischrg:>16,.2f}{branch_osprin:>18,.2f}"
                        f"{branch_marketvl:>18,.2f}{branch_sp:>16,.2f}{branch_sppl:>16,.2f}\n")
                f.write(f"{'─'*200}\n\n")
                f.write("\f")
                
                f.write(f"PUBLIC BANK - (NEWLY SURFACE NPL) AS AT {rdate}\n\n")
                f.write(f"{'BRANCH':<10}{'NAME':<25}{'A/C NO.':<12}{'NOTE NO':<10}{'DAYS':<7}{'BORSTAT':<9}"
                        f"{'ISSUE DATE':<15}{'NET BALANCE':>18}{'NET PROCEED':>18}{'TOTAL IIS':>16}"
                        f"{'IIS CHARGE':>16}{'BALANCE':>18}{'MARKET VALUE':>18}{'SP':>16}"
                        f"{'SA CHARGE':>16}{'RISK':<8}{'ARREARS':<10}{'SECTOR':<8}\n")
                f.write("─" * 200 + "\n\n")
            
            current_branch = branch
            branch_netbal = 0
            branch_netproc = 0
            branch_totiis = 0
            branch_iischrg = 0
            branch_osprin = 0
            branch_marketvl = 0
            branch_sp = 0
            branch_sppl = 0
            
            f.write(f"BRANCH : {branch}\n\n")
        
        name = (row.get('NAME', '') or '')[:25]
        acctno = f"{row.get('ACCTNO', 0):10.0f}"
        noteno = f"{row.get('NOTENO', 0):7.0f}"
        days = f"{row.get('DAYS', 0):5.0f}"
        borstat = row.get('BORSTAT', '')
        issdte = row.get('ISSDTE_STR', '')
        
        netbal = row.get('NETBAL', 0)
        netproc = row.get('NETPROC', 0)
        totiis = row.get('TOTIIS', 0)
        iischrg = row.get('IISCHRG', 0)
        osprin = row.get('OSPRIN', 0)
        marketvl = row.get('MARKETVL', 0)
        sp = row.get('SP', 0)
        sppl = row.get('SPPL', 0)
        risk = row.get('RISK', '')
        arrears = f"{row.get('ARREARS', 0):.0f}"
        sectorcd = row.get('SECTORCD', '')
        
        branch_netbal += netbal
        branch_netproc += netproc
        branch_totiis += totiis
        branch_iischrg += iischrg
        branch_osprin += osprin
        branch_marketvl += marketvl
        branch_sp += sp
        branch_sppl += sppl
        
        f.write(f"{branch:<10}{name:<25}{acctno:<12}{noteno:<10}{days:>5}  {borstat:<7}  "
                f"{issdte:<15}{netbal:>18,.2f}{netproc:>18,.2f}{totiis:>16,.2f}"
                f"{iischrg:>16,.2f}{osprin:>18,.2f}{marketvl:>18,.2f}{sp:>16,.2f}"
                f"{sppl:>16,.2f}{risk:<8}{arrears:<10}{sectorcd:<8}\n")
    
    if current_branch is not None:
        f.write(f"\n{'─'*200}\n")
        f.write(f"{'SUBTOTAL':<35}{' '*35}{branch_netbal:>18,.2f}{branch_netproc:>18,.2f}"
                f"{branch_totiis:>16,.2f}{branch_iischrg:>16,.2f}{branch_osprin:>18,.2f}"
                f"{branch_marketvl:>18,.2f}{branch_sp:>16,.2f}{branch_sppl:>16,.2f}\n")
        f.write(f"{'─'*200}\n")

print(f"\nNewly Surfaced NPL Report Complete")
print(f"\nOutput files generated:")
print(f"  {OUTPUT_FILE} (Master Report)")
print(f"  {OUTPUT_RPS} (RPS Branch Report)")
print(f"  {NPL6_DIR}TOTNMN{reptmon}.parquet")
print(f"\nNewly surfaced NPL accounts: {len(df_newca)}")
print(f"Total exposure: RM {df_newca['NETBAL'].sum():,.2f}")
print(f"\nIdentification criteria:")
print(f"  - Accounts in {reptmon} NPL file")
print(f"  - NOT in {reptmon1} NPL file")
print(f"  - Therefore: newly classified as NPL this month")
