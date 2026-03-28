"""
EIFMNP3Z - Newly Surfaced NPL Report
Provides additional fields (REGNO, IIS & SP CHARGE) in HP Account Reports
"""

import polars as pl
from datetime import datetime, timedelta
from calendar import monthrange
import os

LOAN_DIR = 'data/loan/'
NPL6_DIR = 'data/npl6/'
NPLX_DIR = 'data/nplx/'
OUTPUT_DIR = 'data/output/'

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 80)
print("EIFMNP3Z - NEWLY SURFACED NPL REPORT")
print("PUBLIC BANK - HP ACCOUNT CONVENTIONAL")
print("=" * 80)

try:
    df_reptdate = pl.read_parquet(f'{LOAN_DIR}REPTDATE.parquet')
    reptdate = df_reptdate['REPTDATE'][0]
    
    if isinstance(reptdate, str):
        reptdate = datetime.strptime(reptdate, '%Y-%m-%d')
    
    mm = reptdate.month
    mm1 = mm - 1
    if mm1 == 0:
        mm1 = 12
    
    first_of_month = datetime(reptdate.year, reptdate.month, 1)
    prevdate = first_of_month - timedelta(days=1)
    
    prevmon = str(prevdate.month).zfill(2)
    prevyear = str(prevdate.year)[-2:]
    reptmon = str(reptdate.month).zfill(2)
    reptmon1 = str(mm1).zfill(2)
    rdate = reptdate.strftime('%d/%m/%y')
    
    print(f"\nReport Date: {reptdate.strftime('%d %B %Y')} ({rdate})")
    print(f"Current Month: {reptmon}")
    print(f"Previous Month: {reptmon1}")

except Exception as e:
    print(f"✗ ERROR reading REPTDATE: {e}")
    import sys
    sys.exit(1)

print("\n" + "=" * 80)

print("\n1. Reading NPL data...")
try:
    df_thismth = pl.read_parquet(f'{NPL6_DIR}LOAN3M{reptmon}.parquet')
    df_thismth = df_thismth.sort('ACCTNO')
    print(f"  ✓ This month: {len(df_thismth):,} records")
except Exception as e:
    print(f"  ✗ This month: {e}")
    import sys
    sys.exit(1)

try:
    df_lastmth = pl.read_parquet(f'{NPL6_DIR}LOAN3M{reptmon1}.parquet')
    df_lastmth = df_lastmth.sort('ACCTNO')
    print(f"  ✓ Last month: {len(df_lastmth):,} records")
except Exception as e:
    print(f"  ✗ Last month: {e}")
    df_lastmth = pl.DataFrame({'ACCTNO': []})

print("\n2. Identifying newly surfaced NPL...")
df_newca = df_thismth.join(
    df_lastmth.select(['ACCTNO']).with_columns([pl.lit(True).alias('IN_LAST')]),
    on='ACCTNO',
    how='left'
)
df_newca = df_newca.filter(pl.col('IN_LAST').is_null())
df_newca = df_newca.drop('IN_LAST')
df_newca = df_newca.sort(['ACCTNO', 'NOTENO'])

print(f"  ✓ Newly surfaced: {len(df_newca):,} accounts")

print("\n3. Reading SP and IIS data...")
try:
    df_sp2 = pl.read_parquet(f'{NPLX_DIR}SP2.parquet')
    df_sp2 = df_sp2.drop('NETBAL') if 'NETBAL' in df_sp2.columns else df_sp2
    df_sp2 = df_sp2.sort(['ACCTNO', 'NOTENO'])
    print(f"  ✓ SP2: {len(df_sp2):,} records")
except Exception as e:
    print(f"  ⚠ SP2: {e}")
    df_sp2 = pl.DataFrame({'ACCTNO': [], 'NOTENO': []})

try:
    df_iis = pl.read_parquet(f'{NPLX_DIR}IIS.parquet')
    df_iis = df_iis.select(['ACCTNO', 'NOTENO', 'TOTIIS', 'OISUSP', 'SUSPEND'])
    df_iis = df_iis.sort(['ACCTNO', 'NOTENO'])
    print(f"  ✓ IIS: {len(df_iis):,} records")
except Exception as e:
    print(f"  ⚠ IIS: {e}")
    df_iis = pl.DataFrame({'ACCTNO': [], 'NOTENO': [], 'TOTIIS': [], 
                           'OISUSP': [], 'SUSPEND': []})

print("\n4. Merging data...")
df_newca = df_newca.join(df_sp2, on=['ACCTNO', 'NOTENO'], how='left')
df_newca = df_newca.join(df_iis, on=['ACCTNO', 'NOTENO'], how='left')

df_newca = df_newca.with_columns([
    (pl.col('SUSPEND').fill_null(0) + pl.col('OISUSP').fill_null(0)).alias('IISCHRG')
])

special_loantypes = [981, 982, 983, 991, 992, 993, 984, 985, 994, 995,
                     678, 679, 698, 699]

df_newca = df_newca.with_columns([
    pl.when(pl.col('LOANTYPE').is_in(special_loantypes))
      .then(pl.lit(999))
      .otherwise(pl.col('ARREARS'))
      .alias('ARREARS')
])

df_newca = df_newca.with_columns([
    pl.when(pl.col('LOANTYPE').is_in(special_loantypes))
      .then(pl.lit(0))
      .otherwise(pl.col('OSPRIN'))
      .alias('OSPRIN')
])

print("\n5. Calculating totals...")
df_newca = df_newca.sort('GUAREND')

df_totname = df_newca.group_by('GUAREND').agg([
    pl.col('NETBAL').sum().alias('TOTNET'),
    pl.col('SPPL').sum().alias('SPPLT'),
    pl.col('IISCHRG').sum().alias('IISCHRGT')
])

df_grand = df_totname.select([
    pl.col('TOTNET').sum().alias('GRANDTOT'),
    pl.col('SPPLT').sum().alias('SPPLG'),
    pl.col('IISCHRGT').sum().alias('IISCHRGG')
])

df_totname = df_totname.with_columns([
    pl.lit('SUB-TOTAL  ').alias('SUBTOT')
])

df_grand = df_grand.with_columns([
    pl.lit('GRAND TOTAL').alias('SUBTOT'),
    pl.col('GRANDTOT').alias('NETBAL'),
    pl.col('SPPLG').alias('SPPL'),
    pl.col('IISCHRGG').alias('IISCHRG')
])

print("\n6. Generating reports...")

df_rps = df_newca.sort(['BRANCH', 'NETBAL'], descending=[False, True])

with open(f'{OUTPUT_DIR}NPLHP3N_RPS.txt', 'w') as f:
    branches = df_rps['BRANCH'].unique().sort().to_list()
    
    for branch in branches:
        df_branch = df_rps.filter(pl.col('BRANCH') == branch)
        
        f.write(f"PUBLIC BANK - (NEWLY SURFACE NPL) AS AT {rdate}\n\n")
        f.write(f"BRANCH : {branch:03d}\n\n")
        
        f.write(f"{'A/C NO.':>10} {'NOTE NO':>7} {'NAME':<25} {'DAYS':>5} {'BORSTAT':<7} ")
        f.write(f"{'ISSUE DATE':<10} {'NET BALANCE':>15} {'NET PROCEED':>15} ")
        f.write(f"{'TOTAL IIS':>13} {'IIS CHARGE':>13} {'BALANCE':>15} ")
        f.write(f"{'MARKET VALUE':>15} {'SP':>13} {'SA CHARGE':>13} ")
        f.write(f"{'RISK':>4} {'ARREARS':>7} {'SECTOR':<6}\n")
        f.write("-" * 200 + "\n")
        
        for row in df_branch.iter_rows(named=True):
            issdte = row.get('ISSDTE', '') or ''
            
            f.write(f"{row['ACCTNO']:10.0f} {row['NOTENO']:7.0f} {row['NAME']:<25} ")
            f.write(f"{row['DAYS']:5.0f} {row['BORSTAT']:<7} {issdte:<10} ")
            f.write(f"{row['NETBAL']:15,.2f} {row['NETPROC']:15,.2f} ")
            f.write(f"{row.get('TOTIIS', 0):13,.2f} {row.get('IISCHRG', 0):13,.2f} ")
            f.write(f"{row['OSPRIN']:15,.2f} {row['MARKETVL']:15,.2f} ")
            f.write(f"{row.get('SP', 0):13,.2f} {row.get('SPPL', 0):13,.2f} ")
            f.write(f"{row.get('RISK', ''):>4} {row.get('ARREARS', 0):7.0f} ")
            f.write(f"{row.get('SECTORCD', ''):<6}\n")
        
        f.write("\f")

print(f"  ✓ Generated: NPLHP3N_RPS.txt")

df_final = pl.concat([df_newca, df_totname.drop(['TOTNET', 'SPPLT', 'IISCHRGT'])])
df_final = df_final.sort(['TOTNET', 'GUAREND', 'SUBTOT'], descending=[True, False, False])
df_final = pl.concat([df_final, df_grand])

with open(f'{OUTPUT_DIR}NPLHP3N_TXT.txt', 'w') as f:
    f.write("\n")
    f.write(f"PUBLIC BANK - (NEWLY SURFACE NPL) AS AT {rdate}\n")
    f.write("\n")
    
    f.write(f"{'ACCOUNT NO':>11} {'NOTE NO':>8} {'CUSTOMER NAME':<31} ")
    f.write(f"{'':>6} {'BRANCH':>6} {'DAYS':>4} {'BORSTAT':<7} {'ISSUE DATE':<10} ")
    f.write(f"{'NET PROCEED':>13} {'NET BAL':>13} {'IIS':>13} {'IIS CHARGE':>13} ")
    f.write(f"{'PRINCIPAL':>13} {'REALISABLE VALUE':>16} {'SP MADE':>13} ")
    f.write(f"{'SP CHARGE':>13} {'CLASS':>5} {'MTH PST DUE':>10} {'SECTOR CODE':<11} ")
    f.write(f"{'MAKE':<16} {'MODEL':<23} {'CAC NAME':<20} {'REGION OFF':<15} ")
    f.write(f"{'REG NO':<15} {'DEALER NO':<13} {'DEALER NAME':<29} ")
    f.write(f"{'ORI BRANCH':<11} {'CUSTFISS':<9} {'MODELDES':<9} ")
    f.write(f"{'USER5':<7} {'CAPCLOSE':>17} {'SCORE2':<10}\n")
    f.write("\n")
    
    for row in df_final.iter_rows(named=True):
        issdte = row.get('ISSDTE', '') or ''
        subtot = row.get('SUBTOT', '') or ''
        
        f.write(f"{row.get('ACCTNO', 0):11.0f} {row.get('NOTENO', 0):8.0f} ")
        f.write(f"{row.get('NAME', ''):<31} {subtot:>6} ")
        f.write(f"{row.get('BRANCH', 0):6.0f} {row.get('DAYS', 0):4.0f} ")
        f.write(f"{row.get('BORSTAT', ''):<7} {issdte:<10} ")
        f.write(f"{row.get('NETPROC', 0):13,.2f} {row.get('NETBAL', 0):13,.2f} ")
        f.write(f"{row.get('TOTIIS', 0):13,.2f} {row.get('IISCHRG', 0):13,.2f} ")
        f.write(f"{row.get('OSPRIN', 0):13,.2f} {row.get('MARKETVL', 0):16,.2f} ")
        f.write(f"{row.get('SP', 0):13,.2f} {row.get('SPPL', 0):13,.2f} ")
        f.write(f"{row.get('RISK', ''):>5} {row.get('ARREARS', 0):10.0f} ")
        f.write(f"{row.get('SECTORCD', ''):<11} {row.get('MAKE', ''):<16} ")
        f.write(f"{row.get('MODEL', ''):<23} {row.get('CACNAME', ''):<20} ")
        f.write(f"{row.get('REGION', ''):<15} {row.get('REGNO', ''):<15} ")
        f.write(f"{row.get('DEALERNO', ''):<13} {row.get('COMPNAME', ''):<29} ")
        f.write(f"{row.get('PRIMOFHP', ''):<11} {row.get('CUSTFISS', ''):<9} ")
        f.write(f"{row.get('MODELDES', ''):<9} {row.get('USER5', ''):<7} ")
        f.write(f"{row.get('CAP', 0):17,.2f} {row.get('SCORE2', ''):<10}\n")

print(f"  ✓ Generated: NPLHP3N_TXT.txt")

df_final.write_parquet(f'{NPL6_DIR}TOTNMN{reptmon}.parquet')
print(f"  ✓ Saved: TOTNMN{reptmon}.parquet")

total_accounts = len(df_newca)
total_balance = df_newca['NETBAL'].sum()
total_iis = df_newca['IISCHRG'].sum()
total_sp = df_newca['SPPL'].sum()

print(f"\n{'='*80}")
print(f"Newly Surfaced NPL: {total_accounts:,} accounts")
print(f"Total Net Balance: RM {total_balance:,.2f}")
print(f"Total IIS Charge: RM {total_iis:,.2f}")
print(f"Total SP Charge: RM {total_sp:,.2f}")
print(f"{'='*80}")
print("✓ EIFMNP3Z Complete")
print(f"{'='*80}")
