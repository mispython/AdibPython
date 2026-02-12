import polars as pl
from datetime import datetime

MNITB_DIR = 'data/mnitb/'
CISDPC_DIR = 'data/cisdpc/'
CISDPO_DIR = 'data/cisdpo/'
EALIS_DIR = 'data/ealis/'
TEMPF_DIR = 'data/tempf/'
OUTPUT_RMSA = 'data/nrc_rmsa.txt'
OUTPUT_RMCA = 'data/nrc_rmca.txt'
OUTPUT_RMFD = 'data/nrc_rmfd.txt'
OUTPUT_VOST = 'data/nrc_vost.txt'
OUTPUT_FXCA = 'data/nrc_fxca.txt'
OUTPUT_FXFD = 'data/nrc_fxfd.txt'

df_reptdate = pl.read_parquet(f'{MNITB_DIR}REPTDATE.parquet')
reptdate = df_reptdate['REPTDATE'][0]

mmd = reptdate.month
qtr_map = {1: 'Q1', 2: 'Q1', 3: 'Q1',
           4: 'Q2', 5: 'Q2', 6: 'Q2',
           7: 'Q3', 8: 'Q3', 9: 'Q3',
           10: 'Q4', 11: 'Q4', 12: 'Q4'}
qtr = qtr_map[mmd]
yyd = f'{reptdate.year % 100:02d}'
mmd_str = f'{mmd:02d}'
rdate = reptdate.strftime('%d%m%y')
xdate = f'{reptdate.year:04d}{mmd:02d}'

print(f"Processing Non-Resident Customer Report")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"Quarter: {qtr}{yyd}")

df_cisdpc = pl.read_parquet(f'{CISDPC_DIR}DEPOSIT.parquet').select([
    'ACCTNO', 'CITIZEN', 'NEWICIND', 'CUSTNO'
]).sort('ACCTNO')

df_rmca = pl.read_parquet(f'{EALIS_DIR}RMCA{qtr}{yyd}.parquet').filter(
    (pl.col('CUSTCODE') >= 80) & (pl.col('CUSTCODE') <= 99)
).with_columns([
    pl.lit('RMCA').alias('IND')
])

df_fxca = pl.read_parquet(f'{EALIS_DIR}FXCA{qtr}{yyd}.parquet').filter(
    (pl.col('CUSTCODE') >= 80) & (pl.col('CUSTCODE') <= 99)
).with_columns([
    pl.lit('FXCA').alias('IND')
])

df_vost = pl.read_parquet(f'{EALIS_DIR}VOST{qtr}{yyd}.parquet').filter(
    (pl.col('CUSTCODE') >= 80) & (pl.col('CUSTCODE') <= 99)
).with_columns([
    pl.lit('VOST').alias('IND')
])

df_current = pl.concat([df_rmca, df_fxca, df_vost]).sort('ACCTNO')

df_current = df_current.unique(subset=['ACCTNO'], keep='first')

df_nrcca = df_current.join(df_cisdpc, on='ACCTNO', how='left')

df_rmsa = pl.read_parquet(f'{EALIS_DIR}RMSA{qtr}{yyd}.parquet').filter(
    (pl.col('CUSTCODE') >= 80) & (pl.col('CUSTCODE') <= 99)
).with_columns([
    pl.lit('RMSA').alias('IND')
])

df_rmfd = pl.read_parquet(f'{EALIS_DIR}RMFD{qtr}{yyd}.parquet').filter(
    (pl.col('CUSTCODE') >= 80) & (pl.col('CUSTCODE') <= 99)
).with_columns([
    pl.lit('RMFD').alias('IND')
])

df_fxfd = pl.read_parquet(f'{EALIS_DIR}FXFD{qtr}{yyd}.parquet').filter(
    (pl.col('CUSTCODE') >= 80) & (pl.col('CUSTCODE') <= 99)
).with_columns([
    pl.lit('FXFD').alias('IND')
])

df_safd = pl.concat([df_rmsa, df_rmfd, df_fxfd]).sort('ACCTNO')

df_safd = df_safd.unique(subset=['ACCTNO'], keep='first')

df_cisdpo = pl.read_parquet(f'{CISDPO_DIR}DEPOSIT.parquet').select([
    'ACCTNO', 'CITIZEN', 'NEWICIND', 'CUSTNO'
]).sort('ACCTNO')

df_nrcco = df_safd.join(df_cisdpo, on='ACCTNO', how='left')

df_ctry = pl.concat([df_nrcca, df_nrcco])
df_ctry.write_parquet(f'{TEMPF_DIR}CTRY{mmd_str}.parquet')

df_ctryall = df_ctry.sort('ACCTNO').unique(subset=['ACCTNO'], keep='first')

df_nrcca = df_ctryall.filter(
    ~(
        (pl.col('PURPOSE') == '2') & 
        (pl.col('CITIZEN') == 'MY') & 
        (pl.col('NEWICIND') == 'IC')
    )
).filter(
    (pl.col('NEWICIND') == 'PP') & 
    pl.col('CITIZEN').is_in(['MY', '99', ' '])
)

def write_output_file(df, filename, ind_value):
    """Write filtered data to semicolon-delimited output file"""
    df_out = df.filter(pl.col('IND') == ind_value)
    
    if len(df_out) > 0:
        with open(filename, 'w') as f:
            for row in df_out.iter_rows(named=True):
                acctno = row.get('ACCTNO', 0)
                branch = row.get('BRANCH', 0)
                custcode = row.get('CUSTCODE', 0)
                purpose = row.get('PURPOSE', ' ')
                product = row.get('PRODUCT', 0)
                citizen = row.get('CITIZEN', '  ')
                curcd = row.get('CURCD', '   ')
                currte = row.get('CURRTE', 0.0)
                creditfy = row.get('CREDITFY', 0.0)
                debitfy = row.get('DEBITFY', 0.0)
                creditmy = row.get('CREDITMY', 0.0)
                debitmy = row.get('DEBITMY', 0.0)
                sector = row.get('SECTOR', 0)
                trxdate = row.get('TRXDATE', '        ')
                apprlimt = row.get('APPRLIMT', 0)
                name = row.get('NAME', '')
                custno = row.get('CUSTNO', 0)
                newicind = row.get('NEWICIND', '  ')
                
                f.write(f"{acctno:10.0f};")
                f.write(f"{branch:3d};")
                f.write(f"{custcode:2d};")
                f.write(f"{purpose};")
                f.write(f"{product:3d};")
                f.write(f"{citizen:2};")
                f.write(f"{curcd:3};")
                f.write(f"{currte:7.5f};")
                f.write(f"{creditfy:12.2f};")
                f.write(f"{debitfy:12.2f};")
                f.write(f"{creditmy:12.2f};")
                f.write(f"{debitmy:12.2f};")
                f.write(f"{sector:4d};")
                f.write(f"{trxdate:8};")
                f.write(f"{apprlimt:6.0f};")
                f.write(f"{name:15};")
                f.write(f"{custno:11.0f};")
                f.write(f"{newicind:2}\n")
        
        return len(df_out)
    return 0

rmsa_count = write_output_file(df_nrcca, OUTPUT_RMSA, 'RMSA')
rmca_count = write_output_file(df_nrcca, OUTPUT_RMCA, 'RMCA')
rmfd_count = write_output_file(df_nrcca, OUTPUT_RMFD, 'RMFD')
vost_count = write_output_file(df_nrcca, OUTPUT_VOST, 'VOST')
fxca_count = write_output_file(df_nrcca, OUTPUT_FXCA, 'FXCA')
fxfd_count = write_output_file(df_nrcca, OUTPUT_FXFD, 'FXFD')

print(f"\nNon-Resident Customer Report Complete")
print(f"\nOutput files generated:")
print(f"  {OUTPUT_RMSA} ({rmsa_count} records) - RM Savings")
print(f"  {OUTPUT_RMCA} ({rmca_count} records) - RM Current")
print(f"  {OUTPUT_RMFD} ({rmfd_count} records) - RM Fixed Deposit")
print(f"  {OUTPUT_VOST} ({vost_count} records) - Vostro")
print(f"  {OUTPUT_FXCA} ({fxca_count} records) - FX Current")
print(f"  {OUTPUT_FXFD} ({fxfd_count} records) - FX Fixed Deposit")
print(f"\nTotal NRC accounts: {rmsa_count + rmca_count + rmfd_count + vost_count + fxca_count + fxfd_count}")

print(f"\nFiltering criteria applied:")
print(f"  - Customer codes: 80-99 (Non-Resident)")
print(f"  - Excluded: PURPOSE='2' with MY citizen and IC ID type")
print(f"  - Included: Passport holders (PP) only")
print(f"  - Citizen: MY, 99, or blank")
