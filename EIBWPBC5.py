import polars as pl
from datetime import datetime
import sys

PBIF_REPTDATE = 'data/pbif/reptdate.parquet'
PBIF_DIR = 'data/pbif/'
PROVISIO = 'data/provisio.dat'
ACCTCRED = 'data/acctcred.dat'
SUBACRED = 'data/subacred.dat'
CREDITPO = 'data/creditpo.dat'

df_reptdate = pl.read_parquet(PBIF_REPTDATE)
reptdate = df_reptdate['REPTDATE'][0]

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

mm = reptdate.month
mm1 = mm
if wk == '1':
    mm1 = mm - 1
    if mm1 == 0:
        mm1 = 12

nowk = wk
nowk1 = wk1
reptmon = f'{mm:02d}'
reptyear = reptdate.strftime('%y')
reptyea4 = reptdate.strftime('%Y')
reptday = f'{reptdate.day:02d}'
rdate = int(reptdate.strftime('%y%j'))
mdate = rdate

df_pbif = pl.read_parquet(f'{PBIF_DIR}pbif.parquet')

df_client = df_pbif.filter(pl.col('ENTITY') == 'PBBH')
df_client = df_client.filter(
    (pl.col('FIU') != 0.00) | (pl.col('PRMTHFIU') != 0.00)
)

df_client = df_client.with_columns([
    pl.when(pl.col('CUSTCD').is_in([41, 42, 43, 66]))
      .then(pl.lit(41))
      .when(pl.col('CUSTCD').is_in([44, 47, 67]))
      .then(pl.lit(44))
      .when(pl.col('CUSTCD').is_in([46]))
      .then(pl.lit(46))
      .when(pl.col('CUSTCD').is_in([48, 49, 51, 68]))
      .then(pl.lit(48))
      .when(pl.col('CUSTCD').is_in([52, 53, 54, 69]))
      .then(pl.lit(52))
      .otherwise(pl.col('CUSTCD'))
      .alias('CUSTFISS'),
    pl.lit(0).alias('APCODE'),
    pl.lit('34450').alias('FACILITY'),
    pl.lit('34450').alias('NOTENO'),
    pl.when(pl.col('RPERIOD') == 0)
      .then(pl.lit(180))
      .otherwise(pl.col('RPERIOD'))
      .alias('RPERIOD')
])

df_client = df_client.with_columns([
    pl.when(pl.col('CLIENTNO') == 'FLN01D0KL')
      .then(pl.lit(120))
      .when(pl.col('CLIENTNO') == 'COM02D0KL')
      .then(pl.lit(120))
      .when(pl.col('CLIENTNO') == 'DPT21D5PG')
      .then(pl.lit(90))
      .when(pl.col('CLIENTNO') == 'SHW01D0PG')
      .then(pl.lit(150))
      .otherwise(pl.col('RPERIOD'))
      .alias('RPERIOD')
])

df_client = df_client.drop('DISPUTE')

df_cclnk = pl.read_parquet(f'{PBIF_DIR}CCLNK{reptyear}{reptmon}{reptday}.parquet')

df_client = df_client.join(df_cclnk, on='CLIENTNO', how='inner').sort(['CLIENTNO', 'CUSTNO'])

df_invoi = pl.read_parquet(f'{PBIF_DIR}INVOI{reptyear}{reptmon}{reptday}.parquet').sort(['CLIENTNO', 'CUSTNO'])

df_invcr = df_client.join(df_invoi, on=['CLIENTNO', 'CUSTNO'], how='inner')

df_invcr = df_invcr.with_columns([
    (pl.col('MATDTE') + 92).alias('NPLDTE'),
    (pl.lit(rdate) - pl.col('MATDTE')).alias('NODAYS'),
    pl.col('MATDTE').dt.day().alias('MATDD'),
    pl.col('MATDTE').dt.month().alias('MATMM'),
    pl.col('MATDTE').dt.year().alias('MATYY'),
    (pl.col('INTAMT') + pl.col('CREAMT')).alias('PMT'),
    pl.col('AMOUNT').round(2).alias('AMT')
])

df_invcr = df_invcr.with_columns([
    pl.col('PMT').round(2).alias('PMT')
])

df_invcr = df_invcr.with_columns([
    pl.when(pl.col('AMOUNT') == pl.col('PMT'))
      .then(pl.lit(0))
      .otherwise(pl.col('NODAYS'))
      .alias('NODAYS')
])

def calculate_arrears(nodays):
    if nodays <= 0:
        return 0
    if nodays <= 30:
        return 1
    elif nodays <= 60:
        return 2
    elif nodays <= 90:
        return 3
    elif nodays <= 120:
        return 4
    elif nodays <= 150:
        return 5
    elif nodays <= 180:
        return 6
    elif nodays <= 210:
        return 7
    elif nodays <= 240:
        return 8
    elif nodays <= 270:
        return 9
    elif nodays <= 300:
        return 10
    elif nodays <= 330:
        return 11
    elif nodays <= 360:
        return 12
    elif nodays <= 390:
        return 13
    elif nodays <= 420:
        return 14
    elif nodays <= 450:
        return 15
    elif nodays <= 480:
        return 16
    elif nodays <= 510:
        return 17
    elif nodays <= 540:
        return 18
    elif nodays <= 570:
        return 19
    elif nodays <= 600:
        return 20
    elif nodays <= 630:
        return 21
    elif nodays <= 660:
        return 22
    elif nodays <= 690:
        return 23
    else:
        return round(nodays / 30)

df_invcr = df_invcr.with_columns([
    pl.lit(0).alias('ARREARS'),
    pl.lit(0).alias('INSTALM')
])

df_invcr = df_invcr.with_columns([
    pl.when(pl.col('NODAYS') > 0)
      .then(pl.lit(1))
      .otherwise(pl.lit(0))
      .alias('INSTALM'),
    pl.when(pl.col('NODAYS') > 0)
      .then(pl.col('NODAYS').map_elements(calculate_arrears, return_dtype=pl.Int64))
      .otherwise(pl.lit(0))
      .alias('ARREARS')
])

df_invcr = df_invcr.with_columns([
    (pl.col('NPLAMT') + pl.col('NPLCINT')).alias('TOTAMT')
])

df_invcr = df_invcr.with_columns([
    pl.when((pl.col('NODAYS') >= 92) & (pl.col('NODAYS') <= 182))
      .then(pl.lit('D'))
      .when(pl.col('NODAYS') > 182)
      .then(pl.lit('B'))
      .otherwise(pl.lit(None))
      .alias('CLASSIFY')
])

df_prov = df_invcr.filter(
    (pl.col('NPLIND') == 'T') & 
    (pl.col('REASSIG') == 'F') & 
    (pl.col('NPLCINT') > 0.00)
)

df_prov = df_prov.sort(['ACCTNO', 'NODAYS'], descending=[False, True])

df_prov1 = df_prov.unique(subset=['ACCTNO'], keep='first')

df_prov2 = df_prov.group_by('ACCTNO').agg([
    pl.col('TOTAMT').sum(),
    pl.col('NPLCINT').sum(),
    pl.col('NPLAMT').sum(),
    pl.col('NPLCHG').sum()
])

df_prov_final = df_prov2.join(df_prov1, on='ACCTNO', how='inner')

df_prov_final = df_prov_final.with_columns([
    pl.lit(0).alias('APCODE'),
    pl.lit('34450').alias('FACILITY'),
    pl.lit('34450').alias('NOTENO'),
    (pl.col('TOTAMT') * 100).alias('BALANCE'),
    (pl.col('NPLAMT') * 100).alias('IISOPBAL'),
    (pl.col('NPLCINT') * 100).alias('IISAMT'),
    (pl.col('NPLCHG') * 100).alias('INTAMT'),
    pl.lit(0).alias('TOTIISR'),
    pl.lit(0).alias('TOTWOF'),
    pl.lit(0).alias('OLDBRH'),
    pl.lit(0).alias('REALISVL'),
    pl.lit(0).alias('IISDANAH'),
    pl.lit(0).alias('FEEAMT'),
    pl.lit(0).alias('SPTRANS'),
    pl.lit(0).alias('SPDANAH'),
    pl.lit(0).alias('IISTRANS'),
    pl.lit(0).alias('SPOPBAL'),
    pl.lit(0).alias('SPCHARGE'),
    pl.lit(0).alias('SPWOAMT'),
    pl.lit(0).alias('SPWBAMT')
])

with open(PROVISIO, 'wb') as f:
    for row in df_prov_final.iter_rows(named=True):
        record = bytearray(362)
        
        record[6:9] = f"{row['BRANCH']:03d}".encode('ascii')
        record[9:12] = f"{row['APCODE']:03d}".encode('ascii')
        record[12:22] = f"{row['ACCTNO']:>10}".encode('ascii')
        record[42:47] = row['FACILITY'].encode('ascii')
        record[72:74] = reptday.encode('ascii')
        record[74:76] = reptmon.encode('ascii')
        record[76:80] = reptyea4.encode('ascii')
        record[80:81] = (row['CLASSIFY'] if row['CLASSIFY'] else ' ').encode('ascii')
        record[81:84] = f"{int(row['ARREARS']):03d}".encode('ascii')
        record[84:101] = f"{int(row['BALANCE']):017d}".encode('ascii')
        record[101:118] = f"{int(row['INTAMT']):017d}".encode('ascii')
        record[118:134] = f"{int(row['FEEAMT']):016d}".encode('ascii')
        record[134:151] = f"{int(row['REALISVL']):017d}".encode('ascii')
        record[151:168] = f"{int(row['IISOPBAL']):017d}".encode('ascii')
        record[168:185] = f"{int(row['IISAMT']):017d}".encode('ascii')
        record[185:202] = f"{int(row['TOTIISR']):017d}".encode('ascii')
        record[202:219] = f"{int(row['TOTWOF']):017d}".encode('ascii')
        record[219:236] = f"{int(row['IISDANAH']):017d}".encode('ascii')
        record[236:253] = f"{int(row['IISTRANS']):017d}".encode('ascii')
        record[253:270] = f"{int(row['SPOPBAL']):017d}".encode('ascii')
        record[270:287] = f"{int(row['SPCHARGE']):017d}".encode('ascii')
        record[287:304] = f"{int(row['SPWBAMT']):017d}".encode('ascii')
        record[304:321] = f"{int(row['SPWOAMT']):017d}".encode('ascii')
        record[321:338] = f"{int(row['SPDANAH']):017d}".encode('ascii')
        record[338:355] = f"{int(row['SPTRANS']):017d}".encode('ascii')
        record[356:361] = f"{int(row['OLDBRH']):05d}".encode('ascii')
        
        f.write(record)

df_invcr = df_invcr.sort(['ACCTNO', 'NODAYS']).unique(subset=['ACCTNO'], keep='first')

df_cred = df_invcr.with_columns([
    pl.lit(0).alias('OLDBRH'),
    pl.lit(0).alias('LMTAMT'),
    pl.when(pl.col('BALANCE') < 0.00)
      .then(pl.lit(0.00))
      .otherwise(pl.col('BALANCE'))
      .alias('BALANCE'),
    pl.col('STDATES').dt.day().alias('ISSUEDD'),
    pl.col('STDATES').dt.month().alias('ISSUEMM'),
    pl.col('STDATES').dt.year().alias('ISSUEYY'),
    (pl.col('INLIMIT') * 100).alias('APPRLIMT'),
    pl.lit('N').alias('SYNDICAT'),
    pl.lit('00').alias('SPECIALF'),
    pl.lit(60).alias('FCONCEPT'),
    pl.lit('18').alias('PAYFREQC'),
    pl.lit('5300').alias('FISSPURP')
])

df_cred = df_cred.with_columns([
    pl.col('APPRLIMT').alias('APPRLIM2')
])

with open(ACCTCRED, 'wb') as f:
    for row in df_cred.iter_rows(named=True):
        record = bytearray(115)
        
        record[6:9] = f"{row['BRANCH']:03d}".encode('ascii')
        record[9:12] = f"{row['APCODE']:03d}".encode('ascii')
        record[12:22] = f"{row['ACCTNO']:>10}".encode('ascii')
        record[42:45] = row['CURRENCY'].encode('ascii')
        record[45:69] = f"{int(row['APPRLIMT']):024d}".encode('ascii')
        record[69:85] = f"{int(row['APPRLIM2']):016d}".encode('ascii')
        record[85:87] = f"{int(row['ISSUEDD']):02d}".encode('ascii')
        record[87:89] = f"{int(row['ISSUEMM']):02d}".encode('ascii')
        record[89:93] = f"{int(row['ISSUEYY']):04d}".encode('ascii')
        record[93:98] = f"{int(row['OLDBRH']):05d}".encode('ascii')
        record[98:114] = f"{int(row['LMTAMT']):016d}".encode('ascii')
        
        f.write(record)

with open(SUBACRED, 'wb') as f:
    for row in df_cred.iter_rows(named=True):
        record = bytearray(121)
        
        record[6:9] = f"{row['BRANCH']:03d}".encode('ascii')
        record[9:12] = f"{row['APCODE']:03d}".encode('ascii')
        record[12:22] = f"{row['ACCTNO']:>10}".encode('ascii')
        record[42:47] = f"{row['NOTENO']:>5}".encode('ascii')
        record[47:52] = row['FACILITY'].encode('ascii')
        record[72:77] = row['FACILITY'].encode('ascii')
        record[77:78] = row['SYNDICAT'].encode('ascii')
        record[78:80] = row['SPECIALF'].encode('ascii')
        record[80:84] = row['FISSPURP'].encode('ascii')
        record[84:86] = f"{int(row['FCONCEPT']):02d}".encode('ascii')
        record[86:89] = f"{int(row['TERM']):03d}".encode('ascii')
        record[89:91] = row['PAYFREQC'].encode('ascii')
        record[109:111] = f"{int(row['CUSTFISS']):02d}".encode('ascii')
        record[111:115] = row['SECTORCD'].encode('ascii')
        record[115:120] = f"{int(row['OLDBRH']):05d}".encode('ascii')
        
        f.write(record)

df_cred = df_cred.with_columns([
    pl.lit(0.00).alias('BILTOT'),
    pl.lit(0.00).alias('ODXSAMT'),
    pl.lit('O').alias('ACCTSTAT'),
    pl.lit(0).alias('UNDRAWN')
])

df_cred = df_cred.with_columns([
    pl.when(pl.col('INLIMIT') > 0)
      .then(
          pl.when(((pl.col('INLIMIT') - pl.col('BALANCE')) * 0.2) < 0)
            .then(pl.lit(0))
            .otherwise((pl.col('INLIMIT') - pl.col('BALANCE')) * 0.2)
      )
      .otherwise(pl.lit(0))
      .alias('UNDRAWN')
])

df_cred = df_cred.with_columns([
    (pl.col('BALANCE') * 100).alias('OUTSTAND'),
    (pl.col('UNDRAWN') * 100).alias('UNDRAWN')
])

with open(CREDITPO, 'wb') as f:
    for row in df_cred.iter_rows(named=True):
        record = bytearray(207)
        
        record[6:9] = f"{row['BRANCH']:03d}".encode('ascii')
        record[9:12] = f"{row['APCODE']:03d}".encode('ascii')
        record[12:22] = f"{row['ACCTNO']:>10}".encode('ascii')
        record[42:47] = f"{row['NOTENO']:>5}".encode('ascii')
        record[72:74] = reptday.encode('ascii')
        record[74:76] = reptmon.encode('ascii')
        record[76:80] = reptyea4.encode('ascii')
        record[80:96] = f"{int(row['OUTSTAND']):016d}".encode('ascii')
        record[96:99] = f"{int(row['ARREARS']):03d}".encode('ascii')
        record[99:102] = f"{int(row['INSTALM']):03d}".encode('ascii')
        record[102:119] = f"{int(row['UNDRAWN']):017d}".encode('ascii')
        record[119:120] = row['ACCTSTAT'].encode('ascii')
        record[120:125] = f"{int(row['NODAYS']):05d}".encode('ascii')
        record[125:130] = f"{int(row['OLDBRH']):05d}".encode('ascii')
        record[130:147] = f"{int(row['BILTOT']):017d}".encode('ascii')
        record[147:164] = f"{int(row['ODXSAMT']):017d}".encode('ascii')
        record[164:181] = f"{int(row['DISBURSE']):017d}".encode('ascii')
        record[181:198] = f"{int(row['REPAID']):017d}".encode('ascii')
        record[198:200] = f"{int(row['MATDD']):02d}".encode('ascii')
        record[200:202] = f"{int(row['MATMM']):02d}".encode('ascii')
        record[202:206] = f"{int(row['MATYY']):04d}".encode('ascii')
        
        f.write(record)

print(f"Generated files:")
print(f"  {PROVISIO}")
print(f"  {ACCTCRED}")
print(f"  {SUBACRED}")
print(f"  {CREDITPO}")
