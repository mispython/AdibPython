import polars as pl
import struct

MIS_FOFMT = 'data/mis/fofmt.parquet'
COLL_FILE = 'data/coll.dat'
DESC_FILE = 'data/desc.dat'
LOAN_COLL = 'data/loan/coll.parquet'
LOAN_COLLATER = 'data/loan/collater.parquet'

FDR = ['001', '006', '007', '014', '016', '024', '025', '026', '046',
       '048', '049', '112', '113', '114', '115', '116', '117']

GUARANT = ['000', '011', '012', '013', '017', '018', '019', '021',
           '027', '028', '029', '030', '031', '105', '106', '124']

DEBENT = ['002', '003', '041', '042', '043', '058', '059',
          '067', '068', '069', '070', '071', '072', '078', '079',
          '084', '107', '111', '123']

HP = ['004', '005', '125', '126', '127', '128', '129',
      '131', '132', '133', '134', '135', '136', '137', '138', '139',
      '141', '142', '143']

PROPER = ['032', '033', '034', '035', '036', '037', '038', '039',
          '040', '044', '050', '051', '052', '053', '054', '055', '056',
          '057', '060', '061', '062',
          '118', '119', '121', '122']

OTHERHP = ['065', '066', '075', '076', '082', '083',
           '093', '094', '095', '096', '097', '098',
           '101', '102', '103', '104']

PLTMACH = ['063', '064', '073', '074', '080', '081']

CONCESS = ['010', '085', '086', '087', '088', '089', '090', '091',
           '092', '144']

df_fofmt = pl.read_parquet(MIS_FOFMT)
df_fofmt = df_fofmt.filter(pl.col('CURCODE').str.strip_chars() != '').sort('CURCODE')

def read_coll_file():
    records = []
    with open(COLL_FILE, 'rb') as f:
        while True:
            record = f.read(350)
            if len(record) < 350:
                break
            
            ccollno = struct.unpack('>q', b'\x00\x00' + record[3:9])[0]
            acctapp = chr(record[9])
            cissuer = record[16:22].decode('ascii', errors='ignore').strip()
            cissue = record[22:24].decode('ascii', errors='ignore').strip()
            uniqcode = chr(record[24])
            cclassc = record[25:28].decode('ascii', errors='ignore').strip()
            totunits = struct.unpack('>q', b'\x00' + record[28:35])[0] / 100.0
            lstdepdt = struct.unpack('>q', b'\x00\x00' + record[77:83])[0]
            depnoprd = struct.unpack('>I', b'\x00' + record[83:86])[0]
            depfreq = chr(record[86])
            marginp = struct.unpack('>H', record[88:90])[0]
            origval = struct.unpack('>q', b'\x00' + record[90:97])[0] / 100.0
            cdolarv = struct.unpack('>q', b'\x00' + record[97:104])[0] / 100.0
            cprvdolv = struct.unpack('>q', b'\x00' + record[104:111])[0] / 100.0
            curcode = record[134:137].decode('ascii', errors='ignore').strip()
            purgeind = chr(record[137])
            appcode = chr(record[144])
            acctno = struct.unpack('>q', b'\x00\x00' + record[145:151])[0]
            oblgcode = chr(record[151])
            noteno = struct.unpack('>q', b'\x00\x00' + record[152:158])[0]
            obbal = struct.unpack('>q', record[164:172])[0] / 100.0
            totobbal = struct.unpack('>q', record[184:192])[0] / 100.0
            aano = record[192:205].decode('ascii', errors='ignore').strip().upper()
            bondid = chr(record[222])
            couponrt = struct.unpack('>H', record[242:244])[0] / 10000.0
            marginvl = struct.unpack('>q', b'\x00\x00\x00' + record[295:300])[0] / 100.0
            accosct = struct.unpack('>I', record[308:312])[0]
            costctr = struct.unpack('>I', record[312:316])[0]
            
            ucoll = 'Y' if (not (2500000000 <= acctno <= 2599999999 or 
                                2850000000 <= acctno <= 2859999999) and 
                           appcode == 'C' and oblgcode == 'A') else None
            
            records.append({
                'CCOLLNO': ccollno,
                'ACCTAPP': acctapp,
                'CISSUER': cissuer,
                'CISSUE': cissue,
                'UNIQCODE': uniqcode,
                'CCLASSC': cclassc,
                'TOTUNITS': totunits,
                'LSTDEPDT': lstdepdt,
                'DEPNOPRD': depnoprd,
                'DEPFREQ': depfreq,
                'MARGINP': marginp,
                'ORIGVAL': origval,
                'CDOLARV': cdolarv,
                'CPRVDOLV': cprvdolv,
                'CURCODE': curcode,
                'PURGEIND': purgeind,
                'APPCODE': appcode,
                'ACCTNO': acctno,
                'OBLGCODE': oblgcode,
                'NOTENO': noteno,
                'OBBAL': obbal,
                'TOTOBBAL': totobbal,
                'AANO': aano,
                'BONDID': bondid,
                'COUPONRT': couponrt,
                'MARGINVL': marginvl,
                'ACCOSCT': accosct,
                'COSTCTR': costctr,
                'UCOLL': ucoll
            })
    
    return pl.DataFrame(records) if records else pl.DataFrame()

df_coll = read_coll_file().sort('CURCODE')

df_coll = df_coll.join(df_fofmt, on='CURCODE', how='left')

df_coll = df_coll.with_columns([
    pl.col('SPOTRATE').fill_null(1.00),
    pl.col('CDOLARV').alias('CDOLARVX')
])

df_coll = df_coll.with_columns([
    pl.when(~pl.col('CURCODE').is_in(['MYR', '   ']))
      .then((pl.col('CDOLARV') * pl.col('SPOTRATE')).round(2))
      .otherwise(pl.col('CDOLARV'))
      .alias('CDOLARV')
])

df_coll = df_coll.unique(subset=['ACCTNO', 'NOTENO', 'CCOLLNO']).drop(['START', 'LABEL', 'FMTNAME', 'TYPE', 'REPTDATE'], strict=False)

df_coll.write_parquet(LOAN_COLL)

def read_desc_file():
    records = []
    with open(DESC_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if len(line) < 15:
                continue
            
            ccollno = int(line[0:11].strip()) if line[0:11].strip() else 0
            cclassc = line[11:14].strip()
            purgeind = line[14] if len(line) > 14 else ''
            
            record = {
                'CCOLLNO': ccollno,
                'CCLASSC': cclassc,
                'PURGEIND': purgeind
            }
            
            if cclassc in PROPER and len(line) >= 2990:
                record.update({
                    'CINSTCL': line[50:52].strip(),
                    'IDTY1OWN': line[52:54].strip(),
                    'NATGUAR': line[54:56].strip(),
                    'IDTY2OWN': line[54:56].strip(),
                    'IDTY3OWN': line[56:58].strip(),
                    'CPRRANKC': line[58] if len(line) > 58 else '',
                    'CPRSHARE': line[59] if len(line) > 59 else '',
                    'CPRCHARG': float(line[60:68].strip()) if line[60:68].strip() else 0.0,
                    'CPRLANDU': line[76:78].strip(),
                    'CPRPROPD': line[78:80].strip(),
                    'NAME1OWN': line[90:130].strip(),
                    'IDNO1OWN': line[130:170].strip(),
                    'NAME2OWN': line[170:210].strip(),
                    'CENSUS': int(line[210:220].strip()) if line[210:220].strip() else 0,
                    'IDNO2OWN': line[210:250].strip(),
                    'NAME3OWN': line[250:290].strip(),
                    'IDNO3OWN': line[290:330].strip(),
                    'OWNOCUPY': line[339] if len(line) > 339 else '',
                    'HOLDEXPD': line[340] if len(line) > 340 else '',
                    'EXPDATE': line[341:349].strip(),
                    'BLTNAREA': line[365:369].strip(),
                    'BLTNUNIT': line[369] if len(line) > 369 else '',
                    'QUITRENT': line[378:382].strip(),
                    'ASSESSDT': line[382:386].strip(),
                    'CPRSTAT': line[386:394].strip(),
                    'CPOLYNUM': line[394:410].strip(),
                    'TTLPARTCLR': line[410] if len(line) > 410 else '',
                    'MASTOWNR': line[450:490].strip(),
                    'TTLENO': line[490:530].strip(),
                    'TTLMUKIM': line[530:650].strip(),
                    'TTLID': line[650:665].strip(),
                    'LANDAREA': line[665:669].strip(),
                    'FIREIND': line[669:671].strip(),
                    'FIREDATE': line[671:679].strip(),
                    'FIREPREM': line[679:688].strip(),
                    'ESCL': line[687] if len(line) > 687 else '',
                    'LANDUNIT': line[688] if len(line) > 688 else '',
                    'MRESERVE': line[689] if len(line) > 689 else '',
                    'DEVNAME': line[690:730].strip(),
                    'PRJCTNAM': line[730:770].strip(),
                    'CTRYSTAT': line[770:772].strip(),
                    'PRABANDT': line[772:780].strip(),
                    'CPRDISDT': line[780:788].strip(),
                    'CPOSCODE': line[796:801].strip(),
                    'CPRVALIN2': line[802] if len(line) > 802 else '',
                    'STDESIGN': line[803] if len(line) > 803 else '',
                    'CPRFORSV': float(line[810:818].strip()) if line[810:818].strip() else 0.0,
                    'CTRYCODE': line[818:820].strip(),
                    'CPRESVAL': float(line[826:834].strip()) if line[826:834].strip() else 0.0,
                    'CPRVALDT': line[850:858].strip(),
                    'CPRVALIN': line[858] if len(line) > 858 else '',
                    'CPPVALDT': line[859:867].strip(),
                    'CPHSENO': line[890:900].strip(),
                    'CPBUILNO': line[900:930].strip(),
                    'CPRPARC2': line[930:970].strip(),
                    'CPRPARC3': line[970:1010].strip(),
                    'CPRPARC4': line[1010:1050].strip(),
                    'CPSTATE': line[1050:1052].strip(),
                    'CPSTATEN': line[1052:1066].strip(),
                    'CPTOWN': line[1066:1069].strip(),
                    'CPTOWNN': line[1069:1089].strip(),
                    'CPLOCAT': line[1089] if len(line) > 1089 else '',
                    'CPRVALU1': line[1090:1130].strip(),
                    'CPRVALU2': line[1130:1170].strip(),
                    'CPRVALU3': line[1170:1250].strip(),
                    'REMARKS': line[1250:1290].strip(),
                    'AUCIND': line[2988] if len(line) > 2988 else '',
                    'AUCCOMDT': line[2989:2997].strip() if len(line) > 2997 else '',
                    'NOAUC': int(line[2997:2999].strip()) if len(line) > 2999 and line[2997:2999].strip() else 0,
                    'AUCSUCCIND': line[2999] if len(line) > 2999 else '',
                    'SUCCAUCDT': line[3016:3024].strip() if len(line) > 3024 else ''
                })
            
            elif cclassc in HP and len(line) >= 331:
                record.update({
                    'CINSTCL': line[50:52].strip(),
                    'CHPVECON': line[73:75].strip(),
                    'CHPVEHNO': line[75:87].strip(),
                    'CHPENGIN': line[130:150].strip(),
                    'CHPCHASS': line[150:170].strip(),
                    'CHPMAKE': line[330:370].strip()
                })
            
            elif cclassc in OTHERHP and len(line) >= 75:
                record.update({
                    'CINSTCL': line[50:52].strip(),
                    'COVDESCR': line[54:56].strip(),
                    'COVVALDT': line[74:82].strip()
                })
            
            elif cclassc in PLTMACH and len(line) >= 75:
                record.update({
                    'CINSTCL': line[50:52].strip(),
                    'CPMDESCR': line[54:56].strip(),
                    'CPMVALDT': line[74:82].strip()
                })
            
            elif cclassc in CONCESS and len(line) >= 75:
                record.update({
                    'CINSTCL': line[50:52].strip(),
                    'CCCDESCR': line[54:56].strip(),
                    'CCCVALDT': line[74:82].strip()
                })
            
            elif cclassc in FDR and len(line) >= 451:
                record.update({
                    'CINSTCL': line[50:52].strip(),
                    'CFDDESCR': line[58:60].strip(),
                    'NAME1OWN': line[90:130].strip(),
                    'IDNO1OWN': line[130:170].strip(),
                    'NAME2OWN': line[170:210].strip(),
                    'IDNO2OWN': line[210:250].strip(),
                    'NAME3OWN': line[250:290].strip(),
                    'IDNO3OWN': line[290:330].strip(),
                    'CFDVALDT': line[450:458].strip()
                })
            
            elif cclassc in DEBENT and len(line) >= 91:
                record.update({
                    'CINSTCL': line[50:52].strip(),
                    'CDEBAMT': float(line[54:62].strip()) if line[54:62].strip() else 0.0,
                    'CDEBDESC': line[70:72].strip(),
                    'CDEBVALD': line[90:98].strip()
                })
            
            elif cclassc in GUARANT and len(line) >= 743:
                record.update({
                    'CINSTCL': line[50:52].strip(),
                    'CGUARNAT': line[54:56].strip(),
                    'CGUARCTY': line[56:58].strip(),
                    'CGEFFDAT': line[58:66].strip(),
                    'CGEXPDAT': line[66:74].strip(),
                    'GTEEFEE': float(line[90:98].strip()) if line[90:98].strip() else 0.0,
                    'CGEXAMTG': float(line[106:114].strip()) if line[106:114].strip() else 0.0,
                    'CGUARNAM': line[130:170].strip(),
                    'CGUARID': line[170:210].strip(),
                    'CGUARLG': line[210:250].strip(),
                    'SBETRAN': line[290:330].strip(),
                    'GFEERS': float(line[627:633].strip()) if line[627:633].strip() else 0.0,
                    'GFEERU': float(line[633:639].strip()) if line[633:639].strip() else 0.0,
                    'GFAMTS': float(line[639:647].strip()) if line[639:647].strip() else 0.0,
                    'GFAMTU': float(line[655:663].strip()) if line[655:663].strip() else 0.0,
                    'GCAMTS': float(line[671:679].strip()) if line[671:679].strip() else 0.0,
                    'GCAMTU': float(line[687:695].strip()) if line[687:695].strip() else 0.0,
                    'CUSTNO': int(line[742:753].strip()) if len(line) > 753 and line[742:753].strip() else 0
                })
            
            if 'CPHSENO' in record and 'CPBUILNO' in record:
                cprparc1 = (record['CPHSENO'] + ' ' + record['CPBUILNO']).strip()
                record['CPRPARC1'] = ' '.join(cprparc1.split())
            
            records.append(record)
    
    return pl.DataFrame(records) if records else pl.DataFrame()

df_desc = read_desc_file().sort('CCOLLNO')

df_collater = df_coll.sort('CCOLLNO').join(df_desc.sort('CCOLLNO'), on='CCOLLNO', how='left', suffix='_desc')

date_fields = ['EXPDATE', 'FIREDATE', 'CPRVALDT', 'COVVALDT', 'CPMVALDT', 
               'CCCVALDT', 'CFDVALDT', 'CDEBVALD', 'CGEFFDAT', 'CGEXPDAT',
               'PRABANDT', 'AUCCOMDT', 'SUCCAUCDT']

for field in date_fields:
    if field in df_collater.columns:
        df_collater = df_collater.with_columns([
            pl.when(pl.col(field).cast(pl.Int64, strict=False) < 0)
              .then(pl.lit('00000000'))
              .otherwise(pl.col(field))
              .alias(field)
        ])

df_collater = df_collater.with_columns([
    pl.lit(0).alias('CPRABNDT')
])

if 'PRABANDT' in df_collater.columns:
    df_collater = df_collater.with_columns([
        pl.when(pl.col('PRABANDT').str.strip_chars().str.len_chars() > 0)
          .then(
              pl.col('PRABANDT').map_elements(
                  lambda x: int(x) if x and x.isdigit() else 0,
                  return_dtype=pl.Int64
              )
          )
          .otherwise(pl.lit(0))
          .alias('CPRABNDT')
    ])

df_collater = df_collater.drop(['START', 'LABEL', 'FMTNAME', 'TYPE', 'REPTDATE'], strict=False)

df_collater.write_parquet(LOAN_COLLATER)

print(f"Files generated:")
print(f"  {LOAN_COLL}")
print(f"  {LOAN_COLLATER}")
print(f"Collateral records: {len(df_coll)}")
print(f"Collateral descriptions: {len(df_desc)}")
print(f"Final collateral file: {len(df_collater)}")
