import polars as pl
import struct

MIS_DIR = 'data/mis/'
MNITB_DIR = 'data/mnitb/'
MIITB_DIR = 'data/miitb/'
MNILN_DIR = 'data/mniln/'
MIILN_DIR = 'data/miiln/'
LOAN_DIR = 'data/loan/'
LOANI_DIR = 'data/loani/'
COLL_FILE = 'data/coll.dat'
DESC_FILE = 'data/desc.dat'

FDR = ['001','006','007','014','016','024','025','026','046',
       '048','049','112','113','114','115','116','117','149']
GUARANT = ['000','011','012','013','017','018','019','021',
           '027','028','029','030','031','105','106','124']
DEBENT = ['002','003','041','042','043','058','059',
          '067','068','069','070','071','072','078','079',
          '084','107','111','123']
HP = ['004','005','125','126','127','128','129',
      '131','132','133','134','135','136','137','138','139',
      '141','142','143']
PROPER = ['032','033','034','035','036','037','038','039',
          '040','044','050','051','052','053','054','055','056',
          '057','060','061','062','118','119','121','122']
OTHERHP = ['065','066','075','076','082','083',
           '093','094','095','096','097','098',
           '101','102','103','104']
PLTMACH = ['063','064','073','074','080','081']
CONCESS = ['010','085','086','087','088','089','090','091',
           '092','144']
SHARE = ['008','015','045','047','108','147']
NP = ['020']

df_fofmt = pl.read_parquet(f'{MIS_DIR}FOFMT.parquet').filter(
    pl.col('CURCODE').is_not_null() & (pl.col('CURCODE') != '')
).sort('CURCODE')

def read_packed_decimal(data, start, length, decimals=0):
    """Read packed decimal from byte data"""
    try:
        bytes_data = data[start-1:start-1+length]
        value = 0
        for i, byte in enumerate(bytes_data):
            if i == len(bytes_data) - 1:
                high = (byte >> 4) & 0x0F
                value = value * 10 + high
            else:
                high = (byte >> 4) & 0x0F
                low = byte & 0x0F
                value = value * 10 + high
                value = value * 10 + low
        return value / (10 ** decimals) if decimals > 0 else value
    except:
        return None

def read_coll_record(line):
    """Parse COLL file record"""
    try:
        record = {
            'CCOLLNO': read_packed_decimal(line, 4, 3),
            'ACCTAPP': line[10:11].decode('utf-8', errors='ignore').strip(),
            'CISSUER': line[16:22].decode('utf-8', errors='ignore').strip(),
            'CISSUE': line[22:24].decode('utf-8', errors='ignore').strip(),
            'UNIQCODE': line[24:25].decode('utf-8', errors='ignore').strip(),
            'CCLASSC': line[25:28].decode('utf-8', errors='ignore').strip(),
            'TOTUNITS': read_packed_decimal(line, 29, 4, 2),
            'LSTDEPDT': read_packed_decimal(line, 78, 3),
            'DEPNOPRD': read_packed_decimal(line, 84, 2),
            'DEPFREQ': line[86:87].decode('utf-8', errors='ignore').strip(),
            'MARGINP': read_packed_decimal(line, 89, 1),
            'ORIGVAL': read_packed_decimal(line, 91, 4, 2),
            'CDOLARV': read_packed_decimal(line, 98, 4, 2),
            'CPRVDOLV': read_packed_decimal(line, 105, 4, 2),
            'CURCODE': line[134:137].decode('utf-8', errors='ignore').strip(),
            'PURGEIND': line[137:138].decode('utf-8', errors='ignore').strip(),
            'APPCODE': line[144:145].decode('utf-8', errors='ignore').strip(),
            'ACCTNO': read_packed_decimal(line, 146, 3),
            'OBLGCODE': line[151:152].decode('utf-8', errors='ignore').strip(),
            'NOTENO': read_packed_decimal(line, 153, 3),
            'COLLATERAL_EFF_DT': read_packed_decimal(line, 159, 3),
            'OBBAL': read_packed_decimal(line, 165, 4, 2),
            'TOTOBBAL': read_packed_decimal(line, 185, 4, 2),
            'AANO': line[192:205].decode('utf-8', errors='ignore').strip().upper(),
            'PROPERTY_IND': line[206:207].decode('utf-8', errors='ignore').strip(),
            'BONDID': line[222:223].decode('utf-8', errors='ignore').strip(),
            'COUPONRT': struct.unpack('>f', line[242:246])[0] if len(line) > 245 else 0,
            'MARGINVL': struct.unpack('>d', line[295:303])[0] if len(line) > 302 else 0,
            'ACCOSCT': read_packed_decimal(line, 309, 2),
            'COSTCTR1': read_packed_decimal(line, 313, 2),
            'RANK_OF_CHARGE': line[350:351].decode('utf-8', errors='ignore').strip(),
            'COLLATERAL_START_DT': read_packed_decimal(line, 352, 3)
        }
        
        if not (2500000000 <= record['ACCTNO'] <= 2599999999 or
                2850000000 <= record['ACCTNO'] <= 2859999999) and \
           record['APPCODE'] == 'C' and record['OBLGCODE'] == 'A':
            record['UCOLL'] = 'Y'
        
        return record
    except Exception as e:
        return None

coll_records = []
with open(COLL_FILE, 'rb') as f:
    for line in f:
        record = read_coll_record(line)
        if record:
            coll_records.append(record)

df_coll = pl.DataFrame(coll_records)

df_coll = df_coll.join(df_fofmt, on='CURCODE', how='left').with_columns([
    pl.when(pl.col('SPOTRATE').is_null()).then(1.00).otherwise(pl.col('SPOTRATE')).alias('SPOTRATE')
])

df_coll = df_coll.with_columns([
    pl.col('CDOLARV').alias('CDOLARVX')
]).with_columns([
    pl.when(~pl.col('CURCODE').is_in(['MYR', '   ']))
    .then((pl.col('CDOLARV') * pl.col('SPOTRATE')).round(2))
    .otherwise(pl.col('CDOLARV'))
    .alias('CDOLARV')
])

df_coll = df_coll.unique(subset=['ACCTNO', 'NOTENO', 'CCOLLNO']).sort(['ACCTNO', 'NOTENO'])

df_curr = pl.concat([
    pl.read_parquet(f'{MNITB_DIR}CURRENT.parquet'),
    pl.read_parquet(f'{MIITB_DIR}CURRENT.parquet')
]).with_columns([
    pl.col('ACCTNO').alias('NOTENO')
]).select(['ACCTNO', 'COSTCTR', 'NOTENO', 'BRANCH'])

df_note = pl.concat([
    pl.read_parquet(f'{MNILN_DIR}LNNOTE.parquet'),
    pl.read_parquet(f'{MIILN_DIR}LNNOTE.parquet')
]).with_columns([
    pl.col('NTBRCH').alias('BRANCH')
]).select(['ACCTNO', 'NOTENO', 'BRANCH', 'COSTCTR'])

df_lnnote = pl.concat([df_curr, df_note]).unique(subset=['ACCTNO', 'NOTENO'])

df_coll = df_lnnote.join(
    df_coll, on=['ACCTNO', 'NOTENO'], how='right'
).with_columns([
    pl.when(pl.col('COSTCTR').is_null())
    .then(pl.col('COSTCTR1'))
    .otherwise(pl.col('COSTCTR'))
    .alias('COSTCTR')
]).with_columns([
    pl.when((pl.col('COSTCTR') == 0) | pl.col('COSTCTR').is_null())
    .then(pl.col('ACCOSCT'))
    .otherwise(pl.col('COSTCTR'))
    .alias('COSTCTR')
]).with_columns([
    pl.when(
        (pl.col('APPCODE') == 'C') & 
        (pl.col('OBLGCODE') == 'A') &
        (((pl.col('ACCTNO') >= 2500000000) & (pl.col('ACCTNO') <= 2599999999)) |
         ((pl.col('ACCTNO') >= 2850000000) & (pl.col('ACCTNO') <= 2859999999)))
    )
    .then(pl.col('ACCOSCT'))
    .otherwise(pl.col('COSTCTR'))
    .alias('COSTCTR')
])

def read_desc_record(line, cclassc):
    """Parse DESC file record based on collateral class"""
    try:
        record = {
            'CCOLLNO': int(line[0:11].decode('utf-8', errors='ignore').strip() or 0),
            'CCLASSC': line[11:14].decode('utf-8', errors='ignore').strip(),
            'PURGEIND': line[14:15].decode('utf-8', errors='ignore').strip()
        }
        
        if cclassc in PROPER:
            record.update({
                'CINSTCL': line[50:52].decode('utf-8', errors='ignore').strip(),
                'IDTY1OWN': line[52:54].decode('utf-8', errors='ignore').strip(),
                'NATGUAR': line[54:56].decode('utf-8', errors='ignore').strip(),
                'IDTY2OWN': line[54:56].decode('utf-8', errors='ignore').strip(),
                'IDTY3OWN': line[56:58].decode('utf-8', errors='ignore').strip(),
                'CPRRANKC': line[58:59].decode('utf-8', errors='ignore').strip(),
                'CPRSHARE': line[59:60].decode('utf-8', errors='ignore').strip(),
                'CPRCHARG': read_packed_decimal(line, 61, 4, 2),
                'CPRLANDU': line[76:78].decode('utf-8', errors='ignore').strip(),
                'CPRPROPD': line[78:80].decode('utf-8', errors='ignore').strip(),
                'NAME1OWN': line[90:130].decode('utf-8', errors='ignore').strip(),
                'IDNO1OWN': line[130:170].decode('utf-8', errors='ignore').strip(),
                'NAME2OWN': line[170:210].decode('utf-8', errors='ignore').strip(),
                'IDNO2OWN': line[210:250].decode('utf-8', errors='ignore').strip(),
                'NAME3OWN': line[250:290].decode('utf-8', errors='ignore').strip(),
                'IDNO3OWN': line[290:330].decode('utf-8', errors='ignore').strip(),
                'OWNOCUPY': line[339:340].decode('utf-8', errors='ignore').strip(),
                'HOLDEXPD': line[340:341].decode('utf-8', errors='ignore').strip(),
                'EXPDATE': line[341:349].decode('utf-8', errors='ignore').strip(),
                'BLTNAREA': line[365:369].decode('utf-8', errors='ignore').strip(),
                'BLTNUNIT': line[369:370].decode('utf-8', errors='ignore').strip(),
                'QUITRENT': line[378:382].decode('utf-8', errors='ignore').strip(),
                'ASSESSDT': line[382:386].decode('utf-8', errors='ignore').strip(),
                'CPRSTAT': line[386:394].decode('utf-8', errors='ignore').strip(),
                'CPOLYNUM': line[394:410].decode('utf-8', errors='ignore').strip(),
                'TTLPARTCLR': line[410:411].decode('utf-8', errors='ignore').strip(),
                'MASTOWNR': line[450:490].decode('utf-8', errors='ignore').strip(),
                'TTLENO': line[490:530].decode('utf-8', errors='ignore').strip(),
                'TTLMUKIM': line[530:570].decode('utf-8', errors='ignore').strip(),
                'TTLID': line[650:665].decode('utf-8', errors='ignore').strip(),
                'LANDAREA': line[665:669].decode('utf-8', errors='ignore').strip(),
                'INSURER': line[669:671].decode('utf-8', errors='ignore').strip(),
                'FIREDATE': line[671:679].decode('utf-8', errors='ignore').strip(),
                'SUMINSUR': read_packed_decimal(line, 680, 4),
                'ESCL': line[687:688].decode('utf-8', errors='ignore').strip(),
                'LANDUNIT': line[688:689].decode('utf-8', errors='ignore').strip(),
                'MRESERVE': line[689:690].decode('utf-8', errors='ignore').strip(),
                'DEVNAME': line[690:730].decode('utf-8', errors='ignore').strip(),
                'PRJCTNAM': line[730:770].decode('utf-8', errors='ignore').strip(),
                'CTRYSTAT': line[770:772].decode('utf-8', errors='ignore').strip(),
                'PRABANDT': line[772:780].decode('utf-8', errors='ignore').strip(),
                'CPRDISDT': line[780:788].decode('utf-8', errors='ignore').strip(),
                'COLLATERAL_END_DT': line[780:788].decode('utf-8', errors='ignore').strip(),
                'CPOSCODE': line[796:801].decode('utf-8', errors='ignore').strip(),
                'CPRVALIN2': line[802:803].decode('utf-8', errors='ignore').strip(),
                'STDESIGN': line[803:804].decode('utf-8', errors='ignore').strip(),
                'CPRFORSV': read_packed_decimal(line, 811, 4, 2),
                'CTRYCODE': line[818:820].decode('utf-8', errors='ignore').strip(),
                'CPRESVAL': read_packed_decimal(line, 827, 4, 2),
                'CPTYPOS': line[834:835].decode('utf-8', errors='ignore').strip(),
                'CPRVALDT': line[850:858].decode('utf-8', errors='ignore').strip(),
                'CPRVALIN': line[858:859].decode('utf-8', errors='ignore').strip(),
                'CPPVALDT': line[859:867].decode('utf-8', errors='ignore').strip(),
                'PROJPHSE': line[867:890].decode('utf-8', errors='ignore').strip(),
                'CPHSENO': line[890:900].decode('utf-8', errors='ignore').strip(),
                'CPBUILNO': line[900:930].decode('utf-8', errors='ignore').strip(),
                'CPRPARC2': line[930:970].decode('utf-8', errors='ignore').strip(),
                'CPRPARC3': line[970:1010].decode('utf-8', errors='ignore').strip(),
                'CPRPARC4': line[1010:1050].decode('utf-8', errors='ignore').strip(),
                'CPSTATE': line[1050:1052].decode('utf-8', errors='ignore').strip(),
                'CPSTATEN': line[1052:1066].decode('utf-8', errors='ignore').strip(),
                'CPTOWN': line[1066:1069].decode('utf-8', errors='ignore').strip(),
                'CPTOWNN': line[1069:1089].decode('utf-8', errors='ignore').strip(),
                'CPLOCAT': line[1089:1090].decode('utf-8', errors='ignore').strip(),
                'CPRVALU1': line[1090:1130].decode('utf-8', errors='ignore').strip(),
                'CPRVALU2': line[1130:1170].decode('utf-8', errors='ignore').strip(),
                'CPRVALU3': line[1170:1210].decode('utf-8', errors='ignore').strip(),
                'ENV_NO_REF': line[1210:1250].decode('utf-8', errors='ignore').strip(),
                'REMARKS': line[1250:1290].decode('utf-8', errors='ignore').strip(),
                'VALSOURCE_CD': line[1290:1291].decode('utf-8', errors='ignore').strip()
            })
            
            cprparc1 = f"{record.get('CPHSENO', '')} {record.get('CPBUILNO', '')}".strip()
            record['CPRPARC1'] = ' '.join(cprparc1.split())
            
        elif cclassc in HP:
            record.update({
                'CINSTCL': line[50:52].decode('utf-8', errors='ignore').strip(),
                'CHPVECON': line[73:75].decode('utf-8', errors='ignore').strip(),
                'CHPVEHNO': line[75:87].decode('utf-8', errors='ignore').strip(),
                'CHPENGIN': line[130:150].decode('utf-8', errors='ignore').strip(),
                'CHPCHASS': line[150:170].decode('utf-8', errors='ignore').strip(),
                'CHPMAKE': line[330:370].decode('utf-8', errors='ignore').strip(),
                'REMARKS': line[490:530].decode('utf-8', errors='ignore').strip(),
                'COLLATERAL_END_DT': line[530:538].decode('utf-8', errors='ignore').strip()
            })
            
        elif cclassc in OTHERHP:
            record.update({
                'CINSTCL': line[50:52].decode('utf-8', errors='ignore').strip(),
                'COVDESCR': line[54:56].decode('utf-8', errors='ignore').strip(),
                'COVVALDT': line[74:82].decode('utf-8', errors='ignore').strip(),
                'REMARKS': line[250:290].decode('utf-8', errors='ignore').strip(),
                'COLLATERAL_END_DT': line[290:298].decode('utf-8', errors='ignore').strip(),
                'ACTUAL_SALE_VALUE': read_packed_decimal(line, 299, 4, 2),
                'CPRFORSV': read_packed_decimal(line, 307, 4, 2)
            })
            
        elif cclassc in PLTMACH:
            record.update({
                'CINSTCL': line[50:52].decode('utf-8', errors='ignore').strip(),
                'CPMDESCR': line[54:56].decode('utf-8', errors='ignore').strip(),
                'CPMVALDT': line[74:82].decode('utf-8', errors='ignore').strip(),
                'REMARKS': line[250:290].decode('utf-8', errors='ignore').strip(),
                'COLLATERAL_END_DT': line[290:298].decode('utf-8', errors='ignore').strip(),
                'ACTUAL_SALE_VALUE': read_packed_decimal(line, 299, 4, 2),
                'CPRFORSV': read_packed_decimal(line, 307, 4, 2)
            })
            
        elif cclassc in CONCESS:
            record.update({
                'CINSTCL': line[50:52].decode('utf-8', errors='ignore').strip(),
                'CCCDESCR': line[54:56].decode('utf-8', errors='ignore').strip(),
                'CCCVALDT': line[74:82].decode('utf-8', errors='ignore').strip(),
                'REMARKS': line[250:290].decode('utf-8', errors='ignore').strip(),
                'COLLATERAL_END_DT': line[290:298].decode('utf-8', errors='ignore').strip()
            })
            
        elif cclassc in FDR:
            record.update({
                'CINSTCL': line[50:52].decode('utf-8', errors='ignore').strip(),
                'CFDDESCR': line[58:60].decode('utf-8', errors='ignore').strip(),
                'NAME1OWN': line[90:130].decode('utf-8', errors='ignore').strip(),
                'IDNO1OWN': line[130:170].decode('utf-8', errors='ignore').strip(),
                'NAME2OWN': line[170:210].decode('utf-8', errors='ignore').strip(),
                'IDNO2OWN': line[210:250].decode('utf-8', errors='ignore').strip(),
                'NAME3OWN': line[250:290].decode('utf-8', errors='ignore').strip(),
                'IDNO3OWN': line[290:330].decode('utf-8', errors='ignore').strip(),
                'REMARKS': line[410:450].decode('utf-8', errors='ignore').strip(),
                'CFDVALDT': line[450:458].decode('utf-8', errors='ignore').strip(),
                'FDMATDT': line[458:466].decode('utf-8', errors='ignore').strip(),
                'FDACCTNO': int(line[466:476].decode('utf-8', errors='ignore').strip() or 0),
                'FDCDNO': int(line[476:486].decode('utf-8', errors='ignore').strip() or 0),
                'COLLATERAL_UPLIFT_DT': line[490:498].decode('utf-8', errors='ignore').strip(),
                'COLLATERAL_END_DT': line[490:498].decode('utf-8', errors='ignore').strip(),
                'ACTUAL_SALE_VALUE': read_packed_decimal(line, 499, 4, 2),
                'CPRFORSV': read_packed_decimal(line, 507, 4, 2)
            })
            
        elif cclassc in DEBENT:
            record.update({
                'CINSTCL': line[50:52].decode('utf-8', errors='ignore').strip(),
                'CDEBAMT': read_packed_decimal(line, 55, 4, 2),
                'CDEBDESC': line[70:72].decode('utf-8', errors='ignore').strip(),
                'CDEBVALD': line[90:98].decode('utf-8', errors='ignore').strip(),
                'REMARKS': line[290:330].decode('utf-8', errors='ignore').strip(),
                'COLLATERAL_END_DT': line[330:338].decode('utf-8', errors='ignore').strip(),
                'ACTUAL_SALE_VALUE': read_packed_decimal(line, 339, 4, 2),
                'CPRESVAL': read_packed_decimal(line, 347, 4, 2),
                'DEBENTURE_REALISABLE_VALUE': read_packed_decimal(line, 355, 4, 2)
            })
            
        elif cclassc in GUARANT:
            record.update({
                'CINSTCL': line[50:52].decode('utf-8', errors='ignore').strip(),
                'CGUARNAT': line[54:56].decode('utf-8', errors='ignore').strip(),
                'CGUARCTY': line[56:58].decode('utf-8', errors='ignore').strip(),
                'CGEFFDAT': line[58:66].decode('utf-8', errors='ignore').strip(),
                'CGEXPDAT': line[66:74].decode('utf-8', errors='ignore').strip(),
                'SCHEME_CD': line[74:76].decode('utf-8', errors='ignore').strip(),
                'GUARANTOR_ENTITY_TYPE': line[76:78].decode('utf-8', errors='ignore').strip(),
                'GUARANTOR_BIRTH_REGISTER_DT': line[78:86].decode('utf-8', errors='ignore').strip(),
                'GTEEFEE': read_packed_decimal(line, 91, 4, 2),
                'CGEXAMTG': read_packed_decimal(line, 107, 4, 2),
                'CGCGSR': line[124:127].decode('utf-8', errors='ignore').strip(),
                'CGCGUR': line[127:130].decode('utf-8', errors='ignore').strip(),
                'CGUARNAM': line[130:170].decode('utf-8', errors='ignore').strip(),
                'CGUARID': line[170:210].decode('utf-8', errors='ignore').strip(),
                'CGUARLG': line[210:250].decode('utf-8', errors='ignore').strip(),
                'SBETRAN': line[290:330].decode('utf-8', errors='ignore').strip(),
                'REMARKS': line[330:370].decode('utf-8', errors='ignore').strip(),
                'COLLATERAL_END_DT': line[703:711].decode('utf-8', errors='ignore').strip(),
                'GUARANTOR_ID_TYPE': line[711:713].decode('utf-8', errors='ignore').strip(),
                'EAST_MALAYSIA_IND': line[713:714].decode('utf-8', errors='ignore').strip(),
                'BNMASSIGNID': line[730:742].decode('utf-8', errors='ignore').strip(),
                'CUSTNO': int(line[742:753].decode('utf-8', errors='ignore').strip() or 0)
            })
            
        elif cclassc in SHARE:
            record.update({
                'COLLATERAL_END_DT': line[1650:1658].decode('utf-8', errors='ignore').strip()
            })
            
        elif cclassc in NP:
            record.update({
                'COLLATERAL_END_DT': line[50:58].decode('utf-8', errors='ignore').strip()
            })
        
        return record
    except Exception as e:
        return None

desc_records = []
with open(DESC_FILE, 'rb') as f:
    for line in f:
        try:
            cclassc = line[11:14].decode('utf-8', errors='ignore').strip()
            record = read_desc_record(line, cclassc)
            if record:
                desc_records.append(record)
        except:
            continue

df_desc = pl.DataFrame(desc_records).sort('CCOLLNO')

df_collater = df_coll.join(df_desc, on='CCOLLNO', how='left').with_columns([
    pl.when((pl.col('EXPDATE') < 0) | pl.col('EXPDATE').is_null()).then(pl.lit('00000000')).otherwise(pl.col('EXPDATE')).alias('EXPDATE'),
    pl.when((pl.col('FIREDATE') < 0) | pl.col('FIREDATE').is_null()).then(pl.lit('00000000')).otherwise(pl.col('FIREDATE')).alias('FIREDATE'),
    pl.when((pl.col('CPRVALDT') < 0) | pl.col('CPRVALDT').is_null()).then(pl.lit('00000000')).otherwise(pl.col('CPRVALDT')).alias('CPRVALDT'),
    pl.when((pl.col('COVVALDT') < 0) | pl.col('COVVALDT').is_null()).then(pl.lit('00000000')).otherwise(pl.col('COVVALDT')).alias('COVVALDT'),
    pl.when((pl.col('CPMVALDT') < 0) | pl.col('CPMVALDT').is_null()).then(pl.lit('00000000')).otherwise(pl.col('CPMVALDT')).alias('CPMVALDT'),
    pl.when((pl.col('CCCVALDT') < 0) | pl.col('CCCVALDT').is_null()).then(pl.lit('00000000')).otherwise(pl.col('CCCVALDT')).alias('CCCVALDT'),
    pl.when((pl.col('CFDVALDT') < 0) | pl.col('CFDVALDT').is_null()).then(pl.lit('00000000')).otherwise(pl.col('CFDVALDT')).alias('CFDVALDT'),
    pl.when((pl.col('CDEBVALD') < 0) | pl.col('CDEBVALD').is_null()).then(pl.lit('00000000')).otherwise(pl.col('CDEBVALD')).alias('CDEBVALD'),
    pl.when((pl.col('CGEFFDAT') < 0) | pl.col('CGEFFDAT').is_null()).then(pl.lit('00000000')).otherwise(pl.col('CGEFFDAT')).alias('CGEFFDAT'),
    pl.when((pl.col('CGEXPDAT') < 0) | pl.col('CGEXPDAT').is_null()).then(pl.lit('00000000')).otherwise(pl.col('CGEXPDAT')).alias('CGEXPDAT'),
    pl.when((pl.col('PRABANDT') < 0) | pl.col('PRABANDT').is_null()).then(pl.lit('00000000')).otherwise(pl.col('PRABANDT')).alias('PRABANDT'),
    pl.when((pl.col('SUMINSUR') == 404040404040404) | pl.col('SUMINSUR').is_null()).then(0).otherwise(pl.col('SUMINSUR')).alias('SUMINSUR')
]).sort('ACCTNO')

df_collater = df_collater.with_columns([
    pl.col('CPRFORSV').alias('FORCE_SALE_VALUE_X'),
    (pl.col('CPRFORSV') * pl.col('SPOTRATE')).round(2).alias('CPRFORSV'),
    pl.col('CPRESVAL').alias('RESERVED_VALUE_X'),
    (pl.col('CPRESVAL') * pl.col('SPOTRATE')).round(2).alias('CPRESVAL'),
    pl.col('ACTUAL_SALE_VALUE').alias('ACTUAL_SALE_VALUE_X'),
    (pl.col('ACTUAL_SALE_VALUE') * pl.col('SPOTRATE')).round(2).alias('ACTUAL_SALE_VALUE'),
    pl.col('DEBENTURE_REALISABLE_VALUE').alias('DEBENTURE_REALISABLE_VALUE_X'),
    (pl.col('DEBENTURE_REALISABLE_VALUE') * pl.col('SPOTRATE')).round(2).alias('DEBENTURE_REALISABLE_VALUE')
])

df_collater_islamic = df_collater.filter(
    (pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999)
)

df_collater_conv = df_collater.filter(
    ~((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))
)

df_collater_islamic.write_parquet(f'{LOANI_DIR}COLLATER.parquet')
df_collater_conv.write_parquet(f'{LOAN_DIR}COLLATER.parquet')

print(f"Collateral Data Processing Complete")
print(f"\nOutput files generated:")
print(f"  {LOAN_DIR}COLLATER.parquet ({len(df_collater_conv)} records)")
print(f"  {LOANI_DIR}COLLATER.parquet ({len(df_collater_islamic)} records)")
print(f"\nFirst 20 records from LOAN.COLLATER:")
print(df_collater_conv.head(20))
print(f"\nFirst 20 records from LOANI.COLLATER:")
print(df_collater_islamic.head(20))
