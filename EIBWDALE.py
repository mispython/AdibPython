import polars as pl
import struct
from datetime import datetime

DEPOSIT_DIR = 'data/deposit/'
IDEPOSIT_DIR = 'data/ideposit/'
ODWOF_DIR = 'data/odwof/'
IODWOF_DIR = 'data/iodwof/'
FCYCA_DIR = 'data/fcyca/'
DPTRBL_FILE = 'data/dptrbl.dat'
DPTRBL1_FILE = 'data/dptrbl1.dat'
FROZEN_FILE = 'data/frozen.dat'
ENTP_FILE = 'data/entp.dat'
MADB_FILE = 'data/madb.dat'
DPACC_FILE = 'data/dpacc.dat'
OMNI_FILE = 'data/omni.dat'
RNRDB_FILE = 'data/rnrdb.dat'

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

with open(DPTRBL_FILE, 'rb') as f:
    first_line = f.readline()
    tbdate = read_packed_decimal(first_line, 106, 3)
    
reptdate_str = str(int(tbdate)).zfill(11)[:8]
reptdate = datetime.strptime(reptdate_str, '%m%d%Y').date()

noday = reptdate.day
reptyear = reptdate.year
reptmon = f'{reptdate.month:02d}'
reptday = f'{reptdate.day:02d}'
rdate = reptdate.strftime('%d%m%y')

df_reptdate = pl.DataFrame({
    'REPTDATE': [reptdate]
})

df_reptdate.write_parquet(f'{DEPOSIT_DIR}REPTDATE.parquet')
df_reptdate.write_parquet(f'{IDEPOSIT_DIR}REPTDATE.parquet')

def parse_deposit_record(line):
    """Parse deposit account record from DPTRBL"""
    try:
        record = {}
        
        bankno = read_packed_decimal(line, 3, 1)
        reptno = read_packed_decimal(line, 24, 2)
        fmtcode = read_packed_decimal(line, 27, 1)
        
        if bankno != 33 or reptno != 1001 or fmtcode not in [1, 5, 10, 22]:
            return None
        
        record['FMTCODE'] = fmtcode
        record['BRANCH'] = read_packed_decimal(line, 106, 2)
        record['ACCTNO'] = read_packed_decimal(line, 110, 3)
        record['NAME'] = line[115:130].decode('utf-8', errors='ignore').strip()
        record['TAXNO'] = line[130:141].decode('utf-8', errors='ignore').strip()
        record['DEBIT'] = read_packed_decimal(line, 143, 4, 2)
        record['CREDIT'] = read_packed_decimal(line, 151, 4, 2)
        record['CLOSEDT'] = read_packed_decimal(line, 158, 3)
        record['REOPENDT'] = read_packed_decimal(line, 164, 3)
        record['CUSTCODE'] = read_packed_decimal(line, 176, 2)
        record['INTPAID'] = read_packed_decimal(line, 193, 4, 2)
        record['ODPLAN'] = read_packed_decimal(line, 200, 1)
        record['RATE1'] = read_packed_decimal(line, 202, 2, 3)
        record['RATE2'] = read_packed_decimal(line, 205, 2, 3)
        record['RATE3'] = read_packed_decimal(line, 208, 2, 3)
        record['RATE4'] = read_packed_decimal(line, 211, 2, 3)
        record['RATE5'] = read_packed_decimal(line, 214, 2, 3)
        record['TODRATE'] = read_packed_decimal(line, 217, 2, 3)
        record['FLATRATE'] = read_packed_decimal(line, 220, 2, 3)
        record['BASERATE'] = read_packed_decimal(line, 223, 2, 3)
        record['ODSTAT'] = line[225:227].decode('utf-8', errors='ignore').strip()
        record['ORGCODE'] = line[227:230].decode('utf-8', errors='ignore').strip()
        record['ORGTYPE'] = line[230:231].decode('utf-8', errors='ignore').strip()
        record['LIMIT1'] = read_packed_decimal(line, 232, 3)
        record['LIMIT2'] = read_packed_decimal(line, 238, 3)
        record['LIMIT3'] = read_packed_decimal(line, 244, 3)
        record['LIMIT4'] = read_packed_decimal(line, 250, 3)
        record['LIMIT5'] = read_packed_decimal(line, 256, 3)
        record['INTYTD'] = read_packed_decimal(line, 268, 3, 2)
        record['FEEPD'] = read_packed_decimal(line, 274, 3, 2)
        record['PURPOSE'] = line[285:286].decode('utf-8', errors='ignore').strip()
        record['COL1'] = line[286:291].decode('utf-8', errors='ignore').strip()
        record['COL2'] = line[291:296].decode('utf-8', errors='ignore').strip()
        record['COL3'] = line[296:301].decode('utf-8', errors='ignore').strip()
        record['COL4'] = line[301:306].decode('utf-8', errors='ignore').strip()
        record['COL5'] = line[306:311].decode('utf-8', errors='ignore').strip()
        record['SECTOR'] = read_packed_decimal(line, 312, 2)
        record['USER2'] = line[314:315].decode('utf-8', errors='ignore').strip()
        record['USER3'] = line[315:316].decode('utf-8', errors='ignore').strip()
        record['RISKCODE'] = line[316:317].decode('utf-8', errors='ignore').strip()
        record['LEDGBAL'] = read_packed_decimal(line, 319, 4, 2)
        record['DATELSTDEP'] = read_packed_decimal(line, 326, 3)
        record['L_DEP'] = read_packed_decimal(line, 331, 4, 2)
        record['OPENIND'] = line[337:338].decode('utf-8', errors='ignore').strip()
        record['STATCD'] = line[337:347].decode('utf-8', errors='ignore').strip()
        record['LASTTRAN'] = read_packed_decimal(line, 357, 3)
        record['RETURNS_Y'] = read_packed_decimal(line, 375, 2)
        record['STMT_CYCLEX'] = read_packed_decimal(line, 381, 1)
        record['CHGIND'] = int(line[384:385].decode('utf-8', errors='ignore').strip() or 0)
        record['AVGAMT'] = read_packed_decimal(line, 391, 3)
        record['PRODUCT'] = read_packed_decimal(line, 397, 1)
        record['RACE'] = line[398:399].decode('utf-8', errors='ignore').strip()
        record['DEPTYPE'] = line[430:431].decode('utf-8', errors='ignore').strip()
        record['INT1'] = read_packed_decimal(line, 432, 3, 2)
        record['INTPD'] = read_packed_decimal(line, 438, 3, 2)
        record['INTPLAN'] = read_packed_decimal(line, 444, 1)
        record['CURBAL'] = read_packed_decimal(line, 466, 4, 2)
        record['CHQFLOAT'] = read_packed_decimal(line, 474, 4, 2)
        record['IA_LRU'] = line[505:506].decode('utf-8', errors='ignore').strip()
        record['INT2'] = read_packed_decimal(line, 516, 5, 9)
        record['MTDLOWBA'] = read_packed_decimal(line, 526, 3, 2)
        record['BENINTPD'] = read_packed_decimal(line, 532, 3, 2)
        record['STATE'] = line[544:546].decode('utf-8', errors='ignore').strip()
        record['INTCYCODE'] = line[562:565].decode('utf-8', errors='ignore').strip()
        record['APPRLIMT'] = read_packed_decimal(line, 566, 3)
        record['ODINTCHR'] = read_packed_decimal(line, 572, 3, 2)
        record['ODINTACC'] = read_packed_decimal(line, 578, 5, 9)
        record['CURCODE'] = line[587:590].decode('utf-8', errors='ignore').strip()
        record['PBIND'] = line[627:628].decode('utf-8', errors='ignore').strip()
        record['INTRATE'] = read_packed_decimal(line, 629, 2, 3)
        record['STMT_DT'] = read_packed_decimal(line, 650, 3)
        record['YTDAVAMT'] = read_packed_decimal(line, 671, 4, 2)
        record['BDATE'] = read_packed_decimal(line, 717, 3)
        record['INACTIVE'] = line[722:723].decode('utf-8', errors='ignore').strip()
        record['SECOND'] = read_packed_decimal(line, 724, 2)
        record['ODXSAMT'] = read_packed_decimal(line, 727, 4, 2)
        record['BONUTYPE'] = read_packed_decimal(line, 734, 1)
        record['SERVICE'] = read_packed_decimal(line, 736, 1)
        record['BONUSANO'] = read_packed_decimal(line, 738, 5)
        record['USER5'] = line[755:756].decode('utf-8', errors='ignore').strip()
        record['TRACKCD'] = line[775:787].decode('utf-8', errors='ignore').strip()
        record['EXODDATE'] = read_packed_decimal(line, 801, 3)
        record['TEMPODDT'] = read_packed_decimal(line, 807, 3)
        record['AGENT_CD'] = read_packed_decimal(line, 813, 1)
        record['SCHIND'] = line[815:817].decode('utf-8', errors='ignore').strip()
        record['PREVBRNO'] = read_packed_decimal(line, 818, 2)
        record['AVGBAL'] = read_packed_decimal(line, 822, 4, 2)
        record['COSTCTR'] = read_packed_decimal(line, 830, 2)
        record['AUTHORISE_LIMIT'] = read_packed_decimal(line, 840, 3)
        record['CRRCODE'] = line[845:848].decode('utf-8', errors='ignore').strip()
        record['CCRICODE'] = read_packed_decimal(line, 849, 2)
        record['FAACRR'] = line[851:854].decode('utf-8', errors='ignore').strip()
        record['POST_IND'] = line[854:855].decode('utf-8', errors='ignore').strip()
        record['CENSUST'] = read_packed_decimal(line, 856, 2)
        record['ACCPROF'] = read_packed_decimal(line, 864, 5, 9)
        record['MAXPROF'] = read_packed_decimal(line, 874, 3, 2)
        record['INTRSTPD'] = read_packed_decimal(line, 880, 5, 2)
        record['MTDAVBAL'] = read_packed_decimal(line, 890, 4, 2)
        record['SM_STATUS'] = line[897:898].decode('utf-8', errors='ignore').strip()
        record['SM_DATE'] = read_packed_decimal(line, 899, 3)
        record['VB'] = line[915:916].decode('utf-8', errors='ignore').strip()
        record['BILLERIND'] = line[916:917].decode('utf-8', errors='ignore').strip()
        record['MODIFIED_FACILITY_IND'] = line[917:921].decode('utf-8', errors='ignore').strip()
        record['DTLSTCUST'] = read_packed_decimal(line, 922, 3)
        record['INTPDPYR'] = read_packed_decimal(line, 927, 3, 2)
        record['OPENDT'] = read_packed_decimal(line, 933, 3)
        record['PRIN_ACCT'] = read_packed_decimal(line, 939, 3)
        record['CASH_DEPOSIT_LIMIT_IND'] = int(line[951:952].decode('utf-8', errors='ignore').strip() or 0)
        record['CASH_DEPOSIT_AMOUNT_AGG'] = read_packed_decimal(line, 959, 3, 2)
        record['NXTREVDT'] = read_packed_decimal(line, 965, 3)
        record['LSTREVDT'] = read_packed_decimal(line, 971, 3)
        
        record['DNBFISME'] = '0'
        
        if record['OPENDT'] == record['REOPENDT']:
            record['REOPENDT'] = None
        
        custcode = record['CUSTCODE']
        if 0 < custcode <= 99:
            record['DNBFISME'] = '0'
        elif 100 <= custcode <= 999:
            custcode_str = str(int(custcode)).zfill(3)
            record['CUSTCODE'] = int(custcode_str[1:3])
            record['DNBFISME'] = custcode_str[0]
        elif 1000 <= custcode <= 9999:
            custcode_str = str(int(custcode)).zfill(4)
            record['CUSTCODE'] = int(custcode_str[2:4])
            record['DNBFISME'] = custcode_str[1]
        elif 10000 <= custcode <= 99999:
            custcode_str = str(int(custcode)).zfill(5)
            record['CUSTCODE'] = int(custcode_str[3:5])
            record['DNBFISME'] = custcode_str[2]
        
        if record['DATELSTDEP'] and record['DATELSTDEP'] != 0:
            dlstdep_str = str(int(record['DATELSTDEP'])).zfill(9)[:6]
            try:
                record['DATE_LST_DEP'] = datetime.strptime(dlstdep_str, '%m%d%y').date()
            except:
                record['DATE_LST_DEP'] = None
        
        if record['SM_DATE'] and record['SM_DATE'] != 0:
            sm_str = str(int(record['SM_DATE'])).zfill(10)
            try:
                record['SM_DATE'] = datetime.strptime(f"{sm_str[1:3]}{sm_str[3:5]}{sm_str[5:9]}", '%m%d%Y').date()
            except:
                record['SM_DATE'] = None
        
        if record['STMT_DT'] and record['STMT_DT'] != 0:
            stmt_str = str(int(record['STMT_DT'])).zfill(8)
            try:
                record['NXT_STMT_CYCLE_DT'] = datetime.strptime(stmt_str, '%Y%m%d').date()
            except:
                record['NXT_STMT_CYCLE_DT'] = None
        
        record['STMT_CYCLE'] = str(record['STMT_CYCLEX']).zfill(3) if record['STMT_CYCLEX'] else None
        record['AC_OPEN_STATUS_CD'] = str(record['AGENT_CD']).zfill(3) if record['AGENT_CD'] else None
        
        if fmtcode in [1, 10, 22]:
            record['DPMTDBAL'] = record['MTDAVBAL']
            record['INTPAYBL'] = (record.get('INT1', 0) or 0) + (record.get('INT2', 0) or 0)
            
            if record['MTDAVBAL'] and record['MTDAVBAL'] != 0:
                record['MTDAVBAL'] = record['MTDAVBAL'] / noday
            
            if record['OPENDT'] and record['OPENDT'] != 0:
                opendt_str = str(int(record['OPENDT'])).zfill(11)[:8]
                try:
                    opendt = datetime.strptime(opendt_str, '%m%d%Y').date()
                    if opendt.month == reptdate.month and opendt.year == reptdate.year:
                        record['OPENMH'] = 1
                    else:
                        record['OPENMH'] = 0
                except:
                    record['OPENMH'] = 0
            else:
                record['OPENMH'] = 0
            
            if record['CLOSEDT'] and record['CLOSEDT'] != 0:
                closedt_str = str(int(record['CLOSEDT'])).zfill(11)[:8]
                try:
                    closedt = datetime.strptime(closedt_str, '%m%d%Y').date()
                    if closedt.month == reptdate.month and closedt.year == reptdate.year:
                        record['CLOSEMH'] = 1
                    else:
                        record['CLOSEMH'] = 0
                except:
                    record['CLOSEMH'] = 0
            else:
                record['CLOSEMH'] = 0
            
            record['ACCYTD'] = 0
            if record['OPENDT'] and record['OPENDT'] != 0 and (not record['CLOSEDT'] or record['CLOSEDT'] == 0):
                opendt_str = str(int(record['OPENDT'])).zfill(11)[:8]
                try:
                    opendt = datetime.strptime(opendt_str, '%m%d%Y').date()
                    if opendt.year == reptdate.year:
                        record['ACCYTD'] = 1
                except:
                    pass
        
        return record
        
    except Exception as e:
        return None

deposit_records = []
with open(DPTRBL_FILE, 'rb') as f:
    for line in f:
        record = parse_deposit_record(line)
        if record:
            deposit_records.append(record)

df_all = pl.DataFrame(deposit_records)

df_saving = df_all.filter(pl.col('DEPTYPE') == 'S')
df_uma = df_all.filter(pl.col('PRODUCT').is_in([297, 298]))
df_current = df_all.filter(pl.col('DEPTYPE').is_in(['D', 'N']))
df_fd = df_all.filter(pl.col('DEPTYPE') == 'C')
df_vostro = df_all.filter(pl.col('PRODUCT').is_in([500, 507, 509, 510, 521, 526, 528, 530, 531]))

df_fofmt = pl.read_parquet(f'{FCYCA_DIR}FOFMT.parquet')
forate_dict = dict(zip(df_fofmt['CURCODE'].to_list(), df_fofmt['SPOTRATE'].to_list()))

def apply_forex(df):
    """Apply foreign exchange rates"""
    if len(df) == 0:
        return df
    
    df = df.with_columns([
        pl.when(pl.col('CURCODE') != 'MYR')
        .then(pl.col('CURBAL'))
        .otherwise(None)
        .alias('FORBAL'),
        pl.when(pl.col('CURCODE') != 'MYR')
        .then((pl.col('CURBAL') * pl.col('CURCODE').map_dict(forate_dict, default=1.0)).round(2))
        .otherwise(pl.col('CURBAL'))
        .alias('CURBAL')
    ])
    
    return df

df_saving_conv = df_saving.filter(~((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999)))
df_saving_islamic = df_saving.filter((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))

df_uma_conv = df_uma.filter(~((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999)))
df_uma_islamic = df_uma.filter((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))

df_current_conv = df_current.filter(~((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999)))
df_current_islamic = df_current.filter((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))

df_fd_conv = df_fd.filter(~((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999)))
df_fd_islamic = df_fd.filter((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))
df_fd_tia = df_fd_islamic.filter(pl.col('PRODUCT').is_in([317, 318]))
df_fd_islamic = df_fd_islamic.filter(~pl.col('PRODUCT').is_in([317, 318]))

df_vostro_conv = df_vostro.filter(~((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999)))
df_vostro_islamic = df_vostro.filter((pl.col('COSTCTR') >= 3000) & (pl.col('COSTCTR') <= 4999))

df_current_conv = apply_forex(df_current_conv)
df_current_islamic = apply_forex(df_current_islamic)
df_vostro_conv = apply_forex(df_vostro_conv)
df_vostro_islamic = apply_forex(df_vostro_islamic)
df_fd_conv = apply_forex(df_fd_conv)
df_fd_islamic = apply_forex(df_fd_islamic)

df_saving_conv.write_parquet(f'{DEPOSIT_DIR}SAVING.parquet')
df_saving_islamic.write_parquet(f'{IDEPOSIT_DIR}SAVING.parquet')
df_uma_conv.write_parquet(f'{DEPOSIT_DIR}UMA.parquet')
df_uma_islamic.write_parquet(f'{IDEPOSIT_DIR}UMA.parquet')
df_current_conv.write_parquet(f'{DEPOSIT_DIR}CURRENT.parquet')
df_current_islamic.write_parquet(f'{IDEPOSIT_DIR}CURRENT.parquet')
df_fd_conv.write_parquet(f'{DEPOSIT_DIR}FD.parquet')
df_fd_islamic.write_parquet(f'{IDEPOSIT_DIR}FD.parquet')
df_fd_tia.write_parquet(f'{IDEPOSIT_DIR}TIA.parquet')
df_vostro_conv.write_parquet(f'{DEPOSIT_DIR}VOSTRO.parquet')
df_vostro_islamic.write_parquet(f'{IDEPOSIT_DIR}VOSTRO.parquet')

print(f"Deposit Account Data Extraction Complete")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
print(f"\nConventional Banking:")
print(f"  SAVING:  {len(df_saving_conv)} records")
print(f"  UMA:     {len(df_uma_conv)} records")
print(f"  CURRENT: {len(df_current_conv)} records")
print(f"  FD:      {len(df_fd_conv)} records")
print(f"  VOSTRO:  {len(df_vostro_conv)} records")
print(f"\nIslamic Banking:")
print(f"  SAVING:  {len(df_saving_islamic)} records")
print(f"  UMA:     {len(df_uma_islamic)} records")
print(f"  CURRENT: {len(df_current_islamic)} records")
print(f"  FD:      {len(df_fd_islamic)} records")
print(f"  TIA:     {len(df_fd_tia)} records")
print(f"  VOSTRO:  {len(df_vostro_islamic)} records")
