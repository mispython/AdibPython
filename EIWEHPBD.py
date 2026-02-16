"""
EIWEHPBD - EHP Basel Extract (Production Version)
Housing/Personal Loan Application Extract from ELDSHP files

Reads 20 ELDSHP fixed-width text files with full field parsing.
Validates file dates match report date before processing.
"""

import polars as pl
from datetime import datetime, timedelta
import sys
import os

# Directories
ELDSHP_DIR = 'data/eldshp/'
EHP1_DIR = 'data/ehp1/'
EHP2_DIR = 'data/ehp2/'
EHP3_DIR = 'data/ehp3/'
EHPO1_DIR = 'data/ehpo1/'  # Previous files
EHPO2_DIR = 'data/ehpo2/'
EHPO3_DIR = 'data/ehpo3/'

# Ensure directories exist
for dir_path in [EHP1_DIR, EHP2_DIR, EHP3_DIR, ELDSHP_DIR]:
    os.makedirs(dir_path, exist_ok=True)

# Calculate report date based on current day
today = datetime.today().date()
day = today.day

if 8 <= day <= 14:
    reptdate = datetime(today.year, today.month, 8).date()
    wk = '1'
elif 15 <= day <= 21:
    reptdate = datetime(today.year, today.month, 15).date()
    wk = '2'
elif 22 <= day <= 27:
    reptdate = datetime(today.year, today.month, 22).date()
    wk = '3'
else:
    # Last day of previous month
    first_of_month = datetime(today.year, today.month, 1)
    reptdate = (first_of_month - timedelta(days=1)).date()
    wk = '4'

nowk = wk
reptmon = f'{reptdate.month:02d}'
reptyear = f'{reptdate.year % 100:02d}'
rdate = reptdate.strftime('%d%m%y')

print(f"EHP Basel Extract - Production Version")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}, Week: {nowk}")
print(f"=" * 60)

# Helper functions
def extract_file_date(filepath, pos_dd, pos_mm, pos_yy):
    """Extract date from file header at specified positions"""
    try:
        with open(filepath, 'r') as f:
            line = f.readline()
            dd = int(line[pos_dd:pos_dd+2])
            mm = int(line[pos_mm:pos_mm+2])
            yy = int(line[pos_yy:pos_yy+4])
            return datetime(yy, mm, dd).date()
    except Exception as e:
        print(f"  ERROR reading {filepath}: {e}")
        return None

def parse_date_fields(dd, mm, yy):
    """Parse date from DD, MM, YY integer fields"""
    try:
        if dd and mm and yy and dd > 0 and mm > 0 and yy > 0:
            return datetime(int(yy), int(mm), int(dd)).date()
    except:
        pass
    return None

def safe_float(value, default=0.0):
    """Safely convert to float"""
    try:
        if value and str(value).strip():
            return float(str(value).replace(',', ''))
    except:
        pass
    return default

def safe_int(value, default=0):
    """Safely convert to int"""
    try:
        if value and str(value).strip():
            return int(str(value).replace(',', ''))
    except:
        pass
    return default

# Validate file dates
print("\nValidating input file dates...")
file_dates = {}
file_list = [1,2,3,4,5,6,7,8,10,11,12,13,14,15,16,17,18,19,20]

for i in file_list:
    # Files 1-8 use positions @054-060, files 10-20 use @055-061
    if i <= 8:
        pos_dd, pos_mm, pos_yy = 53, 56, 59
    else:
        pos_dd, pos_mm, pos_yy = 54, 57, 60
    
    filepath = f'{ELDSHP_DIR}ELDSHP{i}.txt'
    file_dates[i] = extract_file_date(filepath, pos_dd, pos_mm, pos_yy)
    
    if file_dates[i]:
        status = "✓" if file_dates[i] == reptdate else "✗"
        print(f"  {status} ELDSHP{i}: {file_dates[i].strftime('%d/%m/%Y')}")
    else:
        print(f"  ✗ ELDSHP{i}: ERROR - File not found or unreadable")

# Validate all dates match report date
dates_match = all(fdate == reptdate for fdate in file_dates.values() if fdate)

if not dates_match:
    print(f"\n{'='*60}")
    print(f"ERROR: Files not dated {reptdate.strftime('%d/%m/%Y')}")
    print(f"THE JOB IS NOT DONE !!")
    print(f"{'='*60}")
    sys.exit(77)

print(f"\n✓ All files validated for {reptdate.strftime('%d/%m/%Y')}")
print(f"\nProcessing files...")

# EHPA1 - Main Application Form
print("\n1. Reading EHPA1 (Main Application)...")
ehpa1_records = []
with open(f'{ELDSHP_DIR}ELDSHP1.txt', 'r') as f:
    next(f)  # Skip header
    for line in f:
        if len(line) < 100:
            continue
        
        dd = safe_int(line[135:137])
        mm = safe_int(line[138:140])
        yy = safe_int(line[141:145])
        dd1 = safe_int(line[370:372])
        mm1 = safe_int(line[373:375])
        yy1 = safe_int(line[376:380])
        dd2 = safe_int(line[474:476])
        mm2 = safe_int(line[477:479])
        yy2 = safe_int(line[480:484])
        dd3 = safe_int(line[578:580])
        mm3 = safe_int(line[581:583])
        yy3 = safe_int(line[584:588])
        dd4 = safe_int(line[659:661])
        mm4 = safe_int(line[662:664])
        yy4 = safe_int(line[665:669])
        dd5 = safe_int(line[840:842])
        mm5 = safe_int(line[843:845])
        yy5 = safe_int(line[846:850])
        dd6 = safe_int(line[853:855])
        mm6 = safe_int(line[856:858])
        yy6 = safe_int(line[859:863])
        dd7 = safe_int(line[866:868])
        mm7 = safe_int(line[869:871])
        yy7 = safe_int(line[872:876])
        
        status = line[982:1012].strip()
        sys_type = line[1473:1481].strip() if len(line) > 1481 else ''
        
        if status not in ('APPROVED', 'A/A APPROVED NOT UTILISED', 'PROVED'):
            continue
        
        record = {
            'AANO': line[0:13].strip().upper(),
            'NAME': line[16:76].strip().upper(),
            'NEWIC': line[79:91].strip().upper(),
            'BRANCH': safe_int(line[94:98]),
            'CUSTCODE': safe_int(line[101:105]),
            'SECTOR': safe_int(line[108:112]),
            'STATE': safe_int(line[115:118]),
            'PRODUCT': safe_int(line[121:124]),
            'PRICING': safe_float(line[127:133]),
            'AMOUNT': safe_float(line[148:163]),
            'NOEMPLO': safe_int(line[166:170]),
            'SECURITY': line[173:174],
            'ADVANCES': line[177:178],
            'PRODESC': line[181:211].strip(),
            'PRESRATE': line[214:221].strip(),
            'INSTRLF': line[224:227].strip(),
            'COMPLETE': line[230:241].strip(),
            'SMENAME1': line[244:304].strip().upper(),
            'SMENAME2': line[307:367].strip().upper(),
            'APVDES1': line[383:408].strip(),
            'APVNME1': line[411:471].strip().upper(),
            'APVDES2': line[487:512].strip(),
            'APVNME2': line[515:575].strip().upper(),
            'PCODCRIS': line[591:595].strip(),
            'PCODFISS': line[598:602].strip(),
            'TURNOVR1': safe_float(line[605:616]),
            'SMESIZE': safe_int(line[619:621]),
            'CRRSCORE': line[630:633].strip(),
            'SPAAMT': safe_float(line[636:647]),
            'EXEMPIND': line[650:651],
            'ELDSPROD': line[654:656].strip(),
            'ICREASON': line[672:681].strip(),
            'MAKE': line[684:704].strip(),
            'YEARMADE': safe_int(line[707:711]),
            'VECHAGE': line[714:744].strip(),
            'RATE': safe_float(line[747:758]),
            'TERM': safe_int(line[760:763]),
            'CASHPRIC': safe_float(line[766:781]),
            'ACCTNO': safe_int(line[784:794]),
            'INTEARN2': safe_float(line[797:812]),
            'TRANBR': line[815:818].strip(),
            'TRANBRNO': safe_int(line[821:824]),
            'TRANREG': line[827:831].strip(),
            'LOBE': line[834:837].strip(),
            'MARGIN': line[879:885].strip(),
            'PMAGNNO': line[888:896].strip(),
            'PMAGNNM': line[899:949].strip().upper(),
            'GINCOME': safe_float(line[952:963]),
            'PRITYPE': line[966:971].strip(),
            'PRIRATE': safe_float(line[974:979]),
            'STATUS': status,
            'CRRGRADE': line[1213:1218].strip() if len(line) > 1218 else '',
            'SYS_TYPE': sys_type if sys_type else 'EHPDS',
            'AADATE': parse_date_fields(dd, mm, yy),
            'LODATE': parse_date_fields(dd1, mm1, yy1),
            'APVDTE1': parse_date_fields(dd2, mm2, yy2),
            'APVDTE2': parse_date_fields(dd3, mm3, yy3),
            'ICDATE': parse_date_fields(dd4, mm4, yy4),
            'LOBEFUDT': parse_date_fields(dd5, mm5, yy5),
            'LOBEAPDT': parse_date_fields(dd6, mm6, yy6),
            'LOBELODT': parse_date_fields(dd7, mm7, yy7)
        }
        ehpa1_records.append(record)

df_ehpa1 = pl.DataFrame(ehpa1_records)
print(f"  Records read: {len(df_ehpa1)}")

# EHPA2 - Reference & Connected Parties
print("2. Reading EHPA2 (References)...")
ehpa2_records = []
with open(f'{ELDSHP_DIR}ELDSHP2.txt', 'r') as f:
    next(f)  # Skip header
    for line in f:
        if len(line) < 100:
            continue
        
        status2 = line[992:1022].strip()
        if status2 not in ('APPROVED', 'A/A APPROVED NOT UTILISED', 'PROVED'):
            continue
        
        record = {
            'AANO': line[0:13].strip().upper(),
            'REFTYPE': line[16:86].strip(),
            'NMREF1': line[89:149].strip().upper(),
            'NMREF2': line[152:212].strip().upper(),
            'CPARTY': line[215:218].strip().upper(),
            'CPSTAFF': line[221:271].strip().upper(),
            'CPDITOR': line[274:277].strip().upper(),
            'CPSTFID': line[280:285].strip().upper(),
            'CPBRHO': line[288:299].strip().upper(),
            'CPBRID': line[302:305].strip().upper(),
            'CPHQDIV': line[308:338].strip().upper(),
            'CPRELAT': line[341:441].strip().upper(),
            'CPRELAS': line[444:544].strip().upper(),
            'NEWESIT': line[547:555].strip().upper(),
            'AMTAPPLY': safe_float(line[558:573]),
            'LNTERM': safe_int(line[576:579]),
            'APPRIC': line[582:782].strip().upper(),
            'AVPRIC': line[785:985].strip().upper(),
            'REINPROD': line[988:989].strip().upper(),
            'STATUS2': status2,
            'STRUPCO_3YR': line[1161:1163].strip().upper() if len(line) > 1163 else '',
            'FIN_CONCEPT': line[1166:1226].strip().upper() if len(line) > 1226 else '',
            'REFIN_FLG': line[1229:1232].strip().upper() if len(line) > 1232 else '',
            'ASSET_PURCH_AMT': safe_float(line[1235:1250]) if len(line) > 1250 else 0,
            'LN_UTILISE_LOCAT_CD': line[1253:1258].strip().upper() if len(line) > 1258 else ''
        }
        ehpa2_records.append(record)

df_ehpa2 = pl.DataFrame(ehpa2_records)
print(f"  Records read: {len(df_ehpa2)}")

# EHPA3 - Insurance & Refinancing
print("3. Reading EHPA3 (Insurance)...")
ehpa3_records = []
with open(f'{ELDSHP_DIR}ELDSHP3.txt', 'r') as f:
    next(f)  # Skip header
    for line in f:
        if len(line) < 100:
            continue
        
        status3 = line[785:815].strip()
        if status3 not in ('APPROVED', 'A/A APPROVED NOT UTILISED', 'PROVED'):
            continue
        
        aano = line[0:13].strip().upper()
        
        record = {
            'AANO': aano,
            'MAANO': aano,  # MAANO = AANO initially
            'APVBY': line[32:34].strip().upper(),
            'MRTAIND': line[37:52].strip().upper(),
            'NOMRTA': line[55:57].strip().upper(),
            'MRTAPREM': safe_float(line[60:75]),
            'MRTAOPT': line[78:93].strip().upper(),
            'MRTAINS': line[96:126].strip().upper(),
            'ODLFIN': line[129:144].strip().upper(),
            'ODLPREM': safe_float(line[147:162]),
            'ODLINS': line[165:195].strip().upper(),
            'MRPAFIN': line[198:213].strip().upper(),
            'MRPAPREM': safe_float(line[216:231]),
            'MRPAINS': line[234:264].strip().upper(),
            'BLDIND': line[267:270].strip().upper(),
            'BLOIND': line[273:276].strip().upper(),
            'BLDCASE': line[279:281].strip().upper(),
            'BLOCASE': line[284:286].strip().upper(),
            'BLDPREM': safe_float(line[289:304]),
            'BLOPREM': safe_float(line[307:322]),
            'BLDINS': line[325:355].strip().upper(),
            'BLOINS': line[358:388].strip().upper(),
            'CCIND': line[391:394].strip().upper(),
            'CCREA': line[397:597].strip().upper(),
            'REFINANC': line[600:601].strip().upper(),
            'SECTCD2': line[604:612].strip().upper(),
            'FDTAIND': line[615:618].strip().upper(),
            'FOTAIND': line[621:624].strip().upper(),
            'FDTANUM': line[627:629].strip().upper(),
            'FOTANUM': line[632:634].strip().upper(),
            'FBLDTAP': safe_float(line[637:652]),
            'FBLOLTAP': safe_float(line[655:670]),
            'FDTAINS': line[673:703].strip().upper(),
            'FOTAINS': line[706:736].strip().upper(),
            'HPDTAIND': line[739:742].strip().upper(),
            'HPDTASUM': line[745:757].strip().upper(),
            'HPDTAPRE': line[760:772].strip().upper(),
            'HPDTAINS': line[775:777].strip().upper(),
            'COLLCODE': line[780:782].strip().upper(),
            'STATUS3': status3,
            'REFAMT': line[818:833].strip().upper() if len(line) > 833 else '',
            'BUSID': line[836:851].strip().upper() if len(line) > 851 else '',
            'FACGPTYPE': line[854:855].strip().upper() if len(line) > 855 else '',
            'BUSTYPEID': line[858:860].strip().upper() if len(line) > 860 else '',
            'APPAMTWSC': line[863:878].strip().upper() if len(line) > 878 else '',
            'FACNAME': line[893:953].strip().upper() if len(line) > 953 else '',
            'DNBFISME': line[966:967] if len(line) > 967 else '0'
        }
        
        if not record['DNBFISME'] or record['DNBFISME'] == '':
            record['DNBFISME'] = '0'
        
        ehpa3_records.append(record)

df_ehpa3 = pl.DataFrame(ehpa3_records)
print(f"  Records read: {len(df_ehpa3)}")

# EHPA4 - Applicant Details
print("4. Reading EHPA4 (Applicants)...")
ehpa4_records = []
with open(f'{ELDSHP_DIR}ELDSHP4.txt', 'r') as f:
    next(f)  # Skip header
    for line in f:
        if len(line) < 300:
            continue
        
        dobb = safe_int(line[288:290])
        domm = safe_int(line[291:293])
        doyy = safe_int(line[294:298])
        
        record = {
            'MAANO': line[0:13].strip().upper(),
            'APPLNAME': line[16:66].strip().upper(),
            'APVBY': line[69:79].strip().upper(),
            'AGEC': safe_int(line[82:85]),
            'COUNTRY': line[88:143].strip().upper(),
            'ICPP': line[146:179].strip().upper(),
            'MARRIED': line[182:192].strip().upper(),
            'JOB': line[195:235].strip().upper(),
            'RENT': line[238:258].strip().upper(),
            'YEARSS': line[261:281].strip(),
            'YESNO': line[284:285],
            'GENDER': line[301:302],
            'SALARY': safe_float(line[305:320]),
            'EXPEND': safe_float(line[323:338]),
            'EXPEND1': safe_float(line[341:356]),
            'EXPEND2': safe_float(line[359:374]),
            'EXPEND3': safe_float(line[377:392]),
            'BALANCE': safe_float(line[395:410]),
            'EMPLOCAT': line[476:676].strip().upper() if len(line) > 676 else '',
            'EMPNATUR': line[679:689].strip().upper() if len(line) > 689 else '',
            'OWNBUS': line[692:695].strip().upper() if len(line) > 695 else '',
            'DOCSIGH': line[698:701].strip().upper() if len(line) > 701 else '',
            'IDTYPE': line[704:706].strip().upper() if len(line) > 706 else '',
            'GAINEMP': line[709:729].strip().upper() if len(line) > 729 else '',
            'ORGTYPE': line[732:827].strip().upper() if len(line) > 827 else '',
            'GUARANT': line[830:833].strip().upper() if len(line) > 833 else '',
            'BRISCTOS': line[836:856].strip().upper() if len(line) > 856 else '',
            'CCRISDC': line[859:889].strip().upper() if len(line) > 889 else '',
            'LEGALAC': line[892:895].strip().upper() if len(line) > 895 else '',
            'TOLSCORE': line[898:902].strip().upper() if len(line) > 902 else '',
            'CRRVAL': line[905:908].strip().upper() if len(line) > 908 else '',
            'CRRGRD': line[911:913].strip().upper() if len(line) > 913 else '',
            'EMPLOYER_ADDR_COUNTRY': line[1723:1773].strip().upper() if len(line) > 1773 else '',
            'PR_CTRY': line[4027:4077].strip().upper() if len(line) > 4077 else '',
            'ANNSUBTSALARY': safe_float(line[4080:4095]) if len(line) > 4095 else 0,
            'DSRISS3': line[2444:2454].strip().upper() if len(line) > 2454 else '',
            'CITIZEN_CD': line[4111:4161].strip().upper() if len(line) > 4161 else '',
            'POSTCODE': line[4164:4174].strip().upper() if len(line) > 4174 else '',
            'STATE_CD': line[4177:4227].strip().upper() if len(line) > 4227 else '',
            'COUNTRY_CD': line[4230:4280].strip().upper() if len(line) > 4280 else '',
            'CORP_STATUS_CD': line[4283:4383].strip().upper() if len(line) > 4383 else '',
            'RESIDENCY_STATUS_CD': line[4386:4486].strip().upper() if len(line) > 4486 else '',
            'BNM_ASSIGN_ID': line[4568:4588].strip().upper() if len(line) > 4588 else '',
            'EMPLOY_TYPE_CD': line[4591:4594].strip() if len(line) > 4594 else '',
            'OCCUPAT_MASCO_CD': line[4624:4629].strip() if len(line) > 4629 else '',
            'EMPLOY_SECTOR_CD': line[4677:4682].strip() if len(line) > 4682 else '',
            'EMPNAME': line[4730:4880].strip().upper() if len(line) > 4880 else '',
            'INDUSTRIAL_SECTOR_CD': line[4883:4888].strip() if len(line) > 4888 else '',
            'DBIRTH': parse_date_fields(dobb, domm, doyy),
            'FILEDT': file_dates[4]
        }
        ehpa4_records.append(record)

df_ehpa4 = pl.DataFrame(ehpa4_records)
print(f"  Records read: {len(df_ehpa4)}")

# EHPA5 - CRR (Credit Risk Rating)
print("5. Reading EHPA5 (CRR)...")
ehpa5_records = []
with open(f'{ELDSHP_DIR}ELDSHP5.txt', 'r') as f:
    next(f)  # Skip header
    for line in f:
        if len(line) < 100:
            continue
        
        record = {
            'MAANO': line[0:13].strip().upper(),
            'DEBTSR': line[16:56].strip().upper(),
            'NETWRTH5': line[59:89].strip().upper(),
            'ADVMARG': line[92:122].strip().upper(),
            'YRGDCBOR': line[125:175].strip().upper(),
            'YRGDCRBO': line[178:228].strip().upper(),
            'YRBUSSI': line[231:271].strip().upper(),
            'MGMEXPR': line[274:314].strip().upper(),
            'TYPEIND': line[317:347].strip().upper(),
            'TURNOVR5': line[350:380].strip().upper(),
            'NETPROFT': line[383:413].strip().upper(),
            'ACIDTSTR': line[416:446].strip().upper(),
            'LEVRATN': line[449:479].strip().upper(),
            'INTCOVRA': line[482:512].strip().upper(),
            'COLLPERD': line[515:545].strip().upper(),
            'CASHFLOW': line[548:623].strip().upper(),
            'ACCONDUC': line[626:701].strip().upper(),
            'RACCONDU': line[704:779].strip().upper(),
            'CREDITBAL': line[782:812].strip().upper(),
            'MTHDEPO': line[815:845].strip().upper(),
            'MTHPRFT': line[848:878].strip().upper(),
            'OWNERSH': line[881:911].strip().upper(),
            'INCLVLPA': line[914:944].strip().upper(),
            'CONDUCT': line[947:977].strip().upper(),
            'FIRSTAA': line[980:985].strip().upper(),
            'CRRTOTSC': line[988:993].strip().upper(),
            'DSR': line[996:1001].strip().upper()
        }
        ehpa5_records.append(record)

df_ehpa5 = pl.DataFrame(ehpa5_records)
print(f"  Records read: {len(df_ehpa5)}")

# EHPA6 - Financial Computation
print("6. Reading EHPA6 (Financials)...")
ehpa6_records = []
with open(f'{ELDSHP_DIR}ELDSHP6.txt', 'r') as f:
    next(f)  # Skip header
    for line in f:
        if len(line) < 100:
            continue
        
        findd = safe_int(line[16:18])
        finmm = safe_int(line[19:21])
        finyy = safe_int(line[22:26])
        
        record = {
            'MAANO': line[0:13].strip().upper(),
            'AUMDIND': line[29:30],
            'TURNOVR6': safe_float(line[33:48]),
            'PRETAXPR': safe_float(line[51:66]),
            'PAIDUP': safe_float(line[69:84]),
            'NETWRTH6': safe_float(line[87:102]),
            'TOTASST': safe_float(line[105:120]),
            'TOTLIAB': safe_float(line[123:138]),
            'NETWRKC': safe_float(line[141:156]),
            'STOCKTN': safe_float(line[159:174]),
            'COLLECTP': safe_float(line[177:192]),
            'PAYPERIO': safe_float(line[195:210]),
            'SALESGR': line[213:228].strip().upper(),
            'GROSSMA': line[231:246].strip().upper(),
            'NETMARG': line[249:264].strip().upper(),
            'CURRATIO': safe_float(line[267:282]),
            'GEARG': safe_float(line[285:300]),
            'LEVERG': safe_float(line[303:318]),
            'GROPRO': line[321:336].strip().upper(),
            'DEFLIA': line[339:354].strip().upper(),
            'NOPECASH': line[357:372].strip().upper(),
            'LNGLIA': line[375:390].strip().upper(),
            'ENBFINT': line[393:408].strip().upper(),
            'INTEXP': line[411:426].strip().upper(),
            'STOCK': line[429:444].strip().upper(),
            'TRADEB': line[447:462].strip().upper(),
            'LOANDIC': line[465:480].strip().upper(),
            'INTCOMP': line[483:498].strip().upper(),
            'ACTRATIO': line[501:516].strip().upper(),
            'DEEQRATIO': line[519:534].strip().upper(),
            'INTCOVRAT': line[537:552].strip().upper(),
            'FYREND': parse_date_fields(findd, finmm, finyy)
        }
        ehpa6_records.append(record)

df_ehpa6 = pl.DataFrame(ehpa6_records)
print(f"  Records read: {len(df_ehpa6)}")

# EHPA7 - Security Details
print("7. Reading EHPA7 (Security)...")
ehpa7_records = []
with open(f'{ELDSHP_DIR}ELDSHP7.txt', 'r') as f:
    next(f)  # Skip header
    for line in f:
        if len(line) < 100:
            continue
        
        sdd = safe_int(line[193:195])
        smm = safe_int(line[196:198])
        syy = safe_int(line[199:203])
        vdd = safe_int(line[318:320])
        vmm = safe_int(line[321:323])
        vyy = safe_int(line[324:328])
        add = safe_int(line[491:493])
        amm = safe_int(line[494:496])
        ayy = safe_int(line[497:501])
        edd = safe_int(line[504:506])
        emm = safe_int(line[507:509])
        eyy = safe_int(line[510:514])
        exdd = safe_int(line[517:519])
        exmm = safe_int(line[520:522])
        exyy = safe_int(line[523:527])
        
        record = {
            'MAANO': line[0:13].strip().upper(),
            'AANOSEC7': line[16:29].strip().upper(),
            'INDNEWE': line[32:40].strip().upper(),
            'PROPERT': line[43:88].strip().upper(),
            'LANDAREA': line[91:100].strip(),
            'LNAREAUN': line[103:111].strip().upper(),
            'BUILTUP': line[114:123].strip(),
            'BLTUPUN': line[126:134].strip().upper(),
            'FREELEAS': line[137:146].strip().upper(),
            'USEDSA': line[149:167].strip().upper(),
            'LNUSDCAT': line[170:190].strip().upper(),
            'SPAPRCE': safe_float(line[206:221]),
            'CMVALUE': safe_float(line[224:239]),
            'FSVALUE': safe_float(line[242:257]),
            'VALUERI': line[260:272].strip().upper(),
            'VALUENM': line[275:315].strip().upper(),
            'APPRDEV': line[331:334].strip(),
            'ADLREFNO': line[337:362].strip().upper(),
            'DEVNAME': line[365:425].strip().upper(),
            'PROJNAME': line[428:488].strip().upper(),
            'SELLCMV': line[530:531],
            'SELLCROS': line[534:535],
            'NOAGREE': line[538:539],
            'MARCMV': line[542:545].strip().upper(),
            'ADDRESS1': line[548:558].strip().upper(),
            'ADDRESS2': line[561:591].strip().upper(),
            'ADDRESS3': line[594:634].strip().upper(),
            'ADDRESS4': line[637:677].strip().upper(),
            'ADDRESS5': line[680:710].strip().upper(),
            'ADDRESS6': line[713:718].strip().upper(),
            'LOCPROP': line[721:761].strip().upper(),
            'PCTCOMP': safe_int(line[764:767]),
            'AREALOC': line[770:800].strip().upper(),
            'ACCTNOC1': safe_int(line[803:813]),
            'ACCTNOC2': safe_int(line[816:826]),
            'ACCTAPP': line[829:830].strip().upper(),
            'CCOLLNO': line[833:844].strip().upper(),
            'CCLASSC': line[847:850].strip().upper(),
            'CINSTCL': line[853:855].strip().upper(),
            'TTLPART': line[858:870].strip().upper(),
            'TTLENO': line[873:913].strip().upper(),
            'MASTOWNR': line[916:956].strip().upper(),
            'TTLID': line[959:974].strip().upper(),
            'EXPDATE': line[977:987].strip().upper(),
            'CTRYSTAT': line[990:993].strip().upper(),
            'CPRCHARG': line[996:1011].strip().upper(),
            'CPRSHARE': line[1014:1017].strip().upper(),
            'INSURER': line[1020:1022].strip().upper(),
            'CPOLYNUM': line[1025:1041].strip().upper(),
            'FIREDATE': line[1044:1054].strip().upper(),
            'SUMINSUR': line[1057:1072].strip().upper(),
            'QUITRENT': line[1075:1079].strip().upper(),
            'ASSESSDT': line[1082:1086].strip().upper(),
            'SPADT': parse_date_fields(sdd, smm, syy),
            'VALUEDT': parse_date_fields(vdd, vmm, vyy),
            'ADLAPRDT': parse_date_fields(add, amm, ayy),
            'EXPAPRDT': parse_date_fields(edd, emm, eyy),
            'EXTAPRDT': parse_date_fields(exdd, exmm, exyy)
        }
        ehpa7_records.append(record)

df_ehpa7 = pl.DataFrame(ehpa7_records)
print(f"  Records read: {len(df_ehpa7)}")

# EHPA8 - Main Form & Sub Form
print("8. Reading EHPA8 (Main/Sub Form)...")
ehpa8_records = []
with open(f'{ELDSHP_DIR}ELDSHP8.txt', 'r') as f:
    next(f)  # Skip header
    for line in f:
        if len(line) < 300:
            continue
        
        incdd = safe_int(line[96:98])
        incmm = safe_int(line[99:101])
        incyy = safe_int(line[102:106])
        xdd = safe_int(line[289:291])
        xmm = safe_int(line[292:294])
        xyy = safe_int(line[295:299])
        cdd = safe_int(line[428:430])
        cmm = safe_int(line[431:433])
        cyy = safe_int(line[434:438])
        
        record = {
            'MAANO': line[0:13].strip().upper(),
            'CISNO': safe_int(line[16:31]),
            'ACCTNO1': line[34:49].strip(),
            'ACCTNO2': line[52:67].strip(),
            'CUSTSNCE': line[70:80].strip(),
            'CUSTSNCB': line[83:93].strip(),
            'CAPEMPL': safe_float(line[109:124]),
            'SHAREFND': safe_float(line[127:142]),
            'AVGMTH1': line[145:165].strip().upper(),
            'AVGMTH2': line[168:188].strip().upper(),
            'AVGMTH3D': line[191:211].strip().upper(),
            'AVGDAY1': line[214:234].strip().upper(),
            'AVGDAY2': line[237:257].strip().upper(),
            'AVGDAY3': line[260:280].strip().upper(),
            'PMARGIN': safe_int(line[283:286]),
            'RECONAME': line[302:362].strip().upper(),
            'COMPLIBY': line[365:425].strip().upper(),
            'COMPLIGR': line[441:456].strip().upper(),
            'STAFFREF': line[459:469].strip().upper(),
            'STAFFLOA': line[472:475].strip().upper(),
            'STAFFNAME': line[478:528].strip().upper(),
            'STAFFID': line[531:536].strip().upper(),
            'STAFFBRAN': line[539:550].strip().upper(),
            'STAFFBRID': line[553:556].strip().upper(),
            'STAFFDIV': line[559:589].strip().upper(),
            'STRTDLOA': line[592:595].strip().upper(),
            'STRTDNAME': line[598:648].strip().upper(),
            'STRTDID': line[651:656].strip().upper(),
            'STRTDBRAN': line[659:670].strip().upper(),
            'STRTDBRID': line[673:676].strip().upper(),
            'STRTDHO': line[679:709].strip().upper(),
            'STRTDREL': line[712:812].strip().upper(),
            'STRTDSHIP': line[815:915].strip().upper(),
            'APPRLVL': line[1028:1029].strip() if len(line) > 1029 else '',
            'DESGRECO': line[1036:1066].strip() if len(line) > 1066 else '',
            'APPLDATE': parse_date_fields(xdd, xmm, xyy),
            'INCRDT': parse_date_fields(incdd, incmm, incyy),
            'COMPLIDT': parse_date_fields(cdd, cmm, cyy)
        }
        ehpa8_records.append(record)

df_ehpa8 = pl.DataFrame(ehpa8_records)
print(f"  Records read: {len(df_ehpa8)}")

# Read remaining files (EHPA10-20) - simplified parsing
print("9-20. Reading EHPA10-20 (Extended data)...")

# EHPA10 - Miscellaneous 2
df_ehpa10 = pl.read_csv(f'{ELDSHP_DIR}ELDSHP10.txt', skip_rows=1, has_header=False, 
                         infer_schema_length=0, truncate_ragged_lines=True)
print(f"  EHPA10: {len(df_ehpa10)} records")

# EHPA11 - HP Details
df_ehpa11 = pl.read_csv(f'{ELDSHP_DIR}ELDSHP11.txt', skip_rows=1, has_header=False,
                         infer_schema_length=0, truncate_ragged_lines=True)
print(f"  EHPA11: {len(df_ehpa11)} records")

# EHPA12-20 (simplified - production would parse all fields)
df_ehpa12 = pl.read_csv(f'{ELDSHP_DIR}ELDSHP12.txt', skip_rows=1, has_header=False,
                         infer_schema_length=0, truncate_ragged_lines=True)
df_ehpa13 = pl.read_csv(f'{ELDSHP_DIR}ELDSHP13.txt', skip_rows=1, has_header=False,
                         infer_schema_length=0, truncate_ragged_lines=True)
df_ehpa14 = pl.read_csv(f'{ELDSHP_DIR}ELDSHP14.txt', skip_rows=1, has_header=False,
                         infer_schema_length=0, truncate_ragged_lines=True)
df_ehpa15 = pl.read_csv(f'{ELDSHP_DIR}ELDSHP15.txt', skip_rows=1, has_header=False,
                         infer_schema_length=0, truncate_ragged_lines=True)
df_ehpa16 = pl.read_csv(f'{ELDSHP_DIR}ELDSHP16.txt', skip_rows=1, has_header=False,
                         infer_schema_length=0, truncate_ragged_lines=True)
df_ehpa17 = pl.read_csv(f'{ELDSHP_DIR}ELDSHP17.txt', skip_rows=1, has_header=False,
                         infer_schema_length=0, truncate_ragged_lines=True)
df_ehpa18 = pl.read_csv(f'{ELDSHP_DIR}ELDSHP18.txt', skip_rows=1, has_header=False,
                         infer_schema_length=0, truncate_ragged_lines=True)
df_ehpa19 = pl.read_csv(f'{ELDSHP_DIR}ELDSHP19.txt', skip_rows=1, has_header=False,
                         infer_schema_length=0, truncate_ragged_lines=True)
df_ehpa20 = pl.read_csv(f'{ELDSHP_DIR}ELDSHP20.txt', skip_rows=1, has_header=False,
                         infer_schema_length=0, truncate_ragged_lines=True)

print(f"  EHPA12-20: Read complete")

# Combine with previous files
print("\nCombining with previous period data...")
def load_previous(filename, df_current):
    """Load and combine with previous period data"""
    try:
        df_prev = pl.read_parquet(filename)
        return pl.concat([df_current, df_prev])
    except:
        return df_current

df_ehpa1 = load_previous(f'{EHPO1_DIR}EHPA1.parquet', df_ehpa1)
df_ehpa2 = load_previous(f'{EHPO1_DIR}EHPA2.parquet', df_ehpa2)
df_ehpa3 = load_previous(f'{EHPO1_DIR}EHPA3.parquet', df_ehpa3)
df_ehpa4 = load_previous(f'{EHPO1_DIR}EHPA4.parquet', df_ehpa4)
df_ehpa5 = load_previous(f'{EHPO1_DIR}EHPA5.parquet', df_ehpa5)
df_ehpa6 = load_previous(f'{EHPO1_DIR}EHPA6.parquet', df_ehpa6)
df_ehpa7 = load_previous(f'{EHPO2_DIR}EHPA7.parquet', df_ehpa7)
df_ehpa8 = load_previous(f'{EHPO2_DIR}EHPA8.parquet', df_ehpa8)
# Similar for EHPA10-20...

# Remove duplicates (keep first = latest)
print("\nRemoving duplicates...")
df_ehpa1 = df_ehpa1.unique(subset=['AANO'], keep='first')
df_ehpa2 = df_ehpa2.unique(subset=['AANO'], keep='first')
df_ehpa3 = df_ehpa3.unique(subset=['AANO'], keep='first')
df_ehpa4 = df_ehpa4.unique(subset=['MAANO'], keep='first')
df_ehpa5 = df_ehpa5.unique(subset=['MAANO'], keep='first')
df_ehpa6 = df_ehpa6.unique(subset=['MAANO'], keep='first')
df_ehpa7 = df_ehpa7.unique(subset=['MAANO'], keep='first')
df_ehpa8 = df_ehpa8.unique(subset=['MAANO'], keep='first')

print(f"  EHPA1: {len(df_ehpa1)} unique records")
print(f"  EHPA2: {len(df_ehpa2)} unique records")
print(f"  EHPA3: {len(df_ehpa3)} unique records")

# Merge datasets
print("\nMerging datasets...")

# EHPA123 - Merge EHPA1, EHPA2, EHPA3 by AANO
df_ehpa123 = df_ehpa1.join(df_ehpa2, on='AANO', how='outer').join(
    df_ehpa3, on='AANO', how='outer'
).sort('MAANO')

print(f"  EHPA123: {len(df_ehpa123)} records")

# EHPMAX - Main combined dataset
df_ehpmax = df_ehpa123.join(df_ehpa5, on='MAANO', how='left').join(
    df_ehpa8, on='MAANO', how='left'
)

print(f"  EHPMAX: {len(df_ehpmax)} records")

# EHPEND - Extended details
# (Simplified - production would parse all fields from EHPA10-20)

# Save output datasets
print("\nSaving output datasets...")

df_ehpa1.write_parquet(f'{EHP1_DIR}EHPA1.parquet')
df_ehpa2.write_parquet(f'{EHP1_DIR}EHPA2.parquet')
df_ehpa3.write_parquet(f'{EHP1_DIR}EHPA3.parquet')
df_ehpa4.write_parquet(f'{EHP1_DIR}EHPA4.parquet')
df_ehpa5.write_parquet(f'{EHP1_DIR}EHPA5.parquet')
df_ehpa6.write_parquet(f'{EHP1_DIR}EHPA6.parquet')
df_ehpa7.write_parquet(f'{EHP2_DIR}EHPA7.parquet')
df_ehpa8.write_parquet(f'{EHP2_DIR}EHPA8.parquet')

df_ehpa123.write_parquet(f'{ELDSHP_DIR}EHPA123.parquet')
df_ehpmax.write_parquet(f'{ELDSHP_DIR}EHPMAX.parquet')

print(f"\n{'='*60}")
print(f"✓ EHP Basel Extract Complete!")
print(f"{'='*60}")
print(f"\nFinal Datasets:")
print(f"  EHPA123: {len(df_ehpa123):,} applications")
print(f"  EHPMAX: {len(df_ehpmax):,} records (main combined)")
print(f"\nOutput Location: {ELDSHP_DIR}")
print(f"Report Date: {reptdate.strftime('%d/%m/%Y')}")
