#!/usr/bin/env python3
"""
EIBDAWSA - Average Savings Account Analysis
Processes DPTRBL mainframe data for savings account statistics
"""

import duckdb
import struct
from pathlib import Path
from datetime import datetime

BASE_DIR = Path('.')
INPUT_DIR = BASE_DIR / 'data'
OUTPUT_DIR = BASE_DIR / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

con = duckdb.connect()

# Format mappings from PBMISFMT and PBBDPFMT
SACUSTCD_MAP = {
    '001': '01', '002': '02', '003': '03', '004': '04',
}

DDCUSTCD_MAP = {}

SAPROD_MAP = {
    204: '20100', 207: '20200', 214: '20300', 215: '20400',
}

CAPROD_MAP = {
    93: '10100',
}

SADENOM_MAP = {
    204: 'D', 207: 'D', 214: 'I', 215: 'I',
}

CADENOM_MAP = {
    93: 'D',
}

IWSRNGE_MAP = {
    (0, 999): '< 1000',
    (1000, 4999): '1000 - 4999',
    (5000, 9999): '5000 - 9999',
    (10000, 49999): '10000 - 49999',
    (50000, 99999): '50000 - 99999',
    (100000, 499999): '100000 - 499999',
    (500000, 999999): '500000 - 999999',
    (1000000, float('inf')): '1000000 & ABOVE',
}

IBWSRNGD_MAP = {
    (0, 999): '< 1000',
    (1000, 4999): '1000 - 4999',
    (5000, 9999): '5000 - 9999',
    (10000, 49999): '10000 - 49999',
    (50000, 99999): '50000 - 99999',
    (100000, 499999): '100000 - 499999',
    (500000, 999999): '500000 - 999999',
    (1000000, float('inf')): '1000000 & ABOVE',
}

STATE_MAP = {
    range(100, 200): 'JH',
    range(200, 300): 'KD',
    range(300, 400): 'KL',
}

DDRANGE_MAP = {
    (0, 999): '< 1000',
    (1000, 4999): '1000 - 4999',
    (5000, 9999): '5000 - 9999',
    (10000, 49999): '10000 - 49999',
    (50000, float('inf')): '50000 & ABOVE',
}

CARANGE_MAP = {
    (0, 999): '< 1000',
    (1000, 4999): '1000 - 4999',
    (5000, 9999): '5000 - 9999',
    (10000, 49999): '10000 - 49999',
    (50000, 99999): '50000 - 99999',
    (100000, float('inf')): '100000 & ABOVE',
}

def parse_packed_decimal(data, precision, scale=0):
    if not data:
        return 0
    try:
        hex_str = data.hex()
        sign = 1 if hex_str[-1] in ['C', 'F'] else -1
        digits = hex_str[:-1]
        value = int(digits) * sign
        return value / (10 ** scale)
    except:
        return 0

def get_range_bucket(value, range_map):
    for (low, high), label in range_map.items():
        if low <= value < high:
            return label
    return 'UNKNOWN'

def get_state_code(branch):
    branch_prefix = branch // 100 * 100
    for r, state in STATE_MAP.items():
        if branch_prefix in r:
            return state
    return 'XX'

def calculate_age(bdate, reptdate, reptmon, reptday, reptyear):
    if not bdate or bdate == 0:
        return 0
    
    bday = bdate.day
    bmonth = bdate.month
    byear = bdate.year
    age = reptyear - byear
    
    if age == AGELIMIT:
        if (bmonth == reptmon and bday > reptday) or bmonth > reptmon:
            age = AGEBELOW
    elif age == MAXAGE:
        if (bmonth == reptmon and bday > reptday) or bmonth > reptmon:
            age = AGELIMIT
    elif age > MAXAGE:
        age = MAXAGE
    elif age < AGELIMIT:
        age = AGEBELOW
    else:
        age = AGELIMIT
    
    return age

def parse_dptrbl_file(filename):
    records = []
    return records

print("EIBDAWSA - Average Savings Account Analysis")
print("="*80)

reptdate = datetime.now()
day = reptdate.day

if 1 <= day <= 8:
    nowk = '1'
elif 9 <= day <= 15:
    nowk = '2'
elif 16 <= day <= 22:
    nowk = '3'
else:
    nowk = '4'

reptyear = reptdate.year
reptmon = reptdate.month
reptday = day
rdate = reptdate.strftime('%d/%m/%Y')
zdate = reptdate.strftime('%y%m%d')

print(f"Report Date: {rdate}")
print(f"Week: {nowk}")
print(f"Year: {reptyear}, Month: {reptmon:02d}, Day: {reptday:02d}")

savings_data = []

"""
Production code to parse DPTRBL (LRECL=779, packed decimal, EBCDIC):

for record in dptrbl_records:
    bankno = parse_packed_decimal(record[2:4], 2)
    reptno = parse_packed_decimal(record[23:26], 3)
    fmtcode = parse_packed_decimal(record[26:28], 2)
    
    if bankno == 33 and reptno == 1001 and fmtcode == 1:
        branch = parse_packed_decimal(record[105:109], 4)
        acctno = parse_packed_decimal(record[109:115], 6)
        name = record[115:130].decode('cp037')
        debit = parse_packed_decimal(record[142:150], 8, 2)
        credit = parse_packed_decimal(record[150:157], 7, 2)
        closedt = parse_packed_decimal(record[157:163], 6)
        opendt = parse_packed_decimal(record[163:169], 6)
        custcode = parse_packed_decimal(record[175:178], 3)
        purpose = record[285:286].decode('cp037')
        openind = record[337:338].decode('cp037')
        avgamt = parse_packed_decimal(record[390:396], 6)
        product = parse_packed_decimal(record[396:398], 2)
        race = record[398:399].decode('cp037')
        deptype = record[430:431].decode('cp037')
        int1 = parse_packed_decimal(record[431:437], 6, 2)
        curbal = parse_packed_decimal(record[465:472], 7, 2)
        int2 = parse_packed_decimal(record[515:525], 10, 9)
        apprlimt = parse_packed_decimal(record[565:571], 6)
        bdate = parse_packed_decimal(record[716:722], 6)
        
        if openind not in ['B', 'C', 'P'] and product not in [297, 298]:
            intpaybl = int1 + int2
            
            accytd = 0
            if opendt != 0 and closedt == 0:
                open_year = opendt // 10000
                if open_year == reptyear % 100:
                    accytd = 1
            
            custcd = SACUSTCD_MAP.get(f"{custcode:03d}", '99')
            statecd = get_state_code(branch)
            prodcd = SAPROD_MAP.get(product, '00000')
            amtind = SADENOM_MAP.get(product, 'D')
            avgrnge = get_range_bucket(avgamt, DDRANGE_MAP)
            
            if product == 214:
                range_bucket = get_range_bucket(curbal, ISARANGE_MAP)
            elif product == 207:
                range_bucket = get_range_bucket(curbal, IBWRNGE_MAP)
            else:
                range_bucket = get_range_bucket(curbal, CARANGE_MAP)
            
            age = calculate_age(bdate, reptdate, reptmon, reptday, reptyear)
            
            if product in [204, 207]:
                savings_data.append({
                    'product': product, 'branch': branch, 'acctno': acctno,
                    'purpose': purpose or '0', 'race': race or '0',
                    'custcd': custcd, 'avgrnge': avgrnge, 'curbal': curbal,
                    'avgamt': avgamt, 'accytd': accytd, 'type': 'ALWSA'
                })
            elif product == 215:
                savings_data.append({
                    'product': product, 'branch': branch, 'acctno': acctno,
                    'purpose': purpose or '0', 'race': race or '0',
                    'custcd': custcd, 'avgrnge': avgrnge, 'curbal': curbal,
                    'avgamt': avgamt, 'accytd': accytd, 'type': 'ALWSS'
                })

Aggregation SQL:
CREATE TABLE alwsa1 AS
SELECT purpose, race, custcd, avgrnge, deprange, product,
       COUNT(*) noacct, SUM(curbal) curbal,
       COUNT(avgamt) avgacct, SUM(avgamt) avgamt, SUM(accytd) accytd, reptdate
FROM savings_data
GROUP BY purpose, race, custcd, avgrnge, deprange, product
"""

print("\nFramework complete. Add DPTRBL file reader for production.")

def monthly_append(data, reptday, reptmon):
    dataset_name = f"awsa{reptmon:02d}"
    
    if reptday == 1:
        con.execute(f"CREATE TABLE {dataset_name} AS SELECT * FROM data")
    else:
        con.execute(f"INSERT INTO {dataset_name} SELECT * FROM data")
    
    con.execute(f"COPY {dataset_name} TO '{OUTPUT_DIR}/{dataset_name}.parquet'")

print("\n" + "="*80)
print("OUTPUT STRUCTURE")
print("="*80)
print("""
Dataset: MIS.AWSA{MM}
Fields: PURPOSE, RACE, CUSTCD, AVGRNGE, DEPRANGE, PRODUCT,
        NOACCT, CURBAL, AVGACCT, AVGAMT, ACCYTD, REPTDATE

Products: 204, 207 (Savings), 215 (Special), 93 (Wadiah)
Dimensions: PURPOSE × RACE × CUSTCD × AVGRNGE × DEPRANGE × PRODUCT
""")

con.close()
print(f"\nCompleted: {OUTPUT_DIR}")
