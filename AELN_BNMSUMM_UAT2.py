"""
EIBWTRBL - Simplified Billings Processing
"""

import polars as pl
import pyreadstat
from datetime import datetime, timedelta
import os

print("Billings Transaction Processing...")

BASE_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/Job'
OUTPUT_DIR = f'{BASE_PATH}/BTRADE/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Get REPTDATE from SAS dataset
df_sas, meta = pyreadstat.read_sas7bdat('/stgsrcsys/host/uat/reptdate.sas7bdat')
reptdate = datetime(1960, 1, 1) + timedelta(days=int(df_sas['REPTDATE'][0]))

# Determine week
day = reptdate.day
nowk = '4'  # default
if day == 8: nowk = '1'
elif day == 15: nowk = '2'
elif day == 22: nowk = '3'

reptyear = reptdate.strftime('%y')
reptmon = reptdate.strftime('%m')

print(f"📅 Processing date: {reptdate.strftime('%Y-%m-%d')} (Week {nowk})")

# Read and parse text file
print("📊 Processing transaction data...")

data = []
with open('/stgsrcsys/host/uat/EXTCOMM.TXT', 'r') as f:
    lines = f.readlines()
    
for i, line in enumerate(lines):
    if i == 0:  # Skip header (FIRSTOBS=2)
        continue
    
    if len(line) < 246:
        continue
    
    # Parse fixed-width positions (0-indexed in Python)
    trandate_str = line[72:80]
    exprdat_raw = line[80:86]
    
    # Convert dates
    try:
        trandate = datetime.strptime(trandate_str, '%Y%m%d').date()
    except:
        trandate = None
    
    try:
        if exprdat_raw[:2] < '50':
            exprdat = datetime.strptime('20' + exprdat_raw, '%Y%m%d').date()
        else:
            exprdat = datetime.strptime('19' + exprdat_raw, '%Y%m%d').date()
    except:
        exprdat = None
    
    data.append({
        'RECTYPE': line[0:2],
        'TRANSREF': line[2:12],
        'COSTCTR': int(line[20:24]) if line[20:24].strip() else 0,
        'ACCTNO': line[26:41],
        'SUBACCT': line[41:54],
        'GLMNEMONIC': line[63:68],
        'LIABCODE': line[68:71],
        'TRANDATE': trandate,
        'EXPRDATE': exprdat,
        'TRANAMT': float(line[86:104]) if line[86:104].strip() else 0.0,
        'EXCHANGE': float(line[104:120]) if line[104:120].strip() else 0.0,
        'CURRENCY': line[120:124],
        'BTREL': line[124:137],
        'RELFROM': line[137:150],
        'TRANSREFPG': line[168:178],
        'TRANAMT_CCY': float(line[179:197]) if line[179:197].strip() else 0.0,
        'TRANS_NUM': line[197:207],
        'TRANS_IND': line[207:210],
        'MNEMONIC_CD': line[210:215],
        'ACCT_INFO': line[215:235],
        'CR_DR_IND': line[235:236],
        'VOUCHER_NUM': line[236:246]
    })

# Create DataFrame and save
df = pl.DataFrame(data)
output_file = f'{OUTPUT_DIR}/BILLSTRAN{reptmon}{nowk}{reptyear}.parquet'
df.write_parquet(output_file)

print(f"📄 Records loaded: {len(df)}")
print(f"💾 Output saved: {output_file}")
print("🎉 Processing completed successfully!")
