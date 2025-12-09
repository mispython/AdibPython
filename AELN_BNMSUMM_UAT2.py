"""
EIBWTRBL - Billings Transaction Processing
"""

import duckdb
import polars as pl
import pyreadstat
from datetime import datetime
import os

print("Billings Transaction Processing...")

# Configuration
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/Job'
OUTPUT_DIR = f'{BASE_PATH}/BTRADE/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ========================================
# DATE PROCESSING (from SAS dataset)
# ========================================
print("📅 Processing reporting dates...")

# Read REPTDATE from SAS dataset (like DATA REPTDATE)
df_sas, meta = pyreadstat.read_sas7bdat('/stgsrcsys/host/uat/reptdate.sas7bdat')
reptdate_numeric = df_sas['REPTDATE'][0]

# Convert SAS date (days since 1960-01-01) to Python datetime
reptdate = datetime(1960, 1, 1) + datetime.timedelta(days=int(reptdate_numeric))

# Determine week (equivalent to SAS SELECT/WHEN)
day_of_month = reptdate.day
if day_of_month == 8:
    nowk = '1'
elif day_of_month == 15:
    nowk = '2'
elif day_of_month == 22:
    nowk = '3'
else:
    nowk = '4'

reptyear = reptdate.strftime('%y')
reptmon = reptdate.strftime('%m')

print(f"📅 Processing date: {reptdate.strftime('%Y-%m-%d')} (Week {nowk})")

# ========================================
# PROCESS TRANSACTION DATA (like INFILE EXTCOMM)
# ========================================
print("📊 Processing transaction data...")

# Read fixed-width text file (like INFILE EXTCOMM)
extcomm_path = '/stgsrcsys/host/uat/EXTCOMM.TXT'

# Parse fixed-width format using DuckDB (matching SAS INPUT statements)
query = f"""
SELECT 
    SUBSTRING(line, 1, 2) as RECTYPE,
    SUBSTRING(line, 3, 10) as TRANSREF,
    CAST(SUBSTRING(line, 21, 4) as INTEGER) as COSTCTR,
    SUBSTRING(line, 27, 15) as ACCTNO,
    SUBSTRING(line, 42, 13) as SUBACCT,
    SUBSTRING(line, 64, 5) as GLMNEMONIC,
    SUBSTRING(line, 69, 3) as LIABCODE,
    DATE_PARSE(
        CONCAT(
            CASE WHEN SUBSTRING(line, 73, 2)::INTEGER < 50 THEN '20' ELSE '19' END,
            SUBSTRING(line, 73, 2), '-', 
            SUBSTRING(line, 75, 2), '-', 
            SUBSTRING(line, 77, 2)
        ), '%Y-%m-%d'
    ) as TRANDATE,
    DATE_PARSE(
        CONCAT(
            CASE WHEN SUBSTRING(line, 81, 2)::INTEGER < 50 THEN '20' ELSE '19' END,
            SUBSTRING(line, 81, 2), '-', 
            SUBSTRING(line, 83, 2), '-', 
            SUBSTRING(line, 85, 2)
        ), '%Y-%m-%d'
    ) as EXPRDATE,
    CAST(SUBSTRING(line, 87, 18) as DOUBLE) as TRANAMT,
    CAST(SUBSTRING(line, 105, 16) as DOUBLE) as EXCHANGE,
    SUBSTRING(line, 121, 4) as CURRENCY,
    SUBSTRING(line, 125, 13) as BTREL,
    SUBSTRING(line, 138, 13) as RELFROM,
    SUBSTRING(line, 169, 10) as TRANSREFPG,
    CAST(SUBSTRING(line, 180, 18) as DOUBLE) as TRANAMT_CCY,
    SUBSTRING(line, 198, 10) as TRANS_NUM,
    SUBSTRING(line, 208, 3) as TRANS_IND,
    SUBSTRING(line, 211, 5) as MNEMONIC_CD,
    SUBSTRING(line, 216, 20) as ACCT_INFO,
    SUBSTRING(line, 236, 1) as CR_DR_IND,
    SUBSTRING(line, 237, 10) as VOUCHER_NUM
FROM read_text('{extcomm_path}', delim='\x1e')
WHERE line_number > 1  -- Skip header (FIRSTOBS=2)
"""

# Execute query and convert to Polars DataFrame
conn = duckdb.connect()
btrbl_df = conn.execute(query).pl()
conn.close()

print(f"📄 Records loaded: {len(btrbl_df)}")

# ========================================
# SAVE OUTPUT FILE (like SAS DATA step)
# ========================================
output_file = f"{OUTPUT_DIR}/BILLSTRAN{reptmon}{nowk}{reptyear}.parquet"
btrbl_df.write_parquet(output_file)

print(f"💾 Output saved: {output_file}")
print(f"🎉 Processing completed successfully!")


Billings Transaction Processing...
📅 Processing reporting dates...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBWTRBL_BILLING_TRANSACTION_PROCESSING.py", line 28, in <module>
    reptdate = datetime(1960, 1, 1) + datetime.timedelta(days=int(reptdate_numeric))
AttributeError: type object 'datetime.datetime' has no attribute 'timedelta'
You have mail in /var/spool/mail/sas_edw_dev
