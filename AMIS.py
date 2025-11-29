 Please help to create the last month base parquet as below. 

libname crm '/dwh/crm';

 PROC APPEND DATA=CHANNEL BASE=CRMWH.CHANNEL_SUM FORCE;RUN; 
 PROC PRINT DATA=CRMWH.CHANNEL_SUM;RUN;                     

PROC APPEND DATA=UAT BASE=CHN.CHANNEL_UPDATE FORCE; RUN; 

import polars as pl
import pyreadstat
import os
import duckdb
from datetime import datetime, timedelta
from pathlib import Path
from sas7bdat import SAS7BDAT

BILL_SRC = '/restore/pythontbc/loan_bill_d27.sas7bdat'
BILL_DF, meta_dp = pyreadstat.read_sas7bdat(BILL_SRC)


OUTPUT_DATA_PATH = "/host/mis/parquet/crm/year=2025/month=10"
os.makedirs(OUTPUT_DATA_PATH, exist_ok=True)

BILL_DF.to_parquet(f"{OUTPUT_DATA_PATH}/LOAN_BILL.parquet", engine='pyarrow', index=False)

