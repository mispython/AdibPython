import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import saspy
import sys
from datetime import datetime, timedelta
import numpy as np

batch_dt = datetime.today() - timedelta(days=20)
batch_dt_str = batch_dt.strftime("%Y%m%d")
output_file_name = 'billstran'
month_str = f"{batch_dt.month:02d}"
year_str = f"{batch_dt.year % 100:02d}"
day_str = f"{batch_dt.day:02d}"
BATCH_MODE = 'M'

print("BATCH MODE = ", BATCH_MODE)

if BATCH_MODE == 'M':
    print("Monthly")
    sas_path = "/dwh/btrade"
    output_file = f"billstran{month_str}4{year_str}"
    bill_table = pq.read_table('/sas/python/virt_edw/Data_Warehouse/TF/input/staging/STG_TF_BILLSTRAN_M.parquet')
    pq_bill = bill_table.to_pandas()
elif BATCH_MODE == 'D':
    print("Daily")
    sas_path = "/dwh/btrade_d"
    output_file = f"billstran_{day_str}"
    bill_table = pq.read_table('/sas/python/virt_edw/Data_Warehouse/TF/input/staging/STG_TF_BILLSTRAN_D.parquet')
    pq_bill = bill_table.to_pandas()

def convert_yyyymmdd_to_sas_date(date_series):
    """Convert YYYYMMDD format to SAS date"""
    result = pd.Series([np.nan] * len(date_series), dtype='float64')
    
    for idx, val in date_series.items():
        if pd.isna(val) or val == 0:
            result[idx] = np.nan
        else:
            try:
                date_str = str(int(val))
                if len(date_str) == 8:
                    date_obj = pd.to_datetime(date_str, format='%Y%m%d')
                    sas_epoch = pd.Timestamp('1960-01-01')
                    result[idx] = (date_obj - sas_epoch).days
                else:
                    result[idx] = np.nan
            except:
                result[idx] = np.nan
    
    return result

def convert_yymmdd_to_sas_date(date_series):
    """Convert YYMMDD format to SAS date (assumes 20xx for YY)"""
    result = pd.Series([np.nan] * len(date_series), dtype='float64')
    
    for idx, val in date_series.items():
        if pd.isna(val) or val == 0:
            result[idx] = np.nan
        else:
            try:
                date_str = str(int(val)).zfill(6)
                if len(date_str) == 6:
                    # Add century: assume 20xx
                    full_date_str = '20' + date_str
                    date_obj = pd.to_datetime(full_date_str, format='%Y%m%d')
                    sas_epoch = pd.Timestamp('1960-01-01')
                    result[idx] = (date_obj - sas_epoch).days
                else:
                    result[idx] = np.nan
            except:
                result[idx] = np.nan
    
    return result

pq_bill["TRANDATE"] = convert_yyyymmdd_to_sas_date(pq_bill["TRANDATE"])
pq_bill["EXPRDATE"] = convert_yymmdd_to_sas_date(pq_bill["EXPRDATE"])

for col in pq_bill.select_dtypes(include=['object']).columns:
    pq_bill[col] = pq_bill[col].fillna('')

sas = saspy.SASsession()
billstran_ctl = "billstran_ctl"

def assign_libname(lib_name, sas_path):
    log = sas.submit(f"""libname {lib_name} '{sas_path}';""")
    return log

def set_data(df, lib_name, ctrl_name, cur_data, prev_data):
    sas.df2sd(df, table=cur_data, libref='work')
    log = sas.submit(f"""
            proc sql noprint;
               create table colmeta as 
               select name, type, length
               from dictionary.columns
               where libname = upcase("{ctrl_name}")  
                     and memname = upcase("{prev_data}");
            quit;
               """)
    
    print(log["LOG"])
    df_meta = sas.sasdata("colmeta", libref="work").to_df()
    cols = df_meta["name"].dropna().tolist()
    col_list = ", ".join(cols)
    casted_cols = []
    
    for _, row in df_meta.iterrows():
        col = row["name"]
        length = row['length']
        if row['type'].strip().lower() == 'char' and pd.notnull(length) and length > 0:
            casted_cols.append(f"input(trim({col}), ${int(length)}.) as {col}")
        else:
            casted_cols.append(col)
    
    casted_cols = ",\n ".join(casted_cols)
    
    log = sas.submit(f"""
                proc sql noprint;
                     create table {lib_name}.{cur_data} as
                     select {col_list} from {ctrl_name}.{prev_data}
                     union all corr
                     select {casted_cols} from work.{cur_data};
                quit;
                """)
    print(f"Final table created: {log['LOG']}") 
    return log

assign_libname("bt", sas_path)
assign_libname("ctl", "/stgsrcsys/host/uat")

log1 = set_data(pq_bill, "bt", "ctl", output_file, billstran_ctl)
