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


def convert_to_sas_date_series(date_series):
    """Convert a pandas series of YYYYMMDD dates to SAS dates"""
    # Convert to string, handling NaN and invalid values
    date_str_series = date_series.astype(str).str.strip()
    
    # Create mask for valid 8-digit dates
    valid_mask = date_str_series.str.match(r'^\d{8}$') & (date_str_series != '00000000')
    
    # Initialize result with NaN
    result = pd.Series([np.nan] * len(date_series), dtype='float64')
    
    if valid_mask.any():
        # Convert valid dates
        valid_dates = pd.to_datetime(date_str_series[valid_mask], format='%Y%m%d', errors='coerce')
        sas_epoch = pd.Timestamp('1960-01-01')
        result[valid_mask] = (valid_dates - sas_epoch).dt.days
    
    return result

# Use vectorized conversion instead of apply
pq_bill["TRANDATE"] = convert_to_sas_date_series(pq_bill["TRANDATE"])
pq_bill["EXPRDATE"] = convert_to_sas_date_series(pq_bill["EXPRDATE"])

# Fill string columns
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
