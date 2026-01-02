import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import saspy
import sys
from datetime import datetime, timedelta
import numpy as np

# Get today's date (no subtraction)
batch_dt = datetime.today()
day_str = f"{batch_dt.day:02d}"
BATCH_MODE = 'M'

print("BATCH MODE = ", BATCH_MODE)

if BATCH_MODE == 'M':
    print("Monthly")
    # For monthly, we want to process the previous month
    # If today is the first day of the month, we still want previous month
    # Calculate first day of current month, then subtract 1 day to get last day of previous month
    batch_dt_monthly = batch_dt.replace(day=1) - timedelta(days=1)
    month_str = f"{batch_dt_monthly.month:02d}"
    year_str = f"{batch_dt_monthly.year % 100:02d}"
    print(f"Processing month: {month_str}/{year_str}")
    print(f"Processing date range: Previous month ({batch_dt_monthly.strftime('%B %Y')})")
    sas_path = "/dwh/btrade"
    output_file = f"billstran{month_str}4{year_str}"
    bill_table = pq.read_table('/sas/python/virt_edw/Data_Warehouse/TF/input/staging/STG_TF_BILLSTRAN_M.parquet')
    pq_bill = bill_table.to_pandas()
elif BATCH_MODE == 'D':
    print("Daily")
    # For daily, we want to process yesterday's data
    batch_dt_daily = batch_dt - timedelta(days=1)
    day_str = f"{batch_dt_daily.day:02d}"
    month_str = f"{batch_dt_daily.month:02d}"
    year_str = f"{batch_dt_daily.year % 100:02d}"
    print(f"Processing date: {batch_dt_daily.strftime('%Y-%m-%d')}")
    sas_path = "/dwh/btrade_d"
    output_file = f"billstran_{day_str}"
    bill_table = pq.read_table('/sas/python/virt_edw/Data_Warehouse/TF/input/staging/STG_TF_BILLSTRAN_D.parquet')
    pq_bill = bill_table.to_pandas()

def convert_date_to_sas(date_series, date_format='YYYYMMDD'):
    
    sas_epoch = pd.Timestamp('1960-01-01')
    
    def convert_single_date(val):
        if pd.isna(val) or val == 0:
            return np.nan
        try:
            date_str = str(int(val))
            if date_format == 'YYMMDD':
                date_str = date_str.zfill(6)
                if len(date_str) == 6:
                    date_str = '20' + date_str
                else:
                    return np.nan

            if len(date_str) == 8:
                date_obj = pd.to_datetime(date_str, format='%Y%m%d')
                return (date_obj - sas_epoch).days
            return np.nan
        except (ValueError, TypeError):
            return np.nan
    
    return date_series.apply(convert_single_date)

pq_bill["TRANDATE"] = convert_date_to_sas(pq_bill["TRANDATE"], date_format='YYYYMMDD')
pq_bill["EXPRDATE"] = convert_date_to_sas(pq_bill["EXPRDATE"], date_format='YYMMDD')

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
    return log

assign_libname("bt", sas_path)
assign_libname("ctl", "/stgsrcsys/host/uat")

log1 = set_data(pq_bill, "bt", "ctl", output_file, billstran_ctl)
