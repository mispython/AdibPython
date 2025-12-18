import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import saspy
import sys
from datetime import datetime, timedelta
import numpy as np

batch_dt = datetime.today() - timedelta(days=1)
batch_dt_str = batch_dt.strftime("%Y%m%d")
output_file_name = 'billstran'

# For monthly, use previous month's date
if 'M' in str(locals().get('BATCH_MODE', 'D')):
    batch_dt_monthly = (batch_dt.replace(day=1) - timedelta(days=1))
    month_str = f"{batch_dt_monthly.month:02d}"
    year_str = f"{batch_dt_monthly.year % 100:02d}"
else:
    month_str = f"{batch_dt.month:02d}"
    year_str = f"{batch_dt.year % 100:02d}"

day_str = f"{batch_dt.day:02d}"
BATCH_MODE = 'D'

print("BATCH MODE = ", BATCH_MODE)

if BATCH_MODE == 'M':
    print("Monthly")
    print(f"Processing month: {month_str}/{year_str}")
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

def convert_date_to_sas(date_series, date_format='YYYYMMDD'):
    """
    Convert date series to SAS date format (days since 1960-01-01)
    
    Args:
        date_series: pandas Series containing dates
        date_format: 'YYYYMMDD' for 8-digit format or 'YYMMDD' for 6-digit format
    
    Returns:
        pandas Series with SAS date values
    """
    sas_epoch = pd.Timestamp('1960-01-01')
    expected_len = 8 if date_format == 'YYYYMMDD' else 6
    
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
        except:
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
