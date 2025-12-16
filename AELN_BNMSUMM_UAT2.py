import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import saspy
import sys
from datetime import datetime, timedelta

batch_dt = datetime.today() - timedelta(days=1)
batch_dt_str = batch_dt.strftime("%Y%m%d")
output_file_name = 'billstran'
month_str = f"{batch_dt.month:02d}"
year_str = f"{batch_dt.year % 100:02d}"
day_str = f"{batch_dt.day:02d}"
BATCH_MODE = 'D'

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

pq_bill["TRANDATE"] = pq_bill["TRANDATE"].astype(int)
pq_bill["EXPRDATE"] = pq_bill["EXPRDATE"].astype(int)

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
