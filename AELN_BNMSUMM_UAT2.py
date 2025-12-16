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

# Print initial record count
print(f"Initial record count from Parquet: {len(pq_bill)}")

pq_bill["TRANDATE"] = pq_bill["TRANDATE"].astype(int)
pq_bill["EXPRDATE"] = pq_bill["EXPRDATE"].astype(int)

# Fill NULL values in all object/string columns with empty string
for col in pq_bill.select_dtypes(include=['object']).columns:
    pq_bill[col] = pq_bill[col].fillna('')
    
print(f"NULL values in character columns handled")

# Initialize SAS session
sas = saspy.SASsession()
billstran_ctl = "billstran_ctl"

def assign_libname(lib_name, sas_path):
    log = sas.submit(f"""libname {lib_name} '{sas_path}';""")
    print(f"Libname assignment for {lib_name}: {log['LOG']}")
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
    
    # Debug: Print metadata
    print("\nColumn Metadata:")
    print(df_meta[['name', 'type', 'length']])
    
    cols = df_meta["name"].dropna().tolist()
    col_list = ", ".join(cols)
    casted_cols = []
    
    # Handle character and numeric columns appropriately
    for _, row in df_meta.iterrows():
        col = row["name"]
        col_type = row['type'].strip().lower()
        length = row['length']
        
        if col_type == 'char' and pd.notnull(length) and length > 0:
            # For character columns: use coalescec to handle nulls, then substr to trim
            casted_cols.append(f"substr(coalescec({col}, ''), 1, {int(length)}) as {col}")
        elif col_type == 'num':
            # For numeric columns: pass through as-is
            casted_cols.append(col)
        else:
            # Default: pass through
            casted_cols.append(col)
    
    print("\nGenerated casted columns:")
    print("\n".join(casted_cols[:5]))  # Print first 5 for debugging
    
    casted_cols = ",\n ".join(casted_cols)
    
    # First, let's check the count in work table
    count_log = sas.submit(f"""
        proc sql noprint;
            select count(*) into :work_count from work.{cur_data};
            select count(*) into :ctl_count from {ctrl_name}.{prev_data};
        quit;
        %put Work table count: &work_count;
        %put Control table count: &ctl_count;
    """)
    print(f"Record counts before union: {count_log['LOG']}")
    
    # First create a temporary transformed table to debug
    log = sas.submit(f"""
                proc sql noprint;
                     create table work.{cur_data}_transformed as
                     select {casted_cols} from work.{cur_data};
                     
                     select count(*) into :transformed_count from work.{cur_data}_transformed;
                quit;
                %put Transformed table count: &transformed_count;
                """)
    print(f"After transformation: {log['LOG']}")
    
    # Now do the union
    log = sas.submit(f"""
                proc sql noprint;
                     create table {lib_name}.{cur_data} as
                     select {col_list} from {ctrl_name}.{prev_data}
                     union all corr
                     select {col_list} from work.{cur_data}_transformed;
                     
                     select count(*) into :final_count from {lib_name}.{cur_data};
                quit;
                %put Final table count: &final_count;
                """)
    print(f"Final table created: {log['LOG']}") 
    return log

# Assign libnames
assign_libname("bt", sas_path)
assign_libname("ctl", "/stgsrcsys/host/uat")

# Set data
log1 = set_data(pq_bill, "bt", "ctl", output_file, billstran_ctl)

print("Process completed successfully!")
