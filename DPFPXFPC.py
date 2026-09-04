import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime,timedelta
import saspy
import os
import duckdb

sas = saspy.SASsession()

reptdate =  (datetime.now() - timedelta(days=1))
batch_dt_str = reptdate.strftime("%Y%m%d")

# month and year
month_str = f"{reptdate.month:02d}"
year_str = f"{reptdate.year % 100:02d}"

con = duckdb.connect()

src_path = f'/parquet/dwh/ELDS/year={reptdate.strftime("%Y")}/month={reptdate.strftime("%m")}'

# Use glob pattern to read all daily files for SUMM1
summ1_df = con.execute(f"""
            SELECT *
            FROM read_parquet('{src_path}/day=*/SUMM1_today_temp.parquet')
""").fetchdf()

print("SUMM1 row count: ", len(summ1_df))
summ1_df = summ1_df.rename(columns={"CCPT_TAG":"CCPT_CLASS", "DATE":"REPTDATE"})

# Use glob pattern to read all daily files for SUMM2
summ2_df = con.execute(f"""
            SELECT *
            FROM read_parquet('{src_path}/day=*/SUMM2_today_temp.parquet')
""").fetchdf()

print("SUMM2 row count: ", len(summ2_df))
summ2_df = summ2_df.rename(columns={"CCPT_TAG":"CCPT_CLASS", "DATE":"REPTDATE"})

# Convert the specific problem columsn froms tring to SAS numeric dates
date_columns = ['DBIRTH', 'DTECOMPLETE', 'DATEXT']

for col in date_columns:
    if col in summ1_df.columns and summ1_df[col].dtype == 'object':
        summ1_df[col] = pd.to_datetime(summ1_df[col], format='%d/%m/%Y', errors='coerce')
        summ1_df[col] = (summ1_df[col] - pd.Timestamp('1960-01-01')).dt.days
        print(f"SUMM1 - AFTER CONVERSION {col}: dtype={summ1_df[col].dtype}, sample head={summ1_df[col].head(3).tolist()}, sample tail={summ1_df[col].tail(3).tolist()}")

    if col in summ2_df.columns and summ2_df[col].dtype == 'object':
        summ2_df[col] = pd.to_datetime(summ2_df[col], format='%d/%m/%Y', errors='coerce')
        summ2_df[col] = (summ2_df[col] - pd.Timestamp('1960-01-01')).dt.days
        print(f"SUMM2 - AFTER CONVERSION {col}: dtype={summ2_df[col].dtype}, sample head={summ2_df[col].head(3).tolist()}, sample tail={summ2_df[col].tail(3).tolist()}")

# Special conversion for datetime columns (seconds since 1960-01-01)
def convert_dtcomplete_column(df, col_name):
    if col_name in df.columns and df[col_name].dtype == 'object':
        print(f"Before conversion {col_name}: dtype={df[col_name].dtype}, sample={df[col_name].head(3).tolist()}")

        # Create a mask to identify which format each value has
        # Format 1: contains '/' (eg: 19/11/2025 08:58:58 AM)
        mask_format1 = df[col_name].str.contains('/', na=False)
        # Format 2: contains letters for month (eg: 19NOV2025:17:24:30)
        mask_format2 = df[col_name].str.contains('[A-Za-z]', na=False) & ~mask_format1

        # Initialize result series
        result = pd.Series(index=df.index, dtype='datetime64[ns]')

        # Convert format 1
        if mask_format1.any():
            format1_converted = pd.to_datetime(
                df.loc[mask_format1, col_name],
                format='%d/%m/%Y %I:%M:%S %p',
                errors='coerce'
            )
            result[mask_format1] = format1_converted

        # Convert format 2
        if mask_format2.any():
            # First standardize the format by replacing ':' with space in the date part
            standardized = df.loc[mask_format2, col_name].str.replace(':', ' ', 1)
            format2_converted = pd.to_datetime(
                standardized,
                format='%d%b%Y %H:%M:%S',
                errors='coerce'
            )
            result[mask_format2] = format2_converted

        # For any remaining values, try coerce
        remaining_mask = result.isna() & df[col_name].notna()
        if remaining_mask.any():
            result[remaining_mask] = pd.to_datetime(df.loc[remaining_mask, col_name], errors='coerce')

        # Convert to SAS datetime (seconds since 1960-01-01)
        sas_datetime = (result - pd.Timestamp('1960-01-01')).dt.total_seconds()

        print(f"After conversion {col_name}: dtype={sas_datetime.dtype}, sample={sas_datetime.head(3).tolist()}")
        print(f"Conversion success rate: {sas_datetime.notna().sum()}/{len(sas_datetime)}")

        return sas_datetime
    else:
        return df[col_name]

# Apply the custom conversion for DTCOMPLETE
if 'DTCOMPLETE' in summ1_df.columns:
    summ1_df['DTCOMPLETE'] = convert_dtcomplete_column(summ1_df, 'DTCOMPLETE')
if 'DTCOMPLETE' in summ2_df.columns:
    summ2_df['DTCOMPLETE'] = convert_dtcomplete_column(summ2_df, 'DTCOMPLETE')

ctrl1_data = "bnmsumm1_ctrl"
bnm1_data  = f"bnmsumm1_{year_str}{month_str}"
ctrl2_data = "bnmsumm2_ctrl"
bnm2_data  = f"bnmsumm2_{year_str}{month_str}"

def assign_libname(lib_name, sas_path):
    log = sas.submit(f"""libname {lib_name} '{sas_path}';""")
    return log

def set_data(df, lib_name, ctrl_name, cur_data, prev_data):
    sas.df2sd(df,table=cur_data, libref='work')

    log = sas.submit(f"""
            proc sql noprint;
               create table colmeta as 
               select name, type, length
               from dictionary.columns
               where libname = upcase("{ctrl_name}")  
                     and memname = upcase("{prev_data}");
            quit
               """)
    
    print(log["LOG"])
    df_meta = sas.sasdata("colmeta", libref="work").to_df()
    cols = df_meta["name"].dropna().tolist()
    col_list = ", ".join(cols)

    casted_cols =[]
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
                     select {col_list} from {ctrl_name}.{prev_data}(obs=0)
                     union corr
                     select {casted_cols} from work.{cur_data};
                quit;
                """)
    print(f"Final table created : {log['LOG']}") 
    return log

assign_libname("summ" , "/exdwh/ccris/bnm_summ")
assign_libname("ctrl", "/sas/python/virt_edw/Data_Warehouse/SASTABLE")

log1 = set_data(summ1_df, "summ", "ctrl", bnm1_data, ctrl1_data)
log2 = set_data(summ2_df, "summ", "ctrl", bnm2_data, ctrl2_data)


