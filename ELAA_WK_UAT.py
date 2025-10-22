import pandas as pd
import polars as pl
import os
import ast
import duckdb
from datetime import datetime,timedelta

import pyarrow as pa
import pyarrow.parquet as pq

SPECS = "Specs:"
NAMES = "Names:"
OTHER = "Other:"
CSV = "CSV:"
FIRSTOBS = "Firstobs:"

# Define batch date

def get_reporting_dates():
    currdate = datetime.today()
    day_of_month = currdate.day
    month = currdate.month
    year = currdate.year

    if 1 <= day_of_month <= 8:
        reptdate = currdate = timedelta(days=day_of_month)
        startdte = datetime(year=reptdate.year, month=reptdate.month, day=23)
        day_of_month = 2
    elif 9 <= day_of_month <= 15:
        reptdate = datetime(year=year, month=month, day=8)   
        startdte = datetime(year=reptdate.year, month=reptdate.month, day=1) 
        day_of_month = 10
    elif 16 <= day_of_month <= 22:
        reptdate = datetime(year=year, month=month, day=15)   
        startdte = datetime(year=reptdate.year, month=reptdate.month, day=9)     
        day_of_month = 17
    else:
        reptdate = datetime(year=year, month=month, day=22)   
        startdte = datetime(year=reptdate.year, month=reptdate.month, day=16) 
        day_of_month = 24

    return reptdate, startdte, day_of_month

reptdate, startdte, day_of_month = get_reporting_dates()
file_dt = startdte.strftime('%Y%m%d')  
batch_dt_mth = reptdate.strftime("%Y-%m-%d %H:%M:%S")
batch_dt_str = reptdate.strftime('%Y%m%d')
prevwkdate = reptdate - timedelta(days=7)
prevdate = reptdate - timedelta(days=1)

# Parquet path adapted for duckdb format
# output_folder_path = f'/parquet/dwh/ELDS/year={reptdate.strftime("%Y")}/month={reptdate.strftime("%m")}/day={reptdate.strftime("%d")}'
output_folder_path = f'/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/output/year={reptdate.strftime("%Y")}/month={reptdate.strftime("%m")}/day={reptdate.strftime("%d")}'

# If folder path doesnt exist, create
os.makedirs(output_folder_path, exist_ok=True)

# Define the file path
# input_folder_path = '/host/dwh/input/ELDS'
input_folder_path = '/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS/input'
config_dir = '/sas/python/virt_edw/Data_Warehouse/ELDS/COLUMN_CONFIG/ELDS_ELN'


# -------------------------------
# RAW DATA MAPPING
# -------------------------------
MAPPING = {
    "ELDS_AA_ELN1": {"raw": ["newbnm1"], "config": "ELAA1_output.txt"},
    "ELDS_AA_ELN2": {"raw": ["newbnm2"], "config": "ELAA2_output.txt"},
    "ELDS_AA_ELN3": {"raw": ["newbnm3"], "config": "ELAA3_output.txt"},
    "ELDS_AA_ELN4": {"raw": ["AABaselApp4"], "config": "ELAA4_output.txt"},
    "ELDS_AA_ELN5": {"raw": ["AABaselApp5"], "config": "ELAA5_output.txt"},
    "ELDS_AA_ELN6": {"raw": ["AABaselApp6"], "config": "ELAA6_output.txt"},
    "ELDS_AA_ELN7": {"raw": ["AABaselApp7"], "config": "ELAA7_output.txt"},
    "ELDS_AA_ELN8": {"raw": ["AABaselApp8"], "config": "ELAA8_output.txt"},
}

# -------------------------------
# HELPER: find actual dated file for prefix
# -------------------------------
def find_latest_file(prefix):
    for fname in os.listdir(input_folder_path):
        if fname.lower().startswith(prefix.lower()) and fname.endswith(".txt"):
            return os.path.join(input_folder_path, fname)
    return None

# -------------------------------
# CONFIG LOADER
# -------------------------------
def load_variables(var_path, script_name):
    input_path = os.path.join(var_path, f"{script_name}_output.txt")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Missing config: {input_path}")
    
    with open(input_path, "r") as file:
        content = file.read()
    
    firstobs_start = content.index(FIRSTOBS) + len(FIRSTOBS)
    firstobs_end= content.index(CSV)
    firstobs = int(content[firstobs_start:firstobs_end].strip()) - 1

    csv_start = content.index(CSV) + len(CSV)
    csv_end = content.index(SPECS)
    csv_str = content[csv_start:csv_end].strip()

    specs_start = content.index(SPECS) + len(SPECS)
    names_start = content.index(NAMES) + len(NAMES)
    other_start = content.index(OTHER)

    specs_str = content[specs_start:names_start - + len(NAMES)].strip()
    names_str = content[names_start:other_start].strip()
    other_str = content[other_start + len(OTHER)].strip()

    specs = ast.literal_eval(f"[{specs_str}]")
    names = ast.literal_eval(f"[{names_str}]")
    other = ast.literal_eval(f"[{other_str}]")

    column_types, column_subtypes, column_decimals = [], [], []
    for entry in other:
        col_type, subtype, decimal = [x.strip() for x in entry.split(",")]
        column_types.append(col_type)
        column_subtypes.append(subtype)
        try:
            column_decimals.append(int(decimal))
        except ValueError:
            column_decimals.append(0)
    
    return specs, names, other, column_types, column_subtypes, column_decimals, firstobs, csv_str

# -------------------------------
# GENERIC PARSING
# -------------------------------
def apply_parsing(df, column_types, column_subtypes, column_decimals):
    n = min(len(df.columns), len(column_types))

    if len(df.columns) != len(column_types):
        print(f"Column mismatch: data has(df.columns) cols, config defines {len(column_types)}")

    for i, col in enumerate(df.columns):
        ctype = column_types[i]
        subtype = column_subtypes[i]
        decimal = column_decimals[i]
    
        if ctype == "date" and subtype == "ddmmyy":
            df[col] = pd.to_datetime(df[col], format="%d%m%Y", errors="coerce")
        elif ctype == "ascii" and subtype == "upcase":
            df[col] = df[col].astype(str).str.upper()
        elif ctype == "ascii" and subtype == "comma":
            df[col] = pd.to_numeric(df[col].str.replace(",", ""), errors="coerce")
        elif ctype == "ascii" and subtype == "numeric":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif str(decimal).isdigit() and int(decimal) > 0:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(decimal)
    return df

# -------------------------------
# OUTPUT STG TO PARQUET
# -------------------------------
def save_to_parquet(df, output_file, key):
    if not df.empty:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, output_file)
        print(f"✅ Saved {key} -> {output_file} rows={len(df)}")
    else:
        print(f"No data to save for {key}")

# -------------------------------
# MAIN
# -------------------------------

dfs = {}

def run_eiwelnex():
    print(f"Starting EIWELNEX load for {batch_dt_str}")

    for key, meta in MAPPING.items():
        config_file = meta["config"]
        config_path = os.path.join(config_dir, config_file)
        if not os.path.exists(config_path):
            print(f"Missing config {config_file}, skipping {key}")
            continue
        
        script_name = config_file.replace("_output.txt","")
        specs, names, other, col_types, col_subtypes, col_decimals, firstobs, csv_str = load_variables(config_dir, script_name)

        combined_df = pd.DataFrame()
        for raw_prefix in meta ["raw"]:
            fpath = find_latest_file(raw_prefix)
            if not fpath:
                print("No matching file for prefix {raw_prefix}, skipping..")
                continue

            print(f"Reading {fpath} -> {key} ...")
            try:
                if csv_str == "True":
                    try:
                        df = pd.read_csv(fpath, header=None, dtype=str, skiprows=firstobs, encoding="utf-8")
                    except UnicodeDecodeError:
                        df = pd.read_csv(fpath, header=None, dtype=str, skiprows=firstobs, encoding="ISO-8859-1")
                else:
                    try:
                        df = pd.read_fwf(fpath, header=None, dtype=str, skiprows=firstobs, encoding="utf-8")
                    except UnicodeDecodeError:
                        df = pd.read_fwf(fpath, header=None, dtype=str, skiprows=firstobs, encoding="ISO-8859-1")
            except Exception as e:
                print(f'Error reading {fpath}: {e}')
                continue
            
            if df.empty:
                continue

            if df.shape[1] > len(names):
                df = df.iloc[:, :len(names)]
                df.columns = names
            else:
                df.columns = names[:df.shape[1]]

            expected_cols = df.shape[1]
            if len(col_types) < expected_cols:
                pad = expected_cols - len(col_types)
                col_types.extend(["ascii"] * pad)
                col_subtypes.extend([""] * pad)
                col_decimals.extend(["0"] * pad)        
            elif len(col_types) > expected_cols:
                col_types = col_types[:expected_cols]
                col_subtypes = col_subtypes[:expected_cols]
                col_decimals = col_decimals[:expected_cols]

            df = apply_parsing(df, col_types, col_subtypes, col_decimals)
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        if combined_df.empty:
            print(f'No data for {key}, skipping save')
            continue

        dfs[key] = combined_df.copy()

        output_file = os.path.join(output_folder_path, f"{key}.parquet")
        save_to_parquet(combined_df, output_file, key)

    print("ELAA job completed")

# RUN
if __name__ == "__main__":
    run_eiwelnex()

df_aa1 = dfs["ELDS_AA_ELN1"]
df_aa2 = dfs["ELDS_AA_ELN2"]
df_aa3 = dfs["ELDS_AA_ELN3"]
df_aa4 = dfs["ELDS_AA_ELN4"]
df_aa5 = dfs["ELDS_AA_ELN5"]
df_aa6 = dfs["ELDS_AA_ELN6"]
df_aa7 = dfs["ELDS_AA_ELN7"]
df_aa8 = dfs["ELDS_AA_ELN8"]

def prev_parquet(date,filename):
    full_path = f"{output_folder_path}/year={date.year}/month={date.month:02d}/day={date.day:02d}/{filename}"
    query = f"SELECT * FROM read_parquet('{full_path}')"
    return duckdb.query(query).to_df()

prev_summ1 = prev_parquet(prevdate ,"SUMM1.parquet")
prev_summ2 = prev_parquet(prevdate ,"SUMM2.parquet")
prev_aa1   = prev_parquet(prevwkdate ,"ELDS_AA_ELN1.parquet")
prev_aa2   = prev_parquet(prevwkdate ,"ELDS_AA_ELN2.parquet")
prev_aa3   = prev_parquet(prevwkdate ,"ELDS_AA_ELN3.parquet")
prev_aa4   = prev_parquet(prevwkdate ,"ELDS_AA_ELN4.parquet")
prev_aa5   = prev_parquet(prevwkdate ,"ELDS_AA_ELN5.parquet")
prev_aa6   = prev_parquet(prevwkdate ,"ELDS_AA_ELN6.parquet")
prev_aa7   = prev_parquet(prevwkdate ,"ELDS_AA_ELN7.parquet")
prev_aa8   = prev_parquet(prevwkdate ,"ELDS_AA_ELN8.parquet")

df_aa1_comb = pd.concat([df_aa1, prev_aa1], ignore_index=True)
df_aa1_comb = df_aa1_comb[df_aa1_comb["STATUS"] == 'APPROVED']
