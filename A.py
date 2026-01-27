import pandas as pd
import polars as pl
import numpy as np
from datetime import datetime,timedelta

import pyarrow as pa
import pyarrow.parquet as pq

# !!! Note that this program will only work if SAS License is valid.
import sys
import saspy
import os
import shutil
import ast
from pathlib import Path
import numbers

SPECS = "Specs:"
NAMES = "Names:"
OTHER = "Other:"
CSV = "CSV:"
FIRSTOBS = "Firstobs:"

# Define batch date

def get_reporting_dates():
    currdate = datetime.strptime("2025-11-02 00:00:00", '%Y-%m-%d %H:%M:%S')
    day_of_month = currdate.day
    month = currdate.month
    year = currdate.year

    if 1 <= day_of_month <= 8:
        reptdate = currdate - timedelta(days=day_of_month)
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
print(reptdate)
file_dt = startdte.strftime('%Y%m%d')  
batch_dt_mth = reptdate.strftime("%Y-%m-%d %H:%M:%S")
batch_dt_str = reptdate.strftime('%Y%m%d')
one_week_ago = reptdate - timedelta(days=8)

#sas month and year
month_str = f"{reptdate.month:02d}"
year_str = f"{reptdate.year % 100:02d}"
prevmonth_str = f"{one_week_ago.month:02d}"
prevyear_str = f"{one_week_ago.year % 100:02d}"

# Parquet path adapted for duckdb format
output_folder_path = f'/parquet/dwh/ELDS/year={reptdate.strftime("%Y")}/month={reptdate.strftime("%m")}/day={reptdate.strftime("%d")}'

# If folder path doesnt exist, create
os.makedirs(output_folder_path, exist_ok=True)

# Define the file path
input_folder_path = '/host/dwh/input/ELDS'
var_path = '/sas/python/virt_edw/Data_Warehouse/ELDS/COLUMN_CONFIG/WELN'

# Extract number from file name
def get_file_number(directory_path):
    file_nums = []
    try:
        files = os.listdir(directory_path)

        for file in files:
            full_path = os.path.join(directory_path, file)

            # Only take file names not folders
            if os.path.isfile(full_path):
                parts = file.split("_")
                if parts[1] == batch_dt_str:
                    base_num = file.rsplit('_', 1)[1]
                    file_nums.append((file, base_num))

        return file_nums

    except FileNotFoundError:
        print(f"Error: The directory '{directory_path}' was not found")
        return []
    
    except Exception as e:
        print(f"Error in get_file_number: {e}")
        return []
    
# Get column details from mapping files
def load_variables(var_path, script_name):
    # Get current script name without extension
    input_path = os.path.join(f"{var_path}", f"{script_name}_output.txt")

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file '{input_path}' not found.")

    with open(input_path, 'r') as file:
        content = file.read()

    # Extract specs and names from text using simple markers
    specs = []
    names = []
    other = []
    try:

        # Extract firstobs
        firstobs_start = content.index(FIRSTOBS) + len(FIRSTOBS)
        firstobs_end = content.index(CSV)
        firstobs_str = content[firstobs_start:firstobs_end].strip()
        firstobs = int(firstobs_str) - 1

        # Check if input file type is CSV
        csv_start = content.index(CSV) + len(CSV)
        csv_end = content.index(SPECS)
        csv_str = content[csv_start:csv_end].strip()

        # Locate positions of the metadata sections
        specs_start = content.index(SPECS) + len(SPECS)
        names_start = content.index(NAMES) + len(NAMES)
        other_start = content.index(OTHER)

        # Extract text blocks
        specs_str = content[specs_start:names_start - + len(NAMES)].strip()
        names_str = content[names_start:other_start].strip()
        other_str = content[other_start + len(OTHER):].strip()

        # Safely evaluate to Python lists
        specs = ast.literal_eval(f"[{specs_str}]")
        names = ast.literal_eval(f"[{names_str}]")
        other = ast.literal_eval(f"[{other_str}]")  # this will be list of strings like "ascii, numeric, 0"

        # If needed, split `other` entries into individual parts:
        column_types, column_subtypes, column_decimals = [], [], []
        for entry in other:
            col_type, subtype, decimal = [x.strip() for x in entry.split(",")]
            column_types.append(col_type)
            column_subtypes.append(subtype)
            column_decimals.append(int(decimal)) 

    except Exception as e:
        raise ValueError(f"Failed to parse variable file: {e}")

    return specs, names, other, column_types, column_subtypes, column_decimals, firstobs, csv_str

# Check if table exists as parquet and delete if present
def check_and_delete_table(output_file):
    if os.path.exists(output_file):
        print(f"Table {output_file} exists. Deleting...")
        os.remove(output_file)
        print(f"Deleted: {output_file}")
    else:
        print(f"Table {output_file} does not exist.")

# Remove comma in numerical field     
def parse_comma_format(s):
    s = s.strip().replace(',', '')
    return s

# Check if x is a valid number
def is_number(x):
    try:
        float(x)
        return True
    except (ValueError, TypeError):
        return False

# Character formatting
def parse_ascii_format(subtype, s, decimal):
    if subtype == 'comma':
        if pd.notna(s) and str(s).strip().lower() != 'nan' and str(s).strip() != '-' and str(s).strip() !='':
            return parse_comma_format(s)
    elif subtype == 'numeric':
        if pd.notna(s) and is_number(s) and str(s).strip() !='':
            return format_number(s)
    elif decimal and decimal > 0:
        return round(float(s), decimal)
    elif subtype == 'upcase':
        return s.upper()
    else:
        return s

# Convert dates to SAS format
def parse_date_format(subtype, s):
    if s != 'nan' and pd.notna(s):
        if subtype == 'ddmmyy':
            if s.strip():
                dt = datetime.strptime(s, "%d/%m/%Y").date()
                return safe_to_days(dt.year, dt.month, dt.day)
        else:
            return s
    return s

# Supplementary function to column_format
def parse_value(s, ctype, subtype, decimal):
    s = s.strip()
    if not s:
        return ''
    try:
        if ctype == 'ascii':
            s = parse_ascii_format(subtype, s, decimal)
        elif ctype == 'date':
            s = parse_date_format(subtype, s)
        return s
    except Exception as e:
        print(f"Error in parse_value: {e}")
        return ''

# Ensure numerical values does not contain str
def is_valid_number(x):
    return pd.notna(x) and isinstance(x, numbers.Number) and str(x).strip().lower() != 'nan' and str(x).strip() != '-' and str(x).strip() !=''

def format_number(x):
    return x

# Format values based on column type
def column_format(df, col, ctype, subtype, decimal):

    df[col] = df[col].astype(str).apply(lambda x, ct = ctype, st = subtype, d = decimal: parse_value(x, ct, st, d))

    if subtype == 'numeric' or ctype == 'date':
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].apply(
            lambda x, d = decimal: format_number(x) if is_valid_number(x) else x
        )
    elif subtype == 'comma':
        df[col] = pd.to_numeric(df[col], errors='coerce')
    else:
        df[col] = df[col].astype(str)

    return df

# Parse format on DataFrame
def apply_parsing(df):
# Apply parsing column by column
    for i, col in enumerate(df.columns):
        ctype = column_types[i]
        subtype = column_subtypes[i]
        decimal = column_decimals[i]
        df = column_format(df, col, ctype, subtype, decimal)
            
    return df

# Drop exact duplicate rows from DataFrame
def common_drop_dups(df):
    initial_count = len(df)
    df = df.drop_duplicates()
    removed_count = initial_count - len(df)
    print(f"Removed {removed_count} duplicate rows")
    return df             

# Save to Parquet format using PyArrow
def save_to_parquet(df, output_file):
    if df is not None:
        try:
            table = pa.Table.from_pandas(df, preserve_index = False)
            pq.write_table(table, output_file)
            print(f"Data saved to {output_file}")
        except Exception as e:  
            print(f"Error saving data: {e}")
    else:
        print("No data to save.")

# Validation: Record count checker
def check_record_count(dest_path: str) -> int:
    if Path(dest_path).exists():
        df = pd.read_parquet(dest_path)
        return len(df)
    return 0

# Helper function for date conversion
def safe_int_convert(value, default=1):
    try:
        return int(value)
    except ValueError:
        return default

# Convert to SAS date format (Number of days since 1st Jan 1960)
def safe_to_days(yy, mm, dd):
    if np.isnan(yy) or np.isnan(mm) or np.isnan(dd):
        return np.nan
    else:
        try:
            y = int(yy) if int(yy) > 0 else 1900
            m = int(mm) if int(mm) > 0 else 1
            d = int(dd) if int(dd) > 0 else 1
            dt = datetime (y, m, d)

            # Fault catching (if equals to default value)
            if dt == datetime(1900,1,1,0,0,0):
                return np.nan
            else:
                return(pd.Timestamp(dt) - sas_origin).days
        except Exception as e:
            print(f"Error in safe_to_days: {e}")
            return np.nan

# Entry Point
sas = saspy.SASsession()
sas_origin = pd.Timestamp("1960-01-01")
base_num = ['01','03','12','28','49']

df_01= pl.read_parquet(f"{output_folder_path}/CRMS_ELN_WK_01.parquet")
df_03= pl.read_parquet(f"{output_folder_path}/CRMS_ELN_WK_03.parquet")
df_12= pl.read_parquet(f"{output_folder_path}/CRMS_ELN_WK_12.parquet")
df_28= pl.read_parquet(f"{output_folder_path}/CRMS_ELN_WK_28.parquet")
df_49= pl.read_parquet(f"{output_folder_path}/CRMS_ELN_WK_49.parquet")

ELNA1 = df_01[["AANO","BRANCH","CUSTCODE"]]
print(ELNA1)
ELNA3 = df_03[["AANO","MAANO"]]
print(ELNA3)
ELNA12 = df_12
print(ELNA12)
ELNA28 = df_28
print(ELNA28)
ELNA49 = df_49
print(ELNA49)

########################################################

ECMS = ELNA49[["MAANO","ACCTNO","CCOLLNO","CCOLLTYPE"]]

########################################################
# Convert Polars DataFrames to Pandas for compatibility with existing code
ELNA1 = ELNA1.to_pandas()
ELNA3 = ELNA3.to_pandas()
ELNA12 = ELNA12.to_pandas()
ELNA28 = ELNA28.to_pandas()
ECMS = ECMS.to_pandas()

# ---------------------------- DATA ELNA -------------------------------
# MERGE ELNA1(keep= AANO BRANCH CUSTCODE IN=A) ELNA3(keep= AANO MAANO);
# BY AANO; IF A;  → left join ELNA1 ⋈ ELNA3 on AANO, keep only ELNA1 hits

# make join key same type (string, trimmed, no trailing ".0")
ELNA1["AANO"] = ELNA1["AANO"].astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
ELNA3["AANO"] = ELNA3["AANO"].astype("string").str.strip().str.replace(r"\.0$", "", regex=True)

# PROC SORT DATA=ELNA NODUPKEY; BY MAANO;
# NODUPKEY → keep first per MAANO (even if other cols differ)
ELNA = (
    ELNA1[["AANO", "BRANCH", "CUSTCODE"]]
      .merge(ELNA3[["AANO", "MAANO"]].drop_duplicates("AANO", keep="last"), on="AANO", how="left")
      .dropna(subset=["MAANO"])
      .drop_duplicates(subset=["MAANO"], keep="first")
      .reset_index(drop=True)
)

# --- NODUPRECS on ELNA12 (sort by MAANO, drop exact dup rows) ---
ELNA12 = (
    ELNA12.sort_values("MAANO", kind="mergesort")
          .drop_duplicates(keep="first")
          .reset_index(drop=True)
)

# ensure join key dtype matches
ELNA12["MAANO"] = ELNA12["MAANO"].astype("string").str.strip()
ELNA["MAANO"]   = ELNA["MAANO"].astype("string").str.strip()

# # ------------------------- DATA ELDS.ELNA12_... ------------------------
# # MERGE ELNA12(IN=A) ELNA; BY MAANO; IF A; IF CUSTCODE IN (...);
# # Branch split: BRANCH >= 3000 → IELDS else ELDS; KEEP list

ELNA12_MERGED = ELNA12.merge(
    ELNA[["MAANO", "BRANCH", "CUSTCODE"]],
    on="MAANO",
    how="left",
    validate="m:1"
)

# # Filter CUSTCODE in (77,78,95,96)
# --- MERGE ELNA12 (left) with ELNA on MAANO; then keep only target custcodes ---
ELNA12_MERGED = ELNA12_MERGED[ELNA12_MERGED["CUSTCODE"].isin([77, 78, 95, 96])]

ELNA12_KEEP = [
    "MAANO","FNAME","BANK1","TYPEACCT1","TYPEFAC1","LMTAPP1","BALANCE1","BANK2",
    "TYPEACCT2","TYPEFAC2","LMTAPP2","BALANCE2","OORACC1","OORACC2","TBANK1",
    "TOORACC1","TTYPEACC1","TTYPEFAC1","TLMTAPP1","TBALANCE1","TBANK2",
    "TOORACC2","TTYPEACC2","TTYPEFAC2","TLMTAPP2","TBALANCE2","CADBCR"
]

# Some columns might not exist in source; to emulate SAS keep-as-missing, ensure presence
# add any missing columns as NA to emulate SAS KEEP behavior
for col in ELNA12_KEEP:
    if col not in ELNA12_MERGED.columns:
        ELNA12_MERGED[col] = pd.NA

ELNA12_MERGED = ELNA12_MERGED[ELNA12_KEEP + ["BRANCH"]]

ELDS_ELNA12   = ELNA12_MERGED[ELNA12_MERGED["BRANCH"] < 3000][ELNA12_KEEP].reset_index(drop=True)
IELDS_ELNA12  = ELNA12_MERGED[ELNA12_MERGED["BRANCH"] >= 3000][ELNA12_KEEP].reset_index(drop=True)

# ----------------------- DATA ELDS.ACC_CONDUCT_AA_... --------------------
# --- NODUPRECS on ELNA28 ---
ELNA28 = (
    ELNA28.sort_values("MAANO", kind="mergesort")
          .drop_duplicates(keep="first")
          .reset_index(drop=True)
)

# ensure join key dtype matches
ELNA28["MAANO"] = ELNA28["MAANO"].astype("string").str.strip()

# MERGE ELNA28 (left) with ELNA on MAANO
ELNA28_MERGED = ELNA28.merge(
    ELNA[["MAANO", "BRANCH"]],
    on="MAANO",
    how="left",
    validate="m:1"
)

ACCCOND_KEEP = [
    "MAANO","NAME","ID","BRRWERTYPE","SINCEDT","ACCNATURE","ODLIMIT","AMD","ADB",
    "EXCESSFREQ","CHQRTRN","ACCACTVTY","CACONDUCT","REPAYMENT","BTCONDUCT"
]

for col in ACCCOND_KEEP:
    if col not in ELNA28_MERGED.columns:
        ELNA28_MERGED[col] = pd.NA

ELNA28_MERGED = ELNA28_MERGED[ACCCOND_KEEP + ["BRANCH"]]

ELDS_ACC   = ELNA28_MERGED[(ELNA28_MERGED["BRANCH"] < 3000) | (ELNA28_MERGED["BRANCH"].isna())][ACCCOND_KEEP].reset_index(drop=True)
IELDS_ACC  = ELNA28_MERGED[ELNA28_MERGED["BRANCH"] >= 3000][ACCCOND_KEEP].reset_index(drop=True)

def assign_libname(lib_name, sas_path):
    log = sas.submit(f"""libname {lib_name} '{sas_path}';""")
    return log

def drop_table(lib_name, cur_data):

    sas_code = f"""
            %macro drop_if_exists(lib=work, table=);
                %if %sysfunc(exist(&lib..&table)) %then %do;
                    proc sql;
                        drop table &lib..&table;
                    quit;
                %end;
            %mend;

            %drop_if_exists(lib={lib_name}, table={cur_data});
        """
    log = sas.submit(sas_code)

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
    
    print(log["LOG"]) 
    return log
    
assign_libname("elds" , "/dwh/elds")
assign_libname("ields" , "/dwh/ields")
assign_libname("ctl_elds" , "/sas/python/virt_edw/Data_Warehouse/SASTABLE")

# Determine dataset suffixes
suffix_map = {
    2:  ("3","4"),
    10: ("4","1"),
    17: ("1","2"), 
    24: ("2","3")
}

if day_of_month in suffix_map:
    prev_suffix, cur_suffx = suffix_map[day_of_month]

    labels = ["elna12_", "ielna12_", "acc_conduct_aa_", "iacc_conduct_aa_", "ecms"]
    data   = {}

    for label in labels:
        data[f"c_{label}"] = f"{label}{month_str}{cur_suffx}{year_str}"
else:
    raise ValueError(f"Unsupported batch day: {day_of_month}")

log1 = set_data(ELDS_ELNA12, "elds", "ctl_elds", data["c_elna12_"], "elna12_ctl")
log1 = set_data(IELDS_ELNA12, "ields", "ctl_elds", data["c_ielna12_"], "ielna12_ctl")
log1 = set_data(ELDS_ACC, "elds", "ctl_elds", data["c_acc_conduct_aa_"], "acc_conduct_aa_ctl")
log1 = set_data(IELDS_ACC, "ields", "ctl_elds", data["c_iacc_conduct_aa_"], "iacc_conduct_aa_ctl")
log1 = set_data(ECMS, "elds", "ctl_elds", data["c_ecms"], "ecms_ctl")
