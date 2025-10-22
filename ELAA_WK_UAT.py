import pandas as pd
import numpy as np
import os
import ast
import duckdb
from datetime import datetime, timedelta
import sys
import warnings

warnings.filterwarnings('ignore')

import pyarrow as pa
import pyarrow.parquet as pq

# ========================================
# CONFIGURATION CONSTANTS
# ========================================
SPECS = "Specs:"
NAMES = "Names:"
OTHER = "Other:"
CSV = "CSV:"
FIRSTOBS = "Firstobs:"

# ========================================
# DATE CALCULATION (Mirrors SAS REPTDATE logic)
# ========================================
def get_reporting_dates():
    """
    Mimics SAS REPTDATE calculation exactly:
    WHEN(8 <= DAY(TODAY()) <= 14) -> Use 8th of current month (Week 1)
    WHEN(15 <= DAY(TODAY()) <= 21) -> Use 15th of current month (Week 2)
    WHEN(22 <= DAY(TODAY()) <= 27) -> Use 22nd of current month (Week 3)
    OTHERWISE -> Use 1st of current month minus 1 day (Week 4)
    """
    currdate = datetime.today()
    day_of_month = currdate.day
    month = currdate.month
    year = currdate.year

    if 8 <= day_of_month <= 14:
        reptdate = datetime(year=year, month=month, day=8)
        wk = '1'
    elif 15 <= day_of_month <= 21:
        reptdate = datetime(year=year, month=month, day=15)
        wk = '2'
    elif 22 <= day_of_month <= 27:
        reptdate = datetime(year=year, month=month, day=22)
        wk = '3'
    else:
        reptdate = datetime(year=year, month=month, day=1) - timedelta(days=1)
        wk = '4'
    
    reptdate1 = datetime.today() - timedelta(days=1)
    
    return reptdate, reptdate1, wk

# Initialize reporting dates
reptdate, reptdate1, nowk = get_reporting_dates()
rdate = reptdate.strftime('%d%m%Y')  # DDMMYYYY format for date comparison
reptmon = reptdate.strftime('%m')
reptday1 = reptdate1.strftime('%d')
reptmon1 = reptdate1.strftime('%m')
reptyr1 = reptdate1.strftime('%y')
reptyear = reptdate.strftime('%y')

# Batch date strings
batch_dt_str = reptdate.strftime('%Y%m%d')
prevwkdate = reptdate - timedelta(days=7)
prevdate = reptdate - timedelta(days=1)

print(f"""
╔═══════════════════════════════════════════════════════════╗
║           EIWELNEX - SAS TO PYTHON MIGRATION              ║
╠═══════════════════════════════════════════════════════════╣
║  Reporting Date: {reptdate.strftime('%Y-%m-%d')} (Week {nowk})                      ║
║  Expected File Date: {rdate}                               ║
║  Previous Week: {prevwkdate.strftime('%Y-%m-%d')}                           ║
║  Previous Day: {prevdate.strftime('%Y-%m-%d')}                            ║
╚═══════════════════════════════════════════════════════════╝
""")

# ========================================
# PATH CONFIGURATION
# ========================================
BASE_PATH = '/sas/python/virt_edw/Data_Warehouse/MIS/Job/ELDS'
output_folder_path = f'{BASE_PATH}/output/year={reptdate.strftime("%Y")}/month={reptdate.strftime("%m")}/day={reptdate.strftime("%d")}'
input_folder_path = f'{BASE_PATH}/input'
config_dir = '/sas/python/virt_edw/Data_Warehouse/ELDS/COLUMN_CONFIG/ELDS_ELN'

# Create output folder if it doesn't exist
os.makedirs(output_folder_path, exist_ok=True)

# ========================================
# FILE MAPPING (Maps to SAS DD statements)
# ========================================
MAPPING = {
    "ELNA1": {"raw": ["newbnm1"], "config": "ELAA1_output.txt"},
    "ELNA2": {"raw": ["newbnm2"], "config": "ELAA2_output.txt"},
    "ELNA3": {"raw": ["newbnm3"], "config": "ELAA3_output.txt"},
    "ELNA4": {"raw": ["AABaselApp4"], "config": "ELAA4_output.txt"},
    "ELNA5": {"raw": ["AABaselApp5"], "config": "ELAA5_output.txt"},
    "ELNA6": {"raw": ["AABaselApp6"], "config": "ELAA6_output.txt"},
    "ELNA7": {"raw": ["AABaselApp7"], "config": "ELAA7_output.txt"},
    "ELNA8": {"raw": ["AABaselApp8"], "config": "ELAA8_output.txt"},
}

# ========================================
# UTILITY FUNCTIONS
# ========================================
def find_latest_file(prefix):
    """Find the actual dated file matching the prefix"""
    if not os.path.exists(input_folder_path):
        print(f"❌ Input folder not found: {input_folder_path}")
        return None
    
    for fname in os.listdir(input_folder_path):
        if fname.lower().startswith(prefix.lower()) and fname.endswith(".txt"):
            return os.path.join(input_folder_path, fname)
    return None

def load_variables(var_path, script_name):
    """Load column configuration from output.txt files"""
    input_path = os.path.join(var_path, f"{script_name}_output.txt")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Missing config: {input_path}")
    
    with open(input_path, "r") as file:
        content = file.read()
    
    # Parse FIRSTOBS
    firstobs_start = content.index(FIRSTOBS) + len(FIRSTOBS)
    firstobs_end = content.index(CSV)
    firstobs = int(content[firstobs_start:firstobs_end].strip()) - 1

    # Parse CSV flag
    csv_start = content.index(CSV) + len(CSV)
    csv_end = content.index(SPECS)
    csv_str = content[csv_start:csv_end].strip()

    # Parse SPECS, NAMES, OTHER
    specs_start = content.index(SPECS) + len(SPECS)
    names_start = content.index(NAMES) + len(NAMES)
    other_start = content.index(OTHER)

    specs_str = content[specs_start:names_start - len(NAMES)].strip()
    names_str = content[names_start:other_start].strip()
    other_str = content[other_start + len(OTHER)].strip()

    specs = ast.literal_eval(f"[{specs_str}]")
    names = ast.literal_eval(f"[{names_str}]")
    other = ast.literal_eval(f"[{other_str}]")

    column_types, column_subtypes, column_decimals = [], [], []
    for entry in other:
        parts = [x.strip() for x in entry.split(",")]
        col_type = parts[0] if len(parts) > 0 else "ascii"
        subtype = parts[1] if len(parts) > 1 else ""
        decimal = parts[2] if len(parts) > 2 else "0"
        
        column_types.append(col_type)
        column_subtypes.append(subtype)
        try:
            column_decimals.append(int(decimal))
        except ValueError:
            column_decimals.append(0)
    
    return specs, names, other, column_types, column_subtypes, column_decimals, firstobs, csv_str

def apply_parsing(df, column_types, column_subtypes, column_decimals):
    """Apply data type parsing based on configuration (mirrors SAS INPUT formats)"""
    for i, col in enumerate(df.columns):
        if i >= len(column_types):
            break
            
        ctype = column_types[i]
        subtype = column_subtypes[i]
        decimal = column_decimals[i]
    
        try:
            if ctype == "date" and subtype == "ddmmyy":
                df[col] = pd.to_datetime(df[col], format="%d%m%Y", errors="coerce")
            elif ctype == "ascii" and subtype == "upcase":
                df[col] = df[col].astype(str).str.upper().str.strip()
            elif ctype == "ascii" and subtype == "comma":
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")
            elif ctype == "ascii" and subtype == "numeric":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            elif isinstance(decimal, int) and decimal > 0:
                df[col] = pd.to_numeric(df[col], errors="coerce").round(decimal)
        except Exception as e:
            print(f"⚠ Warning parsing column {col}: {e}")
            continue
    
    return df

def save_to_parquet(df, output_file, key):
    """Save dataframe to parquet file"""
    if not df.empty:
        try:
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, output_file)
            print(f"✅ Saved {key}: {output_file} ({len(df):,} rows, {len(df.columns)} cols)")
        except Exception as e:
            print(f"❌ Error saving {key}: {e}")
    else:
        print(f"⚠ No data to save for {key}")

def prev_parquet(date, filename):
    """Load previous period parquet file using DuckDB"""
    full_path = f"{BASE_PATH}/output/year={date.year}/month={date.month:02d}/day={date.day:02d}/{filename}"
    
    if not os.path.exists(full_path):
        return pd.DataFrame()
    
    try:
        query = f"SELECT * FROM read_parquet('{full_path}')"
        return duckdb.query(query).to_df()
    except Exception as e:
        print(f"⚠ Error loading {full_path}: {e}")
        return pd.DataFrame()

def extract_file_date(filepath):
    """
    Extract date from first record of input file
    Mimics SAS: INPUT @054 DD 2. @057 MM 2. @060 YY 4.
    Returns date in DDMMYYYY format
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            if len(first_line) >= 64:
                dd = first_line[53:55].strip()
                mm = first_line[56:58].strip()
                yy = first_line[59:63].strip()
                
                if dd and mm and yy and dd.isdigit() and mm.isdigit() and yy.isdigit():
                    file_date = f"{dd}{mm}{yy}"
                    return file_date
    except Exception as e:
        print(f"⚠ Error extracting date from {filepath}: {e}")
    
    return None

def create_date_from_components(df, day_col, month_col, year_col, target_col):
    """
    Create date column from separate DD, MM, YY columns
    Mimics SAS: AADATE = MDY(MM,DD,YY);
    """
    if day_col in df.columns and month_col in df.columns and year_col in df.columns:
        try:
            df[target_col] = pd.to_datetime(
                df[[year_col, month_col, day_col]].astype(str).agg('-'.join, axis=1),
                format='%Y-%m-%d',
                errors='coerce'
            )
        except Exception as e:
            print(f"⚠ Warning creating date {target_col}: {e}")
            df[target_col] = pd.NaT
    return df

def translate_characters(series, from_chars, to_chars):
    """
    Mimics SAS TRANSLATE function
    Example: TRANSLATE(text, "[]", "××") replaces × with [ and × with ]
    """
    result = series.copy()
    for f, t in zip(from_chars, to_chars):
        result = result.str.replace(t, f, regex=False)
    return result

# ========================================
# MAIN PROCESSING FUNCTION
# ========================================
def run_eiwelnex():
    """
    Main processing function - 100% mirrors SAS %PROCESS macro
    """
    
    # ========================================
    # STEP 1: CHECK INPUT FILE DATES
    # ========================================
    print("\n" + "="*60)
    print("📋 STEP 1: Validating Input File Dates")
    print("="*60)
    
    file_dates = {}
    all_dates_match = True
    
    for i in range(1, 9):
        key = f"ELNA{i}"
        meta = MAPPING[key]
        raw_prefix = meta["raw"][0]
        fpath = find_latest_file(raw_prefix)
        
        if not fpath:
            print(f"❌ File not found for prefix: {raw_prefix}")
            all_dates_match = False
            continue
        
        file_date = extract_file_date(fpath)
        file_dates[key] = file_date
        
        if file_date != rdate:
            print(f"❌ {key} ({raw_prefix}): Expected {rdate}, Got {file_date}")
            all_dates_match = False
        else:
            print(f"✅ {key} ({raw_prefix}): {file_date}")
    
    if not all_dates_match:
        print("\n" + "="*60)
        print("❌ THE JOB IS NOT DONE!!")
        print(f"❌ Input files NOT dated {rdate}")
        print("="*60)
        sys.exit(77)  # Mirrors SAS ABORT 77
    
    print("\n✅ All input file dates validated successfully!\n")
    
    # ========================================
    # STEP 2: READ AND PROCESS INPUT FILES
    # ========================================
    print("="*60)
    print("📋 STEP 2: Reading and Processing Input Files")
    print("="*60)
    
    dfs = {}
    
    for key, meta in MAPPING.items():
        config_file = meta["config"]
        config_path = os.path.join(config_dir, config_file)
        
        if not os.path.exists(config_path):
            print(f"⚠ Missing config {config_file}, skipping {key}")
            continue
        
        script_name = config_file.replace("_output.txt", "")
        
        try:
            specs, names, other, col_types, col_subtypes, col_decimals, firstobs, csv_str = load_variables(config_dir, script_name)
        except Exception as e:
            print(f"❌ Error loading config for {key}: {e}")
            continue
        
        combined_df = pd.DataFrame()
        
        for raw_prefix in meta["raw"]:
            fpath = find_latest_file(raw_prefix)
            if not fpath:
                print(f"⚠ No file for {raw_prefix}")
                continue
            
            print(f"\n  📖 Reading: {os.path.basename(fpath)} -> {key}")
            
            try:
                if csv_str == "True":
                    try:
                        df = pd.read_csv(fpath, header=None, dtype=str, skiprows=firstobs, encoding="utf-8")
                    except UnicodeDecodeError:
                        df = pd.read_csv(fpath, header=None, dtype=str, skiprows=firstobs, encoding="ISO-8859-1")
                else:
                    try:
                        df = pd.read_fwf(fpath, colspecs=specs, header=None, dtype=str, skiprows=firstobs, encoding="utf-8")
                    except UnicodeDecodeError:
                        df = pd.read_fwf(fpath, colspecs=specs, header=None, dtype=str, skiprows=firstobs, encoding="ISO-8859-1")
            except Exception as e:
                print(f"  ❌ Error reading file: {e}")
                continue
            
            if df.empty:
                print(f"  ⚠ File is empty")
                continue
            
            # Assign column names
            if df.shape[1] > len(names):
                df = df.iloc[:, :len(names)]
            df.columns = names[:df.shape[1]]
            
            # Adjust metadata arrays to match actual columns
            expected_cols = df.shape[1]
            if len(col_types) < expected_cols:
                pad = expected_cols - len(col_types)
                col_types.extend(["ascii"] * pad)
                col_subtypes.extend([""] * pad)
                col_decimals.extend([0] * pad)
            elif len(col_types) > expected_cols:
                col_types = col_types[:expected_cols]
                col_subtypes = col_subtypes[:expected_cols]
                col_decimals = col_decimals[:expected_cols]
            
            # Apply data type parsing
            df = apply_parsing(df, col_types, col_subtypes, col_decimals)
            combined_df = pd.concat([combined_df, df], ignore_index=True)
            print(f"  ✓ Read {len(df):,} rows")
        
        if combined_df.empty:
            print(f"  ⚠ No data for {key}")
            continue
        
        # Filter for APPROVED status (mirrors: IF STATUS IN ('APPROVED'))
        if 'STATUS' in combined_df.columns:
            before = len(combined_df)
            combined_df = combined_df[combined_df['STATUS'].str.strip().str.upper() == 'APPROVED'].copy()
            print(f"  ✓ Filtered APPROVED: {before:,} -> {len(combined_df):,} rows")
        
        dfs[key] = combined_df.copy()
    
    # ========================================
    # STEP 3: APPLY DATASET-SPECIFIC TRANSFORMATIONS
    # ========================================
    print("\n" + "="*60)
    print("📋 STEP 3: Applying Dataset-Specific Transformations")
    print("="*60)
    
    # --- ELNA1 Date Transformations ---
    if 'ELNA1' in dfs:
        print("\n  🔧 ELNA1: Creating date fields...")
        df = dfs['ELNA1']
        
        # AADATE = MDY(MM,DD,YY)
        df = create_date_from_components(df, 'DD', 'MM', 'YY', 'AADATE')
        
        # LODATE = MDY(MM1,DD1,YY1)
        df = create_date_from_components(df, 'DD1', 'MM1', 'YY1', 'LODATE')
        
        # APVDTE1 = MDY(MM2,DD2,YY2)
        df = create_date_from_components(df, 'DD2', 'MM2', 'YY2', 'APVDTE1')
        
        # APVDTE2 = MDY(MM3,DD3,YY3)
        df = create_date_from_components(df, 'DD3', 'MM3', 'YY3', 'APVDTE2')
        
        # ICDATE = MDY(MM4,DD4,YY4)
        df = create_date_from_components(df, 'DD4', 'MM4', 'YY4', 'ICDATE')
        
        # LOBEFUDT = MDY(MM5,DD5,YY5)
        df = create_date_from_components(df, 'DD5', 'MM5', 'YY5', 'LOBEFUDT')
        
        # LOBEAPDT = MDY(MM6,DD6,YY6)
        df = create_date_from_components(df, 'DD6', 'MM6', 'YY6', 'LOBEAPDT')
        
        # LOBELODT = MDY(MM7,DD7,YY7)
        df = create_date_from_components(df, 'DD7', 'MM7', 'YY7', 'LOBELODT')
        
        dfs['ELNA1'] = df
        print(f"  ✓ ELNA1: Created 8 date fields")
    
    # --- ELNA2 Character Translation ---
    if 'ELNA2' in dfs:
        print("\n  🔧 ELNA2: Translating special characters...")
        df = dfs['ELNA2']
        
        # INCEPTION_RISK_RATING_GRADE_SME = TRANSLATE(...,"[]","××")
        if 'INCEPTION_RISK_RATING_GRADE_SME' in df.columns:
            df['INCEPTION_RISK_RATING_GRADE_SME'] = translate_characters(
                df['INCEPTION_RISK_RATING_GRADE_SME'].astype(str),
                "[]",
                "××"
            )
            print(f"  ✓ ELNA2: Translated INCEPTION_RISK_RATING_GRADE_SME")
        
        dfs['ELNA2'] = df
    
    # --- ELNA3 Transformations ---
    if 'ELNA3' in dfs:
        print("\n  🔧 ELNA3: Applying business logic...")
        df = dfs['ELNA3']
        
        # PRODUCT derivation from FACCODE
        if 'FACCODE' in df.columns:
            df['PRODUCT'] = pd.NA
            
            # IF SUBSTR(FACCODE,3,1) EQ 5 THEN PRODUCT = SUBSTR(FACCODE,6,5)*1
            mask_5 = df['FACCODE'].str[2:3] == '5'
            df.loc[mask_5, 'PRODUCT'] = pd.to_numeric(
                df.loc[mask_5, 'FACCODE'].str[5:10],
                errors='coerce'
            )
            
            # ELSE PRODUCT = SUBSTR(FACCODE,8,3)*1
            mask_else = ~mask_5
            df.loc[mask_else, 'PRODUCT'] = pd.to_numeric(
                df.loc[mask_else, 'FACCODE'].str[7:10],
                errors='coerce'
            )
            
            print(f"  ✓ ELNA3: Derived PRODUCT from FACCODE")
        
        # LEFT() functions to trim whitespace
        trim_cols = ['REFINANC', 'SECTCD2', 'FDTAIND', 'FOTAIND', 'FDTANUM', 
                     'FOTANUM', 'FDTAINS', 'FOTAINS', 'HPDTAIND', 'HPDTAINS', 'STATUS']
        for col in trim_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.lstrip()
        print(f"  ✓ ELNA3: Trimmed {len([c for c in trim_cols if c in df.columns])} columns")
        
        # IF DNBFISME='' THEN DNBFISME='0'
        if 'DNBFISME' in df.columns:
            df.loc[df['DNBFISME'].isna() | (df['DNBFISME'].astype(str).str.strip() == ''), 'DNBFISME'] = '0'
            print(f"  ✓ ELNA3: Set default DNBFISME='0'")
        
        dfs['ELNA3'] = df
    
    # --- ELNA4 Date Transformation ---
    if 'ELNA4' in dfs:
        print("\n  🔧 ELNA4: Creating date fields...")
        df = dfs['ELNA4']
        
        # DBIRTH = MDY(DOMM,DOBB,DOYY)
        df = create_date_from_components(df, 'DOBB', 'DOMM', 'DOYY', 'DBIRTH')
        
        dfs['ELNA4'] = df
        print(f"  ✓ ELNA4: Created DBIRTH date field")
    
    # --- ELNA6 Date Transformation ---
    if 'ELNA6' in dfs:
        print("\n  🔧 ELNA6: Creating date fields...")
        df = dfs['ELNA6']
        
        # FYREND = MDY(FINMM,FINDD,FINYY)
        df = create_date_from_components(df, 'FINDD', 'FINMM', 'FINYY', 'FYREND')
        
        dfs['ELNA6'] = df
        print(f"  ✓ ELNA6: Created FYREND date field")
    
    # --- ELNA7 Transformations ---
    if 'ELNA7' in dfs:
        print("\n  🔧 ELNA7: Creating date fields and derived columns...")
        df = dfs['ELNA7']
        
        # Date fields
        df = create_date_from_components(df, 'SDD', 'SMM', 'SYY', 'SPADT')
        df = create_date_from_components(df, 'VDD', 'VMM', 'VYY', 'VALUEDT')
        df = create_date_from_components(df, 'ADD', 'AMM', 'AYY', 'ADLAPRDT')
        df = create_date_from_components(df, 'EDD', 'EMM', 'EYY', 'EXPAPRDT')
        df = create_date_from_components(df, 'EXDD', 'EXMM', 'EXYY', 'EXTAPRDT')
        
        # IF PCTCOMP = 100 THEN COMPSTAT = 'Y'; ELSE COMPSTAT = 'N'
        if 'PCTCOMP' in df.columns:
            df['COMPSTAT'] = df['PCTCOMP'].apply(lambda x: 'Y' if x == 100 else 'N')
            print(f"  ✓ ELNA7: Created COMPSTAT derived field")
        
        dfs['ELNA7'] = df
        print(f"  ✓ ELNA7: Created 5 date fields")
    
    # --- ELNA8 Date Transformations ---
    if 'ELNA8' in dfs:
        print("\n  🔧 ELNA8: Creating date fields...")
        df = dfs['ELNA8']
        
        # APPLDATE = MDY(XMM,XDD,XYY)
        df = create_date_from_components(df, 'XDD', 'XMM', 'XYY', 'APPLDATE')
        
        # INCRDT = MDY(INCMM,INCDD,INCYY)
        df = create_date_from_components(df, 'INCDD', 'INCMM', 'INCYY', 'INCRDT')
        
        # COMPLIDT = MDY(CMM,CDD,CYY)
        df = create_date_from_components(df, 'CDD', 'CMM', 'CYY', 'COMPLIDT')
        
        dfs['ELNA8'] = df
        print(f"  ✓ ELNA8: Created 3 date fields")
    
    # ========================================
    # STEP 4: LOAD PREVIOUS PERIOD DATA
    # ========================================
    print("\n" + "="*60)
    print("📋 STEP 4: Loading Previous Period Data")
    print("="*60)
    
    prev_dfs = {}
    for key in MAPPING.keys():
        prev_df = prev_parquet(prevwkdate, f"{key}.parquet")
        if not prev_df.empty:
            prev_dfs[key] = prev_df
            print(f"  ✅ {key}: {len(prev_df):,} rows from {prevwkdate.strftime('%Y-%m-%d')}")
        else:
            prev_dfs[key] = pd.DataFrame()
            print(f"  ⚠ {key}: No previous data")
    
    # Load SUMM files for interim updates
    print("\n  📖 Loading SUMM files...")
    prev_summ1 = prev_parquet(prevdate, "SUMM1.parquet")
    prev_summ2 = prev_parquet(prevdate, "SUMM2.parquet")
    
    if not prev_summ1.empty:
        print(f"  ✅ SUMM1: {len(prev_summ1):,} rows from {prevdate.strftime('%Y-%m-%d')}")
    else:
        print(f"  ⚠ SUMM1: No data")
    
    if not prev_summ2.empty:
        print(f"  ✅ SUMM2: {len(prev_summ2):,} rows from {prevdate.strftime('%Y-%m-%d')}")
    else:
        print(f"  ⚠ SUMM2: No data")
    
    # ========================================
    # STEP 5: APPEND PREVIOUS DATA
    # ========================================
    print("\n" + "="*60)
    print("📋 STEP 5: Appending Previous Period Data")
    print("="*60)
    
    for key in ['ELNA1', 'ELNA2', 'ELNA3', 'ELNA4', 'ELNA5', 'ELNA6', 'ELNA7', 'ELNA8']:
        if key in dfs and key in prev_dfs and not prev_dfs[key].empty:
            current_count = len(dfs[key])
            prev_count = len(prev_dfs[key])
            
            dfs[key] = pd.concat([dfs[key], prev_
