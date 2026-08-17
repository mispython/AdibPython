import pyreadstat
import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

# =========================================================
# 1. CONFIGURATION
# =========================================================
BASE_INPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIWBTCR")
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIWBTCR")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# =========================================================
# 2. FORMAT DEFINITIONS (PBBBTFMT and PBBLNFMT)
# =========================================================
# Facility/Product Code Mappings ($LIAB format)
LIAB_FORMAT = {
    'BAE': '34471', 'BEI': '34471',
    'BAI': '34472', 'BII': '34472',
    'BAP': '34475', 'BAS': '34475',
    'BPI': '34475', 'BSI': '34475',
    'MUR': '34411', 'IST': '34412',
    'IJA': '34421', 'MUS': '34422',
    'MUD': '34440', 'QAR': '34470',
    'TAW': '34480', 'BAI': '34490',
    'OD': '34710', 'TL': '34720',
    'RL': '34730', 'HL': '34740',
    'CL': '34750', 'PL': '34760',
    'AL': '34770',
    'POS': '34810', 'DAU': '34831',
    'DDU': '34832', 'FFS': '34840',
    'FFU': '34850', 'FFL': '34860',
}

# NSRSLIAB Format
NSRSLIAB_FORMAT = {
    '34411': '34411', '34412': '34412',
    '34421': '34421', '34422': '34422',
    '34440': '34440', '34470': '34470',
    '34480': '34480', '34490': '34490',
}

# Sector Reverse Format ($RVRSE)
SECTOR_REVERSE_FORMAT = {
    '1': '01', '2': '02', '3': '03', '4': '04', '5': '05',
    '6': '06', '7': '07', '8': '08', '9': '09', '10': '10',
    '11': '11', '12': '12', '13': '13', '14': '14', '15': '15',
    '16': '16', '17': '17', '18': '18', '19': '19', '20': '20',
    '21': '21', '22': '22', '23': '23', '24': '24', '25': '25',
    '99': '99', '9999': '9999',
    '01': '01', '02': '02', '03': '03', '04': '04', '05': '05',
    '06': '06', '07': '07', '08': '08', '09': '09'
}

# Customer Code Format ($LOCUSTCD)
CUSTCODE_FORMAT = {
    '1': '01', '2': '02', '3': '03', '4': '04', '5': '05',
    '6': '06', '7': '07', '8': '08', '9': '09', '10': '10',
    '11': '11', '12': '12', '13': '13', '14': '14', '15': '15',
    '16': '16', '17': '17', '18': '18', '19': '19', '20': '20',
    '77': '77', '78': '78', '95': '95', '96': '96',
    '99': '99'
}

# =========================================================
# 3. DATE LOGIC
# =========================================================
TDATE = datetime.now() - timedelta(days=1)
day_val = TDATE.day
month_val = TDATE.month
year_val = TDATE.year

if day_val == 8:
    SDD, WK, WK1 = 1, '1', '4'
elif day_val == 15:
    SDD, WK, WK1 = 9, '2', '1'
elif day_val == 22:
    SDD, WK, WK1 = 16, '3', '2'
else:
    SDD, WK, WK1 = 23, '4', '3'

MM = month_val
MM1 = MM
if WK == '1':
    MM1 = MM - 1
    if MM1 == 0:
        MM1 = 12

REPTMON = f"{MM:02d}"
REPTMON1 = f"{MM1:02d}"
REPTYEAR = str(year_val)
REPTYEA2 = str(year_val)[2:]
REPTDAY = f"{day_val:02d}"
RDATE = TDATE.strftime("%d%m%y")
RDATE2 = (TDATE - timedelta(days=1)).strftime("%y%m%d")

# =========================================================
# 4. HELPER FUNCTIONS
# =========================================================
def read_sas(file_path):
    """Read SAS7BDAT file and convert to Polars DataFrame"""
    try:
        df, meta = pyreadstat.read_sas7bdat(str(file_path))
        # Convert all column names to lowercase
        df.columns = [col.lower() for col in df.columns]
        # Convert all object columns to string to avoid type issues
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).replace('nan', '')
                df[col] = df[col].replace('None', '')
        return pl.from_pandas(df)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

def write_fixed_width(df, filepath, columns_spec):
    """Write DataFrame as fixed-width text file"""
    if df is None or df.is_empty():
        print(f"Warning: No data to write for {filepath}")
        return
    
    with open(filepath, 'w') as f:
        for row in df.iter_rows(named=True):
            line = ""
            for col_name, width, format_type in columns_spec:
                value = row.get(col_name, "")
                
                if format_type == 'Z':  # Zero-padded integer
                    if value is None or value == '' or pd.isna(value):
                        line += '0' * width
                    else:
                        try:
                            line += f"{int(float(value)):0{width}d}"
                        except (ValueError, TypeError):
                            line += '0' * width
                
                elif format_type == 'S':  # String (left-justified)
                    if value is None or pd.isna(value):
                        value = ''
                    line += f"{str(value):<{width}}"
                
                elif format_type == 'D':  # Decimal with 2 decimal places
                    if value is None or value == '' or pd.isna(value):
                        line += ' ' * width
                    else:
                        try:
                            line += f"{float(value):{width}.2f}"
                        except (ValueError, TypeError):
                            line += ' ' * width
                
                elif format_type == 'I':  # Integer
                    if value is None or value == '' or pd.isna(value):
                        line += ' ' * width
                    else:
                        try:
                            line += f"{int(float(value)):{width}d}"
                        except (ValueError, TypeError):
                            line += ' ' * width
                
                else:  # Default to string
                    if value is None or pd.isna(value):
                        value = ''
                    line += f"{str(value):<{width}}"
            
            f.write(line + "\n")

# =========================================================
# 5. READ INPUT FILES (lowercase filenames)
# =========================================================
print("Reading input files...")

# Construct file names based on date (all lowercase)
input_files = {
    'imast': BASE_INPUT / f"imast{REPTDAY}{REPTMON}.sas7bdat",
    'imast2': BASE_INPUT / f"imast2{REPTDAY}{REPTMON}.sas7bdat",
    'icred': BASE_INPUT / f"icred{REPTDAY}{REPTMON}.sas7bdat",
    'isuba': BASE_INPUT / f"isuba{REPTDAY}{REPTMON}.sas7bdat",
    'iprov': BASE_INPUT / f"iprov{REPTDAY}{REPTMON}.sas7bdat",
    'iamsubacc': BASE_INPUT / f"iamsubacc{REPTDAY}{REPTMON}.sas7bdat",
    'ibtrad': BASE_INPUT / f"ibtrad{REPTMON}{WK}.sas7bdat",
    'ibtdtl': BASE_INPUT / f"ibtdtl{REPTYEA2}{REPTMON}{REPTDAY}.sas7bdat",
    'lnacct': BASE_INPUT / "lnacct.sas7bdat"
}

# Read files
data = {}
for key, file_path in input_files.items():
    if file_path.exists():
        data[key] = read_sas(file_path)
        if data[key] is not None:
            print(f"  Read {key}: {data[key].height} rows")
    else:
        print(f"  Missing {key}: {file_path}")
        data[key] = None

# =========================================================
# 6. PROCESS MAST
# =========================================================
print("\nProcessing MAST...")

mast = None
if data.get('imast') is not None:
    # Get the dataframe
    mast = data['imast']
    
    # Step 1: Filter data (using acctnox string column)
    mast = mast.filter(
        pl.col("acctnox").cast(pl.Float64, strict=False) > 2500000000
    )
    
    # Step 2: Add basic columns
    mast = mast.with_columns([
        pl.col("ficode").alias("branch"),
        pl.lit(0).cast(pl.Int64).alias("apcode"),
        pl.lit(0).cast(pl.Int64).alias("oldbrh"),
        pl.lit("     ").alias("ficody")
    ])
    
    # Step 3: Create CUSTFISS (from custcodx which is string)
    mast = mast.with_columns([
        pl.when(
            (pl.col("custcodx").is_null()) | (pl.col("custcodx") == "") | (pl.col("custcodx") == "nan")
        )
        .then(pl.lit("99"))
        .otherwise(pl.col("custcodx"))
        .alias("custcode_clean")
    ])
    
    mast = mast.with_columns([
        pl.col("custcode_clean").replace_strict(
            CUSTCODE_FORMAT, 
            default="99"
        ).alias("custfiss")
    ])
    
    # Step 4: Create SECTFISS (from sector which is string)
    mast = mast.with_columns([
        pl.when(
            (pl.col("sector").is_null()) | (pl.col("sector") == "") | (pl.col("sector") == "nan")
        )
        .then(pl.lit("9999"))
        .otherwise(pl.col("sector"))
        .alias("sector_clean")
    ])
    
    mast = mast.with_columns([
        pl.col("sector_clean").replace_strict(
            SECTOR_REVERSE_FORMAT, 
            default="9999"
        ).alias("sectfiss")
    ])
    
    # Step 5: Apply special sector override
    mast = mast.with_columns([
        pl.when(
            pl.col("custfiss").is_in(['77', '78', '95', '96'])
        ).then(pl.lit("9700")).otherwise(pl.col("sectfiss")).alias("sectfiss")
    ])
    
    # Step 6: Remove duplicates
    mast = mast.unique(subset=["acctnox"], keep="first")
    
    print(f"MAST processed: {mast.height} rows")
    print(f"MAST columns: {mast.columns[:20]}")

# =========================================================
# 7. PROCESS MAST2
# =========================================================
print("\nProcessing MAST2...")

mast2_agg = None
mast2c = None

if data.get('imast2') is not None:
    # Get the dataframe
    mast2_df = data['imast2']
    
    # Filter valid AANO (handle potential non-string columns)
    if 'aano' in mast2_df.columns:
        mast2_filtered = mast2_df.filter(
            (pl.col("aano").cast(pl.Utf8).str.slice(0, 1) != "") &
            (pl.col("aano").cast(pl.Utf8).str.len_chars() == 13)
        )
        
        # Aggregate for ALLREFNO
        if not mast2_filtered.is_empty():
            mast2_agg = mast2_filtered.group_by("acctnox").agg([
                pl.col("aano").cast(pl.Utf8).str.concat("|").alias("allrefno"),
                pl.col("apvdate").filter(pl.col("apvdate") > 0).min().alias("firstdisbdt")
            ])
    
    # MAST2C for CCPT
    if 'facno' in mast2_df.columns and 'ccpt_ltst_review_dt' in mast2_df.columns:
        mast2c = mast2_df.select([
            pl.col("acctnox"),
            pl.col("facno").cast(pl.Utf8).str.zfill(3).alias("facline"),
            pl.col("ccpt_ltst_review_dt")
        ]).unique(subset=["acctnox", "facline"])
    
    print(f"MAST2 processed")

# =========================================================
# 8. PROCESS CRED
# =========================================================
print("\nProcessing CRED...")

cred = None
if data.get('icred') is not None:
    # Get the dataframe
    cred = data['icred']
    
    # Step 1: Filter data
    cred = cred.filter(
        (pl.col("acctnox").cast(pl.Float64, strict=False) > 2500000000) &
        (pl.col("acctnox").cast(pl.Float64, strict=False) != 2501900811) &
        (pl.col("transref").cast(pl.Utf8).str.strip_chars() != "") &
        (pl.col("outstand").cast(pl.Float64, strict=False) >= 0)
    )
    
    # Step 2: Add calculated fields
    cred = cred.with_columns([
        pl.col("transref").cast(pl.Utf8).str.slice(0, 7).alias("transrex"),
        pl.lit(0).cast(pl.Int64).alias("nodays"),
        pl.lit(0).cast(pl.Int64).alias("arrears"),
        pl.lit(0).cast(pl.Int64).alias("instalm")
    ])
    
    # Step 3: Remove duplicates
    cred = cred.unique(subset=["acctnox", "transref"])
    
    print(f"CRED processed: {cred.height} rows")

# =========================================================
# 9. PROCESS SUBA
# =========================================================
print("\nProcessing SUBA...")

suba = None
suba_main = None
suba9 = None

if data.get('isuba') is not None:
    # Get the dataframe
    suba = data['isuba']
    
    # Step 1: Filter data
    suba = suba.filter(
        (pl.col("acctnox").cast(pl.Float64, strict=False) > 2500000000) &
        (pl.col("acctnox").cast(pl.Float64, strict=False) != 2501900811)
    )
    
    # Step 2: Add calculated fields
    suba = suba.with_columns([
        pl.when(
            ~pl.col("liabcode").cast(pl.Utf8).is_in(["FFS", "FFU", "FCS", "FCU", "FFL", "FTI", "FTL"])
        ).then(pl.lit("MYR")).otherwise(pl.lit(None)).alias("forcurr"),
        
        pl.col("tfdesc01").cast(pl.Utf8).str.slice(0, 13).alias("aano"),
        
        # Apply facility mappings
        pl.col("liabcode").cast(pl.Utf8).replace_strict(LIAB_FORMAT, default=None).alias("facility"),
        pl.col("liabcode").cast(pl.Utf8).replace_strict(NSRSLIAB_FORMAT, default=None).alias("faccode")
    ])
    
    # Step 3: Split into SUBA9 and SUBA main
    suba9 = suba.filter(
        (pl.col("subacct").cast(pl.Utf8) == "OV") & 
        (pl.col("transref").cast(pl.Utf8).str.strip_chars() == "")
    )
    
    suba_main = suba.filter(
        pl.col("transref").cast(pl.Utf8).str.strip_chars() != ""
    )
    
    print(f"SUBA processed: {suba.height} rows (SUBA9: {suba9.height}, SUBA_MAIN: {suba_main.height})")

# =========================================================
# 10. PROCESS ACCT (Account Level)
# =========================================================
print("\nProcessing ACCT...")

acct = None
if mast is not None and suba9 is not None:
    # Merge SUBA9 with MAST2C
    if mast2c is not None:
        suba9 = suba9.join(mast2c, on="acctnox", how="left")
    
    # Merge MAST with SUBA9
    acct = mast.join(suba9, on="acctnox", how="inner")
    
    # Merge with MAST2_AGG
    if mast2_agg is not None:
        acct = acct.join(mast2_agg, on="acctnox", how="left")
    
    # Add calculated fields
    acct = acct.with_columns([
        pl.lit(20).cast(pl.Int64).alias("issueya"),
        pl.lit(0).cast(pl.Int64).alias("issueyy"),
        pl.lit(0).cast(pl.Int64).alias("issuemm"),
        pl.lit(0).cast(pl.Int64).alias("issuedd"),
        pl.lit(0).cast(pl.Int64).alias("lmtamt"),
        pl.lit(0).cast(pl.Int64).alias("ladtyy"),
        pl.lit(0).cast(pl.Int64).alias("ladtmm"),
        pl.lit(0).cast(pl.Int64).alias("ladtdd"),
        pl.lit(0).cast(pl.Int64).alias("fxrate"),
        pl.lit("     ").alias("climate_prin_taxonomy_class")
    ])
    
    print(f"ACCT processed: {acct.height} rows")

print("\n" + "="*50)
print("PROCESSING COMPLETE")
print("="*50)
print(f"Processing Date: {TDATE.strftime('%Y-%m-%d')}")
print(f"MAST rows: {mast.height if mast is not None else 0}")
print(f"CRED rows: {cred.height if cred is not None else 0}")
print(f"SUBA rows: {suba.height if suba is not None else 0}")
print(f"SUBA9 rows: {suba9.height if suba9 is not None else 0}")
print(f"ACCT rows: {acct.height if acct is not None else 0}")
print("="*50)
