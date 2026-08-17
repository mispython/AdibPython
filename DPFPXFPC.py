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
    '99': '99',
    '1.0': '01', '2.0': '02', '3.0': '03', '4.0': '04', '5.0': '05',
    '6.0': '06', '7.0': '07', '8.0': '08', '9.0': '09', '10.0': '10',
    '99.0': '99'
}

# Industrial Sector Format ($INDSECT)
INDSECT_FORMAT = {
    '01111': '01', '01112': '01', '01113': '01', '01114': '01',
    '02111': '02', '02112': '02', '02113': '02',
    '03111': '03', '03112': '03', '03113': '03', '03114': '03', '03115': '03',
    '04111': '04',
    '05111': '05', '05112': '05', '05113': '05',
}

# Price Type Format for SFS ($PRCTYPESFS)
PRCTYPESFS_FORMAT = {
    'BAE': '71', 'BEI': '71',
    'BAI': '72', 'BII': '72',
    'BAP': '75', 'BAS': '75',
    'BPI': '75', 'BSI': '75',
}

# Regular Price Type Format ($PRCTYPE)
PRCTYPE_FORMAT = {
    'MUR': '11', 'IST': '12',
    'IJA': '21', 'MUS': '22',
    'MUD': '40', 'QAR': '70',
    'TAW': '80', 'BAI': '90',
    'OD': '71', 'TL': '72',
    'RL': '73', 'HL': '74',
    'CL': '75', 'PL': '76',
    'AL': '77',
}

# BTF Concept Format ($BTFCEPT)
BTFCONCEPT_FORMAT = {
    '34411': 11, '34412': 12,
    '34421': 21, '34422': 22,
    '34440': 40, '34470': 70,
    '34480': 80, '34490': 90,
    '34710': 10, '34720': 20,
    '34730': 30, '34740': 40,
    '34750': 50, '34760': 60,
    '34770': 70,
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
# 6. PROCESS MAST (with PBBLNFMT mappings)
# =========================================================
print("\nProcessing MAST...")

mast = None
if data.get('imast') is not None:
    # Check actual column names and dtypes
    imast_cols = data['imast'].columns
    
    # Determine correct column names - prefer string columns over float
    acctno_col = 'acctnox' if 'acctnox' in imast_cols else 'acctno'
    ficode_col = 'ficode' if 'ficode' in imast_cols else 'ficode'
    custcode_col = 'custcodx' if 'custcodx' in imast_cols else 'custcode'
    sector_col = 'sector' if 'sector' in imast_cols else 'sector'
    retailid_col = 'retailid' if 'retailid' in imast_cols else 'retailid'
    sm_date_col = 'sm_date' if 'sm_date' in imast_cols else 'sm_date'
    industrial_col = 'industrial_sector_cd' if 'industrial_sector_cd' in imast_cols else 'industrial_sector_cd'
    
    print(f"Using columns: acctno={acctno_col}, custcode={custcode_col}, sector={sector_col}")
    
    # Step 1: Filter data - convert acctno to float for comparison
    mast = data['imast'].filter(
        (pl.col(acctno_col).cast(pl.Float64, strict=False) > 2500000000)
    )
    
    # Step 2: Create basic columns with proper type handling
    mast = mast.with_columns([
        pl.col(ficode_col).alias("branch"),
        pl.lit(0).cast(pl.Int32).alias("apcode"),
        
        # Apply CUSTCODE format - keep as string for now
        pl.when(pl.col(custcode_col).is_null() | (pl.col(custcode_col) == ""))
          .then(pl.lit("99"))
          .otherwise(pl.col(custcode_col).cast(pl.Utf8))
          .alias("custcode_str"),
        
        # Apply SECTOR format - keep as string
        pl.when(pl.col(sector_col).is_null() | (pl.col(sector_col) == ""))
          .then(pl.lit("9999"))
          .otherwise(pl.col(sector_col).cast(pl.Utf8))
          .alias("sector_str"),
        
        pl.lit(0).alias("oldbrh"),
        pl.lit("     ").alias("ficody")
    ])
    
    # Step 3: Handle SM_DATE separately
    if sm_date_col in mast.columns:
        mast = mast.with_columns([
            pl.when(pl.col(sm_date_col).is_null())
              .then(pl.lit(""))
              .otherwise(pl.col(sm_date_col).dt.strftime("%d%m%Y"))
              .alias("sm_datestr")
        ])
    
    # Step 4: Create CUSTFISS column
    mast = mast.with_columns([
        pl.col("custcode_str").replace_strict(
            CUSTCODE_FORMAT, 
            default="99"
        ).alias("custfiss")
    ])
    
    # Step 5: Create SECTFISS column
    mast = mast.with_columns([
        pl.col("sector_str").replace_strict(
            SECTOR_REVERSE_FORMAT, 
            default="9999"
        ).alias("sectfiss")
    ])
    
    # Step 6: Apply special sector override
    mast = mast.with_columns([
        pl.when(
            pl.col("custfiss").is_in(['77', '78', '95', '96'])
        ).then(pl.lit("9700")).otherwise(pl.col("sectfiss")).alias("sectfiss")
    ])
    
    # Step 7: Remove duplicates
    mast = mast.unique(subset=[acctno_col], keep="first")
    
    print(f"MAST processed: {mast.height} rows")

# =========================================================
# 7. PROCESS MAST2
# =========================================================
print("\nProcessing MAST2...")

mast2_agg = None
mast2c = None

if data.get('imast2') is not None:
    # Check columns
    imast2_cols = data['imast2'].columns
    
    acctno_col = 'acctnox' if 'acctnox' in imast2_cols else 'acctno'
    aano_col = 'aano' if 'aano' in imast2_cols else 'aano'
    apvdate_col = 'apvdate' if 'apvdate' in imast2_cols else 'apvdate'
    facno_col = 'facno' if 'facno' in imast2_cols else 'facno'
    
    # Filter valid AANO
    mast2_filtered = data['imast2'].filter(
        (pl.col(aano_col).cast(pl.Utf8).str.slice(0, 1) != "") &
        (pl.col(aano_col).cast(pl.Utf8).str.len_chars() == 13)
    ).sort(acctno_col)
    
    # Aggregate for ALLREFNO
    if not mast2_filtered.is_empty():
        mast2_agg = mast2_filtered.group_by(acctno_col).agg([
            pl.col(aano_col).cast(pl.Utf8).str.concat("|").alias("allrefno"),
            pl.col(apvdate_col).filter(pl.col(apvdate_col) > 0).min().alias("firstdisbdt")
        ])
    
    # MAST2C for CCPT
    if facno_col in imast2_cols:
        mast2c = data['imast2'].select([
            pl.col(acctno_col),
            pl.col(facno_col).cast(pl.Utf8).str.zfill(3).alias("facline"),
            pl.col("ccpt_ltst_review_dt")
        ]).unique(subset=[acctno_col, "facline"])
    
    print(f"MAST2 processed: {mast2_filtered.height} rows filtered")

# =========================================================
# 8. PROCESS CRED
# =========================================================
print("\nProcessing CRED...")

cred = None
if data.get('icred') is not None:
    # Check columns
    icred_cols = data['icred'].columns
    
    acctno_col = 'acctnox' if 'acctnox' in icred_cols else 'acctno'
    transref_col = 'transref' if 'transref' in icred_cols else 'transref'
    outstand_col = 'outstand' if 'outstand' in icred_cols else 'outstand'
    maturedX_col = 'maturedx' if 'maturedx' in icred_cols else 'maturedx'
    
    # Step 1: Filter data
    cred = data['icred'].filter(
        (pl.col(acctno_col).cast(pl.Float64, strict=False) > 2500000000) &
        (pl.col(acctno_col).cast(pl.Float64, strict=False) != 2501900811) &
        (pl.col(transref_col).cast(pl.Utf8).str.strip_chars() != "") &
        (pl.col(outstand_col).cast(pl.Float64, strict=False) >= 0)
    )
    
    # Step 2: Calculate MATUREDS
    cred = cred.with_columns([
        pl.when(
            (pl.col(maturedX_col).cast(pl.Utf8) == "000000") | 
            (pl.col(maturedX_col).cast(pl.Utf8).str.strip_chars() == "")
        ).then(pl.lit(99999)).otherwise(
            pl.col(maturedX_col).cast(pl.Utf8).str.strptime(pl.Date, "%y%m%d").cast(pl.Int64)
        ).alias("matureds")
    ])
    
    # Step 3: Calculate NODAYS
    cred = cred.with_columns([
        pl.when(
            (pl.col("matureds") > 0) & (pl.col("matureds") <= int(RDATE))
        ).then(
            pl.lit(int(RDATE)) - pl.col("matureds") + 1
        ).otherwise(pl.lit(0)).alias("nodays")
    ])
    
    # Step 4: Calculate ARREARS
    cred = cred.with_columns([
        pl.when(
            (pl.col("matureds") > 0) & 
            (pl.col("matureds") <= int(RDATE)) &
            ((pl.lit(int(RDATE)) - pl.col("matureds") + 1) > 0)
        ).then(
            ((pl.lit(int(RDATE)) - pl.col("matureds") + 1) / 30.00050).floor().cast(pl.Int32)
        ).otherwise(pl.lit(0)).alias("arrears")
    ])
    
    # Step 5: Calculate INSTALM and other fields
    cred = cred.with_columns([
        pl.when(
            pl.col("nodays") > 0
        ).then(pl.lit(1)).otherwise(pl.lit(0)).alias("instalm"),
        
        # TRANSREX
        pl.col(transref_col).cast(pl.Utf8).str.slice(0, 7).alias("transrex"),
        
        # Backup OUTSTAND
        pl.col(outstand_col).cast(pl.Float64, strict=False).alias("outstandX")
    ])
    
    # Step 6: Remove duplicates
    cred = cred.unique(subset=[acctno_col, transref_col])
    
    print(f"CRED processed: {cred.height} rows")

# =========================================================
# 9. PROCESS SUBA
# =========================================================
print("\nProcessing SUBA...")

suba = None
suba_main = None
suba9 = None

if data.get('isuba') is not None:
    # Check columns
    isuba_cols = data['isuba'].columns
    
    acctno_col = 'acctnox' if 'acctnox' in isuba_cols else 'acctno'
    liabcode_col = 'liabcode' if 'liabcode' in isuba_cols else 'liabcode'
    transref_col = 'transref' if 'transref' in isuba_cols else 'transref'
    tfdesc01_col = 'tfdesc01' if 'tfdesc01' in isuba_cols else 'tfdesc01'
    subacct_col = 'subacct' if 'subacct' in isuba_cols else 'subacct'
    
    # Step 1: Filter data
    suba = data['isuba'].filter(
        (pl.col(acctno_col).cast(pl.Float64, strict=False) > 2500000000) &
        (pl.col(acctno_col).cast(pl.Float64, strict=False) != 2501900811)
    )
    
    # Step 2: Add currency determination
    suba = suba.with_columns([
        pl.when(
            ~pl.col(liabcode_col).cast(pl.Utf8).is_in(["FFS", "FFU", "FCS", "FCU", "FFL", "FTI", "FTL"])
        ).then(pl.lit("MYR")).otherwise(pl.lit(None)).alias("forcurr"),
        
        # AANO extraction
        pl.col(tfdesc01_col).cast(pl.Utf8).str.slice(0, 13).alias("aano")
    ])
    
    # Step 3: Apply facility mappings
    suba = suba.with_columns([
        pl.col(liabcode_col).cast(pl.Utf8).replace_strict(LIAB_FORMAT, default=None).alias("facility"),
        pl.col(liabcode_col).cast(pl.Utf8).replace_strict(NSRSLIAB_FORMAT, default=None).alias("faccode"),
        pl.col(liabcode_col).cast(pl.Utf8).replace_strict(PRCTYPE_FORMAT, default="99").alias("typeprc"),
        pl.col(liabcode_col).cast(pl.Utf8).replace_strict(PRCTYPESFS_FORMAT, default="99").alias("typeprc_sfs"),
        pl.col(liabcode_col).cast(pl.Utf8).replace_strict(BTFCONCEPT_FORMAT, default=99).alias("fconcept")
    ])
    
    # Step 4: Split into SUBA9 and SUBA main
    suba9 = suba.filter(
        (pl.col(subacct_col).cast(pl.Utf8) == "OV") & 
        (pl.col(transref_col).cast(pl.Utf8).str.strip_chars() == "")
    )
    
    suba_main = suba.filter(
        pl.col(transref_col).cast(pl.Utf8).str.strip_chars() != ""
    )
    
    print(f"SUBA processed: {suba.height} rows (SUBA9: {suba9.height}, SUBA_MAIN: {suba_main.height})")

# =========================================================
# 10. PROCESS ACCT (Account Level)
# =========================================================
print("\nProcessing ACCT...")

acct = None
if mast is not None and suba9 is not None:
    # Determine acctno column
    mast_acctno = 'acctnox' if 'acctnox' in mast.columns else 'acctno'
    suba9_acctno = 'acctnox' if 'acctnox' in suba9.columns else 'acctno'
    
    # Merge SUBA9 with CCPT and MAST2C
    if mast2c is not None:
        mast2c_acctno = 'acctnox' if 'acctnox' in mast2c.columns else 'acctno'
        suba9 = suba9.join(mast2c, left_on=suba9_acctno, right_on=mast2c_acctno, how="left")
    
    acct = mast.join(suba9, left_on=mast_acctno, right_on=suba9_acctno, how="inner")
    
    if mast2_agg is not None:
        mast2_acctno = 'acctnox' if 'acctnox' in mast2_agg.columns else 'acctno'
        acct = acct.join(mast2_agg, left_on=mast_acctno, right_on=mast2_acctno, how="left")
    
    # Add calculated fields
    acct = acct.with_columns([
        pl.lit(20).alias("issueya"),
        pl.lit(0).alias("issueyy"),
        pl.lit(0).alias("issuemm"),
        pl.lit(0).alias("issuedd"),
        pl.lit(0).alias("lmtamt"),
        pl.lit(0).alias("ladtyy"),
        pl.lit(0).alias("ladtmm"),
        pl.lit(0).alias("ladtdd"),
        pl.lit(0).alias("fxrate"),
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
print(f"ACCT rows: {acct.height if acct is not None else 0}")
print("="*50)
