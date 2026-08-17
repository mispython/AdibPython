import pyreadstat
import polars as pl
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
    
    # Islamic Facilities
    'MUR': '34411', 'IST': '34412',
    'IJA': '34421', 'MUS': '34422',
    'MUD': '34440', 'QAR': '34470',
    'TAW': '34480', 'BAI': '34490',
    
    # Conventional Facilities
    'OD': '34710', 'TL': '34720',
    'RL': '34730', 'HL': '34740',
    'CL': '34750', 'PL': '34760',
    'AL': '34770',
    
    # Special Facilities
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
    '01': '01', '02': '02', '03': '03', '04': '04', '05': '05',
    '06': '06', '07': '07', '08': '08', '09': '09', '10': '10',
    '11': '11', '12': '12', '13': '13', '14': '14', '15': '15',
    '16': '16', '17': '17', '18': '18', '19': '19', '20': '20',
    '21': '21', '22': '22', '23': '23', '24': '24', '25': '25',
    '99': '99', '9999': '9999'
}

# Customer Code Format ($LOCUSTCD) - Now with string keys
CUSTCODE_FORMAT = {
    '1': '01', '2': '02', '3': '03', '4': '04', '5': '05',
    '6': '06', '7': '07', '8': '08', '9': '09', '10': '10',
    '11': '11', '12': '12', '13': '13', '14': '14', '15': '15',
    '16': '16', '17': '17', '18': '18', '19': '19', '20': '20',
    '77': '77', '78': '78', '95': '95', '96': '96',
    '99': '99'
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
                    if value is None or value == '':
                        line += '0' * width
                    else:
                        try:
                            line += f"{int(float(value)):0{width}d}"
                        except (ValueError, TypeError):
                            line += '0' * width
                
                elif format_type == 'S':  # String (left-justified)
                    if value is None:
                        value = ''
                    line += f"{str(value):<{width}}"
                
                elif format_type == 'D':  # Decimal with 2 decimal places
                    if value is None or value == '':
                        line += ' ' * width
                    else:
                        try:
                            line += f"{float(value):{width}.2f}"
                        except (ValueError, TypeError):
                            line += ' ' * width
                
                elif format_type == 'I':  # Integer
                    if value is None or value == '':
                        line += ' ' * width
                    else:
                        try:
                            line += f"{int(float(value)):{width}d}"
                        except (ValueError, TypeError):
                            line += ' ' * width
                
                else:  # Default to string
                    if value is None:
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
    'ibtrd': BASE_INPUT / f"ibtrd{REPTMON}{WK}.sas7bdat".lower(),
    'ibtdtl': BASE_INPUT / f"ibtdtl{REPTYEA2}{REPTMON}{REPTDAY}.sas7bdat".lower(),
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
    mast = data['imast'].filter(
        (pl.col("acctno") > 2500000000)
    ).with_columns([
        pl.col("ficode").alias("branch"),
        pl.lit(0).cast(pl.Int32).alias("apcode"),
        pl.when(pl.col("retailid") == 'C')
          .then(999)
          .otherwise(0)
          .cast(pl.Int32)
          .alias("apcode"),
        
        # Apply CUSTCODE format - convert to string first, then use replace
        pl.when(pl.col("custcode").is_null())
          .then(99)
          .otherwise(pl.col("custcode"))
          .cast(pl.Int32)
          .alias("custcode"),
        
        # Apply CUSTFISS mapping using replace_strict (Polars 1.x) or map_elements
        pl.col("custcode").cast(pl.Utf8).replace_strict(
            CUSTCODE_FORMAT, 
            default="99"
        ).alias("custfiss"),
        
        # Apply SECTOR format
        pl.when(pl.col("sector").is_null() | (pl.col("sector") == ""))
          .then("9999")
          .otherwise(pl.col("sector"))
          .alias("sector"),
        
        # Apply SECTFISS mapping
        pl.col("sector").replace_strict(
            SECTOR_REVERSE_FORMAT, 
            default="9999"
        ).alias("sectfiss"),
        
        # Apply industrial sector mapping if available
        pl.when(
            (pl.col("industrial_sector_cd").is_not_null()) & 
            (pl.col("industrial_sector_cd").str.len_chars() == 5)
        ).then(
            pl.col("industrial_sector_cd").replace_strict(
                INDSECT_FORMAT, 
                default=None
            )
        ).otherwise(pl.col("sectfiss")).alias("sectfiss"),
        
        # Apply special sector override for specific customer types
        pl.when(
            pl.col("custfiss").is_in(['77', '78', '95', '96'])
        ).then("9700").otherwise(pl.col("sectfiss")).alias("sectfiss"),
        
        pl.lit(0).alias("oldbrh"),
        pl.lit("     ").alias("ficody"),
        pl.when(pl.col("sm_date").is_null())
          .then("")
          .otherwise(pl.col("sm_date").dt.strftime("%d%m%Y"))
          .alias("sm_datestr")
    ]).unique(subset=["acctno"], keep="first")
    
    print(f"MAST processed: {mast.height} rows")

# =========================================================
# 7. PROCESS MAST2
# =========================================================
print("Processing MAST2...")

mast2_agg = None
mast2c = None

if data.get('imast2') is not None:
    # Filter valid AANO
    mast2_filtered = data['imast2'].filter(
        (pl.col("aano").str.slice(0, 1) != "") &
        (pl.col("aano").str.len_chars() == 13)
    ).sort("acctno")
    
    # Aggregate for ALLREFNO
    if not mast2_filtered.is_empty():
        mast2_agg = mast2_filtered.group_by("acctno").agg([
            pl.col("aano").str.concat("|").alias("allrefno"),
            pl.col("apvdate").filter(pl.col("apvdate") > 0).min().alias("firstdisbdt")
        ])
    
    # MAST2C for CCPT
    mast2c = data['imast2'].select([
        pl.col("acctno"),
        pl.col("facno").cast(pl.Utf8).str.zfill(3).alias("facline"),
        pl.col("ccpt_ltst_review_dt")
    ]).unique(subset=["acctno", "facline"])

# =========================================================
# 8. PROCESS CRED
# =========================================================
print("Processing CRED...")

cred = None
if data.get('icred') is not None:
    cred = data['icred'].filter(
        (pl.col("acctno") > 2500000000) &
        (pl.col("acctno") != 2501900811) &
        (pl.col("transref").str.strip_chars() != "") &
        (pl.col("outstand") >= 0)
    ).with_columns([
        # Calculate MATUREDS
        pl.when(
            (pl.col("maturedX").cast(pl.Utf8) == "000000") | 
            (pl.col("maturedX").cast(pl.Utf8).str.strip_chars() == "")
        ).then(99999).otherwise(
            pl.col("maturedX").cast(pl.Utf8).str.strptime(pl.Date, "%y%m%d").cast(pl.Int64)
        ).alias("matureds"),
        
        # Calculate NODAYS
        pl.when(
            (pl.col("matureds") > 0) & (pl.col("matureds") <= int(RDATE))
        ).then(
            int(RDATE) - pl.col("matureds") + 1
        ).otherwise(0).alias("nodays"),
        
        # Calculate ARREARS
        pl.when(
            (pl.col("matureds") > 0) & 
            (pl.col("matureds") <= int(RDATE)) &
            ((int(RDATE) - pl.col("matureds") + 1) > 0)
        ).then(
            ((int(RDATE) - pl.col("matureds") + 1) / 30.00050).floor().cast(pl.Int32)
        ).otherwise(0).alias("arrears"),
        
        # Calculate INSTALM
        pl.when(
            pl.col("nodays") > 0
        ).then(1).otherwise(0).alias("instalm"),
        
        # TRANSREX
        pl.col("transref").str.slice(0, 7).alias("transrex"),
        
        # Backup OUTSTAND
        pl.col("outstand").alias("outstandX")
    ]).unique(subset=["acctno", "transref"])
    
    print(f"CRED processed: {cred.height} rows")

# =========================================================
# 9. PROCESS BNM TRADE DATA
# =========================================================
print("Processing BNM Trade data...")

if data.get('ibtrd') is not None and cred is not None:
    # BTRAD - Balance data (assuming lowercase column names)
    print("IBTRD columns:", data['ibtrd'].columns[:10])
    
    # Check if columns exist, if not try uppercase
    if 'acctnox' in data['ibtrd'].columns:
        acct_col = 'acctnox'
    elif 'ACCTNOX' in data['ibtrd'].columns:
        acct_col = 'ACCTNOX'
    else:
        acct_col = data['ibtrd'].columns[0]  # fallback to first column
    
    if 'transref' in data['ibtrd'].columns:
        trans_col = 'transref'
    elif 'TRANSREF' in data['ibtrd'].columns:
        trans_col = 'TRANSREF'
    else:
        trans_col = data['ibtrd'].columns[1]  # fallback to second column
    
    # Rename columns for consistency
    btrad = data['ibtrd'].rename({acct_col: 'acctno', trans_col: 'transrex'})
    
    # Select available columns
    select_cols = ['acctno', 'transrex']
    if 'balance' in btrad.columns:
        select_cols.append('balance')
    if 'intrecv' in btrad.columns:
        select_cols.append('intrecv')
    if 'unearned' in btrad.columns:
        select_cols.append('unearned')
    if 'liabcode' in btrad.columns:
        select_cols.append('liabcode')
    if 'utrdf' in btrad.columns:
        select_cols.append('utrdf')
    
    btrad = btrad.select(select_cols).sort(["acctno", "transrex"])
    
    # Similar processing for btrax...
    # For now, just merge with cred
    cred = cred.join(btrad, on=["acctno", "transrex"], how="left")

# =========================================================
# 10. PROCESS SUBA
# =========================================================
print("Processing SUBA...")

suba = None
suba_main = None
suba9 = None

if data.get('isuba') is not None:
    suba = data['isuba'].filter(
        (pl.col("acctno") > 2500000000) &
        (pl.col("acctno") != 2501900811)
    ).with_columns([
        # Currency determination
        pl.when(
            ~pl.col("liabcode").is_in(["FFS", "FFU", "FCS", "FCU", "FFL", "FTI", "FTL"])
        ).then("MYR").otherwise(None).alias("forcurr"),
        
        # AANO extraction
        pl.col("tfdesc01").str.slice(0, 13).alias("aano"),
        
        # Apply facility mapping
        pl.col("liabcode").replace_strict(LIAB_FORMAT, default=None).alias("facility"),
        
        # Apply NSRSLIAB mapping
        pl.col("liabcode").replace_strict(NSRSLIAB_FORMAT, default=None).alias("faccode"),
        
        # Apply price type mappings
        pl.col("liabcode").replace_strict(PRCTYPE_FORMAT, default="99").alias("typeprc"),
        pl.col("liabcode").replace_strict(PRCTYPESFS_FORMAT, default="99").alias("typeprc_sfs"),
        
        # Apply BTF concept mapping
        pl.col("liabcode").replace_strict(BTFCONCEPT_FORMAT, default=99).alias("fconcept")
    ])
    
    # Split into SUBA9 and SUBA main
    suba9 = suba.filter(
        (pl.col("subacct") == "OV") & 
        (pl.col("transref").str.strip_chars() == "")
    )
    
    suba_main = suba.filter(
        pl.col("transref").str.strip_chars() != ""
    )
    
    print(f"SUBA processed: {suba.height} rows (SUBA9: {suba9.height}, SUBA_MAIN: {suba_main.height})")

# =========================================================
# 11. PROCESS ACCT (Account Level)
# =========================================================
print("Processing ACCT...")

acct = None
if mast is not None and suba9 is not None:
    # Merge SUBA9 with CCPT and MAST2C
    if mast2c is not None:
        suba9 = suba9.join(mast2c, on=["acctno", "facline"], how="left")
    
    acct = mast.join(suba9, on="acctno", how="inner")
    
    if mast2_agg is not None:
        acct = acct.join(mast2_agg, on="acctno", how="left")
    
    acct = acct.with_columns([
        pl.when(pl.col("currency").is_null() | (pl.col("currency") == ""))
          .then("MYR").otherwise(pl.col("currency")).alias("currency"),
        (pl.col("limtcurm") * 100).alias("apprlimt"),
        (pl.col("limtcurf") * 100).alias("apprlim2"),
        (pl.col("ori_aalimit") * 100).alias("aalimit"),
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

# =========================================================
# 12. WRITE OUTPUT FILES
# =========================================================
print("\nWriting output files...")

output_suffix = f"{REPTYEAR}{REPTMON}{REPTDAY}"

# Only write if we have data
if acct is not None:
    # Convert column names for output
    acct_output = acct.rename({
        'ficody': 'FICODY',
        'ficode': 'FICODE',
        'apcode': 'APCODE',
        'acctno': 'ACCTNO',
        'currency': 'CURRENCY',
        'apprlimt': 'APPRLIMT',
        'apprlim2': 'APPRLIM2',
        'issuedd': 'ISSUEDD',
        'issuemm': 'ISSUEMM',
        'issueya': 'ISSUEYA',
        'issueyy': 'ISSUEYY',
        'oldbrh': 'OLDBRH',
        'lmtamt': 'LMTAMT',
        'aalimit': 'AALIMIT',
        'allrefno': 'ALLREFNO',
        'legal_action_cd': 'LEGAL_ACTION_CD',
        'ladtdd': 'LADTDD',
        'ladtmm': 'LADTMM',
        'ladtyy': 'LADTYY',
        'fxrate': 'FXRATE',
        'climate_prin_taxonomy_class': 'CLIMATE_PRIN_TAXONOMY_CLASS'
    })
    
    acctcred_spec = [
        ("FICODY", 5, 'S'),
        ("FICODE", 4, 'Z'),
        ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'),
        ("CURRENCY", 3, 'S'),
        ("APPRLIMT", 24, 'Z'),
        ("APPRLIM2", 16, 'Z'),
        ("ISSUEDD", 2, 'Z'),
        ("ISSUEMM", 2, 'Z'),
        ("ISSUEYA", 2, 'Z'),
        ("ISSUEYY", 2, 'Z'),
        ("OLDBRH", 5, 'Z'),
        ("LMTAMT", 16, 'Z'),
        ("AALIMIT", 24, 'Z'),
        ("ALLREFNO", 200, 'S'),
        ("LEGAL_ACTION_CD", 2, 'Z'),
        ("LADTDD", 2, 'Z'),
        ("LADTMM", 2, 'Z'),
        ("LADTYY", 4, 'Z'),
        ("FXRATE", 8, 'Z'),
        ("CLIMATE_PRIN_TAXONOMY_CLASS", 5, 'S')
    ]
    
    write_fixed_width(acct_output, BASE_OUTPUT / f"ACCTCRED_{output_suffix}.txt", acctcred_spec)
    print(f"ACCTCRED written: {acct.height} records")

print("\n" + "="*50)
print("PROCESSING COMPLETE")
print("="*50)
print(f"Processing Date: {TDATE.strftime('%Y-%m-%d')}")
print(f"Output files written to: {BASE_OUTPUT}")
print("="*50)
