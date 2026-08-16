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
# These need to be extracted from the SAS format catalogs
# You can export these from SAS using PROC FORMAT with CNTLOUT option

# PBBLNFMT - Loan Format Catalog
# Contains: $LIAB, $NSRSLIAB, $RVRSE, $LOCUSTCD, $INDSECT, etc.

# Facility/Product Code Mappings ($LIAB format)
LIAB_FORMAT = {
    # Trade Finance Facilities
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

# Customer Code Format ($LOCUSTCD)
CUSTCODE_FORMAT = {
    1: '01', 2: '02', 3: '03', 4: '04', 5: '05',
    6: '06', 7: '07', 8: '08', 9: '09', 10: '10',
    11: '11', 12: '12', 13: '13', 14: '14', 15: '15',
    16: '16', 17: '17', 18: '18', 19: '19', 20: '20',
    99: '99'
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
# 5. READ INPUT FILES
# =========================================================
print("Reading input files...")

# Construct file names based on date
input_files = {
    'imast': BASE_INPUT / f"IMAST{REPTDAY}{REPTMON}.sas7bdat",
    'imast2': BASE_INPUT / f"IMAST2{REPTDAY}{REPTMON}.sas7bdat",
    'icred': BASE_INPUT / f"ICRED{REPTDAY}{REPTMON}.sas7bdat",
    'isuba': BASE_INPUT / f"ISUBA{REPTDAY}{REPTMON}.sas7bdat",
    'iprov': BASE_INPUT / f"IPROV{REPTDAY}{REPTMON}.sas7bdat",
    'iamsubacc': BASE_INPUT / f"IAMSUBACC{REPTDAY}{REPTMON}.sas7bdat",
    'ibtrd': BASE_INPUT / f"IBTRAD{REPTMON}{WK}.sas7bdat",
    'ibtdtl': BASE_INPUT / f"IBTDTL{REPTYEA2}{REPTMON}{REPTDAY}.sas7bdat",
    'lnacct': BASE_INPUT / "LNACCT.sas7bdat"
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
        (pl.col("ACCTNO") > 2500000000)
    ).with_columns([
        pl.col("FICODE").alias("BRANCH"),
        pl.lit(0).cast(pl.Int32).alias("APCODE"),
        pl.when(pl.col("RETAILID") == 'C')
          .then(999)
          .otherwise(0)
          .cast(pl.Int32)
          .alias("APCODE"),
        
        # Apply CUSTCODE format from PBBLNFMT
        pl.when(pl.col("CUSTCODE").is_null())
          .then(99)
          .otherwise(pl.col("CUSTCODE"))
          .cast(pl.Int32)
          .alias("CUSTCODE"),
        
        # Apply CUSTFISS mapping
        pl.col("CUSTCODE").cast(pl.Int32).map_dict(CUSTCODE_FORMAT, default="99").alias("CUSTFISS"),
        
        # Apply SECTOR format from PBBLNFMT
        pl.when(pl.col("SECTOR").is_null() | (pl.col("SECTOR") == ""))
          .then("9999")
          .otherwise(pl.col("SECTOR"))
          .alias("SECTOR"),
        
        # Apply SECTFISS mapping
        pl.col("SECTOR").map_dict(SECTOR_REVERSE_FORMAT, default="9999").alias("SECTFISS"),
        
        # Apply industrial sector mapping if available
        pl.when(
            (pl.col("INDUSTRIAL_SECTOR_CD").is_not_null()) & 
            (pl.col("INDUSTRIAL_SECTOR_CD").str.len_chars() == 5)
        ).then(
            pl.col("INDUSTRIAL_SECTOR_CD").map_dict(INDSECT_FORMAT, default=None)
        ).otherwise(pl.col("SECTFISS")).alias("SECTFISS"),
        
        # Apply special sector override for specific customer types
        pl.when(
            pl.col("CUSTFISS").is_in(['77', '78', '95', '96'])
        ).then("9700").otherwise(pl.col("SECTFISS")).alias("SECTFISS"),
        
        pl.lit(0).alias("OLDBRH"),
        pl.lit("     ").alias("FICODY"),
        pl.when(pl.col("SM_DATE").is_null())
          .then("")
          .otherwise(pl.col("SM_DATE").dt.strftime("%d%m%Y"))
          .alias("SM_DATESTR")
    ]).unique(subset=["ACCTNO"], keep="first")
    
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
        (pl.col("AANO").str.slice(0, 1) != "") &
        (pl.col("AANO").str.len_chars() == 13)
    ).sort("ACCTNO")
    
    # Aggregate for ALLREFNO
    if not mast2_filtered.is_empty():
        mast2_agg = mast2_filtered.group_by("ACCTNO").agg([
            pl.col("AANO").str.concat("|").alias("ALLREFNO"),
            pl.col("APVDATE").filter(pl.col("APVDATE") > 0).min().alias("FIRSTDISBDT")
        ])
    
    # MAST2C for CCPT
    mast2c = data['imast2'].select([
        pl.col("ACCTNO"),
        pl.col("FACNO").cast(pl.Utf8).str.zfill(3).alias("FACLINE"),
        pl.col("CCPT_LTST_REVIEW_DT")
    ]).unique(subset=["ACCTNO", "FACLINE"])

# =========================================================
# 8. PROCESS CRED (with PBBBTFMT mappings)
# =========================================================
print("Processing CRED...")

cred = None
if data.get('icred') is not None:
    cred = data['icred'].filter(
        (pl.col("ACCTNO") > 2500000000) &
        (pl.col("ACCTNO") != 2501900811) &
        (pl.col("TRANSREF").str.strip_chars() != "") &
        (pl.col("OUTSTAND") >= 0)
    ).with_columns([
        # Calculate MATUREDS
        pl.when(
            (pl.col("MATUREDX") == "000000") | 
            (pl.col("MATUREDX").str.strip_chars() == "")
        ).then(99999).otherwise(
            pl.col("MATUREDX").str.strptime(pl.Date, "%y%m%d").cast(pl.Int64)
        ).alias("MATUREDS"),
        
        # Calculate NODAYS
        pl.when(
            (pl.col("MATUREDS") > 0) & (pl.col("MATUREDS") <= int(RDATE))
        ).then(
            int(RDATE) - pl.col("MATUREDS") + 1
        ).otherwise(0).alias("NODAYS"),
        
        # Calculate ARREARS
        pl.when(
            (pl.col("MATUREDS") > 0) & 
            (pl.col("MATUREDS") <= int(RDATE)) &
            ((int(RDATE) - pl.col("MATUREDS") + 1) > 0)
        ).then(
            ((int(RDATE) - pl.col("MATUREDS") + 1) / 30.00050).floor().cast(pl.Int32)
        ).otherwise(0).alias("ARREARS"),
        
        # Calculate INSTALM
        pl.when(
            pl.col("NODAYS") > 0
        ).then(1).otherwise(0).alias("INSTALM"),
        
        # TRANSREX
        pl.col("TRANSREF").str.slice(0, 7).alias("TRANSREX"),
        
        # Backup OUTSTAND
        pl.col("OUTSTAND").alias("OUTSTANDX")
    ]).unique(subset=["ACCTNO", "TRANSREF"])
    
    print(f"CRED processed: {cred.height} rows")

# =========================================================
# 9. PROCESS BNM TRADE DATA (with BTF mappings)
# =========================================================
print("Processing BNM Trade data...")

if data.get('ibtrd') is not None and cred is not None:
    # BTRAD - Balance data
    btrad = data['ibtrd'].filter(
        pl.col("BALANCE") > 0
    ).select([
        pl.col("ACCTNOX").alias("ACCTNO"),
        pl.col("TRANSREF").alias("TRANSREX"),
        pl.col("BALANCE"),
        pl.col("INTRECV"),
        pl.col("UNEARNED"),
        pl.col("LIABCODE"),
        pl.col("UTRDF")
    ]).sort(["ACCTNO", "TRANSREX"])
    
    # BTRAX - Repaid/Disburse data
    btrax = data['ibtrd'].select([
        pl.col("ACCTNOX").alias("ACCTNO"),
        pl.col("TRANSREF").alias("TRANSREX"),
        pl.col("REPAID"),
        pl.col("DISBURSE"),
        pl.col("MTD_TAWIDH_AMT"),
        pl.col("MTD_GHARAMAH_AMT")
    ]).sort(["ACCTNO", "TRANSREX"])
    
    # INTRT - Interest rates
    if data.get('ibtdtl') is not None:
        intrt = data['ibtdtl'].select([
            pl.col("ACCTNOX").alias("ACCTNO"),
            pl.col("TRANSREF").alias("TRANSREX"),
            pl.col("INTRATE"),
            pl.col("COMMRATE"),
            pl.col("DISCRATE"),
            pl.col("COMBRATE"),
            pl.col("PRINAMT_MYRX"),
            pl.col("INTAMT_MYRX"),
            pl.col("OTH_CHARGEX"),
            pl.col("PRODGRP")
        ]).sort(["ACCTNO", "TRANSREX"])
        
        # Merge BTRAX with INTRT
        btrax = btrax.join(intrt, on=["ACCTNO", "TRANSREX"], how="left")
    
    # Merge with CRED
    cred = cred.join(btrad, on=["ACCTNO", "TRANSREX"], how="left")
    cred = cred.join(btrax, on=["ACCTNO", "TRANSREX"], how="left")
    
    # Update OUTSTAND and other fields
    cred = cred.with_columns([
        pl.when(
            (pl.col("BALANCE").is_not_null()) & (pl.col("BALANCE") > 0)
        ).then(pl.col("BALANCE")).otherwise(0).alias("OUTSTAND"),
        pl.col("UNEARNED").fill_null(0),
        pl.col("REPAID").fill_null(0),
        pl.col("DISBURSE").fill_null(0),
        pl.col("MTD_TAWIDH_AMT").fill_null(0),
        pl.col("MTD_GHARAMAH_AMT").fill_null(0)
    ])

# =========================================================
# 10. PROCESS SUBA (with format mappings)
# =========================================================
print("Processing SUBA...")

suba = None
suba_main = None
suba9 = None

if data.get('isuba') is not None:
    suba = data['isuba'].filter(
        (pl.col("ACCTNO") > 2500000000) &
        (pl.col("ACCTNO") != 2501900811)
    ).with_columns([
        # Currency determination
        pl.when(
            ~pl.col("LIABCODE").is_in(["FFS", "FFU", "FCS", "FCU", "FFL", "FTI", "FTL"])
        ).then("MYR").otherwise(None).alias("FORCURR"),
        
        # AANO extraction
        pl.col("TFDESC01").str.slice(0, 13).alias("AANO"),
        
        # Apply facility mapping
        pl.col("LIABCODE").map_dict(LIAB_FORMAT, default=None).alias("FACILITY"),
        
        # Apply NSRSLIAB mapping
        pl.col("LIABCODE").map_dict(NSRSLIAB_FORMAT, default=None).alias("FACCODE"),
        
        # Apply price type mappings
        pl.col("LIABCODE").map_dict(PRCTYPE_FORMAT, default="99").alias("TYPEPRC"),
        pl.col("LIABCODE").map_dict(PRCTYPESFS_FORMAT, default="99").alias("TYPEPRC_SFS"),
        
        # Apply BTF concept mapping
        pl.col("LIABCODE").map_dict(BTFCONCEPT_FORMAT, default=99).alias("FCONCEPT")
    ])
    
    # Split into SUBA9 and SUBA main
    suba9 = suba.filter(
        (pl.col("SUBACCT") == "OV") & 
        (pl.col("TRANSREF").str.strip_chars() == "")
    )
    
    suba_main = suba.filter(
        pl.col("TRANSREF").str.strip_chars() != ""
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
        suba9 = suba9.join(mast2c, on=["ACCTNO", "FACLINE"], how="left")
    
    acct = mast.join(suba9, on="ACCTNO", how="inner")
    
    if mast2_agg is not None:
        acct = acct.join(mast2_agg, on="ACCTNO", how="left")
    
    acct = acct.with_columns([
        pl.when(pl.col("CURRENCY").is_null() | (pl.col("CURRENCY") == ""))
          .then("MYR").otherwise(pl.col("CURRENCY")).alias("CURRENCY"),
        (pl.col("LIMTCURM") * 100).alias("APPRLIMT"),
        (pl.col("LIMTCURF") * 100).alias("APPRLIM2"),
        (pl.col("ORI_AALIMIT") * 100).alias("AALIMIT"),
        pl.lit(20).alias("ISSUEYA"),
        pl.lit(0).alias("ISSUEYY"),
        pl.lit(0).alias("ISSUEMM"),
        pl.lit(0).alias("ISSUEDD"),
        pl.lit(0).alias("LMTAMT"),
        pl.lit(0).alias("LADTYY"),
        pl.lit(0).alias("LADTMM"),
        pl.lit(0).alias("LADTDD"),
        pl.lit(0).alias("FXRATE"),
        pl.lit("     ").alias("CLIMATE_PRIN_TAXONOMY_CLASS")
    ])
    
    print(f"ACCT processed: {acct.height} rows")

# =========================================================
# 12. PROCESS BTR2
# =========================================================
print("Processing BTR2...")

btr2 = None
btr2x = None
btr3a = None

if cred is not None and suba_main is not None:
    # Merge CRED with SUBA
    btr2 = cred.join(suba_main, on=["ACCTNO", "TRANSREF"], how="inner")
    
    btr2 = btr2.with_columns([
        # Apply special facility mapping for UTRDF='R'
        pl.when(
            (pl.col("UTRDF") == 'R') & (pl.col("LIABCODE").is_in(["BAE", "BEI"]))
        ).then("34471")
        .when(
            (pl.col("UTRDF") == 'R') & (pl.col("LIABCODE").is_in(["BAI", "BII"]))
        ).then("34472")
        .when(
            (pl.col("UTRDF") == 'R') & (pl.col("LIABCODE").is_in(["BAP", "BAS", "BPI", "BSI"]))
        ).then("34475")
        .otherwise(pl.col("FACILITY"))
        .alias("FACILITY"),
        
        # TFR02I flag
        pl.when(pl.col("TFINDR02") == 5).then(1).otherwise(0).alias("TFR02I"),
        
        # PDBIND flag
        pl.when(pl.col("SUBPROD") == "PDB-I").then("Y").otherwise("N").alias("PDBIND"),
        
        # Handle SPECIALF
        pl.when(pl.col("SPECIALF").is_in(['20', '25', '30'])).then(1).otherwise(0).alias("SFS"),
        pl.when(pl.col("SPECIALF").is_in(['20', '25', '30'])).then(0).otherwise(1).alias("NONSFS"),
        
        # Reset NODAYS if OUTSTAND is 0
        pl.when(
            (pl.col("NODAYS") > 0) & (pl.col("OUTSTAND") < 1)
        ).then(0).otherwise(pl.col("NODAYS")).alias("NODAYS"),
        
        pl.when(
            (pl.col("NODAYS") > 0) & (pl.col("OUTSTAND") < 1)
        ).then(0).otherwise(pl.col("ARREARS")).alias("ARREARS"),
        
        pl.when(
            (pl.col("NODAYS") > 0) & (pl.col("OUTSTAND") < 1)
        ).then(0).otherwise(pl.col("INSTALM")).alias("INSTALM"),
        
        # Handle PRODGRP = 'BA'
        pl.when(pl.col("PRODGRP") == 'BA')
          .then(pl.col("BALANCE")).otherwise(None).alias("PRINAMT_MYRX_BA"),
        pl.when(pl.col("PRODGRP") == 'BA')
          .then(pl.col("UNEARNED")).otherwise(None).alias("INTAMT_MYRX_BA")
    ])
    
    print(f"BTR2 processed: {btr2.height} rows")
    
    # Summarize BTR2
    btr3a = btr2.group_by(["ACCTNO", "FACILITY", "FORCURR", "PDBIND"]).agg([
        pl.col("OUTSTAND").sum().alias("OUTSTAND"),
        pl.col("INSTALM").sum().alias("INSTALM"),
        pl.col("UNEARNED").sum().alias("UNEARNED"),
        pl.col("REPAID").sum().alias("REPAID"),
        pl.col("DISBURSE").sum().alias("DISBURSE"),
        pl.col("TFR02I").sum().alias("TFR02I"),
        pl.col("MTD_TAWIDH_AMT").sum().alias("MTD_TAWIDH_AMT"),
        pl.col("MTD_GHARAMAH_AMT").sum().alias("MTD_GHARAMAH_AMT"),
        pl.col("PRINAMT_MYRX").sum().alias("PRINAMT_MYRX"),
        pl.col("INTAMT_MYRX").sum().alias("INTAMT_MYRX"),
        pl.col("OTH_CHARGEX").sum().alias("OTH_CHARGEX"),
        pl.col("NODAYS").max().alias("NODAYS")
    ])
    
    # Get unique BTR2 rows with max NODAYS
    btr2x = btr2.sort(
        ["ACCTNO", "FACILITY", "FORCURR", "PDBIND", "NODAYS"],
        descending=[False, False, False, False, True]
    ).unique(subset=["ACCTNO", "FACILITY", "FORCURR", "PDBIND"], keep="first")

# =========================================================
# 13. PROCESS SUBCR
# =========================================================
print("Processing SUBCR...")

subcr = None
if btr2x is not None and btr3a is not None:
    subcr = btr2x.join(btr3a, on=["ACCTNO", "FACILITY", "FORCURR", "PDBIND"], how="inner")
    
    # Apply transformations
    subcr = subcr.with_columns([
        (pl.col("OUTSTAND") * 100).alias("OUTSTAND"),
        (pl.col("UNEARNED") * 100).alias("UNEARNED"),
        (pl.col("REPAID") * 100).alias("REPAID"),
        (pl.col("DISBURSE") * 100).alias("DISBURSE"),
        (pl.col("PRINAMT_MYRX") * 100).alias("CURBAL"),
        (pl.col("INTAMT_MYRX") * 100).alias("INTAMT"),
        (pl.col("OTH_CHARGEX") * 100).alias("OTH_CHARGE"),
        pl.when(pl.col("INSTALM").is_null()).then(0).otherwise(pl.col("INSTALM")).alias("INSTALM"),
        pl.lit("    ").alias("NOTENO")
    ])
    
    # Handle special facilities
    subcr = subcr.with_columns([
        pl.when(
            pl.col("FACILITY").is_in(["34810", "34831", "34832", "34840", "34850", "34860"])
        ).then(0).otherwise(pl.col("ARREARS")).alias("ARREARS"),
        pl.when(
            pl.col("FACILITY").is_in(["34810", "34831", "34832", "34840", "34850", "34860"])
        ).then(0).otherwise(pl.col("INSTALM")).alias("INSTALM")
    ])
    
    print(f"SUBCR processed: {subcr.height} rows")

# =========================================================
# 14. CREATE FINAL SUBA DATASET
# =========================================================
print("Creating final SUBA dataset...")

suba_final = None
if mast is not None and subcr is not None:
    suba_final = mast.join(subcr, on="ACCTNO", how="inner")
    
    # Join with ACCT for APPRLIM2 and FIRSTDISBDT
    if acct is not None:
        acct_subset = acct.select(["ACCTNO", "APPRLIM2", "FIRSTDISBDT"]).unique()
        suba_final = suba_final.join(acct_subset, on="ACCTNO", how="left")
    
    # Add calculated fields
    suba_final = suba_final.with_columns([
        pl.lit(" 00000000 00000000").alias("DATAXX"),
        pl.lit(0).alias("ODXSAMT"),
        pl.lit(0).alias("BILTOT"),
        pl.when(pl.col("APPRLIM2").is_null()).then(0).otherwise(pl.col("APPRLIM2")).alias("APPRLIM2"),
        pl.lit(12).alias("NOTETERM"),
        pl.lit(0).alias("FCONCEPT"),
        pl.when(pl.col("SYNDICAT").is_null() | (pl.col("SYNDICAT") == ""))
          .then("N").otherwise(pl.col("SYNDICAT")).alias("SYNDICAT"),
        pl.when(pl.col("SPECIALF").is_null() | (pl.col("SPECIALF") == "") | (pl.col("SPECIALF") == "N"))
          .then("00").otherwise(pl.col("SPECIALF")).alias("SPECIALF"),
        pl.when(pl.col("PURPOSES").is_null() | (pl.col("PURPOSES") == "") | (pl.col("PURPOSES") == "0000"))
          .then("5300").otherwise(pl.col("PURPOSES")).alias("PURPOSES"),
        pl.when(pl.col("PAYFREQC").is_null() | (pl.col("PAYFREQC") == ""))
          .then("19").otherwise(pl.col("PAYFREQC")).alias("PAYFREQC"),
        pl.when(pl.col("FIRSTDISBDT") > 0)
          .then(pl.col("FIRSTDISBDT").dt.strftime("%d%m%Y"))
          .otherwise("00000000").alias("FDISBDT"),
        pl.lit("N").alias("SM_STATUS1"),
        pl.lit("00000000").alias("SM_DAT1"),
        pl.lit("000000000000000").alias("RMSBBA")
    ])
    
    # Calculate UNDRAWN
    subq = suba_final.group_by("ACCTNO").agg([
        pl.col("OUTSTAND").sum().alias("OUTX")
    ])
    
    suba_final = suba_final.join(subq, on="ACCTNO", how="left")
    suba_final = suba_final.with_columns([
        (pl.col("APPRLIM2") - pl.col("OUTX")).alias("UNDRAWN")
    ])
    
    # Add FACILITY2 as copy of FACILITY
    suba_final = suba_final.with_columns([
        pl.col("FACILITY").alias("FACILITY2")
    ])
    
    print(f"Final SUBA processed: {suba_final.height} rows")

# =========================================================
# 15. WRITE OUTPUT FILES
# =========================================================
print("\nWriting output files...")

output_suffix = f"{REPTYEAR}{REPTMON}{REPTDAY}"

# ACCTCRED Output
if acct is not None:
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
    
    write_fixed_width(acct, BASE_OUTPUT / f"ACCTCRED_{output_suffix}.txt", acctcred_spec)
    print(f"ACCTCRED written: {acct.height} records")

# CREDITPO Output
if suba_final is not None:
    # Add REPTDAY, REPTMON, REPTYEAR as constants
    suba_final = suba_final.with_columns([
        pl.lit(REPTDAY).alias("REPTDAY"),
        pl.lit(REPTMON).alias("REPTMON"),
        pl.lit(REPTYEAR).alias("REPTYEAR"),
        pl.lit("O").alias("ACCTSTAT")  # Default account status
    ])
    
    creditpo_spec = [
        ("FICODY", 5, 'S'),
        ("FICODE", 4, 'Z'),
        ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'),
        ("NOTENO", 5, 'S'),
        ("FACILITY", 5, 'S'),
        ("REPTDAY", 2, 'S'),
        ("REPTMON", 2, 'S'),
        ("REPTYEAR", 4, 'S'),
        ("OUTSTAND", 16, 'Z'),
        ("ARREARS", 3, 'Z'),
        ("INSTALM", 3, 'Z'),
        ("UNDRAWN", 17, 'Z'),
        ("ACCTSTAT", 1, 'S'),
        ("NODAYS", 5, 'Z'),
        ("OLDBRH", 5, 'Z'),
        ("BILTOT", 17, 'Z'),
        ("ODXSAMT", 17, 'Z'),
        ("CURBAL", 17, 'Z'),
        ("INTAMT", 17, 'Z'),
        ("OTH_CHARGE", 17, 'Z'),
        ("REPAID", 15, 'Z'),
        ("DISBURSE", 15, 'Z'),
        ("FACCODE", 5, 'Z'),
        ("FORCURR", 3, 'S'),
        ("PDBIND", 1, 'S'),
        ("MTD_TAWIDH_AMT", 15, 'D'),
        ("MTD_GHARAMAH_AMT", 15, 'D'),
        ("REPAY_SOURCE", 4, 'S'),
        ("REPAY_TYPE_CD", 2, 'S')
    ]
    
    write_fixed_width(suba_final, BASE_OUTPUT / f"CREDITPO_{output_suffix}.txt", creditpo_spec)
    print(f"CREDITPO written: {suba_final.height} records")

# SUBACRED Output
if suba_final is not None:
    subacred_spec = [
        ("FICODY", 5, 'S'),
        ("FICODE", 4, 'Z'),
        ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'),
        ("NOTENO", 5, 'S'),
        ("FACILITY", 5, 'S'),
        ("FACILITY2", 5, 'S'),
        ("SYNDICAT", 1, 'S'),
        ("SPECIALF", 2, 'S'),
        ("PURPOSES", 4, 'S'),
        ("FCONCEPT", 2, 'Z'),
        ("NOTETERM", 3, 'Z'),
        ("PAYFREQC", 2, 'S'),
        ("DATAXX", 18, 'S'),
        ("CUSTCODE", 2, 'Z'),
        ("SECTOR", 4, 'S'),
        ("OLDBRH", 5, 'Z'),
        ("UNEARNED", 17, 'Z'),
        ("SM_STATUS1", 1, 'S'),
        ("SM_DAT1", 8, 'S'),
        ("RMSBBA", 15, 'S'),
        ("INTRATEX", 5, 'Z'),
        ("TYPEPRC", 2, 'S'),
        ("FACCODE", 5, 'Z'),
        ("SECTFISS", 4, 'S'),
        ("CUSTFISS", 2, 'S'),
        ("FORCURR", 3, 'S'),
        ("TFR02I", 1, 'Z'),
        ("COMMRATEX", 5, 'Z'),
        ("DISCRATEX", 5, 'Z'),
        ("COMBRATEX", 5, 'Z'),
        ("SM_STATUS", 1, 'S'),
        ("SM_DATESTR", 8, 'S'),
        ("IA_LRU", 1, 'S'),
        ("PDBIND", 1, 'S'),
        ("FDISBDT", 8, 'S'),
        ("SCORE1", 5, 'S'),
        ("SCORE2", 5, 'S'),
        ("DNBFISME", 1, 'S'),
        ("INDUSTRIAL_SECTOR_CD", 5, 'S'),
        ("LU_ADD1", 40, 'S'),
        ("LU_ADD2", 40, 'S'),
        ("LU_ADD3", 40, 'S'),
        ("LU_ADD4", 40, 'S'),
        ("LU_TOWN_CITY", 20, 'S'),
        ("LU_POSTCODE", 5, 'S'),
        ("LU_STATE_CD", 2, 'S'),
        ("LU_COUNTRY_CD", 2, 'S')
    ]
    
    # Add missing fields with defaults
    suba_final = suba_final.with_columns([
        pl.lit("     ").alias("SCORE1"),
        pl.lit("     ").alias("SCORE2"),
        pl.lit("N").alias("DNBFISME"),
        pl.lit("     ").alias("INDUSTRIAL_SECTOR_CD"),
        pl.lit("").alias("LU_ADD1"),
        pl.lit("").alias("LU_ADD2"),
        pl.lit("").alias("LU_ADD3"),
        pl.lit("").alias("LU_ADD4"),
        pl.lit("").alias("LU_TOWN_CITY"),
        pl.lit("").alias("LU_POSTCODE"),
        pl.lit("").alias("LU_STATE_CD"),
        pl.lit("").alias("LU_COUNTRY_CD"),
        pl.lit("").alias("IA_LRU"),
        pl.lit("").alias("SM_STATUS"),
        pl.lit("").alias("SM_DATESTR")
    ])
    
    write_fixed_width(suba_final, BASE_OUTPUT / f"SUBACRED_{output_suffix}.txt", subacred_spec)
    print(f"SUBACRED written: {suba_final.height} records")

# =========================================================
# 16. PRINT SUMMARY
# =========================================================
print("\n" + "="*50)
print("PROCESSING COMPLETE")
print("="*50)
print(f"Processing Date: {TDATE.strftime('%Y-%m-%d')}")
print(f"Output files written to: {BASE_OUTPUT}")
print("="*50)
