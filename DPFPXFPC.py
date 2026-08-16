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
# Maps LIABCODE to standardized facility codes
LIAB_FORMAT = {
    # Trade Finance Facilities
    'BAE': '34471',  # Bills Receivable - Export
    'BEI': '34471',  # Bills Receivable - Import
    'BAI': '34472',  # Bills Payable - Import
    'BII': '34472',  # Bills Payable - Import
    'BAP': '34475',  # Bills Payable
    'BAS': '34475',  # Bills Payable
    'BPI': '34475',  # Bills Payable
    'BSI': '34475',  # Bills Payable
    
    # Islamic Facilities
    'MUR': '34411',  # Murabahah
    'IST': '34412',  # Istisna
    'IJA': '34421',  # Ijarah
    'MUS': '34422',  # Musharakah
    'MUD': '34440',  # Mudharabah
    'QAR': '34470',  # Qard
    'TAW': '34480',  # Tawarruq
    'BAI': '34490',  # Bai Bithaman Ajil
    
    # Conventional Facilities
    'OD': '34710',   # Overdraft
    'TL': '34720',   # Term Loan
    'RL': '34730',   # Revolving Loan
    'HL': '34740',   # Housing Loan
    'CL': '34750',   # Commercial Loan
    'PL': '34760',   # Personal Loan
    'AL': '34770',   # Auto Loan
    
    # Special Facilities
    'POS': '34810',  # POS Financing
    'DAU': '34831',  # DAU Financing
    'DDU': '34832',  # DDU Financing
    'FFS': '34840',  # Fund for Small
    'FFU': '34850',  # Fund for Micro
    'FFL': '34860',  # Fund for Large
    
    # Add more mappings based on your actual SAS format catalog
}

# NSRSLIAB Format (NPL/Rescheduled/Recovered)
NSRSLIAB_FORMAT = {
    '34411': '34411',
    '34412': '34412',
    '34421': '34421',
    '34422': '34422',
    '34440': '34440',
    '34470': '34470',
    '34480': '34480',
    '34490': '34490',
    # Add NPL-specific codes
    'NPL1': '34471',
    'NPL2': '34472',
    # Add more mappings
}

# Sector Reverse Format ($RVRSE)
SECTOR_REVERSE_FORMAT = {
    '01': 'AGRICULTURE',
    '02': 'MINING',
    '03': 'MANUFACTURING',
    '04': 'CONSTRUCTION',
    '05': 'UTILITIES',
    '06': 'WHOLESALE',
    '07': 'RETAIL',
    '08': 'TRANSPORT',
    '09': 'FINANCE',
    '10': 'SERVICES',
    '11': 'GOVERNMENT',
    '12': 'HOUSEHOLD',
    '13': 'OTHER',
    '14': 'EDUCATION',
    '15': 'HEALTH',
    '16': 'ICT',
    '17': 'PROFESSIONAL',
    '18': 'ADMINISTRATIVE',
    '19': 'ARTS',
    '20': 'ACCOMMODATION',
    '21': 'FOOD',
    '22': 'REAL_ESTATE',
    '23': 'INSURANCE',
    # Add actual sector codes
}

# Customer Code Format ($LOCUSTCD)
CUSTCODE_FORMAT = {
    1: '01',  # Individual
    2: '02',  # Sole Proprietorship
    3: '03',  # Partnership
    4: '04',  # Private Limited
    5: '05',  # Public Limited
    6: '06',  # Government
    7: '07',  # Statutory Body
    8: '08',  # Cooperative
    9: '09',  # Association
    10: '10',  # Trust
    11: '11',  # Foreign Company
    12: '12',  # Foreign Individual
    13: '13',  # Foreign Government
    14: '14',  # International Organization
    15: '15',  # Financial Institution
    16: '16',  # Insurance Company
    17: '17',  # Fund Manager
    18: '18',  # Stock Broker
    19: '19',  # Unit Trust
    20: '20',  # Other
    99: '99',  # Unknown
    # Add actual customer type codes
}

# Industrial Sector Format ($INDSECT)
INDSECT_FORMAT = {
    '01111': 'AGRICULTURE',
    '01112': 'LIVESTOCK',
    '01113': 'FORESTRY',
    '01114': 'FISHING',
    '02111': 'COAL',
    '02112': 'OIL_GAS',
    '02113': 'METAL_ORE',
    '03111': 'FOOD_PROCESSING',
    '03112': 'TEXTILE',
    '03113': 'WOOD_PRODUCTS',
    '03114': 'CHEMICALS',
    '03115': 'ELECTRONICS',
    '04111': 'CONSTRUCTION',
    '05111': 'ELECTRICITY',
    '05112': 'WATER',
    '05113': 'GAS',
    # Add 5-digit industrial sector codes
}

# Price Type Format for SFS ($PRCTYPESFS)
PRCTYPESFS_FORMAT = {
    'BAE': '71',  # Bills - Export
    'BEI': '71',
    'BAI': '72',  # Bills - Import
    'BII': '72',
    'BAP': '75',  # Bills - Other
    'BAS': '75',
    'BPI': '75',
    'BSI': '75',
    # Add SFS-specific price types
}

# Regular Price Type Format ($PRCTYPE)
PRCTYPE_FORMAT = {
    'MUR': '11',
    'IST': '12',
    'IJA': '21',
    'MUS': '22',
    'MUD': '40',
    'QAR': '70',
    'TAW': '80',
    'BAI': '90',
    'OD': '71',
    'TL': '72',
    'RL': '73',
    'HL': '74',
    'CL': '75',
    'PL': '76',
    'AL': '77',
    # Add price type mappings
}

# BTF Concept Format ($BTFCEPT)
BTFCONCEPT_FORMAT = {
    '34411': 11,  # Murabahah
    '34412': 12,  # Istisna
    '34421': 21,  # Ijarah
    '34422': 22,  # Musharakah
    '34440': 40,  # Mudharabah
    '34470': 70,  # Qard
    '34480': 80,  # Tawarruq
    '34490': 90,  # BBA
    '34710': 10,  # Overdraft
    '34720': 20,  # Term Loan
    '34730': 30,  # Revolving
    '34740': 40,  # Housing
    '34750': 50,  # Commercial
    '34760': 60,  # Personal
    '34770': 70,  # Auto
    # Add BTF concept mappings
}

# PBBBTFMT - BTF Format Catalog
# Contains: Special format mappings for BTF reporting

# Facility to BTF Facility Code mapping
BTF_FACILITY_FORMAT = {
    '34411': '34411',
    '34412': '34412',
    '34421': '34421',
    '34422': '34422',
    '34440': '34440',
    '34470': '34470',
    '34471': '34471',
    '34472': '34472',
    '34475': '34475',
    '34480': '34480',
    '34490': '34490',
    '34810': '34810',
    '34831': '34831',
    '34832': '34832',
    '34840': '34840',
    '34850': '34850',
    '34860': '34860',
    # Add BTF-specific facility mappings
}

# Classification mapping for provisions
CLASSIFY_FORMAT = {
    'D': 'D',  # Doubtful
    'B': 'B',  # Bad
    'P': 'P',  # Performing
    'F': 'D',  # Fully provided
    # Add classification mappings
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

def apply_format_mapping(series, format_dict, default=None):
    """Apply format mapping to a Polars series"""
    if default is None:
        return series.map_dict(format_dict)
    else:
        return series.map_dict(format_dict, default=default)

def write_fixed_width(df, filepath, columns_spec):
    """Write DataFrame as fixed-width text file"""
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
                        except:
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
                        except:
                            line += ' ' * width
                
                elif format_type == 'I':  # Integer
                    if value is None or value == '':
                        line += ' ' * width
                    else:
                        try:
                            line += f"{int(float(value)):{width}d}"
                        except:
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

if data['imast'] is not None:
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
        pl.col("CUSTCODE").map_dict(CUSTCODE_FORMAT, default="99").alias("CUSTFISS"),
        
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
            (pl.col("INDUSTRIAL_SECTOR_CD").str.len_chars() == 5) &
            (pl.col("INDUSTRIAL_SECTOR_CD").cast(pl.Int32, strict=False) > 0)
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

# =========================================================
# 7. PROCESS MAST2
# =========================================================
print("Processing MAST2...")

if data['imast2'] is not None:
    # Filter valid AANO
    mast2_filtered = data['imast2'].filter(
        (pl.col("AANO").str.slice(0, 1) != "") &
        (pl.col("AANO").str.len_chars() == 13)
    ).sort("ACCTNO")
    
    # Aggregate for ALLREFNO
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

if data['icred'] is not None:
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

# =========================================================
# 9. PROCESS BNM TRADE DATA (with BTF mappings)
# =========================================================
print("Processing BNM Trade data...")

if data['ibtrd'] is not None:
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
    
    # Apply BTF facility mapping
    if 'LIABCODE' in btrad.columns:
        btrad = btrad.with_columns([
            pl.col("LIABCODE").map_dict(BTF_FACILITY_FORMAT, default=None).alias("BTF_FACILITY")
        ])
    
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
    if data['ibtdtl'] is not None:
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
    if 'cred' in locals():
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

if data['isuba'] is not None:
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
        
        # Apply NSRSLIAB mapping for special cases
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
    ).select([
        "ACCTNO", "LIMTCURM", "LIMTCURF", "ORI_AALIMIT", "CREATDS", 
        "CURRENCY", "FACLINE", "LIABCODE", "SPECIALF", "SUBPROD",
        "FACILITY", "FACCODE", "TYPEPRC", "FCONCEPT"
    ])
    
    suba_main = suba.filter(
        pl.col("TRANSREF").str.strip_chars() != ""
    )

# =========================================================
# 11. PROCESS ACCT (Account Level - with all mappings)
# =========================================================
print("Processing ACCT...")

if 'mast' in locals() and 'suba9' in locals():
    # Merge SUBA9 with CCPT and MAST2C
    if 'mast2c' in locals():
        suba9 = suba9.join(mast2c, on=["ACCTNO", "FACLINE"], how="left")
    
    acct = mast.join(suba9, on="ACCTNO", how="inner")
    
    if 'mast2_agg' in locals():
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

# =========================================================
# 12. PROCESS BTR2 (with all format mappings)
# =========================================================
print("Processing BTR2...")

if 'cred' in locals() and 'suba_main' in locals():
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

# =========================================================
# 13. WRITE OUTPUT FILES (with complete column specs)
# =========================================================
print("\nWriting output files...")

output_suffix = f"{REPTYEAR}{REPTMON}{REPTDAY}"

# ACCTCRED Output (from PROVD KEEP list)
if 'acct' in locals():
    acctcred_spec = [
        ("FICODY", 5, 'S'),
        ("FICODE", 4, 'Z'),
        ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'),
        ("FACILITY", 5, 'S'),
        ("REPTDAY", 2, 'S'),
        ("REPTMON", 2, 'S'),
        ("REPTYEAR", 4, 'S'),
        ("CLASSIFY", 1, 'S'),
        ("ARREARS", 3, 'Z'),
        ("CURBAL", 17, 'Z'),
        ("INTAMT", 17, 'Z'),
        ("FEEAMT", 17, 'Z'),
        ("REALISVL", 17, 'Z'),
        ("IISOPBAL", 17, 'Z'),
        ("TOTIIS", 17, 'Z'),
        ("TOTIISR", 17, 'Z'),
        ("TOTWOF", 17, 'Z'),
        ("IISDANAH", 17, 'Z'),
        ("IISTRANS", 17, 'Z'),
        ("SPOPBAL", 17, 'Z'),
        ("SPCHARGE", 17, 'Z'),
        ("SPWBAMT", 17, 'Z'),
        ("SPWOAMT", 17, 'Z'),
        ("SPDANAH", 17, 'Z'),
        ("SPTRANS", 17, 'Z'),
        ("GP3IND", 1, 'S'),
        ("OLDBRH", 5, 'Z'),
        ("FACCODE", 5, 'Z')
    ]
    
    # Add required date fields
    acct = acct.with_columns([
        pl.lit(REPTDAY).alias("REPTDAY"),
        pl.lit(REPTMON).alias("REPTMON"),
        pl.lit(REPTYEAR).alias("REPTYEAR"),
        pl.lit(" ").alias("CLASSIFY"),
        pl.lit(0).alias("FEEAMT"),
        pl.lit(0).alias("REALISVL"),
        pl.lit(0).alias("IISOPBAL"),
        pl.lit(0).alias("TOTIIS"),
        pl.lit(0).alias("TOTIISR"),
        pl.lit(0).alias("TOTWOF"),
        pl.lit(0).alias("IISDANAH"),
        pl.lit(0).alias("IISTRANS"),
        pl.lit(0).alias("SPOPBAL"),
        pl.lit(0).alias("SPCHARGE"),
        pl.lit(0).alias("SPWBAMT"),
        pl.lit(0).alias("SPWOAMT"),
        pl.lit(0).alias("SPDANAH"),
        pl.lit(0).alias("SPTRANS"),
        pl.lit(" ").alias("GP3IND")
    ])
    
    write_fixed_width(acct, BASE_OUTPUT / f"ACCTCRED_{output_suffix}.txt", acctcred_spec)
    print(f"ACCTCRED written: {acct.height} records")

# CREDITPO Output (from CREDD KEEP list)
if 'suba_final' in locals():
    creditpo_spec = [
        ("FICODY", 5, 'S'),
        ("FICODE", 4, 'Z'),
        ("APCODE", 3, 'Z'),
        ("ACCTNO", 10, 'Z'),
        ("FACILITY", 5, 'S'),
        ("NOTENO", 5, 'S'),
        ("REPTMON", 2, 'S'),
        ("REPTYEAR", 4, 'S'),
        ("ARREARS", 3, 'Z'),
        ("OUTSTAND", 16, 'Z'),
        ("INSTALM", 3, 'Z'),
        ("UNDRAWN", 17, 'Z'),
        ("ACCTSTAT", 1, 'S'),
        ("NODAYS", 5, 'Z'),
        ("OLDBRH", 5, 'Z'),
        ("BILTOT", 17, 'Z'),
        ("ODXSAMT", 17, 'Z'),
        ("FACCODE", 5, 'Z')
    ]
    
    write_fixed_width(suba_final, BASE_OUTPUT / f"CREDITPO_{output_suffix}.txt", creditpo_spec)
    print(f"CREDITPO written: {suba_final.height} records")

# SUBACRED Output (full detail)
if 'suba_final' in locals():
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
    
    write_fixed_width(suba_final, BASE_OUTPUT / f"SUBACRED_{output_suffix}.txt", subacred_spec)
    print(f"SUBACRED written: {suba_final.height} records")

print("\n" + "="*50)
print("PROCESSING COMPLETE")
print("="*50)
print(f"Processing Date: {TDATE.strftime('%Y-%m-%d')}")
print(f"Output files written to: {BASE_OUTPUT}")
print("="*50)
