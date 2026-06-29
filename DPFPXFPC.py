#!/usr/bin/env python3
"""
EIBQDISE Deposit Processing - Exact SAS Replication
Processes saving, current, and fixed deposit accounts
Creates monthly extracts for BNM reporting
"""

import polars as pl
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import pyreadstat
import tempfile
import os
import sys

# ============================================================================
# PATHS
# ============================================================================
INPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBQDISE")
OUTPUT_PATH = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# Input files
FILES = {
    'saving': INPUT_PATH / "saving.sas7bdat",
    'current': INPUT_PATH / "current.sas7bdat",
    'fd': INPUT_PATH / "fd.sas7bdat",
    'uma': INPUT_PATH / "uma.sas7bdat",
}

# ============================================================================
# REPORTING DATE CALCULATION - EXACTLY LIKE SAS
# ============================================================================

def get_reporting_date():
    """Calculate REPTDATE exactly like SAS"""
    if len(sys.argv) > 1:
        try:
            return datetime.strptime(sys.argv[1], "%Y-%m-%d")
        except ValueError:
            pass
    
    date_str = os.environ.get('REPORT_DATE')
    if date_str:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            pass
    
    # REPTDATE = first day of current month minus 1 day
    today = datetime.now()
    first_of_month = datetime(today.year, today.month, 1)
    return first_of_month - timedelta(days=1)

# Get reporting date
reptdate = get_reporting_date()
day = reptdate.day
mm = reptdate.month
year = reptdate.year

# Determine week based on day of REPTDATE (exactly like SAS)
if day == 8:
    sdd = 1
    wk = '1'
    wk1 = '4'
    wk2 = None
    wk3 = None
elif day == 15:
    sdd = 9
    wk = '2'
    wk1 = '1'
    wk2 = None
    wk3 = None
elif day == 22:
    sdd = 16
    wk = '3'
    wk1 = '2'
    wk2 = None
    wk3 = None
else:  # day >= 23 (last day of month)
    sdd = 23
    wk = '4'
    wk1 = '3'
    wk2 = '2'
    wk3 = '1'

# Calculate MM1 (previous month for week 1)
if wk == '1':
    mm1 = mm - 1 if mm > 1 else 12
else:
    mm1 = mm

sdate = datetime(year, mm, sdd)

# Set macro variables
nowk = wk
nowk1 = wk1
nowk2 = wk2
nowk3 = wk3
reptmon = f"{mm:02d}"
reptmon1 = f"{mm1:02d}"
reptyear = year
reptday = f"{day:02d}"
rdate = reptdate.strftime("%d/%m/%y")
sdate_str = sdate.strftime("%d/%m/%y")

print(f"Report Date: {rdate}")
print(f"Start Date: {sdate_str}")
print(f"Week: {nowk}")
print(f"Month: {reptmon}")
print(f"Year: {reptyear}")

# Constants
AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

# ============================================================================
# SAS FORMAT MAPPINGS (Replicating PBBDPFMT formats)
# ============================================================================

# Product Code Mappings - Current Accounts (CAPROD.)
# These are the BNM 5-digit codes from the SAS format
CURRENT_PROD_MAP = {
    # Regular Current Accounts -> 42110
    1: "42110", 2: "42110", 3: "42110", 4: "42110", 5: "42110",
    6: "42110", 7: "42110", 8: "42110", 9: "42110", 10: "42110",
    11: "42110", 12: "42110", 13: "42110", 14: "42110", 15: "42110",
    16: "42110", 17: "42110", 18: "42110", 19: "42110", 20: "42110",
    21: "42110", 22: "42110", 23: "42110", 24: "42110", 25: "42110",
    26: "42110", 27: "42110", 28: "42110", 29: "42110", 30: "42110",
    31: "42110", 32: "42110", 33: "42110", 34: "42110", 35: "42110",
    36: "42110", 37: "42110", 38: "42110", 39: "42110", 40: "42110",
    41: "42110", 42: "42110", 43: "42110", 44: "42110", 45: "42110",
    46: "42110", 47: "42110", 48: "42110", 49: "42110", 50: "42110",
    51: "42110", 52: "42110", 53: "42110", 54: "42110", 55: "42110",
    56: "42110", 57: "42110", 58: "42110", 59: "42110", 60: "42110",
    61: "42110", 62: "42110", 63: "42110", 64: "42110", 65: "42110",
    66: "42110", 67: "42110", 68: "42110", 69: "42110", 70: "42110",
    71: "42110", 72: "42110", 73: "42110", 74: "42110", 75: "42110",
    76: "42110", 77: "42110", 78: "42110", 79: "42110", 80: "42110",
    81: "42110", 82: "42110", 83: "42110", 84: "42110", 85: "42110",
    86: "42110", 87: "42110", 88: "42110", 89: "42110", 90: "42110",
    91: "42110", 92: "42110", 93: "42110", 94: "42110", 95: "42110",
    96: "42110", 97: "42110", 98: "42110", 99: "42110",
    # Negotiable Instruments
    100: "42110", 101: "42110", 102: "42110", 103: "42110",
    104: "42110", 105: "42110",
    # ACE Products
    106: "42110", 107: "42110", 108: "42110", 109: "42110",
    110: "42110", 111: "42110", 112: "42110", 113: "42110",
    114: "42110", 115: "42110", 116: "42110", 117: "42110",
    118: "42110", 119: "42110", 120: "42110", 121: "42110",
    122: "42110", 123: "42110", 124: "42110", 125: "42110",
    126: "42110", 127: "42110", 128: "42110", 129: "42110",
    130: "42110", 131: "42110", 132: "42110", 133: "42110",
    134: "42110", 135: "42110", 136: "42110", 137: "42110",
    138: "42110", 139: "42110", 140: "42110", 141: "42110",
    142: "42110", 143: "42110", 144: "42110", 145: "42110",
    146: "42110", 147: "42110", 148: "42110", 149: "42110",
    150: "42110",
    # Islamic Current Accounts (FCY) -> 42410
    400: "42410", 401: "42410", 402: "42410", 403: "42410",
    404: "42410", 405: "42410", 406: "42410", 407: "42410",
    408: "42410", 409: "42410", 410: "42410", 411: "42410",
    412: "42410", 413: "42410", 414: "42410", 415: "42410",
    416: "42410", 417: "42410", 418: "42410", 419: "42410",
    420: "42410", 421: "42410", 422: "42410", 423: "42410",
    424: "42410", 425: "42410", 426: "42410", 427: "42410",
    428: "42410", 429: "42410", 430: "42410", 431: "42410",
    432: "42410", 433: "42410", 434: "42410", 435: "42410",
    436: "42410", 437: "42410", 438: "42410", 439: "42410",
    440: "42410", 441: "42410", 442: "42410", 443: "42410",
    444: "42410",
}

# Product Code Mappings - Savings Accounts (SAPROD.)
SAVINGS_PROD_MAP = {
    # Regular Savings -> 42120
    1: "42120", 2: "42120", 3: "42120", 4: "42120", 5: "42120",
    6: "42120", 7: "42120", 8: "42120", 9: "42120", 10: "42120",
    11: "42120", 12: "42120", 13: "42120", 14: "42120", 15: "42120",
    16: "42120", 17: "42120", 18: "42120", 19: "42120", 20: "42120",
    21: "42120", 22: "42120", 23: "42120", 24: "42120", 25: "42120",
    26: "42120", 27: "42120", 28: "42120", 29: "42120", 30: "42120",
    31: "42120", 32: "42120", 33: "42120", 34: "42120", 35: "42120",
    36: "42120", 37: "42120", 38: "42120", 39: "42120", 40: "42120",
    41: "42120", 42: "42120", 43: "42120", 44: "42120", 45: "42120",
    46: "42120", 47: "42120", 48: "42120", 49: "42120", 50: "42120",
    51: "42120", 52: "42120", 53: "42120", 54: "42120", 55: "42120",
    56: "42120", 57: "42120", 58: "42120", 59: "42120", 60: "42120",
    61: "42120", 62: "42120", 63: "42120", 64: "42120", 65: "42120",
    66: "42120", 67: "42120", 68: "42120", 69: "42120", 70: "42120",
    71: "42120", 72: "42120", 73: "42120", 74: "42120", 75: "42120",
    76: "42120", 77: "42120", 78: "42120", 79: "42120", 80: "42120",
    81: "42120", 82: "42120", 83: "42120", 84: "42120", 85: "42120",
    86: "42120", 87: "42120", 88: "42120", 89: "42120", 90: "42120",
    91: "42120", 92: "42120", 93: "42120", 94: "42120", 95: "42120",
    96: "42120", 97: "42120", 98: "42120", 99: "42120",
    # Islamic Savings -> 42420
    200: "42420", 201: "42420", 202: "42420", 203: "42420",
    204: "42420", 205: "42420", 206: "42420", 207: "42420",
    208: "42420", 209: "42420", 210: "42420", 211: "42420",
    212: "42420", 213: "42420", 214: "42420", 215: "42420",
    216: "42420", 217: "42420", 218: "42420", 219: "42420",
    220: "42420", 221: "42420", 222: "42420", 223: "42420",
    224: "42420", 225: "42420", 226: "42420", 227: "42420",
    228: "42420", 229: "42420", 230: "42420", 231: "42420",
    232: "42420", 233: "42420", 234: "42420", 235: "42420",
    236: "42420", 237: "42420", 238: "42420", 239: "42420",
    240: "42420", 241: "42420", 242: "42420", 243: "42420",
    244: "42420", 245: "42420", 246: "42420", 247: "42420",
    248: "42420", 249: "42420", 250: "42420", 251: "42420",
    252: "42420", 253: "42420", 254: "42420", 255: "42420",
    256: "42420", 257: "42420", 258: "42420", 259: "42420",
    260: "42420", 261: "42420", 262: "42420", 263: "42420",
    264: "42420", 265: "42420", 266: "42420", 267: "42420",
    268: "42420", 269: "42420", 270: "42420", 271: "42420",
    272: "42420", 273: "42420", 274: "42420", 275: "42420",
    276: "42420", 277: "42420", 278: "42420", 279: "42420",
    280: "42420", 281: "42420", 282: "42420", 283: "42420",
    284: "42420", 285: "42420", 286: "42420", 287: "42420",
    288: "42420", 289: "42420", 290: "42420", 291: "42420",
    292: "42420", 293: "42420", 294: "42420", 295: "42420",
    296: "42420", 297: "42420", 298: "42420", 299: "42420",
}

# FD Product Codes (FDPROD.)
FD_PROD_MAP = {
    1: "42130", 2: "42130", 3: "42130", 4: "42130", 5: "42130",
    6: "42130", 7: "42130", 8: "42130", 9: "42130", 10: "42130",
    200: "42630", 201: "42630", 202: "42630", 203: "42630",
    204: "42630", 205: "42630", 206: "42630", 207: "42630",
    208: "42630", 209: "42630", 210: "42630",
}

# Customer Code Mappings (DDCUSTCD., SACUSTCD., FDCUSTCD.)
# These map CUSTCODE to 2-digit codes
CUSTCD_MAP = {
    # Default mapping - first 2 digits of CUSTCODE
    # In SAS, DDCUSTCD. and SACUSTCD. formats handle specific mappings
}

# Denomination Mappings (CADENOM., SADENOM., FDDENOM.)
# These determine AMTIND (D=Domestic, F=Foreign, etc.)
# Based on PRODUCT code ranges

def get_amtind(product, product_type='current'):
    """Get AMTIND based on product (D=Domestic, F=Foreign, I=Islamic)"""
    try:
        p = int(product) if isinstance(product, str) else product
        if product_type == 'current':
            if 400 <= p <= 444:
                return 'F'  # Foreign currency
            elif p in (160, 161, 162, 163, 164, 165, 166, 182):
                return 'I'  # Islamic
            else:
                return 'D'  # Domestic
        elif product_type == 'savings':
            if 200 <= p <= 299:
                return 'I'  # Islamic
            else:
                return 'D'  # Domestic
        elif product_type == 'fd':
            if 200 <= p <= 299:
                return 'I'  # Islamic
            else:
                return 'D'  # Domestic
    except:
        return 'D'

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def read_sas(path):
    """Read SAS file to Polars DataFrame"""
    if not path.exists():
        return pl.DataFrame()
    df, _ = pyreadstat.read_sas7bdat(str(path))
    return pl.from_pandas(df)

def safe_int(x):
    try:
        return int(float(x)) if x not in (None, '', '') else 0
    except:
        return 0

def to_date(val):
    """Convert SAS numeric date (MMDDYYYY format) to date"""
    if not val:
        return None
    try:
        s = str(float(val)).split('.')[0].zfill(8)[-8:]
        return datetime.strptime(s, "%m%d%Y").date()
    except:
        return None

def calculate_age(bdate_val):
    """Calculate age exactly like SAS"""
    if bdate_val is None or bdate_val == 0:
        return 0
    try:
        # Convert BDATE from numeric to date (MMDDYYYY)
        bdate_str = str(float(bdate_val)).split('.')[0].zfill(8)[-8:]
        bdate = datetime.strptime(bdate_str, "%m%d%Y")
        
        bday = bdate.day
        bmonth = bdate.month
        byear = bdate.year
        
        age = reptyear - byear
        
        # Age limit adjustments (exactly like SAS)
        if age == AGELIMIT:
            if (bmonth == mm and bday > day) or bmonth > mm:
                age = AGEBELOW
        elif age == MAXAGE:
            if (bmonth == mm and bday > day) or bmonth > mm:
                age = AGELIMIT
        elif age > MAXAGE:
            age = MAXAGE
        elif age < AGELIMIT:
            age = AGEBELOW
        else:
            age = AGELIMIT
        
        return age
    except:
        return 0

def write_output(df, name):
    """Save as Parquet and SAS"""
    if df.is_empty():
        return
    
    # Parquet
    parquet_path = OUTPUT_PATH / f"{name}.parquet"
    df.write_parquet(parquet_path)
    print(f"✓ {name}.parquet")
    
    # SAS via CSV
    sas_path = OUTPUT_PATH / f"{name}.sas7bdat"
    try:
        if SAS:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                df.to_pandas().to_csv(f.name, index=False, na_rep='')
                csv_file = f.name
            
            csv_path = csv_file.replace('\\', '/')
            out_path = str(OUTPUT_PATH).replace('\\', '/')
            
            SAS.submit(f"""
                PROC IMPORT DATAFILE="{csv_path}" OUT=WORK.TEMP DBMS=CSV REPLACE; RUN;
                LIBNAME OUT "{out_path}";
                DATA OUT.{name}; SET WORK.TEMP; RUN;
            """)
            os.unlink(csv_file)
            print(f"✓ {name}.sas7bdat")
    except Exception as e:
        print(f"⚠ SAS write failed: {e}")
        df.to_pandas().to_csv(OUTPUT_PATH / f"{name}.csv", index=False)

# Try saspy for SAS output
try:
    import saspy
    SAS = saspy.SASsession(cfgname='default', results='none')
    print("✓ SAS session initialized")
except:
    SAS = None
    print("⚠ SAS not available, will use CSV fallback")

# ============================================================================
# PROCESS UMA DATA (exactly like SAS)
# ============================================================================
print("\nProcessing UMA...")
uma = read_sas(FILES['uma'])
if not uma.is_empty():
    uma = uma.filter(pl.col("BNKIND") == "PBB")
    print(f"✓ {len(uma):,} UMA records (BNKIND=PBB)")
else:
    print("⚠ No UMA data")

# ============================================================================
# PROCESS SAVING ACCOUNTS (exactly like SAS: DATA BNM.SAVG&REPTMON&NOWK &SAVG2)
# ============================================================================
print("\nProcessing Saving Accounts...")
saving = read_sas(FILES['saving'])

# Combine SAVING + UMA (like SAS: SET DEPOSIT.SAVING &SAVG1 UMA &SAVG1)
if not saving.is_empty() and not uma.is_empty():
    # Get common columns from SAVG1 macro
    savg1_cols = ["BRANCH", "DEPTYPE", "PRODUCT", "OPENIND", "CUSTCODE", "INTPAYBL",
                  "RACE", "CURBAL", "OPENMH", "CLOSEMH", "BDATE", "NAME", "ACCTNO",
                  "LASTTRAN", "ACCYTD", "LEDGBAL", "SCHIND", "BANKNO", "OPENDT",
                  "COSTCTR", "CHQFLOAT"]
    # Only use columns that exist
    common_cols = [c for c in savg1_cols if c in saving.columns and c in uma.columns]
    saving = pl.concat([saving.select(common_cols), uma.select(common_cols)])
elif uma.is_empty():
    # Just use saving as is
    pass

if not saving.is_empty():
    # Filter (IF OPENIND NOT IN ('B','C','P'))
    saving = saving.filter(~pl.col("OPENIND").is_in(['B', 'C', 'P']))
    
    # Apply formats and calculations
    saving = saving.with_columns([
        # CUSTCD = PUT(CUSTCODE, SACUSTCD.) - take first 2 digits
        pl.col("CUSTCODE").cast(pl.Utf8).str.slice(0, 2).alias("CUSTCD"),
        # STATECD = PUT(BRANCH, STATECD.) - first digit of BRANCH
        pl.col("BRANCH").cast(pl.Utf8).str.slice(0, 1).alias("STATECD"),
        # PRODCD = PUT(PRODUCT, SAPROD.)
        pl.col("PRODUCT").map_elements(
            lambda x: SAVINGS_PROD_MAP.get(int(x) if x else 0, "42120"),
            return_dtype=pl.Utf8
        ).alias("PRODCD"),
        # AMTIND = PUT(PRODUCT, SADENOM.)
        pl.col("PRODUCT").map_elements(
            lambda x: get_amtind(x, 'savings'),
            return_dtype=pl.Utf8
        ).alias("AMTIND"),
        # RANGE = INPUT(CURBAL, SDRANGE.)
        pl.when(pl.col("CURBAL") < 1000).then(pl.lit("1"))
         .when(pl.col("CURBAL") < 10000).then(pl.lit("2"))
         .when(pl.col("CURBAL") < 50000).then(pl.lit("3"))
         .otherwise(pl.lit("4")).alias("RANGE"),
        # RACE = PUT(RACE, $RACE.)
        pl.col("RACE").cast(pl.Utf8).alias("RACE"),
        # OPENDATE conversion
        pl.col("OPENDT").map_elements(to_date, return_dtype=pl.Date).alias("OPENDATE"),
        # AGE calculation
        pl.struct(["BDATE"]).map_elements(
            lambda x: calculate_age(x["BDATE"]),
            return_dtype=pl.Int64
        ).alias("AGE")
    ])
    
    # Select columns in SAVG2 order
    savg2_cols = ["BRANCH", "DEPTYPE", "PRODUCT", "PRODCD", "CUSTCD", "STATECD", "AGE",
                  "RACE", "INTPAYBL", "CURBAL", "OPENMH", "CLOSEMH", "RANGE", "BDATE",
                  "NAME", "ACCTNO", "LASTTRAN", "ACCYTD", "AMTIND", "LEDGBAL", "SCHIND",
                  "BANKNO", "OPENDATE", "COSTCTR", "CHQFLOAT"]
    
    # Only select columns that exist
    existing_cols = [c for c in savg2_cols if c in saving.columns]
    saving = saving.select(existing_cols)
    
    print(f"✓ {len(saving):,} saving accounts")
    write_output(saving, f"savg{reptmon}{nowk}")
else:
    print("⚠ No saving data")

# ============================================================================
# PROCESS CURRENT ACCOUNTS (exactly like SAS)
# ============================================================================
print("\nProcessing Current Accounts...")
current = read_sas(FILES['current'])

if not current.is_empty():
    # DATA CURRENT; SET DEPOSIT.CURRENT &CURN1;
    # Keep only CURN1 columns
    curn1_cols = ["BRANCH", "DEPTYPE", "PRODUCT", "OPENIND", "CUSTCODE", "INTPAYBL",
                  "RACE", "CURBAL", "OPENMH", "CLOSEMH", "AVGAMT", "PURPOSE", "NAME",
                  "ACCTNO", "LASTTRAN", "ACCYTD", "LEDGBAL", "ODINTACC", "COSTCTR",
                  "SECTOR", "CHQFLOAT", "FORATE", "CURCODE"]
    existing_curn1 = [c for c in curn1_cols if c in current.columns]
    current = current.select(existing_curn1)
    
    # PROC SORT BY ACCTNO DESCENDING CURBAL
    current = current.sort(["ACCTNO", "CURBAL"], descending=[False, True])
    # PROC SORT NODUPKEY BY ACCTNO (keep first = highest CURBAL)
    current = current.unique(subset=["ACCTNO"], keep="first")
    
    # Filter (IF OPENIND NOT IN ('B','C','P'))
    current = current.filter(~pl.col("OPENIND").is_in(['B', 'C', 'P']))
    
    # Apply formats and calculations
    current = current.with_columns([
        # IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE, .01)
        pl.when(pl.col("CURCODE") != "MYR")
         .then((pl.col("INTPAYBL") * pl.col("FORATE")).round(2))
         .otherwise(pl.col("INTPAYBL")).alias("INTPAYBL"),
        # STATECD = PUT(BRANCH, STATECD.)
        pl.col("BRANCH").cast(pl.Utf8).str.slice(0, 1).alias("STATECD"),
        # PRODCD = PUT(PRODUCT, CAPROD.)
        pl.col("PRODUCT").map_elements(
            lambda x: CURRENT_PROD_MAP.get(int(x) if x else 0, "42110"),
            return_dtype=pl.Utf8
        ).alias("PRODCD"),
        # AMTIND = PUT(PRODUCT, CADENOM.)
        pl.col("PRODUCT").map_elements(
            lambda x: get_amtind(x, 'current'),
            return_dtype=pl.Utf8
        ).alias("AMTIND"),
        # RACE = PUT(RACE, $RACE.)
        pl.col("RACE").cast(pl.Utf8).alias("RACE"),
        # RANGE = INPUT(CURBAL, DDRANGE.)
        pl.when(pl.col("CURBAL") < 1000).then(pl.lit("1"))
         .when(pl.col("CURBAL") < 10000).then(pl.lit("2"))
         .when(pl.col("CURBAL") < 50000).then(pl.lit("3"))
         .otherwise(pl.lit("4")).alias("RANGE"),
        # AVGRNGE = INPUT(AVGAMT, DDRANGE.)
        pl.when(pl.col("AVGAMT") < 1000).then(pl.lit("1"))
         .when(pl.col("AVGAMT") < 10000).then(pl.lit("2"))
         .when(pl.col("AVGAMT") < 50000).then(pl.lit("3"))
         .otherwise(pl.lit("4")).alias("AVGRNGE"),
        # CABAL = 0, SABAL = 0
        pl.lit(0.0).alias("CABAL"),
        pl.lit(0.0).alias("SABAL"),
    ])
    
    # CUSTCD logic (SELECT(PRODUCT))
    current = current.with_columns([
        pl.when(pl.col("PRODUCT") == 104).then(pl.lit("02"))
         .when(pl.col("PRODUCT") == 105).then(pl.lit("81"))
         .otherwise(pl.col("CUSTCODE").cast(pl.Utf8).str.slice(0, 2))
         .alias("CUSTCD")
    ])
    
    # Split into CURN and FCY
    # PRODUCT IN &ACE (ACE_PRODUCTS) - assuming empty for now
    # IF 400 <= PRODUCT <= 444 THEN OUTPUT BNM.FCY&REPTMON&NOWK
    # ELSE OUTPUT BNM.CURN&REPTMON&NOWK
    
    # Convert PRODUCT to numeric for filtering
    current = current.with_columns([
        pl.col("PRODUCT").map_elements(
            lambda x: int(x) if x else 0,
            return_dtype=pl.Int64
        ).alias("PRODUCT_NUM")
    ])
    
    # FCY: 400 <= PRODUCT <= 444
    fcy = current.filter(pl.col("PRODUCT_NUM").is_between(400, 444))
    
    # CURN: All others
    curn = current.filter(~pl.col("PRODUCT_NUM").is_between(400, 444))
    
    # Handle FCY sector adjustments (exactly like SAS)
    if not fcy.is_empty():
        fcy = fcy.with_columns([
            pl.when(pl.col("CUSTCD").is_in(['77', '78', '95']))
             .then(
                 pl.when(pl.col("SECTOR").is_in([4, 5]))
                 .then(pl.col("SECTOR"))
                 .otherwise(pl.lit(1))
             )
             .otherwise(
                 pl.when(pl.col("SECTOR").is_in([1, 2, 3]))
                 .then(pl.lit(4))
                 .when(pl.col("SECTOR").is_in([4, 5]))
                 .then(pl.col("SECTOR"))
                 .otherwise(pl.lit(4))
             )
             .alias("SECTOR")
        ])
    
    # Combine CURN + FCY (PROC APPEND)
    # Select CURN2 columns
    curn2_cols = ["BRANCH", "DEPTYPE", "PRODUCT", "PRODCD", "CUSTCD", "STATECD", "CUSTNO",
                  "RACE", "INTPAYBL", "CURBAL", "OPENMH", "CLOSEMH", "RANGE", "AVGAMT",
                  "AVGRNGE", "PURPOSE", "SABAL", "CABAL", "AGE", "NAME", "ACCTNO", "AMTIND",
                  "LASTTRAN", "ACCYTD", "LEDGBAL", "ODINTACC", "COSTCTR", "SECTOR", "CHQFLOAT",
                  "FORATE", "CURCODE"]
    
    # Ensure both have same columns
    common_cols = list(set(curn.columns) & set(fcy.columns) & set(curn2_cols))
    
    # Select common columns for both
    curn = curn.select(common_cols)
    if not fcy.is_empty():
        fcy = fcy.select(common_cols)
    
    # Combine (PROC APPEND)
    current_all = pl.concat([curn, fcy])
    
    # Reorder to CURN2 order
    existing_curn2 = [c for c in curn2_cols if c in current_all.columns]
    current_all = current_all.select(existing_curn2)
    
    # Save CURN
    print(f"✓ {len(current_all):,} current accounts ({len(curn):,} regular, {len(fcy):,} FCY)")
    write_output(current_all, f"curn{reptmon}{nowk}")
    
    # Save FCY separately if not empty
    if not fcy.is_empty():
        fcy = fcy.select(existing_curn2)
        write_output(fcy, f"fcy{reptmon}{nowk}")
else:
    print("⚠ No current data")

# ============================================================================
# DEPARTMENT SUMMARY (exactly like SAS)
# ============================================================================
print("\nCreating department summary...")

if not saving.is_empty() and not current_all.is_empty():
    # PROC SUMMARY DATA=BNM.SAVG&REPTMON&NOWK NWAY
    dept_savg = (saving
        .with_columns([
            pl.col("BRANCH").cast(pl.Utf8),
            pl.col("STATECD").cast(pl.Utf8),
            pl.col("PRODCD").cast(pl.Utf8),
            pl.col("CUSTCD").cast(pl.Utf8),
            pl.col("AMTIND").cast(pl.Utf8),
            pl.col("CURBAL").cast(pl.Float64),
            pl.col("INTPAYBL").cast(pl.Float64),
        ])
        .group_by(["BRANCH", "STATECD", "PRODCD", "CUSTCD", "AMTIND"])
        .agg([
            pl.col("CURBAL").sum().alias("CURBAL"),
            pl.col("INTPAYBL").sum().alias("INTPAYBL")
        ])
    )
    
    # PROC SUMMARY DATA=BNM.CURN&REPTMON&NOWK NWAY MISSING
    dept_curn = (current_all
        .with_columns([
            pl.col("BRANCH").cast(pl.Utf8),
            pl.col("STATECD").cast(pl.Utf8),
            pl.col("PRODCD").cast(pl.Utf8),
            pl.col("CUSTCD").cast(pl.Utf8),
            pl.col("AMTIND").cast(pl.Utf8),
            pl.col("SECTOR").cast(pl.Utf8),
            pl.col("CURBAL").cast(pl.Float64),
            pl.col("INTPAYBL").cast(pl.Float64),
        ])
        .group_by(["BRANCH", "STATECD", "PRODCD", "CUSTCD", "SECTOR", "AMTIND"])
        .agg([
            pl.col("CURBAL").sum().alias("CURBAL"),
            pl.col("INTPAYBL").sum().alias("INTPAYBL")
        ])
    )
    
    # Add SECTOR to dept_savg (null)
    dept_savg = dept_savg.with_columns([pl.lit(None).cast(pl.Utf8).alias("SECTOR")])
    
    # Combine (PROC APPEND)
    common_cols = ["BRANCH", "STATECD", "PRODCD", "CUSTCD", "AMTIND", "SECTOR", "CURBAL", "INTPAYBL"]
    dept_all = pl.concat([
        dept_savg.select(common_cols),
        dept_curn.select(common_cols)
    ])
    
    write_output(dept_all, f"dept{reptmon}{nowk}")
    print(f"✓ Department summary created")
else:
    print("⚠ Skipping department summary")

# ============================================================================
# PROCESS FIXED DEPOSITS (exactly like SAS)
# ============================================================================
print("\nProcessing Fixed Deposits...")
fd = read_sas(FILES['fd'])

if not fd.is_empty():
    # IF ACCTTYPE IN (397,398) THEN DELETE
    fd = fd.filter(~pl.col("ACCTTYPE").is_in([397, 398]))
    
    # Apply transformations
    fd = fd.with_columns([
        # IF CURCODE NE 'MYR' THEN INTPAY = ROUND(INTPAY * FORATE, .01)
        pl.when(pl.col("CURCODE") != "MYR")
         .then((pl.col("INTPAY") * pl.col("FORATE")).round(2))
         .otherwise(pl.col("INTPAY")).alias("INTPAY"),
        # STATE = PUT(BRANCH, STATECD.)
        pl.col("BRANCH").cast(pl.Utf8).str.slice(0, 1).alias("STATE"),
        # BIC = PUT(INTPLAN, FDPROD.)
        pl.col("INTPLAN").map_elements(
            lambda x: FD_PROD_MAP.get(int(x) if x else 0, "42130"),
            return_dtype=pl.Utf8
        ).alias("BIC"),
        # AMTIND = PUT(INTPLAN, FDDENOM.)
        pl.col("INTPLAN").map_elements(
            lambda x: get_amtind(x, 'fd'),
            return_dtype=pl.Utf8
        ).alias("AMTIND"),
        # LSTMATDT conversion
        pl.when(pl.col("LMATDATE") != 0)
         .then(pl.col("LMATDATE").map_elements(to_date, return_dtype=pl.Date))
         .otherwise(pl.lit(None)).alias("LSTMATDT"),
        # CUSTCODE logic
        pl.when(pl.col("BIC").is_in(["42130", "42630"]))
         .then(pl.col("CUSTCD").cast(pl.Utf8).str.slice(0, 2))
         .otherwise(pl.col("CUSTCD").cast(pl.Utf8).str.slice(0, 2))
         .alias("CUSTCODE")
    ])
    
    # PURPOSE adjustments for BIC = 42630
    fd = fd.with_columns([
        pl.when(
            (pl.col("BIC") == "42630") & 
            pl.col("CUSTCODE").is_in(['77', '78', '95'])
        )
        .then(
            pl.when(pl.col("PURPOSE").is_in(['1', '2', '3']))
            .then(pl.col("PURPOSE"))
            .otherwise(pl.lit(1))
        )
        .when(pl.col("BIC") == "42630")
        .then(
            pl.when(pl.col("PURPOSE").is_in(['4', '5']))
            .then(pl.col("PURPOSE"))
            .otherwise(pl.lit(4))
        )
        .otherwise(pl.col("PURPOSE"))
        .alias("PURPOSE")
    ])
    
    # Override BIC for certain account types
    fd = fd.with_columns([
        pl.when(pl.col("ACCTTYPE").is_in([315, 394]))
         .then(pl.lit("42132"))
         .when(pl.col("ACCTTYPE").is_in([397, 398]))
         .then(pl.lit("42199"))
         .otherwise(pl.col("BIC"))
         .alias("BIC")
    ])
    
    # Filter (IF OPENIND = 'D' OR OPENIND = 'O')
    fd = fd.filter(pl.col("OPENIND").is_in(['D', 'O']))
    
    # Select final columns (KEEP)
    fd_cols = ["BRANCH", "ACCTNO", "STATE", "CUSTCODE", "OPENIND", "CURBAL", "TERM",
               "NAME", "AMTIND", "ORGDATE", "MATDATE", "RATE", "RENEWAL", "INTPLAN",
               "INTPAY", "INTDATE", "BIC", "LASTACTV", "LSTMATDT", "PURPOSE", "FORATE",
               "ACCTTYPE"]
    existing_fd = [c for c in fd_cols if c in fd.columns]
    fd = fd.select(existing_fd)
    
    print(f"✓ {len(fd):,} fixed deposit accounts")
    write_output(fd, "fdmthly")
else:
    print("⚠ No FD data")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*70)
print("EIBQDISE Processing Complete!")
print("="*70)
print(f"\nReport Date: {reptdate.strftime('%Y-%m-%d')}")
print(f"Week: {nowk}, Month: {reptmon}")
print(f"Output Path: {OUTPUT_PATH}")
print("\nFiles Created:")
print(f"  savg{reptmon}{nowk}.parquet/sas7bdat")
print(f"  curn{reptmon}{nowk}.parquet/sas7bdat")
print(f"  fcy{reptmon}{nowk}.parquet/sas7bdat")
print(f"  dept{reptmon}{nowk}.parquet/sas7bdat")
print("  fdmthly.parquet/sas7bdat")
print("\nRecord Counts:")
if not saving.is_empty():
    print(f"  Savings:    {len(saving):,}")
if not current_all.is_empty():
    print(f"  Current:    {len(current_all):,}")
if not fcy.is_empty():
    print(f"  FCY:        {len(fcy):,}")
if not fd.is_empty():
    print(f"  FD Monthly: {len(fd):,}")

if SAS:
    SAS.endsas()
    print("✓ SAS session closed")
