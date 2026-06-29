#!/usr/bin/env python3
"""
EIBQDISE Deposit Processing - Uses PBBDPFMT for format mappings
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

# Import format mappings from PBBDPFMT
from PBBDPFMT import (
    SAProductFormat, SADenomFormat,
    CAProductFormat, CADenomFormat,
    FDProductFormat, FDDenomFormat,
    ProductLists
)

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
# HELPER FUNCTIONS
# ============================================================================

def read_sas(path):
    """Read SAS file to Polars DataFrame"""
    if not path.exists():
        return pl.DataFrame()
    df, _ = pyreadstat.read_sas7bdat(str(path))
    return pl.from_pandas(df)

def safe_float(x):
    """Safe float conversion"""
    try:
        return float(x) if x not in (None, '', '') else 0.0
    except:
        return 0.0

def safe_int(x):
    """Safe int conversion - handles decimal strings like '42.0'"""
    if x is None or x == '' or pd.isna(x):
        return 0
    try:
        if isinstance(x, (int, float)):
            return int(x)
        if isinstance(x, str):
            x = x.strip()
            if '.' in x:
                x = x.split('.')[0]
            return int(x) if x else 0
        return int(x)
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
        bdate_str = str(float(bdate_val)).split('.')[0].zfill(8)[-8:]
        bdate = datetime.strptime(bdate_str, "%m%d%Y")
        
        bday = bdate.day
        bmonth = bdate.month
        byear = bdate.year
        
        age = reptyear - byear
        
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
# PROCESS UMA DATA
# ============================================================================
print("\nProcessing UMA...")
uma = read_sas(FILES['uma'])
if not uma.is_empty():
    uma = uma.filter(pl.col("BNKIND") == "PBB")
    print(f"✓ {len(uma):,} UMA records (BNKIND=PBB)")
else:
    print("⚠ No UMA data")

# ============================================================================
# PROCESS SAVING ACCOUNTS
# ============================================================================
print("\nProcessing Saving Accounts...")
saving = read_sas(FILES['saving'])

if not saving.is_empty() and not uma.is_empty():
    savg1_cols = ["BRANCH", "DEPTYPE", "PRODUCT", "OPENIND", "CUSTCODE", "INTPAYBL",
                  "RACE", "CURBAL", "OPENMH", "CLOSEMH", "BDATE", "NAME", "ACCTNO",
                  "LASTTRAN", "ACCYTD", "LEDGBAL", "SCHIND", "BANKNO", "OPENDT",
                  "COSTCTR", "CHQFLOAT"]
    common_cols = [c for c in savg1_cols if c in saving.columns and c in uma.columns]
    saving = pl.concat([saving.select(common_cols), uma.select(common_cols)])

if not saving.is_empty():
    saving = saving.filter(~pl.col("OPENIND").is_in(['B', 'C', 'P']))
    
    # Convert PRODUCT to int
    saving = saving.with_columns([
        pl.col("PRODUCT").map_elements(safe_int, return_dtype=pl.Int64).alias("PRODUCT_NUM")
    ])
    
    saving = saving.with_columns([
        pl.col("CUSTCODE").cast(pl.Utf8).str.slice(0, 2).alias("CUSTCD"),
        pl.col("BRANCH").cast(pl.Utf8).str.slice(0, 1).alias("STATECD"),
        # Use SAPROD format from PBBDPFMT
        pl.col("PRODUCT_NUM").map_elements(
            lambda x: SAProductFormat.format(x),
            return_dtype=pl.Utf8
        ).alias("PRODCD"),
        # Use SADENOM format from PBBDPFMT
        pl.col("PRODUCT_NUM").map_elements(
            lambda x: SADenomFormat.format(x),
            return_dtype=pl.Utf8
        ).alias("AMTIND"),
        pl.when(pl.col("CURBAL") < 1000).then(pl.lit("1"))
         .when(pl.col("CURBAL") < 10000).then(pl.lit("2"))
         .when(pl.col("CURBAL") < 50000).then(pl.lit("3"))
         .otherwise(pl.lit("4")).alias("RANGE"),
        pl.col("RACE").cast(pl.Utf8).alias("RACE"),
        pl.col("OPENDT").map_elements(to_date, return_dtype=pl.Date).alias("OPENDATE"),
        pl.struct(["BDATE"]).map_elements(
            lambda x: calculate_age(x["BDATE"]),
            return_dtype=pl.Int64
        ).alias("AGE")
    ])
    
    savg2_cols = ["BRANCH", "DEPTYPE", "PRODUCT", "PRODCD", "CUSTCD", "STATECD", "AGE",
                  "RACE", "INTPAYBL", "CURBAL", "OPENMH", "CLOSEMH", "RANGE", "BDATE",
                  "NAME", "ACCTNO", "LASTTRAN", "ACCYTD", "AMTIND", "LEDGBAL", "SCHIND",
                  "BANKNO", "OPENDATE", "COSTCTR", "CHQFLOAT"]
    existing_cols = [c for c in savg2_cols if c in saving.columns]
    saving = saving.select(existing_cols)
    
    print(f"✓ {len(saving):,} saving accounts")
    write_output(saving, f"savg{reptmon}{nowk}")
else:
    print("⚠ No saving data")

# ============================================================================
# PROCESS CURRENT ACCOUNTS
# ============================================================================
print("\nProcessing Current Accounts...")
current = read_sas(FILES['current'])

if not current.is_empty():
    # Keep only CURN1 columns
    curn1_cols = ["BRANCH", "DEPTYPE", "PRODUCT", "OPENIND", "CUSTCODE", "INTPAYBL",
                  "RACE", "CURBAL", "OPENMH", "CLOSEMH", "AVGAMT", "PURPOSE", "NAME",
                  "ACCTNO", "LASTTRAN", "ACCYTD", "LEDGBAL", "ODINTACC", "COSTCTR",
                  "SECTOR", "CHQFLOAT", "FORATE", "CURCODE"]
    existing_curn1 = [c for c in curn1_cols if c in current.columns]
    current = current.select(existing_curn1)
    
    # Sort and deduplicate
    current = current.sort(["ACCTNO", "CURBAL"], descending=[False, True])
    current = current.unique(subset=["ACCTNO"], keep="first")
    
    # Filter
    current = current.filter(~pl.col("OPENIND").is_in(['B', 'C', 'P']))
    
    # Convert columns safely
    current = current.with_columns([
        pl.col("INTPAYBL").map_elements(safe_float, return_dtype=pl.Float64).alias("INTPAYBL_NUM"),
        pl.col("FORATE").map_elements(safe_float, return_dtype=pl.Float64).alias("FORATE_NUM"),
        pl.col("CURBAL").map_elements(safe_float, return_dtype=pl.Float64).alias("CURBAL_NUM"),
        pl.col("AVGAMT").map_elements(safe_float, return_dtype=pl.Float64).alias("AVGAMT_NUM"),
        pl.col("PRODUCT").map_elements(safe_int, return_dtype=pl.Int64).alias("PRODUCT_NUM"),
    ])
    
    # Apply transformations
    current = current.with_columns([
        # IF CURCODE NE 'MYR' THEN INTPAYBL = ROUND(INTPAYBL * FORATE, .01)
        pl.when(pl.col("CURCODE") != "MYR")
         .then((pl.col("INTPAYBL_NUM") * pl.col("FORATE_NUM")).round(2))
         .otherwise(pl.col("INTPAYBL_NUM"))
         .alias("INTPAYBL"),
        
        pl.col("BRANCH").cast(pl.Utf8).str.slice(0, 1).alias("STATECD"),
        # Use CAPROD format from PBBDPFMT
        pl.col("PRODUCT_NUM").map_elements(
            lambda x: CAProductFormat.format(x),
            return_dtype=pl.Utf8
        ).alias("PRODCD"),
        # Use CADENOM format from PBBDPFMT
        pl.col("PRODUCT_NUM").map_elements(
            lambda x: CADenomFormat.format(x),
            return_dtype=pl.Utf8
        ).alias("AMTIND"),
        pl.col("RACE").cast(pl.Utf8).alias("RACE"),
        pl.when(pl.col("CURBAL_NUM") < 1000).then(pl.lit("1"))
         .when(pl.col("CURBAL_NUM") < 10000).then(pl.lit("2"))
         .when(pl.col("CURBAL_NUM") < 50000).then(pl.lit("3"))
         .otherwise(pl.lit("4")).alias("RANGE"),
        pl.when(pl.col("AVGAMT_NUM") < 1000).then(pl.lit("1"))
         .when(pl.col("AVGAMT_NUM") < 10000).then(pl.lit("2"))
         .when(pl.col("AVGAMT_NUM") < 50000).then(pl.lit("3"))
         .otherwise(pl.lit("4")).alias("AVGRNGE"),
        pl.lit(0.0).alias("CABAL"),
        pl.lit(0.0).alias("SABAL"),
    ])
    
    # CUSTCD logic (SELECT(PRODUCT))
    current = current.with_columns([
        pl.when(pl.col("PRODUCT_NUM") == 104).then(pl.lit("02"))
         .when(pl.col("PRODUCT_NUM") == 105).then(pl.lit("81"))
         .otherwise(pl.col("CUSTCODE").cast(pl.Utf8).str.slice(0, 2))
         .alias("CUSTCD")
    ])
    
    # Split into CURN and FCY
    fcy = current.filter(pl.col("PRODUCT_NUM").is_between(400, 444))
    curn = current.filter(~pl.col("PRODUCT_NUM").is_between(400, 444))
    
    # Handle FCY sector adjustments
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
    
    # Combine CURN + FCY
    curn2_cols = ["BRANCH", "DEPTYPE", "PRODUCT", "PRODCD", "CUSTCD", "STATECD", "CUSTNO",
                  "RACE", "INTPAYBL", "CURBAL", "OPENMH", "CLOSEMH", "RANGE", "AVGAMT",
                  "AVGRNGE", "PURPOSE", "SABAL", "CABAL", "AGE", "NAME", "ACCTNO", "AMTIND",
                  "LASTTRAN", "ACCYTD", "LEDGBAL", "ODINTACC", "COSTCTR", "SECTOR", "CHQFLOAT",
                  "FORATE", "CURCODE"]
    
    common_cols = list(set(curn.columns) & set(fcy.columns) & set(curn2_cols))
    curn = curn.select(common_cols)
    if not fcy.is_empty():
        fcy = fcy.select(common_cols)
    
    current_all = pl.concat([curn, fcy])
    existing_curn2 = [c for c in curn2_cols if c in current_all.columns]
    current_all = current_all.select(existing_curn2)
    
    print(f"✓ {len(current_all):,} current accounts ({len(curn):,} regular, {len(fcy):,} FCY)")
    write_output(current_all, f"curn{reptmon}{nowk}")
    
    if not fcy.is_empty():
        fcy = fcy.select(existing_curn2)
        write_output(fcy, f"fcy{reptmon}{nowk}")
else:
    print("⚠ No current data")
    current_all = pl.DataFrame()
    fcy = pl.DataFrame()

# ============================================================================
# DEPARTMENT SUMMARY
# ============================================================================
print("\nCreating department summary...")

if not saving.is_empty() and not current_all.is_empty():
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
    
    dept_savg = dept_savg.with_columns([pl.lit(None).cast(pl.Utf8).alias("SECTOR")])
    
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
# PROCESS FIXED DEPOSITS
# ============================================================================
print("\nProcessing Fixed Deposits...")
fd = read_sas(FILES['fd'])

if not fd.is_empty():
    # Filter out certain account types
    fd = fd.filter(~pl.col("ACCTTYPE").is_in([397, 398]))
    
    # Convert numeric columns
    fd = fd.with_columns([
        pl.col("INTPAY").map_elements(safe_float, return_dtype=pl.Float64).alias("INTPAY_NUM"),
        pl.col("FORATE").map_elements(safe_float, return_dtype=pl.Float64).alias("FORATE_NUM"),
        pl.col("CURBAL").map_elements(safe_float, return_dtype=pl.Float64).alias("CURBAL_NUM"),
        pl.col("ACCTTYPE").map_elements(safe_int, return_dtype=pl.Int64).alias("ACCTTYPE_NUM"),
        pl.col("INTPLAN").map_elements(safe_int, return_dtype=pl.Int64).alias("INTPLAN_NUM"),
        pl.col("BRANCH").cast(pl.Utf8),
        pl.col("CUSTCD").cast(pl.Utf8),
        pl.col("CURCODE").cast(pl.Utf8),
    ])
    
    # First, create BIC and other basic columns
    fd = fd.with_columns([
        # IF CURCODE NE 'MYR' THEN INTPAY = ROUND(INTPAY * FORATE, .01)
        pl.when(pl.col("CURCODE") != "MYR")
         .then((pl.col("INTPAY_NUM") * pl.col("FORATE_NUM")).round(2))
         .otherwise(pl.col("INTPAY_NUM"))
         .alias("INTPAY"),
        
        pl.col("BRANCH").str.slice(0, 1).alias("STATE"),
        # Use FDPROD format from PBBDPFMT
        pl.col("INTPLAN_NUM").map_elements(
            lambda x: FDProductFormat.format(x),
            return_dtype=pl.Utf8
        ).alias("BIC"),
        # Use FDDENOM format from PBBDPFMT
        pl.col("INTPLAN_NUM").map_elements(
            lambda x: FDDenomFormat.format(x),
            return_dtype=pl.Utf8
        ).alias("AMTIND"),
        pl.when(pl.col("LMATDATE") != 0)
         .then(pl.col("LMATDATE").map_elements(to_date, return_dtype=pl.Date))
         .otherwise(pl.lit(None)).alias("LSTMATDT"),
    ])
    
    # Now create CUSTCODE using the BIC column
    fd = fd.with_columns([
        pl.when(pl.col("BIC").is_in(["42130", "42630"]))
         .then(pl.col("CUSTCD").str.slice(0, 2))
         .otherwise(pl.col("CUSTCD").str.slice(0, 2))
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
        pl.when(pl.col("ACCTTYPE_NUM").is_in([315, 394]))
         .then(pl.lit("42132"))
         .when(pl.col("ACCTTYPE_NUM").is_in([397, 398]))
         .then(pl.lit("42199"))
         .otherwise(pl.col("BIC"))
         .alias("BIC")
    ])
    
    # Filter for open accounts
    fd = fd.filter(pl.col("OPENIND").is_in(['D', 'O']))
    
    # Select final columns
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
