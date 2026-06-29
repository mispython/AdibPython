#!/usr/bin/env python3
"""
EIBQDISE Deposit Processing - Simplified Version
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

# Try saspy for SAS output
try:
    import saspy
    SAS = saspy.SASsession(cfgname='default', results='none')
    print("✓ SAS session initialized")
except:
    SAS = None
    print("⚠ SAS not available, will use CSV fallback")

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
# REPORTING DATE CALCULATION
# ============================================================================
today = datetime.now()
reptdate = datetime(today.year, today.month, 1) - timedelta(days=1)
day, mm = reptdate.day, reptdate.month

# Determine week
if day == 8: sdd, wk = 1, '1'
elif day == 15: sdd, wk = 9, '2'
elif day == 22: sdd, wk = 16, '3'
else: sdd, wk = 23, '4'

reptmon, reptyear, reptday = f"{mm:02d}", reptdate.year, f"{day:02d}"
print(f"Report Date: {reptdate.strftime('%d/%m/%y')}, Week: {wk}, Month: {reptmon}")

# Constants
AGELIMIT, MAXAGE, AGEBELOW = 12, 18, 11

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
    """Safe int conversion"""
    try: 
        return int(float(x)) if x not in (None, '', '') else 0
    except: 
        return 0

def to_date(val):
    """Convert SAS numeric date to date"""
    if not val:
        return None
    try:
        s = str(float(val)).split('.')[0].zfill(8)[-8:]
        return datetime.strptime(s, "%m%d%Y").date()
    except:
        return None

def calc_age(bdate):
    """Calculate age with limits"""
    if not bdate:
        return 0
    try:
        s = str(float(bdate)).split('.')[0].zfill(8)[-8:]
        b = datetime.strptime(s, "%m%d%Y")
        age = reptyear - b.year
        if age == AGELIMIT and (b.month > mm or (b.month == mm and b.day > day)):
            age = AGEBELOW
        elif age == MAXAGE and (b.month > mm or (b.month == mm and b.day > day)):
            age = AGELIMIT
        elif age > MAXAGE: 
            age = MAXAGE
        elif age < AGELIMIT: 
            age = AGEBELOW
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
    
    # SAS via CSV (fallback to CSV if SAS fails)
    sas_path = OUTPUT_PATH / f"{name}.sas7bdat"
    try:
        if SAS:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
                df.to_pandas().to_csv(f.name, index=False, na_rep='')
                csv_file = f.name
            
            # Fix: Use variable for the path, not f-string with backslash
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
        print(f"⚠ {name}.csv (SAS fallback)")

# ============================================================================
# PROCESS SAVING ACCOUNTS
# ============================================================================
print("\nProcessing Savings...")
saving = read_sas(FILES['saving'])
uma = read_sas(FILES['uma']).filter(pl.col("BNKIND") == "PBB")
if not uma.is_empty():
    saving = pl.concat([saving, uma])

saving = (saving
    .filter(~pl.col("OPENIND").is_in(['B','C','P']))
    .with_columns([
        pl.col("CUSTCODE").cast(str).str.slice(0,2).alias("CUSTCD"),
        pl.col("BRANCH").cast(str).str.slice(0,1).alias("STATECD"),
        pl.col("PRODUCT").cast(str).str.slice(0,5).alias("PRODCD"),
        pl.col("PRODUCT").cast(str).str.slice(0,1).alias("AMTIND"),
        pl.col("OPENDT").map_elements(to_date, return_dtype=pl.Date).alias("OPENDATE"),
        pl.when(pl.col("CURBAL") < 1000).then("1")
         .when(pl.col("CURBAL") < 10000).then("2")
         .when(pl.col("CURBAL") < 50000).then("3")
         .otherwise("4").alias("RANGE"),
        pl.struct(["BDATE"]).map_elements(lambda x: calc_age(x["BDATE"]), return_dtype=pl.Int64).alias("AGE")
    ])
)
print(f"✓ {len(saving):,} records")
write_output(saving, f"savg{reptmon}{wk}")

# ============================================================================
# PROCESS CURRENT ACCOUNTS
# ============================================================================
print("\nProcessing Current Accounts...")
current = read_sas(FILES['current'])
current = (current
    .sort(["ACCTNO", "CURBAL"], descending=[False, True])
    .unique(subset=["ACCTNO"], keep="first")
    .filter(~pl.col("OPENIND").is_in(['B','C','P']))
    .with_columns([
        pl.col("BRANCH").cast(str),
        pl.col("PRODUCT").cast(str),
        pl.col("CUSTCODE").cast(str),
        pl.col("CURCODE").cast(str),
    ])
    .with_columns([
        pl.col("INTPAYBL").map_elements(safe_float, return_dtype=pl.Float64).alias("INTPAYBL_NUM"),
        pl.col("FORATE").map_elements(safe_float, return_dtype=pl.Float64).alias("FORATE_NUM"),
        pl.col("CURBAL").map_elements(safe_float, return_dtype=pl.Float64).alias("CURBAL_NUM"),
        pl.col("AVGAMT").map_elements(safe_float, return_dtype=pl.Float64).alias("AVGAMT_NUM"),
        pl.col("SECTOR").map_elements(safe_int, return_dtype=pl.Int64).alias("SECTOR_NUM"),
    ])
    .with_columns([
        pl.when(pl.col("CURCODE") != "MYR")
         .then((pl.col("INTPAYBL_NUM") * pl.col("FORATE_NUM")).round(2))
         .otherwise(pl.col("INTPAYBL_NUM")).alias("INTPAYBL_ADJ"),
        pl.col("BRANCH").str.slice(0,1).alias("STATECD"),
        pl.col("PRODUCT").str.slice(0,5).alias("PRODCD"),
        pl.col("PRODUCT").str.slice(0,1).alias("AMTIND"),
        pl.lit(0.0).alias("CABAL"),
        pl.lit(0.0).alias("SABAL"),
        pl.when(pl.col("CURBAL_NUM") < 1000).then("1")
         .when(pl.col("CURBAL_NUM") < 10000).then("2")
         .when(pl.col("CURBAL_NUM") < 50000).then("3")
         .otherwise("4").alias("RANGE"),
        pl.when(pl.col("AVGAMT_NUM") < 1000).then("1")
         .when(pl.col("AVGAMT_NUM") < 10000).then("2")
         .when(pl.col("AVGAMT_NUM") < 50000).then("3")
         .otherwise("4").alias("AVGRNGE"),
    ])
    .with_columns([
        pl.when(pl.col("PRODUCT") == "104").then("02")
         .when(pl.col("PRODUCT") == "105").then("81")
         .otherwise(pl.col("CUSTCODE").str.slice(0,2)).alias("CUSTCD"),
        pl.col("PRODUCT").map_elements(safe_int, return_dtype=pl.Int64).alias("PRODUCT_NUM")
    ])
)

# Split regular and FCY
regular = current.filter(~pl.col("PRODUCT_NUM").is_between(400,444))
fcy = current.filter(pl.col("PRODUCT_NUM").is_between(400,444))

if not fcy.is_empty():
    fcy = fcy.with_columns([
        pl.when(pl.col("CUSTCD").is_in(['77','78','95']))
         .then(pl.when(pl.col("SECTOR_NUM").is_in([4,5])).then(pl.col("SECTOR_NUM")).otherwise(1))
         .otherwise(pl.when(pl.col("SECTOR_NUM").is_in([1,2,3])).then(4)
                  .when(pl.col("SECTOR_NUM").is_in([4,5])).then(pl.col("SECTOR_NUM"))
                  .otherwise(4)).alias("SECTOR_ADJ")
    ])
    fcy = fcy.with_columns([
        pl.col("INTPAYBL_ADJ").alias("INTPAYBL"),
        pl.col("SECTOR_ADJ").cast(str).alias("SECTOR")
    ])

regular = regular.with_columns([
    pl.col("INTPAYBL_ADJ").alias("INTPAYBL"),
    pl.col("SECTOR").cast(str)
])

# Combine
cols = list(set(regular.columns) & set(fcy.columns))
current_all = pl.concat([regular.select(cols), fcy.select(cols)])
print(f"✓ {len(current_all):,} total ({len(regular):,} regular, {len(fcy):,} FCY)")
write_output(current_all, f"curn{reptmon}{wk}")
if not fcy.is_empty():
    write_output(fcy, f"fcy{reptmon}{wk}")

# ============================================================================
# DEPARTMENT SUMMARY
# ============================================================================
print("\nCreating department summary...")
dept_savg = (saving
    .group_by(["BRANCH","STATECD","PRODCD","CUSTCD","AMTIND"])
    .agg([pl.col("CURBAL").sum(), pl.col("INTPAYBL").sum()])
    .with_columns([pl.lit(None).cast(str).alias("SECTOR")])
)

dept_curn = (current_all
    .with_columns([pl.col(c).cast(str) for c in ["BRANCH","STATECD","PRODCD","CUSTCD","AMTIND","SECTOR"]])
    .with_columns([pl.col("CURBAL").cast(float), pl.col("INTPAYBL").cast(float)])
    .group_by(["BRANCH","STATECD","PRODCD","CUSTCD","SECTOR","AMTIND"])
    .agg([pl.col("CURBAL").sum(), pl.col("INTPAYBL").sum()])
)

# Align schemas
common_cols = ["BRANCH","STATECD","PRODCD","CUSTCD","AMTIND","SECTOR","CURBAL","INTPAYBL"]
dept_all = pl.concat([
    dept_savg.select(common_cols),
    dept_curn.select(common_cols)
])
write_output(dept_all, f"dept{reptmon}{wk}")

# ============================================================================
# FIXED DEPOSITS
# ============================================================================
print("\nProcessing Fixed Deposits...")
fd = read_sas(FILES['fd'])
fd = (fd
    .with_columns([
        pl.col(c).cast(str) for c in ["BRANCH","ACCTTYPE","CURCODE","INTPLAN","CUSTCD"]
    ])
    .with_columns([
        pl.col("INTPAY").map_elements(safe_float, return_dtype=pl.Float64).alias("INTPAY_NUM"),
        pl.col("FORATE").map_elements(safe_float, return_dtype=pl.Float64).alias("FORATE_NUM"),
        pl.col("CURBAL").map_elements(safe_float, return_dtype=pl.Float64).alias("CURBAL_NUM"),
        pl.col("ACCTTYPE").map_elements(safe_int, return_dtype=pl.Int64).alias("ACCTTYPE_NUM"),
    ])
    .filter(~pl.col("ACCTTYPE_NUM").is_in([397,398]))
    .with_columns([
        pl.when(pl.col("CURCODE") != "MYR")
         .then((pl.col("INTPAY_NUM") * pl.col("FORATE_NUM")).round(2))
         .otherwise(pl.col("INTPAY_NUM")).alias("INTPAY_ADJ"),
        pl.col("BRANCH").str.slice(0,1).alias("STATE"),
        pl.col("INTPLAN").str.slice(0,5).alias("BIC"),
        pl.col("INTPLAN").str.slice(0,1).alias("AMTIND"),
        pl.col("LMATDATE").map_elements(to_date, return_dtype=pl.Date).alias("LSTMATDT"),
    ])
    .with_columns([
        pl.when(pl.col("BIC").is_in(["42130","42630"]))
         .then(pl.col("CUSTCD").str.slice(0,2))
         .otherwise(pl.col("CUSTCD").str.slice(0,2)).alias("CUSTCODE_FMT"),
        pl.when(pl.col("ACCTTYPE_NUM").is_in([315,394])).then("42132")
         .when(pl.col("ACCTTYPE_NUM").is_in([397,398])).then("42199")
         .otherwise(pl.col("BIC")).alias("BIC_FINAL")
    ])
    .with_columns([
        pl.when((pl.col("BIC") == "42630") & pl.col("CUSTCODE_FMT").is_in(['77','78','95']))
         .then(pl.when(pl.col("PURPOSE").is_in(['1','2','3'])).then(pl.col("PURPOSE")).otherwise(1))
         .when(pl.col("BIC") == "42630")
         .then(pl.when(pl.col("PURPOSE").is_in(['4','5'])).then(pl.col("PURPOSE")).otherwise(4))
         .otherwise(pl.col("PURPOSE")).alias("PURPOSE_ADJ")
    ])
    .filter(pl.col("OPENIND").is_in(['D','O']))
    .with_columns([
        pl.col("INTPAY_ADJ").alias("INTPAY"),
        pl.col("CUSTCODE_FMT").alias("CUSTCODE"),
        pl.col("PURPOSE_ADJ").alias("PURPOSE"),
        pl.col("BIC_FINAL").alias("BIC")
    ])
    .select(["BRANCH","ACCTNO","STATE","CUSTCODE","OPENIND","CURBAL","TERM",
             "NAME","AMTIND","ORGDATE","MATDATE","RATE","RENEWAL","INTPLAN",
             "INTPAY","INTDATE","BIC","LASTACTV","LSTMATDT","PURPOSE","FORATE","ACCTTYPE"])
)
print(f"✓ {len(fd):,} records")
write_output(fd, "fdmthly")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "="*70)
print("EIBQDISE Processing Complete!")
print("="*70)
print(f"\nOutput: {OUTPUT_PATH}")
print(f"  Savings:    {len(saving):,}")
print(f"  Current:    {len(current_all):,}")
print(f"  FCY:        {len(fcy):,}")
print(f"  Dept:       {len(dept_all):,}")
print(f"  FD Monthly: {len(fd):,}")

if SAS:
    SAS.endsas()
