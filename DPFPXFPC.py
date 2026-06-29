#!/usr/bin/env python3
"""
EIBQDISE Deposit Processing
Processes saving, current, and fixed deposit accounts
Creates monthly extracts for BNM reporting
"""

import duckdb
import polars as pl
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import calendar
import pyreadstat
import os

# ============================================================================
# PATH SETUP
# ============================================================================
BASE_PATH = Path("/data")
INPUT_PATH = BASE_PATH / "input"
OUTPUT_PATH = BASE_PATH / "output"

# Input files (SAS datasets)
SAVING_FILE = INPUT_PATH / "saving.sas7bdat"
CURRENT_FILE = INPUT_PATH / "current.sas7bdat"
FD_FILE = INPUT_PATH / "fd.sas7bdat"
UMA_FILE = INPUT_PATH / "uma.sas7bdat"

# Output files (will be created as parquet and sas7bdat)
OUTPUT_SAVG_PATTERN = OUTPUT_PATH / "savg{month}{week}"
OUTPUT_CURN_PATTERN = OUTPUT_PATH / "curn{month}{week}"
OUTPUT_FCY_PATTERN = OUTPUT_PATH / "fcy{month}{week}"
OUTPUT_DEPT_PATTERN = OUTPUT_PATH / "dept{month}{week}"
OUTPUT_FDMTHLY = OUTPUT_PATH / "fdmthly"

# Ensure output directory exists
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


# ============================================================================
# INITIALIZE DUCKDB CONNECTION
# ============================================================================
con = duckdb.connect()


# ============================================================================
# CALCULATE REPORTING DATE AND PARAMETERS
# ============================================================================

# Get first day of current month minus 1 day (last day of previous month)
today = datetime.now()
first_of_month = datetime(today.year, today.month, 1)
reptdate = first_of_month - timedelta(days=1)

day = reptdate.day
mm = reptdate.month

# Determine week based on day
if day == 8:
    sdd, wk, wk1 = 1, '1', '4'
    wk2, wk3 = None, None
elif day == 15:
    sdd, wk, wk1 = 9, '2', '1'
    wk2, wk3 = None, None
elif day == 22:
    sdd, wk, wk1 = 16, '3', '2'
    wk2, wk3 = None, None
else:  # day >= 23 (last day of month)
    sdd, wk, wk1 = 23, '4', '3'
    wk2, wk3 = '2', '1'

# Calculate MM1 (previous month for week 1)
if wk == '1':
    mm1 = mm - 1 if mm > 1 else 12
else:
    mm1 = mm

sdate = datetime(reptdate.year, mm, sdd)

# Format macro variables
nowk = wk
nowk1 = wk1
nowk2 = wk2
nowk3 = wk3
reptmon = f"{mm:02d}"
reptmon1 = f"{mm1:02d}"
reptyear = reptdate.year
reptday = f"{reptdate.day:02d}"
rdate = reptdate.strftime("%d/%m/%y")
sdate_str = sdate.strftime("%d/%m/%y")

print(f"Report Date: {rdate}")
print(f"Start Date: {sdate_str}")
print(f"Week: {nowk}")
print(f"Month: {reptmon}")
print(f"Year: {reptyear}")
print()

# Constants
AGELIMIT = 12
MAXAGE = 18
AGEBELOW = 11

# ACE products (referenced in code)
ACE_PRODUCTS = []  # Define based on requirements


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def read_sas_file(filepath):
    """Read SAS dataset and return Polars DataFrame"""
    if not filepath.exists():
        print(f"⚠ File not found: {filepath}")
        return pl.DataFrame()
    
    try:
        # Read SAS file using pyreadstat
        df, meta = pyreadstat.read_sas7bdat(str(filepath))
        # Convert to Polars
        return pl.from_pandas(df)
    except Exception as e:
        print(f"⚠ Error reading {filepath}: {e}")
        return pl.DataFrame()


def write_output(df, base_path, suffix=""):
    """Write DataFrame to both Parquet and SAS formats"""
    if df.is_empty():
        print(f"⚠ No data to write for {base_path}")
        return
    
    # Parquet output
    parquet_path = Path(str(base_path) + f"{suffix}.parquet")
    df.write_parquet(parquet_path)
    print(f"✓ Saved Parquet: {parquet_path}")
    
    # SAS output (using pyreadstat)
    try:
        sas_path = Path(str(base_path) + f"{suffix}.sas7bdat")
        # Convert to pandas for SAS writing
        pd_df = df.to_pandas()
        pyreadstat.write_sas7bdat(pd_df, str(sas_path))
        print(f"✓ Saved SAS: {sas_path}")
    except Exception as e:
        print(f"⚠ Error saving SAS file {sas_path}: {e}")


def calculate_age(bdate_val, reptdate, reptmon, reptday, reptyear):
    """Calculate age based on birthdate"""
    if bdate_val is None or bdate_val == 0:
        return 0

    try:
        # Convert MMDDYYYY format to date
        bdate_str = str(int(bdate_val)).zfill(11)[:8]
        bdate = datetime.strptime(bdate_str, "%m%d%Y")

        bday = bdate.day
        bmonth = bdate.month
        byear = bdate.year

        age = reptyear - byear

        # Age limit adjustments
        if age == AGELIMIT:
            if (bmonth == int(reptmon) and bday > int(reptday)) or bmonth > int(reptmon):
                age = AGEBELOW
        elif age == MAXAGE:
            if (bmonth == int(reptmon) and bday > int(reptday)) or bmonth > int(reptmon):
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


def convert_opendt(opendt_val):
    """Convert OPENDT from numeric to date"""
    if opendt_val is None or opendt_val == 0:
        return None
    try:
        opendt_str = str(int(opendt_val)).zfill(11)[:8]
        return datetime.strptime(opendt_str, "%m%d%Y")
    except:
        return None


# ============================================================================
# PROCESS UMA DATA
# ============================================================================

print("Loading UMA data...")

uma = read_sas_file(UMA_FILE)
if not uma.is_empty():
    uma = uma.filter(pl.col("BNKIND") == "PBB")
    print(f"✓ Loaded {len(uma)} UMA records")
else:
    print("⚠ UMA file not found or empty, creating empty dataset")


# ============================================================================
# PROCESS SAVING ACCOUNTS
# ============================================================================

print("Processing Saving Accounts...")

# Load saving data
saving = read_sas_file(SAVING_FILE)

if saving.is_empty():
    print("⚠ No saving data found, creating empty dataset")
    saving = pl.DataFrame()

# Combine with UMA if exists
if not uma.is_empty() and not saving.is_empty():
    saving = pl.concat([saving, uma], how="diagonal")

# Filter out closed/blocked accounts
if not saving.is_empty():
    saving = saving.filter(~pl.col("OPENIND").is_in(['B', 'C', 'P']))

# Apply transformations
if not saving.is_empty():
    saving = saving.with_columns([
        # Format assignments (simplified - would need actual format mappings)
        pl.col("CUSTCODE").cast(pl.Utf8).str.slice(0, 2).alias("CUSTCD"),
        pl.col("BRANCH").cast(pl.Utf8).str.slice(0, 1).alias("STATECD"),
        pl.col("PRODUCT").cast(pl.Utf8).str.slice(0, 5).alias("PRODCD"),
        pl.col("PRODUCT").cast(pl.Utf8).str.slice(0, 1).alias("AMTIND"),

        # Convert OPENDT to date
        pl.col("OPENDT").map_elements(convert_opendt, return_dtype=pl.Date).alias("OPENDATE"),

        # Calculate RANGE (simplified)
        pl.when(pl.col("CURBAL") < 1000).then(pl.lit("1"))
        .when(pl.col("CURBAL") < 10000).then(pl.lit("2"))
        .when(pl.col("CURBAL") < 50000).then(pl.lit("3"))
        .otherwise(pl.lit("4"))
        .alias("RANGE"),
    ])

    # Calculate AGE
    saving = saving.with_columns([
        pl.struct(["BDATE"]).map_elements(
            lambda x: calculate_age(x["BDATE"], reptdate, reptmon, reptday, reptyear),
            return_dtype=pl.Int64
        ).alias("AGE")
    ])

    print(f"✓ Processed {len(saving)} saving accounts")

    # Save output
    output_path = OUTPUT_PATH / f"savg{reptmon}{nowk}"
    write_output(saving, output_path)
else:
    print("⚠ No saving data to process")


# ============================================================================
# PROCESS CURRENT ACCOUNTS
# ============================================================================

print("Processing Current Accounts...")

# Load current data
current = read_sas_file(CURRENT_FILE)

if current.is_empty():
    print("⚠ No current data found, creating empty dataset")
    current = pl.DataFrame()

if not current.is_empty():
    # Remove duplicates (keep highest CURBAL per account)
    current = current.sort(["ACCTNO", "CURBAL"], descending=[False, True])
    current = current.unique(subset=["ACCTNO"], keep="first")

    # Filter out closed/blocked accounts
    current = current.filter(~pl.col("OPENIND").is_in(['B', 'C', 'P']))

    # Apply transformations
    current = current.with_columns([
        # Convert foreign currency amounts if needed
        pl.when(pl.col("CURCODE") != "MYR")
        .then((pl.col("INTPAYBL") * pl.col("FORATE")).round(2))
        .otherwise(pl.col("INTPAYBL"))
        .alias("INTPAYBL_ADJ"),

        # Format assignments
        pl.col("BRANCH").cast(pl.Utf8).str.slice(0, 1).alias("STATECD"),
        pl.col("PRODUCT").cast(pl.Utf8).str.slice(0, 5).alias("PRODCD"),
        pl.col("PRODUCT").cast(pl.Utf8).str.slice(0, 1).alias("AMTIND"),

        # Initialize balances
        pl.lit(0.0).alias("CABAL"),
        pl.lit(0.0).alias("SABAL"),

        # RANGE calculations (simplified)
        pl.when(pl.col("CURBAL") < 1000).then(pl.lit("1"))
        .when(pl.col("CURBAL") < 10000).then(pl.lit("2"))
        .when(pl.col("CURBAL") < 50000).then(pl.lit("3"))
        .otherwise(pl.lit("4"))
        .alias("RANGE"),

        pl.when(pl.col("AVGAMT") < 1000).then(pl.lit("1"))
        .when(pl.col("AVGAMT") < 10000).then(pl.lit("2"))
        .when(pl.col("AVGAMT") < 50000).then(pl.lit("3"))
        .otherwise(pl.lit("4"))
        .alias("AVGRNGE"),
    ])

    # Handle VOSTRO accounts
    current = current.with_columns([
        pl.when(pl.col("PRODUCT") == 104)
        .then(pl.lit("02"))
        .when(pl.col("PRODUCT") == 105)
        .then(pl.lit("81"))
        .otherwise(pl.col("CUSTCODE").cast(pl.Utf8).str.slice(0, 2))
        .alias("CUSTCD")
    ])

    # Split into CURN and FCY based on product
    current_regular = current.filter(
        ~pl.col("PRODUCT").is_in(ACE_PRODUCTS) &
        ~pl.col("PRODUCT").is_between(400, 444)
    )

    current_fcy = current.filter(pl.col("PRODUCT").is_between(400, 444))

    # Handle FCY sector adjustments
    if not current_fcy.is_empty():
        current_fcy = current_fcy.with_columns([
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
            .alias("SECTOR_ADJ")
        ])

    # Replace INTPAYBL with adjusted value
    if not current_regular.is_empty():
        current_regular = current_regular.with_columns([
            pl.col("INTPAYBL_ADJ").alias("INTPAYBL")
        ])
    
    if not current_fcy.is_empty():
        current_fcy = current_fcy.with_columns([
            pl.col("INTPAYBL_ADJ").alias("INTPAYBL"),
            pl.col("SECTOR_ADJ").alias("SECTOR")
        ])

    # Combine FCY back to regular current
    current_all = pl.concat([current_regular, current_fcy], how="diagonal")

    print(f"✓ Processed {len(current_all)} current accounts")
    print(f"  - Regular: {len(current_regular)}")
    print(f"  - FCY: {len(current_fcy)}")

    # Save outputs
    output_curn = OUTPUT_PATH / f"curn{reptmon}{nowk}"
    output_fcy = OUTPUT_PATH / f"fcy{reptmon}{nowk}"

    write_output(current_all, output_curn)
    if not current_fcy.is_empty():
        write_output(current_fcy, output_fcy)
    else:
        print("⚠ No FCY data to save")
else:
    print("⚠ No current data to process")


# ============================================================================
# SUMMARIZE AT BRANCH LEVEL FOR RDAL
# ============================================================================

print("Creating branch-level summaries...")

if not saving.is_empty() and not current.is_empty():
    # Summarize savings
    dept_savg = saving.group_by(["BRANCH", "STATECD", "PRODCD", "CUSTCD", "AMTIND"]).agg([
        pl.col("CURBAL").sum().alias("CURBAL"),
        pl.col("INTPAYBL").sum().alias("INTPAYBL")
    ])

    # Summarize current
    dept_curn = current_all.group_by(["BRANCH", "STATECD", "PRODCD", "CUSTCD", "SECTOR", "AMTIND"]).agg([
        pl.col("CURBAL").sum().alias("CURBAL"),
        pl.col("INTPAYBL").sum().alias("INTPAYBL")
    ])

    # Combine department summaries
    dept_all = pl.concat([dept_savg, dept_curn], how="diagonal")

    output_dept = OUTPUT_PATH / f"dept{reptmon}{nowk}"
    write_output(dept_all, output_dept)

    print(f"✓ Created department summary")
else:
    print("⚠ Skipping department summary - insufficient data")


# ============================================================================
# PROCESS ACCOUNT COUNTS (293 REPORTS)
# ============================================================================

print("Processing account count summaries...")

if not saving.is_empty():
    # Savings count
    save293 = saving.with_columns([
        pl.lit(1).alias("OPENMH")
    ])

    save293_sum = save293.group_by(["BRANCH", "STATECD", "PRODCD", "CUSTCD", "AMTIND"]).agg([
        pl.col("OPENMH").sum().alias("OPENMH")
    ])

    # Current count
    if not current.is_empty():
        curr293 = current.filter(~pl.col("OPENIND").is_in(['B', 'C', 'P']))
        curr293 = curr293.filter(pl.col("BRANCH") <= 900)

        # Apply product code transformations
        curr293 = curr293.with_columns([
            pl.col("BRANCH").cast(pl.Utf8).str.slice(0, 1).alias("STATECD"),
            pl.lit("00").alias("CUSTCD"),
            pl.lit(1).alias("OPENMH"),

            # Product code and amount indicator based on product
            pl.when(pl.col("PRODUCT").is_in([160, 161, 162, 163, 164, 165, 166, 182]))
            .then(pl.lit("42310"))
            .otherwise(pl.lit("42110"))
            .alias("PRODCD"),

            pl.when(pl.col("PRODUCT").is_in([160, 161, 162, 163, 164, 165, 166, 182]))
            .then(pl.lit("I"))
            .otherwise(pl.lit("D"))
            .alias("AMTIND")
        ])

        curr293_sum = curr293.group_by(["BRANCH", "STATECD", "PRODCD", "CUSTCD", "AMTIND"]).agg([
            pl.col("OPENMH").sum().alias("OPENMH")
        ])

    print(f"✓ Account count summaries created")


# ============================================================================
# PROCESS FIXED DEPOSITS
# ============================================================================

print("Processing Fixed Deposits...")

# Load FD data
fd = read_sas_file(FD_FILE)

if fd.is_empty():
    print("⚠ No fixed deposit data found")
    fd_final = pl.DataFrame()
else:
    # Exclude certain account types
    fd = fd.filter(~pl.col("ACCTTYPE").is_in([397, 398]))

    # Apply transformations
    fd = fd.with_columns([
        # Convert foreign currency amounts
        pl.when(pl.col("CURCODE") != "MYR")
        .then((pl.col("INTPAY") * pl.col("FORATE")).round(2))
        .otherwise(pl.col("INTPAY"))
        .alias("INTPAY_ADJ"),

        # Format assignments
        pl.col("BRANCH").cast(pl.Utf8).str.slice(0, 1).alias("STATE"),
        pl.col("INTPLAN").cast(pl.Utf8).str.slice(0, 5).alias("BIC"),
        pl.col("INTPLAN").cast(pl.Utf8).str.slice(0, 1).alias("AMTIND"),

        # Initialize LSTMATDT
        pl.lit(0).alias("LSTMATDT_INIT"),
    ])

    # Convert LMATDATE
    fd = fd.with_columns([
        pl.when(pl.col("LMATDATE") != 0)
        .then(pl.col("LMATDATE"))
        .otherwise(pl.lit(None))
        .alias("LSTMATDT")
    ])

    # Handle customer codes based on BIC
    fd = fd.with_columns([
        pl.when(pl.col("BIC").is_in(["42130", "42630"]))
        .then(pl.col("CUSTCD").cast(pl.Utf8).str.slice(0, 2))
        .otherwise(pl.col("CUSTCD").cast(pl.Utf8).str.slice(0, 2))
        .alias("CUSTCODE_FMT")
    ])

    # Handle BIC = 42630 purpose codes
    fd = fd.with_columns([
        pl.when(
            (pl.col("BIC") == "42630") &
            pl.col("CUSTCODE_FMT").is_in(['77', '78', '95'])
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
        .alias("PURPOSE_ADJ")
    ])

    # Override BIC for certain account types
    fd = fd.with_columns([
        pl.when(pl.col("ACCTTYPE").is_in([315, 394]))
        .then(pl.lit("42132"))
        .when(pl.col("ACCTTYPE").is_in([397, 398]))
        .then(pl.lit("42199"))
        .otherwise(pl.col("BIC"))
        .alias("BIC_FINAL")
    ])

    # Filter for open accounts
    fd_monthly = fd.filter(pl.col("OPENIND").is_in(['D', 'O']))

    # Replace adjusted columns
    fd_monthly = fd_monthly.with_columns([
        pl.col("INTPAY_ADJ").alias("INTPAY"),
        pl.col("CUSTCODE_FMT").alias("CUSTCODE"),
        pl.col("PURPOSE_ADJ").alias("PURPOSE"),
        pl.col("BIC_FINAL").alias("BIC")
    ])

    # Select final columns
    fd_final = fd_monthly.select([
        "BRANCH", "ACCTNO", "STATE", "CUSTCODE", "OPENIND", "CURBAL", "TERM",
        "NAME", "AMTIND", "ORGDATE", "MATDATE", "RATE", "RENEWAL", "INTPLAN",
        "INTPAY", "INTDATE", "BIC", "LASTACTV", "LSTMATDT", "PURPOSE", "FORATE",
        "ACCTTYPE"
    ])

    print(f"✓ Processed {len(fd_final)} fixed deposit accounts")

    # Save output
    write_output(fd_final, OUTPUT_FDMTHLY)


# ============================================================================
# SUMMARY
# ============================================================================

print()
print("=" * 70)
print("EIBQDISE Deposit Processing Complete!")
print("=" * 70)
print()
print("Output Files Created:")
print(f"  1. Savings:     {OUTPUT_PATH / f'savg{reptmon}{nowk}.parquet'}")
print(f"  2. Current:     {OUTPUT_PATH / f'curn{reptmon}{nowk}.parquet'}")
print(f"  3. FCY:         {OUTPUT_PATH / f'fcy{reptmon}{nowk}.parquet'}")
print(f"  4. Department:  {OUTPUT_PATH / f'dept{reptmon}{nowk}.parquet'}")
print(f"  5. FD Monthly:  {OUTPUT_PATH / 'fdmthly.parquet'}")
print()
print("Record Counts:")
if not saving.is_empty():
    print(f"  Savings:        {len(saving):,}")
if not current.is_empty():
    print(f"  Current:        {len(current_all):,}")
if not current_fcy.is_empty():
    print(f"  FCY:            {len(current_fcy):,}")
if not fd_final.is_empty():
    print(f"  Fixed Deposit:  {len(fd_final):,}")
print()

# Close DuckDB connection
con.close()
