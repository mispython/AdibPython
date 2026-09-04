from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import pyreadstat
import saspy
import tempfile
import os
import calendar


# =========================
# Paths
# =========================
BASE_INPUT  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRCGCS")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# ---- Inputs (SAS7BDAT) mirroring SAS libs/members ----
MNITB_CURRENT  = BASE_INPUT / "intg_dp_acct_current_m{reptmon}.sas7bdat"    # SAS: MNITB.CURRENT
MNILN_LNNOTE   = BASE_INPUT / "enrh_ln_note_m{reptmon}.sas7bdat"     # SAS: MNILN.LNNOTE

CRFTABL        = BASE_INPUT / "crftabl.txt"   # Text file: SAP.PBB.BTRADE.CRFTABL

# ---- Output ----
OUT_DIR  = BASE_OUTPUT / "excp"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "npgsexcp.sas7bdat"

# ---- Testing configuration ----
MAX_ROWS = 50000  # Limit rows for testing


# =========================
# Helper(s)
# =========================
def calculate_week_of_month(date_obj):
    """
    Calculate week of month:
    Week 1: days 1-8
    Week 2: days 9-15
    Week 3: days 16-22
    Week 4: days 23-end of month
    """
    day = date_obj.day
    
    if day <= 8:
        return 1
    elif day <= 15:
        return 2
    elif day <= 22:
        return 3
    else:
        return 4


def read_sas7bdat(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read SAS7BDAT file and convert to Polars DataFrame with lowercase columns."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Read with row limit if specified
    if max_rows:
        df, meta = pyreadstat.read_sas7bdat(str(file_path), row_limit=max_rows)
        print(f"Read {len(df)} rows from {file_path.name} (limited to {max_rows})")
    else:
        df, meta = pyreadstat.read_sas7bdat(str(file_path))
        print(f"Read {len(df)} rows from {file_path.name}")
    
    # Convert to Polars and lowercase all column names
    pl_df = pl.from_pandas(df)
    pl_df = pl_df.rename({col: col.lower() for col in pl_df.columns})
    
    # Convert acctno to integer if it exists
    if 'acctno' in pl_df.columns:
        try:
            # Handle different types
            if pl_df['acctno'].dtype == pl.Utf8:
                # For string type, strip whitespace and cast to Int64
                pl_df = pl_df.with_columns([
                    pl.col('acctno').str.strip_chars().cast(pl.Int64, strict=False).alias('acctno')
                ])
            elif pl_df['acctno'].dtype == pl.Float64:
                # For float type, cast to Int64
                pl_df = pl_df.with_columns([
                    pl.col('acctno').cast(pl.Int64).alias('acctno')
                ])
            elif pl_df['acctno'].dtype != pl.Int64:
                # For any other type, try casting
                pl_df = pl_df.with_columns([
                    pl.col('acctno').cast(pl.Int64).alias('acctno')
                ])
        except Exception as e:
            print(f"Warning: Could not convert acctno to integer: {e}")
            # Try alternative conversion
            try:
                pl_df = pl_df.with_columns([
                    pl.col('acctno').cast(pl.Utf8).str.strip_chars().cast(pl.Int64, strict=False).alias('acctno')
                ])
            except Exception as e2:
                print(f"Warning: Alternative conversion also failed: {e2}")
    
    return pl_df


def read_crftabl_fixed_width(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read CRFTABL fixed-width text file based on SAS INPUT positions."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Based on SAS code:
    # @001  RECTYP1 $1.
    # @004  TFID    $8.
    # @012  SUBACCT $5.
    # @365  PREIND  $1.
    # @368  CENSUST  1.
    # @377  ACCTNO  10.
    
    # Read the file as text lines
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    # Parse fixed-width data
    parsed_data = []
    row_count = 0
    
    for line in lines:
        # Check if we've reached the row limit
        if max_rows and row_count >= max_rows:
            break
            
        # Skip empty lines
        if not line.strip():
            continue
        
        # Ensure line is long enough (at least 386 characters based on positions)
        if len(line) < 386:
            # Pad the line if it's too short
            line = line.rstrip('\n').ljust(386)
        
        # Extract fields at specified positions (SAS uses 1-based indexing)
        rectyp1 = line[0:1].strip()        # @001 RECTYP1 $1.
        
        # If RECTYP1 is '1', skip this record (SAS: IF RECTYP1='1' THEN DELETE)
        if rectyp1 == '1':
            continue
        
        tfid = line[3:11].strip()          # @004 TFID $8. (positions 4-11)
        subacct = line[11:16].strip()      # @012 SUBACCT $5. (positions 12-16)
        preind = line[364:365].strip()     # @365 PREIND $1. (position 365)
        
        # Parse CENSUST as numeric (position 368)
        censust_str = line[367:368].strip()  # @368 CENSUST 1. (position 368)
        censust = int(censust_str) if censust_str else 0
        
        # Parse ACCTNO as numeric (positions 377-386)
        acctno_str = line[376:386].strip()   # @377 ACCTNO 10. (positions 377-386)
        acctno = int(acctno_str) if acctno_str else 0
        
        parsed_data.append({
            'rectyp1': rectyp1,
            'tfid': tfid,
            'subacct': subacct,
            'preind': preind,
            'censust': censust,
            'acctno': acctno
        })
        
        row_count += 1
    
    # Convert to Polars DataFrame
    if parsed_data:
        df = pl.DataFrame(parsed_data)
        print(f"Read {len(df)} rows from {file_path.name} (limited to {max_rows if max_rows else 'all'})")
    else:
        # Return empty DataFrame with expected columns
        df = pl.DataFrame({
            'rectyp1': pl.Series([], dtype=pl.Utf8),
            'tfid': pl.Series([], dtype=pl.Utf8),
            'subacct': pl.Series([], dtype=pl.Utf8),
            'preind': pl.Series([], dtype=pl.Utf8),
            'censust': pl.Series([], dtype=pl.Int64),
            'acctno': pl.Series([], dtype=pl.Int64)
        })
        print(f"Read 0 rows from {file_path.name}")
    
    return df


def read_coll_binary(file_path: Path, temp_dir: str, max_rows: int = None) -> pl.DataFrame:
    """Read COLL binary file based on SAS INPUT positions."""
    import numpy as np
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Based on SAS code:
    # @004  CCOLLNO  PD6.
    # @146  ACCTNO   PD6.
    
    # PD (packed decimal) format - need to implement packed decimal parsing
    # This is a placeholder - you'll need to implement actual packed decimal reading
    print(f"Warning: COLL binary file reading needs implementation for packed decimal format")
    print(f"File: {file_path}")
    
    # For now, return empty DataFrame with expected columns
    return pl.DataFrame({
        'ccollno': pl.Series([], dtype=pl.Float64),
        'acctno': pl.Series([], dtype=pl.Int64)  # Changed to Int64 for consistency
    })


def read_desc_binary(file_path: Path, temp_dir: str, max_rows: int = None) -> pl.DataFrame:
    """Read DESC binary file based on SAS INPUT positions."""
    import numpy as np
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    # Based on SAS code:
    # @001 CCOLLNO   11.
    # @051 CINSTCL   $2.
    # @055 NATGUAR   $2.
    # @211 CENSUS    10.
    
    # This appears to be a flat text file (not binary) based on the SAS INPUT
    # Let's try reading it as fixed-width text
    print(f"Warning: DESC file reading needs implementation for fixed-width format")
    print(f"File: {file_path}")
    
    # For now, return empty DataFrame with expected columns
    return pl.DataFrame({
        'ccollno': pl.Series([], dtype=pl.Float64),
        'cinstcl': pl.Series([], dtype=pl.Utf8),
        'natguar': pl.Series([], dtype=pl.Utf8),
        'census': pl.Series([], dtype=pl.Float64)
    })


# =========================
# 1) Calculate REPTDATE as yesterday
# =========================
REPTDATE = datetime.now() - timedelta(days=1)

REPTMON   = f"{REPTDATE.month:02d}"         # PUT(MM, Z2.)
REPTYEAR2 = f"{REPTDATE.year % 100:02d}"    # PUT(REPTDATE, YEAR2.)
REPTDAY   = f"{REPTDATE.day:02d}"           # PUT(DAY(REPTDATE), Z2.)

# Calculate week of month (NOWK) using custom logic:
# Week 1: days 1-8
# Week 2: days 9-15
# Week 3: days 16-22
# Week 4: days 23-end of month
NOWK = calculate_week_of_month(REPTDATE)  # Single digit: 1, 2, 3, or 4

print(f"REPTDATE: {REPTDATE}")
print(f"REPTMON: {REPTMON}")
print(f"REPTYEAR2: {REPTYEAR2}")
print(f"REPTDAY: {REPTDAY}")
print(f"NOWK: {NOWK}")
print(f"TESTING MODE: Reading max {MAX_ROWS} rows per file")

# Update file paths with date variables
MNITB_CURRENT = Path(str(MNITB_CURRENT).format(reptmon=REPTMON))
MNILN_LNNOTE = Path(str(MNILN_LNNOTE).format(reptmon=REPTMON))

# BTRSA.MAST&NOWK&REPTMON dataset (using week number as single digit)
MAST_FILE = BASE_INPUT / f"btmast{REPTMON}{NOWK}{REPTYEAR2}.sas7bdat"

# LCCRISEX files (UPPERCASE naming)
COLL_FILE = BASE_INPUT / f"LCCRISEX_{REPTDATE.year}{REPTMON}{REPTDAY}"
DESC_FILE = BASE_INPUT / f"LCCRISEX_DESC_{REPTDATE.year}{REPTMON}{REPTDAY}"


# =========================
# 2) CRFT from CRFTABL.TXT (fixed-width text file)
# =========================
print("\nReading CRFTABL...")
crft = read_crftabl_fixed_width(CRFTABL, max_rows=MAX_ROWS)
print(f"CRFT records after filter: {crft.height}")

# Apply SCH mapping
crft = (
    crft
    .with_columns([
        pl.lit("   ").alias("sch")
    ])
    .with_columns([
        pl.when(pl.col("censust") == 3).then(pl.lit("P51"))
         .when(pl.col("censust") == 4).then(pl.lit("P72"))
         .when(pl.col("censust") == 5).then(pl.lit("P65"))
         .otherwise(pl.col("sch"))
         .alias("sch")
    ])
    # Keep only unmapped ('   ') as in SAS: "IF SCH EQ '   ';"
    .filter(pl.col("sch") == "   ")
)

# NODUPKEY BY acctno censust subacct
crft = crft.unique(subset=["acctno", "censust", "subacct"], keep="first")

# Merge with MAST by acctno; IF A AND B
if not MAST_FILE.exists():
    raise FileNotFoundError(f"Expected MAST file not found: {MAST_FILE}")

print(f"\nReading MAST file: {MAST_FILE}")
mast = read_sas7bdat(MAST_FILE, max_rows=MAX_ROWS)
print(f"MAST acctno dtype after conversion: {mast['acctno'].dtype if 'acctno' in mast.columns else 'NOT FOUND'}")
print(f"CRFT acctno dtype: {crft['acctno'].dtype}")

# Ensure both acctno columns are Int64
if 'acctno' in mast.columns:
    mast = mast.with_columns([
        pl.col('acctno').cast(pl.Int64).alias('acctno')
    ])

crft = crft.with_columns([
    pl.col('acctno').cast(pl.Int64).alias('acctno')
])

# Select only acctno and deduplicate
mast = mast.select(["acctno"]).unique(subset=["acctno"], keep="first")
print(f"MAST unique acctno records: {mast.height}")

# Now perform the join
crft = crft.join(mast, on="acctno", how="inner")
print(f"CRFT records after MAST join: {crft.height}")

crft = crft.filter(pl.col("acctno") > 0).with_columns([
    pl.lit(0).alias("noteno"),
    pl.lit(0).alias("product"),
])

# NODUPKEY BY acctno subacct
crft = crft.unique(subset=["acctno", "subacct"], keep="first")

# KEEP acctno censust product noteno
crft = crft.select(["acctno", "censust", "product", "noteno"])
print(f"CRFT final records: {crft.height}")


# =========================
# 3) CA from MNITB.CURRENT (SAS7BDAT)
# =========================
print(f"\nReading MNITB.CURRENT: {MNITB_CURRENT}")
ca = read_sas7bdat(MNITB_CURRENT, max_rows=MAX_ROWS)

# Ensure consistent types
ca = ca.select(["acctno", "censust", "product"]).with_columns([
    pl.col('acctno').cast(pl.Int64).alias('acctno'),
    pl.col('censust').cast(pl.Int64).alias('censust'),
    pl.col('product').cast(pl.Int64).alias('product'),
    pl.lit(0).cast(pl.Int64).alias("noteno"),
    pl.lit("   ").alias("sch")
])

ca = (
    ca
    .with_columns([
        pl.when((pl.col("product") == 112) & (pl.col("censust") == 301)).then(pl.lit("P70"))
         .when((pl.col("product") == 112) & (pl.col("censust") == 300)).then(pl.lit("P51"))
         .when((pl.col("product") == 112) & (pl.col("censust") == 302)).then(pl.lit("P72"))
         .when((pl.col("product") == 114) & (pl.col("censust") == 303)).then(pl.lit("P72"))
         .when((pl.col("product") == 108) & (pl.col("censust") == 304)).then(pl.lit("P75"))
         .otherwise(pl.col("sch"))
         .alias("sch")
    ])
    .filter(pl.col("sch") == "   ")  # keep only unmapped
    .select(["acctno", "censust", "product", "noteno"])
)
print(f"CA records: {ca.height}")


# =========================
# 4) LN from MNILN.LNNOTE (SAS7BDAT)
# =========================
print(f"\nReading MNILN.LNNOTE: {MNILN_LNNOTE}")
ln = read_sas7bdat(MNILN_LNNOTE, max_rows=MAX_ROWS)

# Ensure consistent types
ln = ln.select(["acctno", "noteno", "loantype", "census"]).with_columns([
    pl.col('acctno').cast(pl.Int64).alias('acctno'),
    pl.col('noteno').cast(pl.Int64).alias('noteno'),
    pl.col('loantype').cast(pl.Int64).alias('product'),
    pl.col('census').cast(pl.Int64).alias('censust'),
    pl.lit("   ").alias("sch"),
])

ln = (
    ln
    .with_columns([
        pl.when((pl.col("loantype") == 510) & (pl.col("census").is_in([5.12, 5.13]))).then(pl.lit("P70"))
         .when((pl.col("loantype") == 532) & (pl.col("census") == 3.00)).then(pl.lit("P51"))
         .when((pl.col("loantype") == 524) & (pl.col("census") == 5.16)).then(pl.lit("P72"))
         .when((pl.col("loantype") == 527) & (pl.col("census") == 5.17)).then(pl.lit("P72"))
         .when((pl.col("loantype") == 531) & (pl.col("census") == 5.00)).then(pl.lit("P63"))
         .when((pl.col("loantype") == 533) & (pl.col("census") == 533.01)).then(pl.lit("P64"))
         .when((pl.col("loantype") == 533) & (pl.col("census") == 533.00)).then(pl.lit("P65"))
         .otherwise(pl.col("sch"))
         .alias("sch")
    ])
    .filter(pl.col("sch") == "   ")  # keep only unmapped
    .select(["acctno", "noteno", "product", "censust"])
)
print(f"LN records: {ln.height}")


# =========================
# 5) COLL/DESC merge
# =========================
print("\nReading COLL and DESC files...")
# Create temporary directory for file conversion
with tempfile.TemporaryDirectory() as temp_dir:
    # Read COLL and DESC files (with row limit)
    coll = read_coll_binary(COLL_FILE, temp_dir, max_rows=MAX_ROWS)
    desc = read_desc_binary(DESC_FILE, temp_dir, max_rows=MAX_ROWS)

# Ensure acctno is Int64 in coll
if 'acctno' in coll.columns:
    coll = coll.with_columns([
        pl.col('acctno').cast(pl.Int64).alias('acctno')
    ])

# Filter DESC census range: (51000000 <= census <= 1099999999)
desc = desc.filter((pl.col("census") >= 51000000) & (pl.col("census") <= 1099999999))

# IF A AND B -> inner join on ccollno
coll = coll.join(desc, on="ccollno", how="inner")
print(f"COLL records after merge: {coll.height}")


# =========================
# 6) AAA = SET CA LN CRFT; sort BY acctno
# =========================
print("\nCombining CA, LN, CRFT...")
# Ensure all dataframes have consistent schema before concat
ca_final = ca.select(["acctno", "censust", "product", "noteno"]).with_columns([
    pl.col('acctno').cast(pl.Int64),
    pl.col('censust').cast(pl.Int64),
    pl.col('product').cast(pl.Int64),
    pl.col('noteno').cast(pl.Int64)
])

ln_final = ln.select(["acctno", "censust", "product", "noteno"]).with_columns([
    pl.col('acctno').cast(pl.Int64),
    pl.col('censust').cast(pl.Int64),
    pl.col('product').cast(pl.Int64),
    pl.col('noteno').cast(pl.Int64)
])

crft_final = crft.select(["acctno", "censust", "product", "noteno"]).with_columns([
    pl.col('acctno').cast(pl.Int64),
    pl.col('censust').cast(pl.Int64),
    pl.col('product').cast(pl.Int64),
    pl.col('noteno').cast(pl.Int64)
])

aaa = pl.concat(
    [ca_final, ln_final, crft_final],
    how="vertical",
    rechunk=True
).sort(by=["acctno"])
print(f"AAA total records: {aaa.height}")


# =========================
# 7) EXCP.NPGSEXCP = MERGE AAA(IN=A) COLL(IN=B) BY acctno; IF A AND B
# =========================
print("\nMerging AAA with COLL...")
# Ensure acctno types match for join
if 'acctno' in coll.columns and coll.height > 0:
    coll = coll.with_columns([
        pl.col('acctno').cast(pl.Int64).alias('acctno')
    ])
    excp = aaa.join(coll, on="acctno", how="inner")
    print(f"EXCP final records: {excp.height}")
else:
    print("COLL is empty, creating empty EXCP")
    excp = pl.DataFrame({
        'acctno': pl.Series([], dtype=pl.Int64),
        'censust': pl.Series([], dtype=pl.Int64),
        'product': pl.Series([], dtype=pl.Int64),
        'noteno': pl.Series([], dtype=pl.Int64)
    })
    print(f"EXCP final records: {excp.height}")

# =========================
# 8) Write output using SASpy
# =========================
if excp.height > 0:
    print("\nWriting output using SASpy...")
    # Initialize SAS session
    sas = saspy.SASsession(cfgname='default')  # You may need to configure this

    # Convert Polars DataFrame to pandas for SASpy
    excp_pandas = excp.to_pandas()

    # Upload the DataFrame to SAS
    sas_df = sas.df2sd(excp_pandas, 'work_excp')

    # Save as SAS7BDAT
    sas_code = f"""
    libname outlib "{OUT_DIR}";
    data outlib.npgsexcp;
        set work_excp;
    run;
    """

    sas.submit(sas_code)

    print(f"Wrote {OUT_FILE}")

    # Close SAS session
    sas.endsas()
else:
    print("\nNo records to write. Skipping output.")
