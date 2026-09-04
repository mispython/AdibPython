from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import pyreadstat
import saspy
import tempfile
import os
import struct


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


def unpack_packed_decimal(data: bytes, scale: int = 0) -> int:
    """
    Unpack a packed decimal (COMP-3) field.
    
    Packed decimal format:
    - Each byte contains two decimal digits (nibbles)
    - The last nibble contains the sign (C = positive, D = negative, F = unsigned)
    - scale parameter specifies number of decimal places
    """
    if not data:
        return 0
    
    # Convert bytes to hex string
    hex_str = data.hex()
    
    # Extract digits (all but last nibble)
    digits = hex_str[:-1]
    
    # Get sign from last nibble
    sign_nibble = hex_str[-1].upper()
    
    # Convert digits to integer
    if digits:
        value = int(digits)
    else:
        value = 0
    
    # Apply sign
    if sign_nibble in ['D', 'B']:  # Negative
        value = -value
    
    # Apply scale (decimal places)
    if scale > 0:
        value = value / (10 ** scale)
    
    return value


def read_coll_file(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """
    Read COLL file based on SAS INPUT:
    @004  CCOLLNO  PD6.
    @146  ACCTNO   PD6.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    print(f"\nReading COLL file: {file_path}")
    print(f"File exists: {file_path.exists()}")
    print(f"File size: {os.path.getsize(file_path)} bytes")
    
    # Try to read as text first
    try:
        with open(file_path, 'rb') as f:
            # Read first 500 bytes to examine
            sample = f.read(500)
            
            # Check if it looks like text
            text_chars = sum(1 for b in sample if 32 <= b <= 126 or b in [10, 13, 9])
            total_chars = len(sample)
            text_ratio = text_chars / total_chars if total_chars > 0 else 0
            
            print(f"Text ratio: {text_ratio:.2%}")
            
            if text_ratio > 0.8:  # Likely text file
                print("File appears to be TEXT format")
                return read_coll_fixed_width(file_path, max_rows)
            else:
                print("File appears to be BINARY format")
                return read_coll_binary_format(file_path, max_rows)
    except Exception as e:
        print(f"Error reading COLL file: {e}")
        return pl.DataFrame({
            'ccollno': pl.Series([], dtype=pl.Int64),
            'acctno': pl.Series([], dtype=pl.Int64)
        })


def read_coll_fixed_width(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read COLL as fixed-width text file."""
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    parsed_data = []
    row_count = 0
    
    for line in lines:
        if max_rows and row_count >= max_rows:
            break
        
        if not line.strip():
            continue
        
        # Print first line for debugging
        if row_count == 0:
            print(f"First COLL line: {line.rstrip()[:200]}")
        
        # Parse based on positions (SAS uses 1-based indexing)
        # @004 CCOLLNO PD6. - position 4, packed decimal (6 bytes)
        # @146 ACCTNO PD6. - position 146, packed decimal (6 bytes)
        
        # For text files, PD6. might be represented differently
        # Try to parse as regular numbers first
        ccollno_str = line[3:9].strip() if len(line) >= 9 else ""
        acctno_str = line[145:151].strip() if len(line) >= 151 else ""
        
        try:
            ccollno = int(ccollno_str) if ccollno_str else 0
            acctno = int(acctno_str) if acctno_str else 0
        except:
            # If not regular numbers, try packed decimal interpretation
            ccollno_bytes = ccollno_str.encode() if ccollno_str else b''
            acctno_bytes = acctno_str.encode() if acctno_str else b''
            ccollno = unpack_packed_decimal(ccollno_bytes)
            acctno = unpack_packed_decimal(acctno_bytes)
        
        parsed_data.append({
            'ccollno': int(ccollno),
            'acctno': int(acctno)
        })
        
        row_count += 1
    
    if parsed_data:
        df = pl.DataFrame(parsed_data)
        print(f"Read {len(df)} rows from COLL file")
    else:
        df = pl.DataFrame({
            'ccollno': pl.Series([], dtype=pl.Int64),
            'acctno': pl.Series([], dtype=pl.Int64)
        })
        print(f"Read 0 rows from COLL file")
    
    return df


def read_coll_binary_format(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read COLL as binary file with packed decimal fields."""
    with open(file_path, 'rb') as f:
        raw_data = f.read()
    
    print(f"Total bytes: {len(raw_data)}")
    
    # Need to determine record length
    # Based on positions: ACCTNO is at position 146, with PD6. (6 bytes)
    # So minimum record length is at least 151 bytes
    # Let's check if we can determine record length from file size
    
    # Try common record lengths
    possible_lengths = [151, 152, 160, 200, 256, 320, 400, 512]
    
    record_length = None
    for length in possible_lengths:
        if len(raw_data) % length == 0:
            record_length = length
            break
    
    if record_length is None:
        # If no exact division, use minimum length
        record_length = 151
        print(f"Using minimum record length: {record_length}")
    else:
        print(f"Detected record length: {record_length}")
    
    parsed_data = []
    row_count = 0
    
    for i in range(0, len(raw_data), record_length):
        if max_rows and row_count >= max_rows:
            break
        
        record = raw_data[i:i+record_length]
        if len(record) < 151:  # Need at least 151 bytes for the fields we're reading
            break
        
        # Extract packed decimal fields
        # @004 CCOLLNO PD6. - bytes 3-8 (6 bytes, 0-indexed)
        ccollno_bytes = record[3:9]
        # @146 ACCTNO PD6. - bytes 145-150 (6 bytes, 0-indexed)
        acctno_bytes = record[145:151]
        
        ccollno = unpack_packed_decimal(ccollno_bytes)
        acctno = unpack_packed_decimal(acctno_bytes)
        
        # Print first few records for debugging
        if row_count < 3:
            print(f"Record {row_count}: CCOLLNO bytes={ccollno_bytes.hex()}, ACCTNO bytes={acctno_bytes.hex()}")
            print(f"  CCOLLNO={ccollno}, ACCTNO={acctno}")
        
        parsed_data.append({
            'ccollno': int(ccollno),
            'acctno': int(acctno)
        })
        
        row_count += 1
    
    if parsed_data:
        df = pl.DataFrame(parsed_data)
        print(f"Read {len(df)} rows from COLL binary file")
    else:
        df = pl.DataFrame({
            'ccollno': pl.Series([], dtype=pl.Int64),
            'acctno': pl.Series([], dtype=pl.Int64)
        })
        print(f"Read 0 rows from COLL binary file")
    
    return df


def read_desc_file(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """
    Read DESC file based on SAS INPUT:
    @001 CCOLLNO   11.
    @051 CINSTCL   $2.
    @055 NATGUAR   $2.
    @211 CENSUS    10.
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    print(f"\nReading DESC file: {file_path}")
    print(f"File exists: {file_path.exists()}")
    print(f"File size: {os.path.getsize(file_path)} bytes")
    
    # Try to read as text first
    try:
        with open(file_path, 'rb') as f:
            # Read first 500 bytes to examine
            sample = f.read(500)
            
            # Check if it looks like text
            text_chars = sum(1 for b in sample if 32 <= b <= 126 or b in [10, 13, 9])
            total_chars = len(sample)
            text_ratio = text_chars / total_chars if total_chars > 0 else 0
            
            print(f"Text ratio: {text_ratio:.2%}")
            
            if text_ratio > 0.8:  # Likely text file
                print("File appears to be TEXT format")
                return read_desc_fixed_width(file_path, max_rows)
            else:
                print("File appears to be BINARY format")
                return read_desc_binary_format(file_path, max_rows)
    except Exception as e:
        print(f"Error reading DESC file: {e}")
        return pl.DataFrame({
            'ccollno': pl.Series([], dtype=pl.Int64),
            'cinstcl': pl.Series([], dtype=pl.Utf8),
            'natguar': pl.Series([], dtype=pl.Utf8),
            'census': pl.Series([], dtype=pl.Int64)
        })


def read_desc_fixed_width(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read DESC as fixed-width text file."""
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    parsed_data = []
    row_count = 0
    
    for line in lines:
        if max_rows and row_count >= max_rows:
            break
        
        if not line.strip():
            continue
        
        # Print first line for debugging
        if row_count == 0:
            print(f"First DESC line (first 220 chars): {line.rstrip()[:220]}")
        
        # Parse based on positions (SAS uses 1-based indexing)
        # @001 CCOLLNO 11. - positions 1-11
        # @051 CINSTCL $2. - positions 51-52
        # @055 NATGUAR $2. - positions 55-56
        # @211 CENSUS 10. - positions 211-220
        
        ccollno_str = line[0:11].strip() if len(line) >= 11 else ""
        cinstcl = line[50:52].strip() if len(line) >= 52 else ""
        natguar = line[54:56].strip() if len(line) >= 56 else ""
        census_str = line[210:220].strip() if len(line) >= 220 else ""
        
        try:
            ccollno = int(ccollno_str) if ccollno_str else 0
            census = int(census_str) if census_str else 0
        except ValueError:
            # Try float conversion
            try:
                ccollno = int(float(ccollno_str)) if ccollno_str else 0
                census = int(float(census_str)) if census_str else 0
            except:
                ccollno = 0
                census = 0
        
        parsed_data.append({
            'ccollno': ccollno,
            'cinstcl': cinstcl,
            'natguar': natguar,
            'census': census
        })
        
        row_count += 1
    
    if parsed_data:
        df = pl.DataFrame(parsed_data)
        print(f"Read {len(df)} rows from DESC file")
    else:
        df = pl.DataFrame({
            'ccollno': pl.Series([], dtype=pl.Int64),
            'cinstcl': pl.Series([], dtype=pl.Utf8),
            'natguar': pl.Series([], dtype=pl.Utf8),
            'census': pl.Series([], dtype=pl.Int64)
        })
        print(f"Read 0 rows from DESC file")
    
    return df


def read_desc_binary_format(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read DESC as binary file."""
    print("DESC binary format reading not yet implemented")
    return pl.DataFrame({
        'ccollno': pl.Series([], dtype=pl.Int64),
        'cinstcl': pl.Series([], dtype=pl.Utf8),
        'natguar': pl.Series([], dtype=pl.Utf8),
        'census': pl.Series([], dtype=pl.Int64)
    })


# =========================
# 1) Calculate REPTDATE as yesterday
# =========================
REPTDATE = datetime.now() - timedelta(days=1)

REPTMON   = f"{REPTDATE.month:02d}"
REPTYEAR2 = f"{REPTDATE.year % 100:02d}"
REPTDAY   = f"{REPTDATE.day:02d}"

NOWK = calculate_week_of_month(REPTDATE)

print(f"REPTDATE: {REPTDATE}")
print(f"REPTMON: {REPTMON}")
print(f"REPTYEAR2: {REPTYEAR2}")
print(f"REPTDAY: {REPTDAY}")
print(f"NOWK: {NOWK}")
print(f"TESTING MODE: Reading max {MAX_ROWS} rows per file")

# Update file paths
MNITB_CURRENT = Path(str(MNITB_CURRENT).format(reptmon=REPTMON))
MNILN_LNNOTE = Path(str(MNILN_LNNOTE).format(reptmon=REPTMON))
MAST_FILE = BASE_INPUT / f"btmast{REPTMON}{NOWK}{REPTYEAR2}.sas7bdat"

# LCCRISEX files (UPPERCASE)
COLL_FILE = BASE_INPUT / f"LCCRISEX_{REPTDATE.year}{REPTMON}{REPTDAY}"
DESC_FILE = BASE_INPUT / f"LCCRISEX_DESC_{REPTDATE.year}{REPTMON}{REPTDAY}"


# =========================
# 2) CRFT from CRFTABL.TXT (fixed-width text file)
# =========================
print("\n" + "="*60)
print("Processing CRFTABL...")
print("="*60)
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
    pl.lit(0).cast(pl.Int64).alias("noteno"),
    pl.lit(0).cast(pl.Int64).alias("product"),
])

# NODUPKEY BY acctno subacct
crft = crft.unique(subset=["acctno", "subacct"], keep="first")

# KEEP acctno censust product noteno
crft = crft.select(["acctno", "censust", "product", "noteno"])
print(f"CRFT final records: {crft.height}")


# =========================
# 3) CA from MNITB.CURRENT (SAS7BDAT)
# =========================
print("\n" + "="*60)
print("Processing MNITB.CURRENT...")
print("="*60)
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
print("\n" + "="*60)
print("Processing MNILN.LNNOTE...")
print("="*60)
ln = read_sas7bdat(MNILN_LNNOTE, max_rows=MAX_ROWS)

# Ensure consistent types
ln = ln.select(["acctno", "noteno", "loantype", "census"]).with_columns([
    pl.col('acctno').cast(pl.Int64).alias('acctno'),
    pl.col('noteno').cast(pl.Int64).alias('noteno'),
    pl.col('loantype').cast(pl.Int64).alias('loantype'),
    pl.col('census').cast(pl.Float64).alias('census'),
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
    .select(["acctno", "noteno", "loantype", "census"])
    .rename({"loantype": "product", "census": "censust"})
    .with_columns([
        pl.col('product').cast(pl.Int64),
        pl.col('censust').cast(pl.Int64)
    ])
)
print(f"LN records: {ln.height}")


# =========================
# 5) COLL/DESC merge
# =========================
print("\n" + "="*60)
print("Processing COLL and DESC files...")
print("="*60)

# Read COLL and DESC files
coll = read_coll_file(COLL_FILE, max_rows=MAX_ROWS)
desc = read_desc_file(DESC_FILE, max_rows=MAX_ROWS)

# Ensure acctno is Int64 in coll
if 'acctno' in coll.columns:
    coll = coll.with_columns([
        pl.col('acctno').cast(pl.Int64).alias('acctno')
    ])

# Filter DESC census range: (51000000 <= census <= 1099999999)
if desc.height > 0:
    desc = desc.filter((pl.col("census") >= 51000000) & (pl.col("census") <= 1099999999))
    print(f"DESC records after census filter: {desc.height}")

# IF A AND B -> inner join on ccollno
if coll.height > 0 and desc.height > 0:
    coll = coll.join(desc, on="ccollno", how="inner")
    print(f"COLL records after merge: {coll.height}")
else:
    print(f"COLL records after merge: 0 (empty input)")
    coll = pl.DataFrame({
        'ccollno': pl.Series([], dtype=pl.Int64),
        'acctno': pl.Series([], dtype=pl.Int64)
    })


# =========================
# 6) AAA = SET CA LN CRFT; sort BY acctno
# =========================
print("\n" + "="*60)
print("Combining CA, LN, CRFT...")
print("="*60)

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
print("\n" + "="*60)
print("Merging AAA with COLL...")
print("="*60)

# Ensure acctno types match for join
if 'acctno' in coll.columns and coll.height > 0:
    coll = coll.with_columns([
        pl.col('acctno').cast(pl.Int64).alias('acctno')
    ])
    
    # Select only acctno from coll for the join
    coll_acctno = coll.select(["acctno"]).unique(subset=["acctno"], keep="first")
    
    excp = aaa.join(coll_acctno, on="acctno", how="inner")
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
    print("\n" + "="*60)
    print("Writing output using SASpy...")
    print("="*60)
    
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
