from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
import pyreadstat
import saspy
import tempfile
import os
import struct
import numpy as np


# =========================
# Paths
# =========================
BASE_INPUT  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRCGCS")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# ---- Inputs ----
MNITB_CURRENT  = BASE_INPUT / "intg_dp_acct_current_m{reptmon}.sas7bdat"
MNILN_LNNOTE   = BASE_INPUT / "enrh_ln_note_m{reptmon}.sas7bdat"
CRFTABL        = BASE_INPUT / "crftabl.txt"

# ---- Output ----
OUT_DIR  = BASE_OUTPUT / "excp"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "npgsexcp.sas7bdat"

# ---- Configuration ----
# Read all rows from smaller files, but limit large files
MAX_ROWS_SAS = None  # Read ALL rows from SAS files
MAX_ROWS_TEXT = None  # Read ALL rows from text file


# EBCDIC to ASCII translation table
EBCDIC_TO_ASCII = {
    0xF0: '0', 0xF1: '1', 0xF2: '2', 0xF3: '3', 0xF4: '4',
    0xF5: '5', 0xF6: '6', 0xF7: '7', 0xF8: '8', 0xF9: '9',
    0xC1: 'A', 0xC2: 'B', 0xC3: 'C', 0xC4: 'D', 0xC5: 'E',
    0xC6: 'F', 0xC7: 'G', 0xC8: 'H', 0xC9: 'I', 0xD1: 'J',
    0xD2: 'K', 0xD3: 'L', 0xD4: 'M', 0xD5: 'N', 0xD6: 'O',
    0xD7: 'P', 0xD8: 'Q', 0xD9: 'R', 0xE2: 'S', 0xE3: 'T',
    0xE4: 'U', 0xE5: 'V', 0xE6: 'W', 0xE7: 'X', 0xE8: 'Y',
    0xE9: 'Z', 0x40: ' ', 0x4B: '.', 0x6B: ',', 0x5A: '!',
    0x7A: ':', 0x7B: '#', 0x7C: '@', 0x6D: '_', 0x4E: '+',
    0x60: '-', 0x61: '/', 0x6C: '%', 0x5C: '*', 0x7D: "'",
    0x7E: '=', 0x4A: '[', 0x5B: ']', 0x6A: '|', 0x7F: '"',
}


# =========================
# Helper(s)
# =========================
def calculate_week_of_month(date_obj):
    """Calculate week of month: Week 1: days 1-8, Week 2: days 9-15, Week 3: days 16-22, Week 4: days 23-end"""
    day = date_obj.day
    if day <= 8:
        return 1
    elif day <= 15:
        return 2
    elif day <= 22:
        return 3
    else:
        return 4


def ebcdic_to_ascii(byte_data: bytes) -> str:
    """Convert EBCDIC bytes to ASCII string."""
    result = []
    for byte in byte_data:
        if byte in EBCDIC_TO_ASCII:
            result.append(EBCDIC_TO_ASCII[byte])
        else:
            result.append(' ')
    return ''.join(result).strip()


def ebcdic_bytes_to_int(byte_data: bytes) -> int:
    """Convert EBCDIC numeric bytes to integer."""
    ascii_str = ebcdic_to_ascii(byte_data)
    try:
        return int(ascii_str) if ascii_str else 0
    except ValueError:
        return 0


def read_sas7bdat(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read SAS7BDAT file and convert to Polars DataFrame with lowercase columns."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if max_rows:
        df, meta = pyreadstat.read_sas7bdat(str(file_path), row_limit=max_rows)
        print(f"Read {len(df)} rows from {file_path.name} (limited to {max_rows})")
    else:
        df, meta = pyreadstat.read_sas7bdat(str(file_path))
        print(f"Read {len(df)} rows from {file_path.name} (ALL rows)")
    
    pl_df = pl.from_pandas(df)
    pl_df = pl_df.rename({col: col.lower() for col in pl_df.columns})
    
    if 'acctno' in pl_df.columns:
        try:
            if pl_df['acctno'].dtype == pl.Utf8:
                pl_df = pl_df.with_columns([
                    pl.col('acctno').str.strip_chars().cast(pl.Int64, strict=False).alias('acctno')
                ])
            elif pl_df['acctno'].dtype == pl.Float64:
                pl_df = pl_df.with_columns([
                    pl.col('acctno').cast(pl.Int64).alias('acctno')
                ])
            elif pl_df['acctno'].dtype != pl.Int64:
                pl_df = pl_df.with_columns([
                    pl.col('acctno').cast(pl.Int64).alias('acctno')
                ])
        except Exception as e:
            print(f"Warning: Could not convert acctno to integer: {e}")
            try:
                pl_df = pl_df.with_columns([
                    pl.col('acctno').cast(pl.Utf8).str.strip_chars().cast(pl.Int64, strict=False).alias('acctno')
                ])
            except Exception as e2:
                print(f"Warning: Alternative conversion also failed: {e2}")
    
    return pl_df


def read_crftabl_fixed_width(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read CRFTABL fixed-width text file."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    parsed_data = []
    row_count = 0
    
    for line in lines:
        if max_rows and row_count >= max_rows:
            break
        if not line.strip():
            continue
        if len(line) < 386:
            line = line.rstrip('\n').ljust(386)
        
        rectyp1 = line[0:1].strip()
        if rectyp1 == '1':
            continue
        
        tfid = line[3:11].strip()
        subacct = line[11:16].strip()
        preind = line[364:365].strip()
        censust_str = line[367:368].strip()
        censust = int(censust_str) if censust_str else 0
        acctno_str = line[376:386].strip()
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
    
    if parsed_data:
        df = pl.DataFrame(parsed_data)
        print(f"Read {len(df)} rows from {file_path.name}" + (f" (limited to {max_rows})" if max_rows else " (ALL rows)"))
    else:
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
    """Unpack a packed decimal (COMP-3) field correctly."""
    if not data:
        return 0
    
    hex_str = data.hex().upper()
    nibbles = list(hex_str)
    sign_nibble = nibbles[-1] if nibbles else 'F'
    digit_nibbles = nibbles[:-1]
    
    if digit_nibbles:
        try:
            value = int(''.join(digit_nibbles))
        except ValueError:
            value = 0
    else:
        value = 0
    
    if sign_nibble in ['D', 'B']:
        value = -value
    
    if scale > 0:
        value = value / (10 ** scale)
    
    return value


def read_coll_binary_efficient(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read COLL binary file efficiently."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    print(f"\nReading COLL file: {file_path}")
    file_size = os.path.getsize(file_path)
    print(f"File size: {file_size / (1024**3):.2f} GB")
    
    record_length = None
    for length in [151, 152, 160, 200, 256, 320, 400, 512, 1024]:
        if file_size % length == 0:
            record_length = length
            break
    
    if record_length is None:
        record_length = 151
        print(f"Using minimum record length: {record_length}")
    else:
        print(f"Detected record length: {record_length}")
    
    total_records = file_size // record_length
    print(f"Total records: {total_records}")
    
    chunk_size = 100000
    all_data = []
    
    with open(file_path, 'rb') as f:
        for chunk_start in range(0, total_records, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_records)
            if max_rows and chunk_start >= max_rows:
                break
            
            records_to_read = min(chunk_end - chunk_start, max_rows - chunk_start if max_rows else chunk_end - chunk_start)
            bytes_to_read = records_to_read * record_length
            chunk_data = f.read(bytes_to_read)
            
            for i in range(records_to_read):
                record_start = i * record_length
                record = chunk_data[record_start:record_start + record_length]
                
                if len(record) < 151:
                    continue
                
                ccollno_bytes = record[3:9]
                acctno_bytes = record[145:151]
                
                ccollno = unpack_packed_decimal(ccollno_bytes)
                acctno = unpack_packed_decimal(acctno_bytes)
                
                if ccollno > 0 and acctno > 0:
                    all_data.append({
                        'ccollno': ccollno,
                        'acctno': acctno
                    })
            
            if chunk_start % 1000000 == 0 and chunk_start > 0:
                print(f"Processed {chunk_start} records...")
    
    print(f"Total valid COLL records: {len(all_data)}")
    
    if all_data:
        df = pl.DataFrame(all_data)
    else:
        df = pl.DataFrame({
            'ccollno': pl.Series([], dtype=pl.Int64),
            'acctno': pl.Series([], dtype=pl.Int64)
        })
    
    return df


def read_desc_ebcdic_efficient(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read DESC file as EBCDIC fixed-width format."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    print(f"\nReading DESC file (EBCDIC): {file_path}")
    file_size = os.path.getsize(file_path)
    print(f"File size: {file_size / (1024**3):.2f} GB")
    
    record_length = 220
    total_records = file_size // record_length
    print(f"Record length: {record_length}")
    print(f"Total records: {total_records}")
    
    chunk_size = 100000
    all_data = []
    processed = 0
    
    with open(file_path, 'rb') as f:
        for chunk_start in range(0, total_records, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_records)
            if max_rows and chunk_start >= max_rows:
                break
            
            records_to_read = min(chunk_end - chunk_start, max_rows - chunk_start if max_rows else chunk_end - chunk_start)
            bytes_to_read = records_to_read * record_length
            chunk_data = f.read(bytes_to_read)
            
            for i in range(records_to_read):
                record_start = i * record_length
                record = chunk_data[record_start:record_start + record_length]
                
                if len(record) < 220:
                    continue
                
                ccollno_bytes = record[0:11]
                cinstcl_bytes = record[50:52]
                natguar_bytes = record[54:56]
                census_bytes = record[210:220]
                
                ccollno = ebcdic_bytes_to_int(ccollno_bytes)
                cinstcl = ebcdic_to_ascii(cinstcl_bytes)
                natguar = ebcdic_to_ascii(natguar_bytes)
                census = ebcdic_bytes_to_int(census_bytes)
                
                if ccollno > 0 and 51000000 <= census <= 1099999999:
                    all_data.append({
                        'ccollno': ccollno,
                        'cinstcl': cinstcl,
                        'natguar': natguar,
                        'census': census
                    })
                
                processed += 1
            
            if processed % 1000000 == 0:
                print(f"Processed {processed} records, found {len(all_data)} valid...")
    
    print(f"Total DESC records processed: {processed}")
    print(f"Total valid DESC records: {len(all_data)}")
    
    if all_data:
        df = pl.DataFrame(all_data)
    else:
        df = pl.DataFrame({
            'ccollno': pl.Series([], dtype=pl.Int64),
            'cinstcl': pl.Series([], dtype=pl.Utf8),
            'natguar': pl.Series([], dtype=pl.Utf8),
            'census': pl.Series([], dtype=pl.Int64)
        })
    
    return df


# =========================
# 1) Calculate REPTDATE as yesterday
# =========================
REPTDATE = datetime.now() - timedelta(days=1)
REPTMON = f"{REPTDATE.month:02d}"
REPTYEAR2 = f"{REPTDATE.year % 100:02d}"
REPTDAY = f"{REPTDATE.day:02d}"
NOWK = calculate_week_of_month(REPTDATE)

print(f"REPTDATE: {REPTDATE}")
print(f"REPTMON: {REPTMON}")
print(f"REPTYEAR2: {REPTYEAR2}")
print(f"REPTDAY: {REPTDAY}")
print(f"NOWK: {NOWK}")
print(f"CONFIGURATION: Reading ALL rows")

# Update file paths
MNITB_CURRENT = Path(str(MNITB_CURRENT).format(reptmon=REPTMON))
MNILN_LNNOTE = Path(str(MNILN_LNNOTE).format(reptmon=REPTMON))
MAST_FILE = BASE_INPUT / f"btmast{REPTMON}{NOWK}{REPTYEAR2}.sas7bdat"
COLL_FILE = BASE_INPUT / f"LCCRISEX_{REPTDATE.year}{REPTMON}{REPTDAY}"
DESC_FILE = BASE_INPUT / f"LCCRISEX_DESC_{REPTDATE.year}{REPTMON}{REPTDAY}"


# =========================
# Step 1: First, get the account numbers from COLL/DESC (smaller result set)
# =========================
print("\n" + "="*60)
print("STEP 1: Reading COLL and DESC to get target accounts...")
print("="*60)

# Read COLL and DESC files
coll = read_coll_binary_efficient(COLL_FILE, max_rows=None)
desc = read_desc_ebcdic_efficient(DESC_FILE, max_rows=None)

# Join COLL with DESC to get filtered accounts
if coll.height > 0 and desc.height > 0:
    coll_filtered = coll.join(desc, on="ccollno", how="inner")
    print(f"COLL records after merge with DESC: {coll_filtered.height}")
    
    # Get unique account numbers from COLL
    target_acctnos = coll_filtered.select(["acctno"]).unique(subset=["acctno"], keep="first")
    print(f"Target account numbers from COLL: {target_acctnos.height}")
else:
    print("COLL or DESC is empty")
    target_acctnos = pl.DataFrame({'acctno': pl.Series([], dtype=pl.Int64)})

# Free up memory
del coll, desc, coll_filtered
import gc
gc.collect()


# =========================
# Step 2: Process CRFTABL (text file, manageable size)
# =========================
print("\n" + "="*60)
print("STEP 2: Processing CRFTABL...")
print("="*60)
crft = read_crftabl_fixed_width(CRFTABL, max_rows=None)
print(f"CRFT records after filter: {crft.height}")

crft = (
    crft
    .with_columns([pl.lit("   ").alias("sch")])
    .with_columns([
        pl.when(pl.col("censust") == 3).then(pl.lit("P51"))
         .when(pl.col("censust") == 4).then(pl.lit("P72"))
         .when(pl.col("censust") == 5).then(pl.lit("P65"))
         .otherwise(pl.col("sch"))
         .alias("sch")
    ])
    .filter(pl.col("sch") == "   ")
)

crft = crft.unique(subset=["acctno", "censust", "subacct"], keep="first")

# Merge with MAST
if not MAST_FILE.exists():
    raise FileNotFoundError(f"Expected MAST file not found: {MAST_FILE}")

print(f"\nReading MAST file: {MAST_FILE}")
mast = read_sas7bdat(MAST_FILE, max_rows=None)

if 'acctno' in mast.columns:
    mast = mast.with_columns([pl.col('acctno').cast(pl.Int64).alias('acctno')])

crft = crft.with_columns([pl.col('acctno').cast(pl.Int64).alias('acctno')])
mast = mast.select(["acctno"]).unique(subset=["acctno"], keep="first")
print(f"MAST unique acctno records: {mast.height}")

crft = crft.join(mast, on="acctno", how="inner")
print(f"CRFT records after MAST join: {crft.height}")

crft = crft.filter(pl.col("acctno") > 0).with_columns([
    pl.lit(0).cast(pl.Int64).alias("noteno"),
    pl.lit(0).cast(pl.Int64).alias("product"),
])

crft = crft.unique(subset=["acctno", "subacct"], keep="first")
crft = crft.select(["acctno", "censust", "product", "noteno"])

# Filter CRFT to only include target accounts from COLL
if target_acctnos.height > 0:
    crft = crft.join(target_acctnos, on="acctno", how="inner")
    print(f"CRFT records matching COLL accounts: {crft.height}")

# Free up memory
del mast
gc.collect()


# =========================
# Step 3: Process MNITB.CURRENT
# =========================
print("\n" + "="*60)
print("STEP 3: Processing MNITB.CURRENT...")
print("="*60)
ca = read_sas7bdat(MNITB_CURRENT, max_rows=None)

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
    .filter(pl.col("sch") == "   ")
    .select(["acctno", "censust", "product", "noteno"])
)

# Filter CA to only include target accounts from COLL
if target_acctnos.height > 0:
    ca = ca.join(target_acctnos, on="acctno", how="inner")
    print(f"CA records matching COLL accounts: {ca.height}")


# =========================
# Step 4: Process MNILN.LNNOTE (large file - filter during read)
# =========================
print("\n" + "="*60)
print("STEP 4: Processing MNILN.LNNOTE...")
print("="*60)

# For large files, read in chunks and filter on the fly
if target_acctnos.height > 0:
    # Convert target acctnos to a set for fast lookup
    target_set = set(target_acctnos['acctno'].to_list())
    print(f"Target accounts to filter: {len(target_set)}")
    
    # Read LNNOTE in chunks using pyreadstat
    chunk_size = 100000
    all_ln_data = []
    offset = 0
    
    print(f"Reading {MNILN_LNNOTE} in chunks...")
    
    # First, get total number of rows
    df_meta, meta = pyreadstat.read_sas7bdat(str(MNILN_LNNOTE), row_limit=0, metadataonly=True)
    total_rows = meta.number_rows
    print(f"Total rows in LNNOTE: {total_rows}")
    
    while offset < total_rows:
        df_chunk, meta = pyreadstat.read_sas7bdat(
            str(MNILN_LNNOTE), 
            row_offset=offset, 
            row_limit=min(chunk_size, total_rows - offset)
        )
        
        # Convert to Polars and filter
        pl_chunk = pl.from_pandas(df_chunk)
        pl_chunk = pl_chunk.rename({col: col.lower() for col in pl_chunk.columns})
        
        # Filter for target accounts and required columns
        if 'acctno' in pl_chunk.columns:
            pl_chunk = pl_chunk.with_columns([
                pl.col('acctno').cast(pl.Int64).alias('acctno')
            ])
            
            # Filter for target accounts
            pl_chunk = pl_chunk.filter(pl.col('acctno').is_in(target_set))
            
            if pl_chunk.height > 0:
                # Keep only needed columns
                needed_cols = ['acctno', 'noteno', 'loantype', 'census']
                available_cols = [c for c in needed_cols if c in pl_chunk.columns]
                pl_chunk = pl_chunk.select(available_cols)
                all_ln_data.append(pl_chunk)
        
        offset += chunk_size
        
        if offset % 1000000 == 0:
            print(f"Processed {offset} rows from LNNOTE...")
    
    # Combine all chunks
    if all_ln_data:
        ln = pl.concat(all_ln_data, how="vertical")
        print(f"LN records matching COLL accounts: {ln.height}")
    else:
        ln = pl.DataFrame({
            'acctno': pl.Series([], dtype=pl.Int64),
            'noteno': pl.Series([], dtype=pl.Int64),
            'loantype': pl.Series([], dtype=pl.Int64),
            'census': pl.Series([], dtype=pl.Float64)
        })
        print(f"LN records matching COLL accounts: 0")
else:
    ln = pl.DataFrame({
        'acctno': pl.Series([], dtype=pl.Int64),
        'noteno': pl.Series([], dtype=pl.Int64),
        'loantype': pl.Series([], dtype=pl.Int64),
        'census': pl.Series([], dtype=pl.Float64)
    })
    print(f"LN records matching COLL accounts: 0")

# Process LN data
if ln.height > 0:
    ln = ln.with_columns([
        pl.col('loantype').alias('product'),
        pl.col('census').alias('censust'),
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
        .filter(pl.col("sch") == "   ")
        .select(["acctno", "noteno", "product", "censust"])
        .with_columns([
            pl.col('product').cast(pl.Int64),
            pl.col('censust').cast(pl.Int64)
        ])
    )
    print(f"LN final records: {ln.height}")


# =========================
# Step 5: Combine all data
# =========================
print("\n" + "="*60)
print("STEP 5: Combining all data...")
print("="*60)

# Ensure consistent types
ca_final = ca.select(["acctno", "censust", "product", "noteno"]).with_columns([
    pl.col('acctno').cast(pl.Int64),
    pl.col('censust').cast(pl.Int64),
    pl.col('product').cast(pl.Int64),
    pl.col('noteno').cast(pl.Int64)
])

if ln.height > 0:
    ln_final = ln.select(["acctno", "censust", "product", "noteno"]).with_columns([
        pl.col('acctno').cast(pl.Int64),
        pl.col('censust').cast(pl.Int64),
        pl.col('product').cast(pl.Int64),
        pl.col('noteno').cast(pl.Int64)
    ])
else:
    ln_final = pl.DataFrame({
        'acctno': pl.Series([], dtype=pl.Int64),
        'censust': pl.Series([], dtype=pl.Int64),
        'product': pl.Series([], dtype=pl.Int64),
        'noteno': pl.Series([], dtype=pl.Int64)
    })

crft_final = crft.select(["acctno", "censust", "product", "noteno"]).with_columns([
    pl.col('acctno').cast(pl.Int64),
    pl.col('censust').cast(pl.Int64),
    pl.col('product').cast(pl.Int64),
    pl.col('noteno').cast(pl.Int64)
])

aaa = pl.concat([ca_final, ln_final, crft_final], how="vertical", rechunk=True).sort(by=["acctno"])
print(f"AAA total records: {aaa.height}")


# =========================
# Step 6: Final output
# =========================
print("\n" + "="*60)
print("STEP 6: Final output...")
print("="*60)

excp = aaa  # Since we already filtered for target accounts

print(f"EXCP final records: {excp.height}")

# Write output
if excp.height > 0:
    print("\nWriting output using SASpy...")
    
    sas = saspy.SASsession(cfgname='default')
    excp_pandas = excp.to_pandas()
    sas_df = sas.df2sd(excp_pandas, 'work_excp')
    
    sas_code = f"""
    libname outlib "{OUT_DIR}";
    data outlib.npgsexcp;
        set work_excp;
    run;
    """
    
    sas.submit(sas_code)
    print(f"Wrote {OUT_FILE}")
    sas.endsas()
else:
    print("\nNo records to write. Skipping output.")
