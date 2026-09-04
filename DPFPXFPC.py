REPTDATE: 2026-08-31 10:36:01.897689
REPTMON: 08
REPTYEAR2: 26
REPTDAY: 31
NOWK: 4
TESTING MODE:from __future__ import annotations

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

# ---- Inputs (SAS7BDAT) mirroring SAS libs/members ----
MNITB_CURRENT  = BASE_INPUT / "intg_dp_acct_current_m{reptmon}.sas7bdat"
MNILN_LNNOTE   = BASE_INPUT / "enrh_ln_note_m{reptmon}.sas7bdat"
CRFTABL        = BASE_INPUT / "crftabl.txt"

# ---- Output ----
OUT_DIR  = BASE_OUTPUT / "excp"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "npgsexcp.sas7bdat"

# ---- Testing configuration ----
MAX_ROWS_SAS = 50000
MAX_ROWS_TEXT = 50000
# COLL and DESC files should be read completely


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


def read_sas7bdat(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read SAS7BDAT file and convert to Polars DataFrame with lowercase columns."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if max_rows:
        df, meta = pyreadstat.read_sas7bdat(str(file_path), row_limit=max_rows)
        print(f"Read {len(df)} rows from {file_path.name} (limited to {max_rows})")
    else:
        df, meta = pyreadstat.read_sas7bdat(str(file_path))
        print(f"Read {len(df)} rows from {file_path.name}")
    
    pl_df = pl.from_pandas(df)
    pl_df = pl_df.rename({col: col.lower() for col in pl_df.columns})
    
    # Convert acctno to integer if it exists
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
        print(f"Read {len(df)} rows from {file_path.name}" + (f" (limited to {max_rows})" if max_rows else ""))
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
    """
    Unpack a packed decimal (COMP-3) field correctly.
    
    Packed decimal format:
    - Each byte contains two decimal digits (nibbles)
    - The last nibble contains the sign (C = positive, D = negative, F = unsigned)
    - Example: 0x13 0x3F means 133F where F is the sign (positive)
    """
    if not data:
        return 0
    
    # Convert to hex string
    hex_str = data.hex().upper()
    
    # Extract all nibbles
    nibbles = list(hex_str)
    
    # The last nibble is the sign
    sign_nibble = nibbles[-1] if nibbles else 'F'
    
    # Remove the sign nibble
    digit_nibbles = nibbles[:-1]
    
    # Join the digit nibbles to form the number
    if digit_nibbles:
        try:
            value = int(''.join(digit_nibbles))
        except ValueError:
            value = 0
    else:
        value = 0
    
    # Apply sign
    if sign_nibble in ['D', 'B']:  # Negative
        value = -value
    
    # Apply scale (decimal places)
    if scale > 0:
        value = value / (10 ** scale)
    
    return value


def read_coll_binary_efficient(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """
    Read COLL binary file efficiently using numpy for large files.
    
    SAS INPUT:
    @004  CCOLLNO  PD6.  (packed decimal, 6 bytes at position 4)
    @146  ACCTNO   PD6.  (packed decimal, 6 bytes at position 146)
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    print(f"\nReading COLL file: {file_path}")
    file_size = os.path.getsize(file_path)
    print(f"File size: {file_size / (1024**3):.2f} GB")
    
    # Determine record length
    # Based on SAS code, ACCTNO ends at position 151 (146 + 6 - 1)
    # So minimum record length is at least 151 bytes
    # Let's check common record lengths
    record_length = None
    for length in [151, 152, 160, 200, 256, 320, 400, 512, 1024]:
        if file_size % length == 0:
            record_length = length
            break
    
    if record_length is None:
        # Try to find record length by looking at file structure
        # For now, use 151 as minimum
        record_length = 151
        print(f"Using minimum record length: {record_length}")
    else:
        print(f"Detected record length: {record_length}")
    
    # Calculate total records
    total_records = file_size // record_length
    print(f"Total records: {total_records}")
    
    # Read file in chunks to avoid memory issues
    chunk_size = 100000  # Read 100k records at a time
    all_data = []
    
    with open(file_path, 'rb') as f:
        for chunk_start in range(0, total_records, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_records)
            if max_rows and chunk_start >= max_rows:
                break
            
            # Read chunk of records
            records_to_read = min(chunk_end - chunk_start, max_rows - chunk_start if max_rows else chunk_end - chunk_start)
            bytes_to_read = records_to_read * record_length
            
            chunk_data = f.read(bytes_to_read)
            
            # Parse records in this chunk
            for i in range(records_to_read):
                record_start = i * record_length
                record = chunk_data[record_start:record_start + record_length]
                
                if len(record) < 151:
                    continue
                
                # Extract packed decimal fields
                ccollno_bytes = record[3:9]  # @004, 6 bytes
                acctno_bytes = record[145:151]  # @146, 6 bytes
                
                ccollno = unpack_packed_decimal(ccollno_bytes)
                acctno = unpack_packed_decimal(acctno_bytes)
                
                if ccollno > 0 and acctno > 0:  # Only keep valid records
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


def read_desc_text_efficient(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """
    Read DESC file as text with fixed-width format, handling binary characters.
    
    SAS INPUT:
    @001 CCOLLNO   11.  (11-character number at position 1)
    @051 CINSTCL   $2.  (2-character string at position 51)
    @055 NATGUAR   $2.  (2-character string at position 55)
    @211 CENSUS    10.  (10-character number at position 211)
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    print(f"\nReading DESC file: {file_path}")
    file_size = os.path.getsize(file_path)
    print(f"File size: {file_size / (1024**3):.2f} GB")
    
    all_data = []
    record_count = 0
    
    # Read in binary mode and decode with error handling
    with open(file_path, 'rb') as f:
        for line_bytes in f:
            if max_rows and record_count >= max_rows:
                break
            
            # Skip empty lines
            if not line_bytes.strip():
                continue
            
            # Decode with error handling
            try:
                line = line_bytes.decode('utf-8', errors='ignore')
            except:
                line = line_bytes.decode('latin-1', errors='ignore')
            
            # Ensure line is long enough
            if len(line) < 220:
                line = line.ljust(220)
            
            # Parse based on positions
            ccollno_str = line[0:11].strip()
            cinstcl = line[50:52].strip()
            natguar = line[54:56].strip()
            census_str = line[210:220].strip()
            
            try:
                ccollno = int(ccollno_str) if ccollno_str else 0
            except:
                ccollno = 0
            
            try:
                census = int(census_str) if census_str else 0
            except:
                census = 0
            
            # Only keep records with valid ccollno and census in range
            if ccollno > 0 and 51000000 <= census <= 1099999999:
                all_data.append({
                    'ccollno': ccollno,
                    'cinstcl': cinstcl,
                    'natguar': natguar,
                    'census': census
                })
            
            record_count += 1
            
            if record_count % 1000000 == 0:
                print(f"Processed {record_count} records, found {len(all_data)} valid...")
    
    print(f"Total DESC records processed: {record_count}")
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
print(f"TESTING MODE:")
print(f"  - SAS7BDAT files: limited to {MAX_ROWS_SAS} rows")
print(f"  - CRFTABL text file: limited to {MAX_ROWS_TEXT} rows")
print(f"  - COLL/DESC files: reading ALL rows")

# Update file paths
MNITB_CURRENT = Path(str(MNITB_CURRENT).format(reptmon=REPTMON))
MNILN_LNNOTE = Path(str(MNILN_LNNOTE).format(reptmon=REPTMON))
MAST_FILE = BASE_INPUT / f"btmast{REPTMON}{NOWK}{REPTYEAR2}.sas7bdat"
COLL_FILE = BASE_INPUT / f"LCCRISEX_{REPTDATE.year}{REPTMON}{REPTDAY}"
DESC_FILE = BASE_INPUT / f"LCCRISEX_DESC_{REPTDATE.year}{REPTMON}{REPTDAY}"


# =========================
# 2) CRFT from CRFTABL.TXT
# =========================
print("\n" + "="*60)
print("Processing CRFTABL...")
print("="*60)
crft = read_crftabl_fixed_width(CRFTABL, max_rows=MAX_ROWS_TEXT)
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

if not MAST_FILE.exists():
    raise FileNotFoundError(f"Expected MAST file not found: {MAST_FILE}")

print(f"\nReading MAST file: {MAST_FILE}")
mast = read_sas7bdat(MAST_FILE, max_rows=MAX_ROWS_SAS)

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
print(f"CRFT final records: {crft.height}")


# =========================
# 3) CA from MNITB.CURRENT
# =========================
print("\n" + "="*60)
print("Processing MNITB.CURRENT...")
print("="*60)
ca = read_sas7bdat(MNITB_CURRENT, max_rows=MAX_ROWS_SAS)

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
print(f"CA records: {ca.height}")


# =========================
# 4) LN from MNILN.LNNOTE
# =========================
print("\n" + "="*60)
print("Processing MNILN.LNNOTE...")
print("="*60)
ln = read_sas7bdat(MNILN_LNNOTE, max_rows=MAX_ROWS_SAS)

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
    .filter(pl.col("sch") == "   ")
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
coll = read_coll_binary_efficient(COLL_FILE, max_rows=None)
desc = read_desc_text_efficient(DESC_FILE, max_rows=None)

# Join COLL with DESC
if coll.height > 0 and desc.height > 0:
    coll = coll.join(desc, on="ccollno", how="inner")
    print(f"COLL records after merge with DESC: {coll.height}")
    
    # Select only needed columns
    coll = coll.select(["acctno"]).unique(subset=["acctno"], keep="first")
    print(f"COLL unique acctno records: {coll.height}")
else:
    print("COLL or DESC is empty")
    coll = pl.DataFrame({'acctno': pl.Series([], dtype=pl.Int64)})


# =========================
# 6) AAA = SET CA LN CRFT
# =========================
print("\n" + "="*60)
print("Combining CA, LN, CRFT...")
print("="*60)

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

aaa = pl.concat([ca_final, ln_final, crft_final], how="vertical", rechunk=True).sort(by=["acctno"])
print(f"AAA total records: {aaa.height}")


# =========================
# 7) EXCP.NPGSEXCP = MERGE AAA with COLL
# =========================
print("\n" + "="*60)
print("Merging AAA with COLL...")
print("="*60)

if coll.height > 0:
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
    print("\n" + "="*60)
    print("Writing output using SASpy...")
    print("="*60)
    
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
  - SAS7BDAT files: limited to 50000 rows
  - CRFTABL text file: limited to 50000 rows
  - COLL/DESC files: reading ALL rows (no limit for proper joins)

============================================================
Processing CRFTABL...
============================================================
Read 50000 rows from crftabl.txt (limited to 50000)
CRFT records after filter: 50000

Reading MAST file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/btmast08426.sas7bdat
Read 50000 rows from btmast08426.sas7bdat (limited to 50000)
MAST unique acctno records: 18301
CRFT records after MAST join: 23367
CRFT final records: 23367

============================================================
Processing MNITB.CURRENT...
============================================================
Read 50000 rows from intg_dp_acct_current_m08.sas7bdat (limited to 50000)
CA records: 49996

============================================================
Processing MNILN.LNNOTE...
============================================================
Read 50000 rows from enrh_ln_note_m08.sas7bdat (limited to 50000)
LN records: 49964

============================================================
Processing COLL and DESC files...
============================================================

Reading COLL file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831
File exists: True
File size: 809192140 bytes
Text ratio: 29.20%
File appears to be BINARY format
Total bytes: 809192140
Using minimum record length: 151
Record 0: CCOLLNO bytes=00000000133f, ACCTNO bytes=03078959107f
  CCOLLNO=133, ACCTNO=3078959107
Error reading COLL file: invalid literal for int() with base 10: '8959107f091'

Reading DESC file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831
File exists: True
File size: 4750088300 bytes
Text ratio: 88.80%
File appears to be TEXT format
Error reading DESC file: 'utf-8' codec can't decode byte 0xf0 in position 0: invalid continuation byte
COLL records after merge: 0 (empty input)

============================================================
Combining CA, LN, CRFT...
============================================================
AAA total records: 123327

============================================================
Merging AAA with COLL...
============================================================
COLL is empty, creating empty EXCP
EXCP final records: 0

No records to write. Skipping output.
