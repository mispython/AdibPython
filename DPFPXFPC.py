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

# ---- Testing configuration ----
MAX_ROWS_SAS = 50000
MAX_ROWS_TEXT = 50000


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


def debug_desc_file(file_path: Path):
    """Debug function to understand DESC file structure."""
    print(f"\nDebugging DESC file: {file_path}")
    print(f"File size: {os.path.getsize(file_path)} bytes")
    
    with open(file_path, 'rb') as f:
        # Read first 2000 bytes
        data = f.read(2000)
        
        print(f"\nFirst 500 bytes (hex):")
        print(data[:500].hex())
        
        print(f"\nFirst 500 bytes (ASCII):")
        print(data[:500].decode('ascii', errors='replace'))
        
        print(f"\nFirst 500 bytes (latin-1):")
        print(data[:500].decode('latin-1', errors='replace'))
        
        # Check for newlines
        newline_count = data.count(b'\n')
        carriage_return_count = data.count(b'\r')
        print(f"\nNewlines in first 2000 bytes: {newline_count}")
        print(f"Carriage returns in first 2000 bytes: {carriage_return_count}")
        
        # Check for patterns
        print(f"\nLooking for patterns...")
        
        # Try to find if it's line-delimited
        lines = data.split(b'\n')
        if len(lines) > 1:
            print(f"Found {len(lines)} lines in first 2000 bytes")
            print(f"First line length: {len(lines[0])}")
            print(f"First line (hex): {lines[0][:100].hex()}")
            print(f"First line (ascii): {lines[0][:100].decode('ascii', errors='replace')}")
        else:
            print("No newlines found - fixed-width format")
            
            # Try to find a pattern - look for repeating structures
            # Check if there's a consistent record length
            for record_len in [200, 220, 240, 250, 256, 300, 320, 400, 500, 512, 640, 800, 1000]:
                if len(data) >= record_len * 2:
                    # Compare first and second "records"
                    rec1 = data[:record_len]
                    rec2 = data[record_len:record_len*2]
                    
                    # Check if they have similar structure
                    similarity = sum(1 for a, b in zip(rec1, rec2) if a == b) / record_len
                    
                    if similarity > 0.5:
                        print(f"Possible record length {record_len}: {similarity:.2%} similarity")
                        print(f"  Record 1 first 50 bytes: {rec1[:50].hex()}")
                        print(f"  Record 1 ascii: {rec1[:50].decode('ascii', errors='replace')}")


def read_desc_fixed_width_binary(file_path: Path, max_rows: int = None) -> pl.DataFrame:
    """Read DESC file as fixed-width binary/text."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    print(f"\nReading DESC file: {file_path}")
    file_size = os.path.getsize(file_path)
    print(f"File size: {file_size / (1024**3):.2f} GB")
    
    # Debug first to understand structure
    debug_desc_file(file_path)
    
    # Based on SAS INPUT, the fields are at specific positions
    # @001 CCOLLNO   11.  - 11 chars at position 1
    # @051 CINSTCL   $2.  - 2 chars at position 51
    # @055 NATGUAR   $2.  - 2 chars at position 55
    # @211 CENSUS    10.  - 10 chars at position 211
    
    # The DESC file might actually be line-delimited, not fixed-width
    # Let's try reading line by line first
    print("\nTrying to read as line-delimited file...")
    
    all_data = []
    line_count = 0
    
    try:
        with open(file_path, 'rb') as f:
            for line_bytes in f:
                if max_rows and line_count >= max_rows:
                    break
                
                if not line_bytes.strip():
                    continue
                
                # Decode with error handling
                line = line_bytes.decode('latin-1', errors='ignore')
                
                # Print first few lines for debugging
                if line_count < 3:
                    print(f"Line {line_count}: {line[:220]}")
                    print(f"Line {line_count} length: {len(line)}")
                
                # Parse based on positions (1-based)
                ccollno_str = line[0:11].strip() if len(line) >= 11 else ""
                cinstcl = line[50:52].strip() if len(line) >= 52 else ""
                natguar = line[54:56].strip() if len(line) >= 56 else ""
                census_str = line[210:220].strip() if len(line) >= 220 else ""
                
                try:
                    ccollno = int(ccollno_str) if ccollno_str else 0
                except:
                    ccollno = 0
                
                try:
                    census = int(census_str) if census_str else 0
                except:
                    census = 0
                
                if ccollno > 0 and 51000000 <= census <= 1099999999:
                    all_data.append({
                        'ccollno': ccollno,
                        'cinstcl': cinstcl,
                        'natguar': natguar,
                        'census': census
                    })
                
                line_count += 1
                
                if line_count % 1000000 == 0:
                    print(f"Processed {line_count} lines, found {len(all_data)} valid...")
    except Exception as e:
        print(f"Error reading line by line: {e}")
    
    print(f"Total lines processed: {line_count}")
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
desc = read_desc_fixed_width_binary(DESC_FILE, max_rows=None)

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
