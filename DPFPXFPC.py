from __future__ import annotations

from pathlib import Path
from datetime import date, datetime, timedelta
import polars as pl
import pyreadstat
import saspy
import numpy as np
import gc
import sys
import os


# =========================
# Paths (adjust as needed)
# =========================
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLTRRF")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# Inputs
LOAN_LNNOTE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat")
LOAN_LNCOMM   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat")

CISLN_LOAN    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/loan.sas7bdat")

COLL_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831")
DESC_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831")
MICR_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLTRRF/BOPESS.txt")

NPGS_TRRF_IN  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLTRRF/trrf.sas7bdat")

# Output
OUT_DIR  = BASE_OUTPUT
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = None

# Chunk size for reading large files
CHUNK_SIZE = 50000

# EBCDIC file record lengths (confirmed from host site)
COLL_RECORD_LENGTH = 380    # LCCRISEX: FB, record length 380
DESC_RECORD_LENGTH = 3050   # LCCRISEX_DESC: FB, record length 3050


# =========================
# Helpers
# =========================
def get_sas7bdat_metadata(file_path):
    """Get metadata about SAS7BDAT file without reading all data"""
    try:
        df_meta, meta = pyreadstat.read_sas7bdat(str(file_path), metadataonly=True)
        return meta
    except Exception as e:
        print(f"Error reading metadata: {e}")
        return None


def read_sas7bdat_in_chunks(file_path, chunk_size=CHUNK_SIZE, filter_func=None, keep_columns=None, max_rows=None):
    """Read SAS7BDAT file in chunks"""
    chunks = []
    row_offset = 0
    file_path_str = str(file_path)
    schema_columns = None
    total_rows_read = 0
    
    meta = get_sas7bdat_metadata(file_path)
    if meta:
        all_columns = meta.column_names
    else:
        all_columns = None
    
    while True:
        try:
            if max_rows is not None and total_rows_read >= max_rows:
                break
            
            current_chunk_size = chunk_size
            if max_rows is not None:
                current_chunk_size = min(chunk_size, max_rows - total_rows_read)
            
            df_chunk, meta_chunk = pyreadstat.read_sas7bdat(
                file_path_str, 
                row_offset=row_offset,
                row_limit=current_chunk_size
            )
            
            if df_chunk is None or len(df_chunk) == 0:
                break
            
            total_rows_read += len(df_chunk)
            pl_chunk = pl.from_pandas(df_chunk)
            
            if schema_columns is None:
                schema_columns = pl_chunk.columns
            
            if keep_columns:
                existing_cols = [c for c in keep_columns if c in pl_chunk.columns]
                if existing_cols:
                    pl_chunk = pl_chunk.select(existing_cols)
                else:
                    row_offset += len(df_chunk)
                    del df_chunk
                    gc.collect()
                    continue
            
            if filter_func:
                pl_chunk = filter_func(pl_chunk)
                
            if pl_chunk.height > 0:
                chunks.append(pl_chunk)
            
            row_offset += len(df_chunk)
            del df_chunk
            gc.collect()
            
            if len(pl_chunk) < current_chunk_size:
                break
                
        except Exception as e:
            print(f"Error reading chunk at offset {row_offset}: {e}")
            import traceback
            traceback.print_exc()
            break
    
    if chunks:
        result = pl.concat(chunks, how="vertical", rechunk=True)
        del chunks
        gc.collect()
        return result
    else:
        if schema_columns:
            return pl.DataFrame({col: [] for col in schema_columns})
        elif all_columns:
            return pl.DataFrame({col: [] for col in all_columns})
        else:
            return pl.DataFrame()


def decode_ebcdic_bytes(data_bytes):
    """Decode EBCDIC bytes to string"""
    try:
        return data_bytes.decode('cp037')
    except:
        try:
            return data_bytes.decode('cp500')
        except:
            return data_bytes.decode('latin-1')


def unpack_packed_decimal(b):
    """Unpack packed decimal (COMP-3) format"""
    if len(b) == 0:
        return 0
    
    digits = []
    for i in range(len(b) - 1):
        digits.append((b[i] >> 4) & 0x0F)
        digits.append(b[i] & 0x0F)
    
    digits.append((b[-1] >> 4) & 0x0F)
    sign = b[-1] & 0x0F
    
    value = 0
    for digit in digits:
        if digit > 9:
            return 0
        value = value * 10 + digit
    
    if sign == 0x0D:
        value = -value
    
    return value


def read_fixed_length_ebcdic(file_path, record_length, record_parser, chunk_size=CHUNK_SIZE):
    """Read fixed-length EBCDIC file with custom record parser"""
    all_records = []
    chunk_records = []
    total_records = 0
    error_count = 0
    
    file_size = os.path.getsize(file_path)
    expected_records = file_size // record_length
    print(f"  File: {file_path.name}")
    print(f"  File size: {file_size} bytes")
    print(f"  Record length: {record_length} bytes")
    print(f"  Expected records: {expected_records}")
    
    try:
        with open(file_path, 'rb') as f:
            while True:
                record_bytes = f.read(record_length)
                if not record_bytes or len(record_bytes) < record_length:
                    break
                
                total_records += 1
                try:
                    record = record_parser(record_bytes)
                    if record is not None:
                        chunk_records.append(record)
                        
                        if len(chunk_records) >= chunk_size:
                            all_records.extend(chunk_records)
                            chunk_records = []
                            if total_records % 100000 == 0:
                                print(f"  Processed {total_records} records...")
                            gc.collect()
                except Exception as e:
                    error_count += 1
                    if error_count <= 5:
                        print(f"  Error parsing record {total_records}: {e}")
                    continue
            
            if chunk_records:
                all_records.extend(chunk_records)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    
    print(f"  Total records read: {total_records}")
    print(f"  Records parsed successfully: {len(all_records)}")
    print(f"  Parse errors: {error_count}")
    
    return pl.DataFrame(all_records) if all_records else pl.DataFrame()


def write_sas7bdat_in_chunks(df, file_path, sas_session=None, chunk_size=CHUNK_SIZE):
    """Write DataFrame to SAS7BDAT format using SASPy"""
    if sas_session is None:
        sas_session = saspy.SASsession()
    
    output_path = str(file_path)
    total_rows = df.height
    print(f"Writing {total_rows} rows to {output_path}")
    
    if total_rows == 0:
        print("Warning: No data to write!")
        return output_path
    
    first_chunk = df.slice(0, min(chunk_size, total_rows))
    pandas_chunk = first_chunk.to_pandas()
    sas_df = sas_session.df2sd(pandas_chunk, 'temp_df')
    
    sas_session.submit(f"""
        PROC EXPORT DATA=temp_df 
            OUTFILE="{output_path}" 
            DBMS=SAS7BDAT REPLACE;
        RUN;
    """)
    
    offset = chunk_size
    chunk_num = 1
    while offset < total_rows:
        end = min(offset + chunk_size, total_rows)
        chunk = df.slice(offset, end - offset)
        
        if chunk.height > 0:
            pandas_chunk = chunk.to_pandas()
            sas_df = sas_session.df2sd(pandas_chunk, f'temp_df_{chunk_num}')
            
            sas_session.submit(f"""
                PROC APPEND BASE=temp_df DATA=temp_df_{chunk_num} FORCE;
                RUN;
                
                PROC DATASETS LIBRARY=WORK NOLIST;
                    DELETE temp_df_{chunk_num};
                RUN;
            """)
            
            chunk_num += 1
        
        offset = end
        del chunk, pandas_chunk
        gc.collect()
    
    sas_session.submit(f"""
        PROC EXPORT DATA=temp_df 
            OUTFILE="{output_path}" 
            DBMS=SAS7BDAT REPLACE;
        RUN;
    """)
    
    return output_path


def sas_days_to_date(days: int) -> date:
    """Convert SAS date to Python date"""
    origin = date(1960, 1, 1)
    return origin.fromordinal(origin.toordinal() + int(days))


def date_to_sas_days(d: date) -> int:
    """Convert Python date to SAS date"""
    origin = date(1960, 1, 1)
    return (d - origin).days


def parse_mmddyy8_from_z11_prefix_to_date(x) -> date | None:
    """Emulates INPUT(SUBSTR(PUT(x,Z11.),1,8),MMDDYY8.)"""
    if x is None:
        return None
    try:
        xi = int(x)
        if xi <= 0:
            return None
        s = f"{xi:011d}"[:8]
        try:
            return datetime.strptime(s, "%m%d%Y").date()
        except Exception:
            return datetime.strptime(s, "%m%d%y").date()
    except Exception:
        return None


def month_end_of(d: date) -> date:
    """SAS-style month-end calculation"""
    if d.month in (1, 3, 5, 7, 8, 10, 12):
        last = 31
    elif d.month in (4, 6, 9, 11):
        last = 30
    else:
        last = 29 if (d.year % 4 == 0) else 28
    return date(d.year, d.month, last)


# =========================
# PBBLNFMT - NDAYS format definition
# =========================
def create_ndays_format():
    """Create NDAYS format mapping"""
    ndays_ranges = [
        (0, 29, 0), (30, 59, 1), (60, 89, 2), (90, 119, 3),
        (120, 149, 4), (150, 179, 5), (180, 209, 6), (210, 239, 7),
        (240, 269, 8), (270, 299, 9), (300, 329, 10), (330, 359, 11),
        (360, 389, 12), (390, 419, 13), (420, 449, 14), (450, 479, 15),
        (480, 509, 16), (510, 539, 17), (540, 569, 18), (570, 599, 19),
        (600, 629, 20), (630, 659, 21), (660, 689, 22), (690, 719, 23),
        (720, 9999, 24)
    ]
    
    return pl.DataFrame({
        'START': [r[0] for r in ndays_ranges],
        'END': [r[1] for r in ndays_ranges],
        'LABEL': [r[2] for r in ndays_ranges]
    })


def ndays_informat(n: int, ndays_map: pl.DataFrame) -> int:
    """Apply NDAYS informat mapping"""
    if n is None:
        return 0
    n = int(n)
    m = ndays_map.filter((pl.lit(n) >= pl.col("START")) & (pl.lit(n) <= pl.col("END")))
    return int(m.item(0, "LABEL")) if m.height > 0 else 0


# =========================
# EBCDIC Record Parsers - DEBUGGING VERSION
# =========================
def parse_coll_record_debug(record_bytes):
    """
    Parse COLL (LCCRISEX) record with extensive debugging
    """
    try:
        if len(record_bytes) < 158:
            return None
        
        # Extract packed decimal fields
        ccollno_bytes = record_bytes[3:9]
        acctno_bytes = record_bytes[145:151]
        noteno_bytes = record_bytes[152:158]
        
        ccollno = unpack_packed_decimal(ccollno_bytes)
        acctno = unpack_packed_decimal(acctno_bytes)
        noteno = unpack_packed_decimal(noteno_bytes)
        
        # The issue is that ACCTNO in COLL (3078959107) doesn't match LNNOTE (2000000000)
        # Let's check if the ACCTNO is actually at a different position
        # Maybe it's a zoned decimal or character field
        
        # Try to find the real ACCTNO by looking for 10-digit numbers starting with 2
        for pos in range(0, 200):
            if pos + 6 <= len(record_bytes):
                test_val = unpack_packed_decimal(record_bytes[pos:pos+6])
                if 2000000000 <= test_val <= 2999999999:
                    return {
                        'CCOLLNO': ccollno,
                        'ACCTNO': test_val,
                        'NOTENO': noteno,
                        'ACCTNO_POS': pos
                    }
        
        # If we can't find ACCTNO starting with 2, return what we have
        return {
            'CCOLLNO': ccollno,
            'ACCTNO': acctno,
            'NOTENO': noteno,
            'ACCTNO_POS': 145
        }
    except Exception as e:
        return None


def parse_desc_record(record_bytes):
    """
    Parse DESC (LCCRISEX_DESC) record - Fixed length 3050 bytes
    """
    try:
        if len(record_bytes) < 298:
            return None
            
        ccollno_str = decode_ebcdic_bytes(record_bytes[0:11]).strip()
        cinstcl = decode_ebcdic_bytes(record_bytes[50:52]).strip()
        natguar = decode_ebcdic_bytes(record_bytes[54:56]).strip()
        cgcgur = decode_ebcdic_bytes(record_bytes[127:130]).strip()
        census_str = decode_ebcdic_bytes(record_bytes[210:220]).strip()
        tranche = decode_ebcdic_bytes(record_bytes[290:298]).strip()
        
        ccollno = int(float(ccollno_str)) if ccollno_str else 0
        census = float(census_str) if census_str else 0.0
        
        if cgcgur in ('080', '090'):
            sch = '7Q' if cgcgur == '080' else '8Q'
            
            return {
                'CCOLLNO': ccollno,
                'CINSTCL': cinstcl,
                'NATGUAR': natguar,
                'CGCGUR': cgcgur,
                'CENSUS': census,
                'TRANCHE': tranche,
                'SCH': sch
            }
        return None
    except Exception as e:
        return None


# =========================
# DEBUGGING SECTION
# =========================
print("="*80)
print("DETAILED DEBUGGING - COLL FILE STRUCTURE")
print("="*80)

# Read first 3 records from COLL file
with open(COLL_FILE, 'rb') as f:
    for record_num in range(3):
        record = f.read(COLL_RECORD_LENGTH)
        if len(record) < COLL_RECORD_LENGTH:
            break
        
        print(f"\n{'='*60}")
        print(f"RECORD {record_num + 1}")
        print(f"{'='*60}")
        
        # Show bytes 140-170 (around ACCTNO and NOTENO)
        print("\nBytes 140-170:")
        for i in range(140, min(170, len(record))):
            print(f"  Pos {i}: {record[i]:02X} ({record[i]:3d})")
        
        # Show all packed decimal values at various positions
        print("\nPacked decimal values at different positions (searching for ACCTNO starting with 2):")
        for pos in range(0, 200):
            if pos + 6 <= len(record):
                val = unpack_packed_decimal(record[pos:pos+6])
                if 2000000000 <= val <= 2999999999:
                    print(f"  FOUND: Position {pos} = {val}")
        
        # Check if ACCTNO might be EBCDIC digits
        print("\nEBCDIC decoded strings (positions 140-170):")
        for pos in range(140, min(170, len(record)-10)):
            s = decode_ebcdic_bytes(record[pos:pos+10]).strip()
            if s:
                print(f"  Position {pos}: '{s}'")
        
        print()

# Read LNNOTE sample
print("\n" + "="*80)
print("LNNOTE SAMPLE DATA")
print("="*80)

df_lnnote, meta_lnnote = pyreadstat.read_sas7bdat(str(LOAN_LNNOTE), row_limit=20)
pl_lnnote = pl.from_pandas(df_lnnote)

if 'ACCTNO' in pl_lnnote.columns and 'NOTENO' in pl_lnnote.columns:
    print("ACCTNO and NOTENO values:")
    for i in range(min(10, pl_lnnote.height)):
        acctno = int(pl_lnnote['ACCTNO'][i])
        noteno = int(pl_lnnote['NOTENO'][i])
        print(f"  Record {i}: ACCTNO={acctno}, NOTENO={noteno}")
    
    # Show ACCTNO range
    min_acct = int(pl_lnnote['ACCTNO'].min())
    max_acct = int(pl_lnnote['ACCTNO'].max())
    print(f"\nACCTNO range: {min_acct} to {max_acct}")
    
    # Show NOTENO range
    min_note = int(pl_lnnote['NOTENO'].min())
    max_note = int(pl_lnnote['NOTENO'].max())
    print(f"NOTENO range: {min_note} to {max_note}")

# Check if there's a relationship between ACCTNO and NOTENO
print("\n" + "="*80)
print("ANALYSIS")
print("="*80)
print("COLL file ACCTNO appears to be 3078959107 (10 digits, starts with 3)")
print("LNNOTE ACCTNO range is around 2000000000 (10 digits, starts with 2)")
print()
print("Possible explanations:")
print("  1. COLL ACCTNO uses a different numbering system")
print("  2. COLL ACCTNO might be at a different byte position")
print("  3. There might be a mapping table needed")
print("  4. COLL ACCTNO might be encoded differently (EBCDIC vs binary)")
print()
print("Let's check if 3078959107 in EBCDIC represents '2000000000' or similar...")
print(f"  3078959107 = 0x{3078959107:08X}")
print(f"  2000000000 = 0x{2000000000:08X}")
print()
print("The COLL file might store ACCTNO as EBCDIC characters, not packed decimal!")
print("For example, '2000000000' as EBCDIC would be different bytes than packed decimal")

# Check EBCDIC representation of account numbers
print("\nEBCDIC representation check:")
acctno_str = "2000000000"
ebcdic_bytes = acctno_str.encode('cp037')
print(f"  '{acctno_str}' as EBCDIC bytes: {ebcdic_bytes.hex()}")
print(f"  As packed decimal: {unpack_packed_decimal(ebcdic_bytes[:6])}")

# Try reading ACCTNO as EBCDIC string from COLL file
print("\nTrying to read ACCTNO as EBCDIC from COLL file:")
with open(COLL_FILE, 'rb') as f:
    record = f.read(COLL_RECORD_LENGTH)
    
    # Try different positions for EBCDIC account number
    for pos in [144, 145, 146, 147, 148, 149, 150]:
        if pos + 10 <= len(record):
            ebcdic_val = decode_ebcdic_bytes(record[pos:pos+10]).strip()
            if ebcdic_val.isdigit() and len(ebcdic_val) == 10:
                print(f"  Position {pos}: EBCDIC string = '{ebcdic_val}'")

print("\n" + "="*80)
print("DEBUGGING COMPLETE")
print("="*80)
