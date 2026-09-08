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
    """
    Read fixed-length EBCDIC file with custom record parser
    
    Args:
        file_path: Path to EBCDIC file
        record_length: Length of each record in bytes
        record_parser: Function that takes bytes for one record and returns dict
        chunk_size: Number of records to process at a time
    """
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
# EBCDIC Record Parsers
# =========================
def parse_coll_record(record_bytes):
    """
    Parse COLL (LCCRISEX) record - Fixed length 380 bytes
    SAS format:
    @004  CCOLLNO  PD6.   (packed decimal, 6 bytes)
    @146  ACCTNO   PD6.   (packed decimal, 6 bytes)
    @153  NOTENO   PD6.   (packed decimal, 6 bytes)
    """
    try:
        if len(record_bytes) < 158:
            return None
            
        # Extract packed decimal fields (0-indexed)
        ccollno_bytes = record_bytes[3:9]     # @004 = index 3, 6 bytes
        acctno_bytes = record_bytes[145:151]  # @146 = index 145, 6 bytes
        noteno_bytes = record_bytes[152:158]  # @153 = index 152, 6 bytes
        
        # Unpack packed decimals
        ccollno = unpack_packed_decimal(ccollno_bytes)
        acctno = unpack_packed_decimal(acctno_bytes)
        noteno = unpack_packed_decimal(noteno_bytes)
        
        # Basic validation - account numbers should be reasonable
        # Account numbers are typically 10 digits or less
        if acctno > 9999999999 or noteno > 9999999999:
            return None
        if acctno == 0 or noteno == 0:
            return None
            
        return {
            'CCOLLNO': ccollno,
            'ACCTNO': acctno,
            'NOTENO': noteno
        }
    except Exception as e:
        return None


def parse_desc_record(record_bytes):
    """
    Parse DESC (LCCRISEX_DESC) record - Fixed length 3050 bytes
    SAS format:
    @001 CCOLLNO   11.   (zoned decimal, 11 bytes)
    @051 CINSTCL   $2.   (character, 2 bytes)
    @055 NATGUAR   $2.   (character, 2 bytes)
    @128 CGCGUR    $3.   (character, 3 bytes)
    @211 CENSUS    10.   (zoned decimal, 10 bytes)
    @291 TRANCHE   $8.   (character, 8 bytes)
    """
    try:
        if len(record_bytes) < 298:
            return None
            
        # Decode EBCDIC strings (0-indexed positions)
        ccollno_str = decode_ebcdic_bytes(record_bytes[0:11]).strip()      # @001 = index 0
        cinstcl = decode_ebcdic_bytes(record_bytes[50:52]).strip()          # @051 = index 50
        natguar = decode_ebcdic_bytes(record_bytes[54:56]).strip()          # @055 = index 54
        cgcgur = decode_ebcdic_bytes(record_bytes[127:130]).strip()         # @128 = index 127
        census_str = decode_ebcdic_bytes(record_bytes[210:220]).strip()     # @211 = index 210
        tranche = decode_ebcdic_bytes(record_bytes[290:298]).strip()        # @291 = index 290
        
        # Convert numeric fields
        ccollno = float(ccollno_str) if ccollno_str else 0.0
        census = float(census_str) if census_str else 0.0
        
        # Filter for CGCGUR IN ('080','090')
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
# REPTDATE - Using datetime timedelta - 1 day
# =========================
REPTDATE = datetime.now().date() - timedelta(days=1)
REPTMON  = f"{REPTDATE.month:02d}"
REPTDAY  = f"{REPTDATE.day:02d}"
REPTYEAR = f"{REPTDATE.year:04d}"
SDATE = date_to_sas_days(REPTDATE)
OUT_FILE  = OUT_DIR / f"LNTRRF{REPTMON}.sas7bdat"

print("="*60)
print(f"Processing date: {REPTDATE} (SAS date: {SDATE})")
print("="*60)


# =========================
# LOAN0 / LOAN1 from LNNOTE (using LOANTYPE=570 as fallback)
# =========================
print("\n" + "="*60)
print("STEP 1: Reading LNNOTE (using LOANTYPE=570)")
print("="*60)

def filter_lnnote(chunk):
    """Filter LNNOTE chunks"""
    if chunk.height == 0:
        return chunk
    
    if "ENTITY_CD" in chunk.columns:
        non_empty = chunk.filter(pl.col("ENTITY_CD") != "").height
        if non_empty > 0:
            chunk = chunk.filter((pl.col("ENTITY_CD") != "PIBB") | (pl.col("ENTITY_CD") == ""))
    
    if "LOANTYPE" in chunk.columns:
        chunk = chunk.filter(pl.col("LOANTYPE").cast(pl.Float64) == 570.0)
    
    if chunk.height > 0:
        chunk = chunk.with_columns([
            pl.col("LOANTYPE").alias("PRODUCT") if "LOANTYPE" in chunk.columns else pl.lit(None).alias("PRODUCT"),
            pl.col("CENSUS").alias("CENSUST") if "CENSUS" in chunk.columns else pl.lit(None).alias("CENSUST"),
        ])
    
    return chunk

essential_cols = ['ACCTNO', 'NAME', 'NOTENO', 'LOANTYPE', 'CENSUS', 'COMMNO', 'ENTITY_CD',
                  'ISSUEDT', 'BLDATE', 'BALANCE', 'CURBAL', 'PENDBRH', 'NETPROC']

loan_base = read_sas7bdat_in_chunks(
    LOAN_LNNOTE, 
    chunk_size=CHUNK_SIZE,
    filter_func=filter_lnnote,
    keep_columns=essential_cols
)

print(f"Filtered LNNOTE rows: {loan_base.height}")

if loan_base.height > 0 and "COMMNO" in loan_base.columns:
    loan1 = loan_base.filter(pl.col("COMMNO") > 0)
    loan0 = loan_base.filter(~(pl.col("COMMNO") > 0))
    print(f"LOAN0: {loan0.height} rows, LOAN1: {loan1.height} rows")
else:
    loan0 = pl.DataFrame()
    loan1 = pl.DataFrame()

del loan_base
gc.collect()


# =========================
# COLL file (EBCDIC) - Fixed length 380
# =========================
print("\n" + "="*60)
print("STEP 6: Reading COLL file (EBCDIC, FB 380)")
print("="*60)

coll = read_fixed_length_ebcdic(
    COLL_FILE,
    COLL_RECORD_LENGTH,
    parse_coll_record,
    chunk_size=10000
)

if coll.height > 0:
    coll = coll.sort(by=["CCOLLNO"])
    print(f"\nCOLL rows parsed: {coll.height}")
    print(f"Sample COLL records:")
    print(coll.head(10))
else:
    print("\nWARNING: No COLL records parsed!")
    print("Debugging first record...")
    with open(COLL_FILE, 'rb') as f:
        first_record = f.read(COLL_RECORD_LENGTH)
        print(f"Record length: {len(first_record)}")
        print(f"Bytes 0-20: {first_record[0:20].hex()}")
        print(f"Bytes 140-160: {first_record[140:160].hex()}")
        # Try to decode as packed decimal
        print(f"\nPacked decimal at positions:")
        print(f"  CCOLLNO (3-9): {first_record[3:9].hex()} = {unpack_packed_decimal(first_record[3:9])}")
        print(f"  ACCTNO (145-151): {first_record[145:151].hex()} = {unpack_packed_decimal(first_record[145:151])}")
        print(f"  NOTENO (152-158): {first_record[152:158].hex()} = {unpack_packed_decimal(first_record[152:158])}")


# =========================
# DESC file (EBCDIC) - Fixed length 3050
# =========================
print("\n" + "="*60)
print("STEP 7: Reading DESC file (EBCDIC, FB 3050)")
print("="*60)

desc = read_fixed_length_ebcdic(
    DESC_FILE,
    DESC_RECORD_LENGTH,
    parse_desc_record,
    chunk_size=10000
)

if desc.height > 0:
    desc = desc.sort(by=["CCOLLNO"])
    print(f"\nDESC rows parsed: {desc.height}")
    print(f"Sample DESC records:")
    print(desc.head(10))
else:
    print("\nWARNING: No DESC records parsed!")
    print("Debugging first record...")
    with open(DESC_FILE, 'rb') as f:
        first_record = f.read(DESC_RECORD_LENGTH)
        print(f"Record length: {len(first_record)}")
        print(f"CCOLLNO (0-11): {decode_ebcdic_bytes(first_record[0:11])}")
        print(f"CINSTCL (50-52): {decode_ebcdic_bytes(first_record[50:52])}")
        print(f"NATGUAR (54-56): {decode_ebcdic_bytes(first_record[54:56])}")
        print(f"CGCGUR (127-130): {decode_ebcdic_bytes(first_record[127:130])}")
        print(f"CENSUS (210-220): {decode_ebcdic_bytes(first_record[210:220])}")
        print(f"TRANCHE (290-298): {decode_ebcdic_bytes(first_record[290:298])}")


# =========================
# Merge COLL and DESC
# =========================
if coll.height > 0 and desc.height > 0:
    print("\n" + "="*60)
    print("STEP 8: Merging COLL and DESC")
    print("="*60)
    
    coll = coll.join(desc, on="CCOLLNO", how="inner")
    coll = coll.filter((pl.col("CINSTCL") == "18") & (pl.col("NATGUAR") == "06"))
    coll = coll.sort(by=["ACCTNO", "NOTENO"])
    
    print(f"COLL rows after merge: {coll.height}")
    
    del desc
    gc.collect()
elif coll.height > 0:
    print("\nUsing COLL data without DESC merge")
else:
    print("\nNo COLL data available")


# =========================
# Continue with processing
# =========================
if loan1.height > 0 and coll.height > 0:
    # NPGS merge
    print("\n" + "="*60)
    print("STEP 9: Creating NPGS")
    print("="*60)
    
    if loan1.height > 0 and coll.height > 0:
        # Ensure data types match for join
        if "ACCTNO" in loan1.columns and "ACCTNO" in coll.columns:
            loan1 = loan1.with_columns(pl.col("ACCTNO").cast(pl.Float64))
            coll = coll.with_columns(pl.col("ACCTNO").cast(pl.Float64))
        if "NOTENO" in loan1.columns and "NOTENO" in coll.columns:
            loan1 = loan1.with_columns(pl.col("NOTENO").cast(pl.Float64))
            coll = coll.with_columns(pl.col("NOTENO").cast(pl.Float64))
        
        npgs = loan1.join(coll, on=["ACCTNO", "NOTENO"], how="inner")
        print(f"NPGS rows: {npgs.height}")
        
        if npgs.height > 0:
            print("\nSample NPGS data:")
            print(npgs.head(10))
    else:
        npgs = pl.DataFrame()
        print("Cannot create NPGS - missing data")
else:
    print("\n" + "="*60)
    print("Insufficient data to create NPGS")
    print(f"LOAN1 rows: {loan1.height if 'loan1' in locals() else 0}")
    print(f"COLL rows: {coll.height if 'coll' in locals() else 0}")
    print("="*60)

print("\n" + "="*60)
print("Processing complete!")
print("="*60)
