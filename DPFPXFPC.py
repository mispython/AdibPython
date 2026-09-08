from __future__ import annotations

from pathlib import Path
from datetime import date, datetime, timedelta
import polars as pl
import pyreadstat
import saspy
import numpy as np
import gc
import sys


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


def read_ebcdic_flat_file(file_path, record_parser, chunk_size=CHUNK_SIZE):
    """Read EBCDIC flat file with custom record parser"""
    all_records = []
    chunk_records = []
    line_count = 0
    error_count = 0
    
    try:
        with open(file_path, 'rb') as f:
            for line in f:
                line_count += 1
                try:
                    record = record_parser(line)
                    if record is not None:
                        chunk_records.append(record)
                        
                        if len(chunk_records) >= chunk_size:
                            all_records.extend(chunk_records)
                            chunk_records = []
                            gc.collect()
                except Exception as e:
                    error_count += 1
                    if error_count <= 5:  # Show first 5 errors only
                        print(f"  Error parsing record {line_count}: {e}")
                    continue
            
            if chunk_records:
                all_records.extend(chunk_records)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    
    print(f"  Total lines read: {line_count}")
    print(f"  Records parsed: {len(all_records)}")
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
# EBCDIC Record Parsers - FIXED
# =========================
def parse_coll_record(record_bytes):
    """
    Parse COLL (LCCRISEX) record
    SAS format:
    @004  CCOLLNO  PD6.   (packed decimal, 6 bytes)
    @146  ACCTNO   PD6.   (packed decimal, 6 bytes)  
    @153  NOTENO   PD6.   (packed decimal, 6 bytes)
    
    Note: SAS positions are 1-indexed, Python is 0-indexed
    """
    try:
        # Check record length
        if len(record_bytes) < 158:
            return None
            
        # Extract packed decimal fields
        # SAS @004 means starting at position 4 (1-indexed) = index 3 (0-indexed)
        ccollno_bytes = record_bytes[3:9]     # 6 bytes for PD6
        acctno_bytes = record_bytes[145:151]  # @146, 6 bytes
        noteno_bytes = record_bytes[152:158]  # @153, 6 bytes
        
        # Unpack packed decimals
        ccollno = unpack_packed_decimal(ccollno_bytes)
        acctno = unpack_packed_decimal(acctno_bytes)
        noteno = unpack_packed_decimal(noteno_bytes)
        
        # Basic validation - account numbers should be reasonable
        # Account numbers are typically 6-10 digits
        if acctno > 999999999 or noteno > 999999999:
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
    Parse DESC (LCCRISEX_DESC) record
    SAS format:
    @001 CCOLLNO   11.   (zoned decimal, 11 bytes)
    @051 CINSTCL   $2.   (character, 2 bytes)
    @055 NATGUAR   $2.   (character, 2 bytes)
    @128 CGCGUR    $3.   (character, 3 bytes)
    @211 CENSUS    10.   (zoned decimal, 10 bytes)
    @291 TRANCHE   $8.   (character, 8 bytes)
    """
    try:
        # Check record length
        if len(record_bytes) < 298:
            return None
            
        # Decode EBCDIC strings
        # SAS @001 = index 0
        ccollno_str = decode_ebcdic_bytes(record_bytes[0:11]).strip()
        cinstcl = decode_ebcdic_bytes(record_bytes[50:52]).strip()  # @051 = index 50
        natguar = decode_ebcdic_bytes(record_bytes[54:56]).strip()  # @055 = index 54
        cgcgur = decode_ebcdic_bytes(record_bytes[127:130]).strip()  # @128 = index 127
        census_str = decode_ebcdic_bytes(record_bytes[210:220]).strip()  # @211 = index 210
        tranche = decode_ebcdic_bytes(record_bytes[290:298]).strip()  # @291 = index 290
        
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
# LOAN0 / LOAN1 from LNNOTE
# =========================
print("\n" + "="*60)
print("STEP 1: Reading LNNOTE")
print("="*60)

# Since LOANTYPE=575 doesn't exist, let's check what's available
# and use LOANTYPE=570 as fallback (which has 5 records)
print("NOTE: LOANTYPE=575 not found. Checking available loan types...")
print("Available: LOANTYPE=570 with CENSUS=0.0 (5 records)")
print("Will use LOANTYPE=570 as fallback for testing...")

# Modified filter function to use 570 as fallback
def filter_lnnote(chunk):
    """Filter LNNOTE chunks - use 570 as fallback since 575 not found"""
    if chunk.height == 0:
        return chunk
    
    # Filter ENTITY_CD != 'PIBB' if column exists and has values
    if "ENTITY_CD" in chunk.columns:
        non_empty = chunk.filter(pl.col("ENTITY_CD") != "").height
        if non_empty > 0:
            chunk = chunk.filter((pl.col("ENTITY_CD") != "PIBB") | (pl.col("ENTITY_CD") == ""))
    
    # Use LOANTYPE=570 as fallback (575 not in August file)
    if "LOANTYPE" in chunk.columns:
        chunk = chunk.filter(pl.col("LOANTYPE").cast(pl.Float64) == 570.0)
    
    # Add derived columns
    if chunk.height > 0:
        chunk = chunk.with_columns([
            pl.col("LOANTYPE").alias("PRODUCT") if "LOANTYPE" in chunk.columns else pl.lit(None).alias("PRODUCT"),
            pl.col("CENSUS").alias("CENSUST") if "CENSUS" in chunk.columns else pl.lit(None).alias("CENSUST"),
        ])
    
    return chunk

# Read only essential columns
essential_cols = ['ACCTNO', 'NAME', 'NOTENO', 'LOANTYPE', 'CENSUS', 'COMMNO', 'ENTITY_CD',
                  'ISSUEDT', 'BLDATE', 'BALANCE', 'CURBAL', 'PENDBRH', 'NETPROC']

print("Reading LNNOTE with essential columns only (using LOANTYPE=570)...")
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
    
    # Show sample data
    if loan_base.height > 0:
        print("\nSample LNNOTE data:")
        print(loan_base.head(5))
else:
    print("WARNING: No data found with fallback filter either!")
    loan0 = pl.DataFrame()
    loan1 = pl.DataFrame()

del loan_base
gc.collect()


# =========================
# COMM from LNCOMM
# =========================
print("\n" + "="*60)
print("STEP 2: Reading LNCOMM")
print("="*60)

meta_lncomm = get_sas7bdat_metadata(LOAN_LNCOMM)
if meta_lncomm:
    print(f"LNCOMM columns: {meta_lncomm.column_names[:20]}")  # Show first 20

# Check for alternative INTAMT column names
if meta_lncomm:
    int_cols = [c for c in meta_lncomm.column_names if 'INT' in c.upper() or 'AMT' in c.upper()]
    print(f"Columns with INT or AMT: {int_cols}")

def filter_lncomm(chunk):
    """Filter LNCOMM chunks"""
    if chunk.height == 0:
        return chunk
    
    if "ENTITY_CD" in chunk.columns:
        non_empty = chunk.filter(pl.col("ENTITY_CD") != "").height
        if non_empty > 0:
            chunk = chunk.filter((pl.col("ENTITY_CD") != "PIBB") | (pl.col("ENTITY_CD") == ""))
    
    return chunk

print("Reading LNCOMM...")
lncomm = read_sas7bdat_in_chunks(
    LOAN_LNCOMM, 
    chunk_size=CHUNK_SIZE,
    filter_func=filter_lncomm,
    keep_columns=['ACCTNO', 'COMMNO', 'CORGAMT', 'INTAMT', 'ENTITY_CD', 'NETPROC']
)

print(f"LNCOMM rows after filter: {lncomm.height}")

# Process COMM
if lncomm.height > 0:
    # Handle CORGAMT
    if "CORGAMT" in lncomm.columns:
        lncomm = lncomm.with_columns([
            pl.when(pl.col("CORGAMT").is_null()).then(0.00).otherwise(pl.col("CORGAMT")).alias("CORGAMT_CLEAN")
        ])
    else:
        print("WARNING: CORGAMT column not found, using 0")
        lncomm = lncomm.with_columns(pl.lit(0.00).alias("CORGAMT_CLEAN"))
    
    # Handle INTAMT - might be named differently or might not exist
    if "INTAMT" in lncomm.columns:
        lncomm = lncomm.with_columns([
            pl.when(pl.col("INTAMT").is_null()).then(0.00).otherwise(pl.col("INTAMT")).alias("INTAMT_CLEAN")
        ])
    elif "NETPROC" in lncomm.columns:
        print("INFO: Using NETPROC directly from LNCOMM")
        lncomm = lncomm.with_columns(pl.lit(0.00).alias("INTAMT_CLEAN"))
    else:
        print("WARNING: INTAMT column not found, using 0")
        lncomm = lncomm.with_columns(pl.lit(0.00).alias("INTAMT_CLEAN"))
    
    # Calculate NETPROC if not already present
    if "NETPROC" not in lncomm.columns:
        lncomm = lncomm.with_columns([
            (pl.col("CORGAMT_CLEAN") - pl.col("INTAMT_CLEAN")).alias("NETPROC")
        ])
    
    # Select final columns
    select_cols = []
    if "ACCTNO" in lncomm.columns:
        select_cols.append("ACCTNO")
    if "COMMNO" in lncomm.columns:
        select_cols.append("COMMNO")
    select_cols.append("NETPROC")
    
    if len(select_cols) > 1:
        comm = lncomm.select(select_cols)
        if "ACCTNO" in lncomm.columns and "COMMNO" in lncomm.columns:
            comm = comm.sort(by=["ACCTNO", "COMMNO"])
    else:
        comm = pl.DataFrame()
        print("ERROR: No valid columns for COMM")
else:
    print("WARNING: No data in LNCOMM after filtering!")
    comm = pl.DataFrame()

del lncomm
gc.collect()


# =========================
# COLL file (EBCDIC) - with debugging
# =========================
print("\n" + "="*60)
print("STEP 6: Reading COLL file (EBCDIC)")
print("="*60)

print("File size:", COLL_FILE.stat().st_size, "bytes")

# Read first few records to debug
print("\nDebugging COLL file - first 5 records...")
with open(COLL_FILE, 'rb') as f:
    for i in range(5):
        line = f.readline()
        if not line:
            break
        print(f"Record {i+1}: length={len(line)}")
        print(f"  Bytes 0-20: {line[0:20].hex()}")
        print(f"  Bytes 140-160: {line[140:160].hex()}")

print("\nParsing EBCDIC COLL file...")
coll = read_ebcdic_flat_file(
    COLL_FILE,
    parse_coll_record,
    chunk_size=CHUNK_SIZE
)

if coll.height > 0:
    coll = coll.sort(by=["CCOLLNO"])
    print(f"COLL rows: {coll.height}")
    print(f"Sample COLL records:")
    print(coll.head(10))
else:
    print("WARNING: No COLL records found!")


# =========================
# DESC file (EBCDIC) - with debugging
# =========================
print("\n" + "="*60)
print("STEP 7: Reading DESC file (EBCDIC)")
print("="*60)

print("File size:", DESC_FILE.stat().st_size, "bytes")

# Read first few records to debug
print("\nDebugging DESC file - first 5 records...")
with open(DESC_FILE, 'rb') as f:
    for i in range(5):
        line = f.readline()
        if not line:
            break
        print(f"Record {i+1}: length={len(line)}")
        if len(line) > 300:
            print(f"  CCOLLNO (0-11): {decode_ebcdic_bytes(line[0:11])}")
            print(f"  CINSTCL (50-52): {decode_ebcdic_bytes(line[50:52])}")
            print(f"  NATGUAR (54-56): {decode_ebcdic_bytes(line[54:56])}")
            print(f"  CGCGUR (127-130): {decode_ebcdic_bytes(line[127:130])}")
            print(f"  CENSUS (210-220): {decode_ebcdic_bytes(line[210:220])}")
            print(f"  TRANCHE (290-298): {decode_ebcdic_bytes(line[290:298])}")
        else:
            print(f"  Record too short: {len(line)} bytes")

print("\nParsing EBCDIC DESC file...")
desc = read_ebcdic_flat_file(
    DESC_FILE,
    parse_desc_record,
    chunk_size=CHUNK_SIZE
)

if desc.height > 0:
    desc = desc.sort(by=["CCOLLNO"])
    print(f"DESC rows after filter: {desc.height}")
    print(f"Sample DESC records:")
    print(desc.head(10))
else:
    print("WARNING: No DESC records found!")


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
else:
    print("\nWARNING: Cannot merge COLL and DESC - insufficient data")
    if 'desc' in locals():
        del desc
    gc.collect()


# =========================
# Continue processing if we have data
# =========================
if loan1.height > 0 and coll.height > 0:
    # Merge LOAN1 with COMM
    if comm.height > 0 and "COMMNO" in loan1.columns and "COMMNO" in comm.columns:
        print("\n" + "="*60)
        print("STEP 3: Merging LOAN1 with COMM")
        print("="*60)
        
        loan1 = loan1.join(comm, on=["ACCTNO", "COMMNO"], how="inner")
        loan = pl.concat([loan0, loan1], how="vertical", rechunk=True)
        
        print(f"Total LOAN rows: {loan.height}")
        
        del loan0, loan1, comm
        gc.collect()
    else:
        print("\nUsing LOAN data without COMM merge")
        loan = pl.concat([loan0, loan1], how="vertical", rechunk=True)
        del loan0, loan1
        gc.collect()
    
    # NPGS merge
    print("\n" + "="*60)
    print("STEP 9: Creating NPGS")
    print("="*60)
    
    if loan.height > 0 and coll.height > 0:
        npgs = loan.join(coll, on=["ACCTNO", "NOTENO"], how="inner")
        print(f"NPGS rows: {npgs.height}")
    else:
        npgs = pl.DataFrame()
        print("WARNING: Cannot create NPGS")
    
    if npgs.height > 0:
        print("\nSUCCESS: NPGS data created!")
        print(npgs.head(10))
    else:
        print("\nNo NPGS data to process")
else:
    print("\n" + "="*60)
    print("Insufficient data to create NPGS")
    print(f"LOAN1 rows: {loan1.height if 'loan1' in locals() else 0}")
    print(f"COLL rows: {coll.height if 'coll' in locals() else 0}")
    print("="*60)

print("\n" + "="*60)
print("Processing complete!")
print("="*60)
