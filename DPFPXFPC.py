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
    """
    Read SAS7BDAT file in chunks and optionally filter/select columns
    
    Args:
        file_path: Path to SAS7BDAT file
        chunk_size: Number of rows to read at a time
        filter_func: Function to filter each chunk
        keep_columns: List of columns to keep
        max_rows: Maximum number of rows to read (None for all)
    """
    chunks = []
    row_offset = 0
    file_path_str = str(file_path)
    schema_columns = None
    total_rows_read = 0
    
    # Get metadata
    meta = get_sas7bdat_metadata(file_path)
    if meta:
        all_columns = meta.column_names
    else:
        all_columns = None
    
    while True:
        try:
            # Check if we've reached max_rows
            if max_rows is not None and total_rows_read >= max_rows:
                break
            
            # Calculate chunk size for this iteration
            current_chunk_size = chunk_size
            if max_rows is not None:
                current_chunk_size = min(chunk_size, max_rows - total_rows_read)
            
            # Read a chunk
            df_chunk, meta_chunk = pyreadstat.read_sas7bdat(
                file_path_str, 
                row_offset=row_offset,
                row_limit=current_chunk_size
            )
            
            if df_chunk is None or len(df_chunk) == 0:
                break
            
            total_rows_read += len(df_chunk)
            
            # Convert to polars
            pl_chunk = pl.from_pandas(df_chunk)
            
            # Store schema from first chunk
            if schema_columns is None:
                schema_columns = pl_chunk.columns
            
            # Apply column selection if specified
            if keep_columns:
                existing_cols = [c for c in keep_columns if c in pl_chunk.columns]
                if existing_cols:
                    pl_chunk = pl_chunk.select(existing_cols)
                else:
                    row_offset += len(df_chunk)
                    del df_chunk
                    gc.collect()
                    continue
            
            # Apply filter function if specified
            if filter_func:
                pl_chunk = filter_func(pl_chunk)
                
            # Only keep non-empty chunks
            if pl_chunk.height > 0:
                chunks.append(pl_chunk)
            
            # Move to next chunk
            row_offset += len(df_chunk)
            
            # Clear memory
            del df_chunk
            gc.collect()
            
            # Break if we got less than chunk_size (end of file)
            if len(pl_chunk) < current_chunk_size:
                break
                
        except Exception as e:
            print(f"Error reading chunk at offset {row_offset}: {e}")
            import traceback
            traceback.print_exc()
            break
    
    # Concatenate all chunks
    if chunks:
        result = pl.concat(chunks, how="vertical", rechunk=True)
        del chunks
        gc.collect()
        return result
    else:
        # Return empty DataFrame with proper schema
        if schema_columns:
            empty_df = pl.DataFrame({col: [] for col in schema_columns})
            return empty_df
        elif all_columns:
            empty_df = pl.DataFrame({col: [] for col in all_columns})
            return empty_df
        else:
            return pl.DataFrame()


def decode_ebcdic_bytes(data_bytes):
    """Decode EBCDIC bytes to string"""
    try:
        return data_bytes.decode('cp037')  # IBM EBCDIC US-Canada
    except:
        try:
            return data_bytes.decode('cp500')  # IBM EBCDIC International
        except:
            return data_bytes.decode('latin-1')


def unpack_packed_decimal(b):
    """Unpack packed decimal (COMP-3) format"""
    if len(b) == 0:
        return 0
    
    # Packed decimal: each byte contains 2 digits except last which has 1 digit + sign
    digits = []
    for i in range(len(b) - 1):
        digits.append((b[i] >> 4) & 0x0F)
        digits.append(b[i] & 0x0F)
    
    # Last byte: 1 digit + sign
    digits.append((b[-1] >> 4) & 0x0F)
    sign = b[-1] & 0x0F
    
    # Convert to integer
    value = 0
    for digit in digits:
        if digit > 9:  # Invalid digit
            return 0
        value = value * 10 + digit
    
    # Check sign (0xC, 0xF = positive, 0xD = negative)
    if sign == 0x0D:
        value = -value
    
    return value


def read_ebcdic_flat_file(file_path, record_parser, chunk_size=CHUNK_SIZE):
    """
    Read EBCDIC flat file with custom record parser
    
    Args:
        file_path: Path to EBCDIC file
        record_parser: Function that takes bytes for one record and returns dict
        chunk_size: Number of records to process at a time
    """
    all_records = []
    chunk_records = []
    
    try:
        with open(file_path, 'rb') as f:
            for line in f:
                try:
                    record = record_parser(line)
                    if record is not None:
                        chunk_records.append(record)
                        
                        if len(chunk_records) >= chunk_size:
                            all_records.extend(chunk_records)
                            chunk_records = []
                            gc.collect()
                except Exception as e:
                    continue
            
            # Add remaining records
            if chunk_records:
                all_records.extend(chunk_records)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    
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
    
    # First chunk - create the dataset
    first_chunk = df.slice(0, min(chunk_size, total_rows))
    pandas_chunk = first_chunk.to_pandas()
    sas_df = sas_session.df2sd(pandas_chunk, 'temp_df')
    
    # Export first chunk
    sas_session.submit(f"""
        PROC EXPORT DATA=temp_df 
            OUTFILE="{output_path}" 
            DBMS=SAS7BDAT REPLACE;
        RUN;
    """)
    
    # Write remaining chunks
    offset = chunk_size
    chunk_num = 1
    while offset < total_rows:
        end = min(offset + chunk_size, total_rows)
        chunk = df.slice(offset, end - offset)
        
        if chunk.height > 0:
            pandas_chunk = chunk.to_pandas()
            sas_df = sas_session.df2sd(pandas_chunk, f'temp_df_{chunk_num}')
            
            # Append to existing dataset
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
    
    # Final export of complete dataset
    sas_session.submit(f"""
        PROC EXPORT DATA=temp_df 
            OUTFILE="{output_path}" 
            DBMS=SAS7BDAT REPLACE;
        RUN;
    """)
    
    return output_path


def sas_days_to_date(days: int) -> date:
    """Convert SAS date (days since 1960-01-01) to Python date"""
    origin = date(1960, 1, 1)
    return origin.fromordinal(origin.toordinal() + int(days))


def date_to_sas_days(d: date) -> int:
    """Convert Python date to SAS date (days since 1960-01-01)"""
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
        (0, 29, 0),    # 0 months
        (30, 59, 1),   # 1 month
        (60, 89, 2),   # 2 months
        (90, 119, 3),  # 3 months
        (120, 149, 4), # 4 months
        (150, 179, 5), # 5 months
        (180, 209, 6), # 6 months
        (210, 239, 7), # 7 months
        (240, 269, 8), # 8 months
        (270, 299, 9), # 9 months
        (300, 329, 10), # 10 months
        (330, 359, 11), # 11 months
        (360, 389, 12), # 12 months
        (390, 419, 13), # 13 months
        (420, 449, 14), # 14 months
        (450, 479, 15), # 15 months
        (480, 509, 16), # 16 months
        (510, 539, 17), # 17 months
        (540, 569, 18), # 18 months
        (570, 599, 19), # 19 months
        (600, 629, 20), # 20 months
        (630, 659, 21), # 21 months
        (660, 689, 22), # 22 months
        (690, 719, 23), # 23 months
        (720, 9999, 24), # 24+ months
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
    Parse COLL (LCCRISEX) record
    Format:
    @004  CCOLLNO  PD6.   (packed decimal, 6 bytes)
    @146  ACCTNO   PD6.   (packed decimal, 6 bytes)
    @153  NOTENO   PD6.   (packed decimal, 6 bytes)
    """
    try:
        # Extract packed decimal fields (0-indexed positions)
        ccollno_bytes = record_bytes[3:9]     # @004, 6 bytes
        acctno_bytes = record_bytes[145:151]  # @146, 6 bytes
        noteno_bytes = record_bytes[152:158]  # @153, 6 bytes
        
        # Unpack packed decimals
        ccollno = unpack_packed_decimal(ccollno_bytes)
        acctno = unpack_packed_decimal(acctno_bytes)
        noteno = unpack_packed_decimal(noteno_bytes)
        
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
    Format:
    @001 CCOLLNO   11.   (zoned decimal, 11 bytes)
    @051 CINSTCL   $2.   (character, 2 bytes)
    @055 NATGUAR   $2.   (character, 2 bytes)
    @128 CGCGUR    $3.   (character, 3 bytes)
    @211 CENSUS    10.   (zoned decimal, 10 bytes)
    @291 TRANCHE   $8.   (character, 8 bytes)
    """
    try:
        # Decode EBCDIC strings
        ccollno_str = decode_ebcdic_bytes(record_bytes[0:11]).strip()
        cinstcl = decode_ebcdic_bytes(record_bytes[50:52]).strip()
        natguar = decode_ebcdic_bytes(record_bytes[54:56]).strip()
        cgcgur = decode_ebcdic_bytes(record_bytes[127:130]).strip()
        census_str = decode_ebcdic_bytes(record_bytes[210:220]).strip()
        tranche = decode_ebcdic_bytes(record_bytes[290:298]).strip()
        
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

# Get metadata first
meta = get_sas7bdat_metadata(LOAN_LNNOTE)
if meta:
    essential_cols = ['ACCTNO', 'NOTENO', 'LOANTYPE', 'CENSUS', 'COMMNO', 'ENTITY_CD', 
                      'ISSUEDT', 'BLDATE', 'BALANCE', 'CURBAL', 'NAME']
    available_essential = [c for c in essential_cols if c in meta.column_names]
    print(f"Essential columns available: {available_essential}")

# Modified filter function - more flexible
def filter_lnnote(chunk):
    """Filter LNNOTE chunks - accept various loan types"""
    if chunk.height == 0:
        return chunk
    
    # Don't filter ENTITY_CD if it's empty (conventional loans might have empty ENTITY_CD)
    if "ENTITY_CD" in chunk.columns:
        # Only filter out 'PIBB' if there are non-empty values
        non_empty_entity = chunk.filter(pl.col("ENTITY_CD") != "").height
        if non_empty_entity > 0:
            chunk = chunk.filter((pl.col("ENTITY_CD") != "PIBB") | (pl.col("ENTITY_CD") == ""))
    
    # Try different loan type values - check if 575 exists in different formats
    if "LOANTYPE" in chunk.columns:
        # Original SAS: IF LOANTYPE=575 AND CENSUS=575.09
        # Try both integer and float comparison
        chunk = chunk.filter(
            (pl.col("LOANTYPE").cast(pl.Float64) == 575.0) | 
            (pl.col("LOANTYPE").cast(pl.Int64, strict=False) == 575)
        )
    
    if "CENSUS" in chunk.columns:
        # Filter for CENSUS=575.09
        chunk = chunk.filter(pl.col("CENSUS").cast(pl.Float64) == 575.09)
    
    # Add derived columns
    if chunk.height > 0:
        chunk = chunk.with_columns([
            pl.col("LOANTYPE").alias("PRODUCT") if "LOANTYPE" in chunk.columns else pl.lit(None).alias("PRODUCT"),
            pl.col("CENSUS").alias("CENSUST") if "CENSUS" in chunk.columns else pl.lit(None).alias("CENSUST"),
        ])
    
    return chunk

# Read only essential columns from LNNOTE
essential_cols = ['ACCTNO', 'NAME', 'NOTENO', 'LOANTYPE', 'CENSUS', 'COMMNO', 'ENTITY_CD',
                  'ISSUEDT', 'BLDATE', 'BALANCE', 'CURBAL', 'PENDBRH', 'NETPROC']

print("Reading LNNOTE with essential columns only...")
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
    print("WARNING: No data with LOANTYPE=575 and CENSUS=575.09 found!")
    print("This is the August file - loan type 575 might not exist in this month.")
    print("Checking what loan types are available...")
    
    # Check what loan types exist with their census values
    sample = read_sas7bdat_in_chunks(
        LOAN_LNNOTE, 
        chunk_size=1000,
        filter_func=None,
        keep_columns=['LOANTYPE', 'CENSUS', 'COMMNO'],
        max_rows=1000
    )
    
    if sample.height > 0:
        # Show loan type and census combinations
        combo = sample.group_by(['LOANTYPE', 'CENSUS']).agg(pl.count().alias('COUNT'))
        combo = combo.sort(by=['LOANTYPE', 'CENSUS'])
        print(f"Available LOANTYPE-CENSUS combinations (showing first 20):")
        print(combo.head(20))
        
        # Check if there's anything close to 575
        loantype_575 = sample.filter(pl.col("LOANTYPE").cast(pl.Float64).is_between(570, 580))
        if loantype_575.height > 0:
            print(f"\nFound {loantype_575.height} rows with LOANTYPE between 570-580:")
            print(loantype_575.group_by(['LOANTYPE', 'CENSUS']).agg(pl.count().alias('COUNT')))
    
    del sample
    gc.collect()
    
    # Create empty dataframes but continue with EBCDIC file processing
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
    essential_lncomm = ['ACCTNO', 'COMMNO', 'CORGAMT', 'INTAMT', 'ENTITY_CD']
    available_lncomm = [c for c in essential_lncomm if c in meta_lncomm.column_names]
    print(f"LNCOMM essential columns available: {available_lncomm}")

def filter_lncomm(chunk):
    """Filter LNCOMM chunks"""
    if chunk.height == 0:
        return chunk
    
    # Don't filter ENTITY_CD if it's empty
    if "ENTITY_CD" in chunk.columns:
        non_empty_entity = chunk.filter(pl.col("ENTITY_CD") != "").height
        if non_empty_entity > 0:
            chunk = chunk.filter((pl.col("ENTITY_CD") != "PIBB") | (pl.col("ENTITY_CD") == ""))
    
    return chunk

print("Reading LNCOMM with essential columns only...")
lncomm = read_sas7bdat_in_chunks(
    LOAN_LNCOMM, 
    chunk_size=CHUNK_SIZE,
    filter_func=filter_lncomm,
    keep_columns=['ACCTNO', 'COMMNO', 'CORGAMT', 'INTAMT', 'ENTITY_CD']
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
    
    # Handle INTAMT
    if "INTAMT" in lncomm.columns:
        lncomm = lncomm.with_columns([
            pl.when(pl.col("INTAMT").is_null()).then(0.00).otherwise(pl.col("INTAMT")).alias("INTAMT_CLEAN")
        ])
    else:
        print("WARNING: INTAMT column not found, using 0")
        lncomm = lncomm.with_columns(pl.lit(0.00).alias("INTAMT_CLEAN"))
    
    # Calculate NETPROC
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
# COLL file (EBCDIC)
# =========================
print("\n" + "="*60)
print("STEP 6: Reading COLL file (EBCDIC)")
print("="*60)

print("Parsing EBCDIC COLL file...")
coll = read_ebcdic_flat_file(
    COLL_FILE,
    parse_coll_record,
    chunk_size=CHUNK_SIZE
)

if coll.height > 0:
    coll = coll.sort(by=["CCOLLNO"])
    print(f"COLL rows: {coll.height}")
    print(f"First few COLL records: {coll.head(3)}")
else:
    print("WARNING: No COLL records found!")


# =========================
# DESC file (EBCDIC)
# =========================
print("\n" + "="*60)
print("STEP 7: Reading DESC file (EBCDIC)")
print("="*60)

print("Parsing EBCDIC DESC file...")
desc = read_ebcdic_flat_file(
    DESC_FILE,
    parse_desc_record,
    chunk_size=CHUNK_SIZE
)

if desc.height > 0:
    desc = desc.sort(by=["CCOLLNO"])
    print(f"DESC rows after filter: {desc.height}")
    print(f"First few DESC records: {desc.head(3)}")
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
    if coll.height > 0:
        del desc
        gc.collect()
    else:
        coll = pl.DataFrame()


# =========================
# Continue processing if we have data
# =========================
if loan1.height > 0 and coll.height > 0:
    # Merge LOAN1 with COMM if we have comm data
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
    
    # Derive fields
    print("\n" + "="*60)
    print("STEP 4: Deriving fields")
    print("="*60)
    
    # ISSUED date
    if "ISSUEDT" in loan.columns:
        loan = loan.with_columns([
            pl.when(pl.col("ISSUEDT").is_not_null() & (pl.col("ISSUEDT") > 0))
              .then(pl.col("ISSUEDT").cast(pl.Int64)
                    .map_elements(parse_mmddyy8_from_z11_prefix_to_date, return_dtype=pl.Date))
              .otherwise(pl.lit(None, dtype=pl.Date))
              .alias("ISSUED")
        ])
    else:
        loan = loan.with_columns(pl.lit(None, dtype=pl.Date).alias("ISSUED"))
    
    # NODAYS
    if "BLDATE" in loan.columns:
        loan = loan.with_columns([
            pl.when((pl.col("BLDATE") > 0) & (pl.lit(SDATE) > pl.col("BLDATE")))
              .then(pl.lit(SDATE) - pl.col("BLDATE"))
              .otherwise(0)
              .alias("NODAYS")
        ])
    else:
        loan = loan.with_columns(pl.lit(0).alias("NODAYS"))
    
    loan = loan.with_columns(pl.lit(None, dtype=pl.Date).alias("NPLDATE"))
    
    # ARREARS
    ndays_map = create_ndays_format()
    loan = loan.with_columns([
        pl.col("NODAYS").map_elements(
            lambda x: ndays_informat(x, ndays_map), 
            return_dtype=pl.Int64
        ).alias("ARREARS")
    ])
    
    # Recalculate ARREARS=24
    loan = loan.with_columns([
        pl.when(pl.col("ARREARS") == 24)
          .then((pl.col("NODAYS").cast(pl.Float64) / 365.0 * 12.0).round(0).cast(pl.Int64))
          .otherwise(pl.col("ARREARS"))
          .alias("ARREARS")
    ])
    
    # NPLDATE
    if "BLDATE" in loan.columns:
        loan = loan.with_columns([
            pl.when(pl.col("NODAYS") > 89)
              .then(pl.col("BLDATE").cast(pl.Int64)
                    .map_elements(lambda d: month_end_of(sas_days_to_date(int(d) + 90)) if d is not None else None,
                                  return_dtype=pl.Date))
              .otherwise(pl.lit(None, dtype=pl.Date))
              .alias("NPLDATE")
        ])
    
    # Deduplicate
    if "ACCTNO" in loan.columns and "NOTENO" in loan.columns:
        loan = loan.unique(subset=["ACCTNO", "NOTENO"], keep="first").sort(by=["ACCTNO", "NOTENO"])
    
    print(f"LOAN rows after dedup: {loan.height}")
    
    # CISLN
    print("\n" + "="*60)
    print("STEP 5: Reading CISLN")
    print("="*60)
    
    meta_cisln = get_sas7bdat_metadata(CISLN_LOAN)
    if meta_cisln:
        essential_cisln = ['ACCTNO', 'NEWIC', 'CUSTNAME', 'SECCUST']
        available_cisln = [c for c in essential_cisln if c in meta_cisln.column_names]
        print(f"CISLN essential columns available: {available_cisln}")
    
    def filter_cisln(chunk):
        """Filter CISLN chunks"""
        if chunk.height == 0:
            return chunk
        if "SECCUST" in chunk.columns:
            chunk = chunk.filter(pl.col("SECCUST") == "901")
        return chunk
    
    print("Reading CISLN with essential columns only...")
    cisln = read_sas7bdat_in_chunks(
        CISLN_LOAN, 
        chunk_size=CHUNK_SIZE,
        filter_func=filter_cisln,
        keep_columns=['ACCTNO', 'NEWIC', 'CUSTNAME', 'SECCUST']
    )
    
    if cisln.height > 0 and "ACCTNO" in cisln.columns:
        cisln = cisln.unique(subset=["ACCTNO"], keep="first")
    
    print(f"CISLN rows: {cisln.height}")
    
    if cisln.height > 0 and "ACCTNO" in cisln.columns:
        loan = loan.join(cisln, on="ACCTNO", how="left")
    
    print(f"LOAN rows after CISLN merge: {loan.height}")
    
    del cisln
    gc.collect()
    
    # NPGS merge
    print("\n" + "="*60)
    print("STEP 9: Creating NPGS")
    print("="*60)
    
    if loan.height > 0 and coll.height > 0:
        npgs = loan.join(coll, on=["ACCTNO", "NOTENO"], how="inner")
    else:
        npgs = pl.DataFrame()
    
    print(f"NPGS rows: {npgs.height}")
    
    del loan, coll
    gc.collect()
    
    # Continue with remaining steps only if NPGS has data
    if npgs.height > 0:
        # MICR file
        print("\n" + "="*60)
        print("STEP 10: Reading MICR file")
        print("="*60)
        
        micr_data = []
        with open(MICR_FILE, 'r') as f:
            for line in f:
                try:
                    pendbrh = int(line[0:3].strip()) if line[0:3].strip() else 0
                    micrcd = line[39:44].strip()
                    
                    micr_data.append({
                        'PENDBRH': pendbrh,
                        'MICRCD': micrcd
                    })
                except Exception as e:
                    continue
        
        micr = pl.DataFrame(micr_data).sort(by=["PENDBRH"])
        
        if "PENDBRH" in npgs.columns and "PENDBRH" in micr.columns:
            npgs = npgs.join(micr, on="PENDBRH", how="left")
        
        print(f"NPGS rows after MICR merge: {npgs.height}")
        
        del micr, micr_data
        gc.collect()
        
        # CVAR02
        print("\n" + "="*60)
        print("STEP 11: Creating CVAR fields")
        print("="*60)
        
        npgs = npgs.with_columns([
            pl.lit("   ").alias("CVAR02")
        ]).with_columns([
            pl.when(pl.col("SCH") == "7Q").then(pl.lit("7Q"))
             .when(pl.col("SCH") == "8Q").then(pl.lit("8Q"))
             .otherwise(pl.col("CVAR02"))
             .alias("CVAR02")
        ])
        
        npgs = npgs.filter(pl.col("CVAR02") != "   ")
        
        # Create final fields
        NORMDT = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"
        
        npgs = npgs.with_columns([
            pl.col("CENSUS").cast(pl.Float64).alias("CVAR01"),
            pl.col("NEWIC").cast(pl.Utf8).alias("CVAR03") if "NEWIC" in npgs.columns else pl.lit("").alias("CVAR03"),
            pl.when((pl.col("CUSTNAME").is_null()) | (pl.col("CUSTNAME") == "  "))
              .then(pl.col("NAME"))
              .otherwise(pl.col("CUSTNAME"))
              .cast(pl.Utf8)
              .alias("CVAR04") if "CUSTNAME" in npgs.columns else pl.lit("").alias("CVAR04"),
            pl.col("ISSUED").alias("CVAR05") if "ISSUED" in npgs.columns else pl.lit(None, dtype=pl.Date).alias("CVAR05"),
            pl.col("ACCTNO").cast(pl.Float64).alias("CVAR06") if "ACCTNO" in npgs.columns else pl.lit(0.0).alias("CVAR06"),
            pl.lit("FL").alias("CVAR07"),
            pl.col("NETPROC").cast(pl.Float64).alias("CVAR08") if "NETPROC" in npgs.columns else pl.lit(0.0).alias("CVAR08"),
            pl.col("BALANCE").cast(pl.Float64).alias("CVAR09") if "BALANCE" in npgs.columns else pl.lit(0.0).alias("CVAR09"),
            pl.lit(0.00).alias("CVAR10"),
            pl.col("ARREARS").cast(pl.Int64).alias("CVAR11") if "ARREARS" in npgs.columns else pl.lit(0).alias("CVAR11"),
            pl.lit("   ").alias("CVAR12"),
            pl.col("NPLDATE").map_elements(
                lambda d: f"{d.day:02d}/{d.month:02d}/{d.year:04d}" if d is not None else "          ",
                return_dtype=pl.Utf8
            ).alias("CVAR13") if "NPLDATE" in npgs.columns else pl.lit("          ").alias("CVAR13"),
            pl.lit("0233").alias("CVAR14"),
            pl.col("MICRCD").cast(pl.Utf8).alias("CVAR15") if "MICRCD" in npgs.columns else pl.lit("").alias("CVAR15"),
            pl.col("PENDBRH").cast(pl.Int64).alias("BRANCH") if "PENDBRH" in npgs.columns else pl.lit(0).alias("BRANCH"),
            pl.lit("TL").alias("CVAR16"),
            pl.col("CURBAL").cast(pl.Float64).alias("CVAR17") if "CURBAL" in npgs.columns else pl.lit(0.0).alias("CVAR17"),
            pl.lit(NORMDT).alias("NORMDT"),
        ])
        
        # IF ARREARS GE 3 AND NPLDATE > 0 THEN CVAR12='NPL'
        npgs = npgs.with_columns([
            pl.when((pl.col("ARREARS") >= 3) & pl.col("NPLDATE").is_not_null())
              .then(pl.lit("NPL"))
              .otherwise(pl.col("CVAR12"))
              .alias("CVAR12")
        ])
        
        # Sort
        npgs = npgs.sort(by=["CVAR06", "CVAR01"])
        
        # Prior TRRF
        print("\n" + "="*60)
        print("STEP 12: Reading prior NPGS.TRRF")
        print("="*60)
        
        if NPGS_TRRF_IN.exists():
            npla = read_sas7bdat_in_chunks(
                NPGS_TRRF_IN,
                chunk_size=CHUNK_SIZE,
                keep_columns=["CVAR06", "CVAR01", "STATUS", "NDATE"]
            )
            if npla.height > 0:
                npla = npla.sort(by=["CVAR06", "CVAR01"])
                npgs = npgs.join(npla, on=["CVAR06", "CVAR01"], how="left")
            
            del npla
            gc.collect()
        else:
            for c in ["STATUS", "NDATE"]:
                if c not in npgs.columns:
                    npgs = npgs.with_columns(pl.lit(None).alias(c))
        
        # Apply CVAR13 logic
        def cvar13_update(row):
            cv12 = row.get("CVAR12")
            status_val = row.get("STATUS")
            status = str(status_val).strip() if status_val is not None else ""
            ndate = row.get("NDATE") or "          "
            normdt = row.get("NORMDT") or "          "
            cur13 = row.get("CVAR13") or "          "

            if cv12 == "NPL":
                if status == "NPL":
                    return ndate
                return cur13
            else:
                if status == "NPL":
                    return normdt
                if (status == "   " or status == "") and ndate != "          ":
                    return ndate
                return cur13
        
        npgs = npgs.with_columns([
            pl.struct(["CVAR12", "STATUS", "NDATE", "NORMDT", "CVAR13"]).map_elements(
                cvar13_update, return_dtype=pl.Utf8
            ).alias("CVAR13")
        ])
        
        # Final sort
        npgs = npgs.sort(by=["CVAR01"])
        
        # Prepare output
        print("\n" + "="*60)
        print("STEP 13: Preparing output")
        print("="*60)
        
        for c in ["COSTCTR", "BALANCE", "CURBAL", "ACCRUAL", "TRANCHE", "CGCGUR",
                  "CENSUST", "PRODUCT", "NATGUAR", "CINSTCL", "SCH"]:
            if c not in npgs.columns:
                npgs = npgs.with_columns(pl.lit(None).alias(c))
        
        keep_cols = [
            "CVAR01","CVAR02","CVAR03","CVAR04","CVAR05","CVAR06","CVAR07",
            "CVAR08","CVAR09","CVAR10","CVAR11","CVAR12","CVAR13","CVAR14",
            "COSTCTR","BALANCE","CURBAL","ACCRUAL","TRANCHE","CGCGUR",
            "BRANCH","CVAR15","CENSUST","PRODUCT","NATGUAR","CINSTCL","SCH",
            "CVAR16","CVAR17"
        ]
        
        existing_keep_cols = [c for c in keep_cols if c in npgs.columns]
        out = npgs.select(existing_keep_cols)
        
        del npgs
        gc.collect()
        
        print(f"Final output rows: {out.height}")
        
        # Write output
        if out.height > 0:
            print("\n" + "="*60)
            print("STEP 14: Writing output")
            print("="*60)
            
            print(f"Writing to {OUT_FILE}...")
            
            try:
                sas = saspy.SASsession()
                output_sas_path = write_sas7bdat_in_chunks(out, OUT_FILE, sas, chunk_size=CHUNK_SIZE)
                print(f"Successfully wrote {output_sas_path}")
                sas.endsas()
            except Exception as e:
                print(f"Error writing SAS output: {e}")
                import traceback
                traceback.print_exc()
        else:
            print("No data to write to output!")
    else:
        print("\nNo NPGS data to process!")
else:
    print("\n" + "="*60)
    print("Insufficient data to create NPGS")
    print(f"LOAN1 rows: {loan1.height if 'loan1' in locals() else 0}")
    print(f"COLL rows: {coll.height if 'coll' in locals() else 0}")
    print("="*60)
    
    # Clean up
    if 'loan0' in locals():
        del loan0
    if 'loan1' in locals():
        del loan1
    if 'comm' in locals():
        del comm
    if 'coll' in locals():
        del coll
    gc.collect()
    
    print("\nProcessing completed with no output generated.")
    print("The August file doesn't contain the required loan type 575.")
    print("This might be expected if loan type 575 is only present in specific months.")

print("\n" + "="*60)
print("Processing complete!")
print("="*60)
