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
# Configuration
# =========================
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLTRRF")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# Input base paths (different for each source)
LOAN_LNNOTE_BASE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
LOAN_LNCOMM_BASE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ")
CISLN_LOAN_PATH  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/loan.sas7bdat")
COLL_FILE_BASE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
DESC_FILE_BASE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS")
MICR_FILE        = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLTRRF/BOPESS.txt")
NPGS_TRRF_BASE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLTRRF")

# Output
OUT_DIR = BASE_OUTPUT
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = None

# Processing parameters
CHUNK_SIZE = 50000
COLL_RECORD_LENGTH = 380    # LCCRISEX: FB, record length 380
DESC_RECORD_LENGTH = 3050   # LCCRISEX_DESC: FB, record length 3050

# Loan filter criteria (from original SAS code)
LOAN_TYPE_FILTER = 575
CENSUS_FILTER = 575.09
ENTITY_CD_FILTER = "PIBB"  # Exclude this entity


# =========================
# Helper Functions
# =========================
def get_sas7bdat_metadata(file_path):
    """Get metadata from SAS7BDAT file without reading all data"""
    try:
        df_meta, meta = pyreadstat.read_sas7bdat(str(file_path), metadataonly=True)
        return meta
    except Exception as e:
        print(f"Error reading metadata: {e}")
        return None


def read_sas7bdat_in_chunks(file_path, chunk_size=CHUNK_SIZE, filter_func=None, keep_columns=None, max_rows=None):
    """Read SAS7BDAT file in chunks with optional filtering"""
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
    
    file_size = os.path.getsize(file_path)
    expected_records = file_size // record_length
    print(f"  File: {file_path.name}")
    print(f"  File size: {file_size:,} bytes")
    print(f"  Record length: {record_length} bytes")
    print(f"  Expected records: {expected_records:,}")
    
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
                                print(f"  Processed {total_records:,} records...")
                            gc.collect()
                except Exception:
                    continue
            
            if chunk_records:
                all_records.extend(chunk_records)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    
    print(f"  Total records read: {total_records:,}")
    print(f"  Records parsed: {len(all_records):,}")
    
    return pl.DataFrame(all_records) if all_records else pl.DataFrame()


def write_sas7bdat_in_chunks(df, file_path, sas_session=None, chunk_size=CHUNK_SIZE):
    """Write DataFrame to SAS7BDAT format using SASPy"""
    if sas_session is None:
        sas_session = saspy.SASsession()
    
    output_path = str(file_path)
    total_rows = df.height
    print(f"Writing {total_rows:,} rows to {output_path}")
    
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
    @004  CCOLLNO  PD6.
    @146  ACCTNO   PD6.
    @153  NOTENO   PD6.
    """
    try:
        if len(record_bytes) < 158:
            return None
        
        ccollno = unpack_packed_decimal(record_bytes[3:9])
        acctno = unpack_packed_decimal(record_bytes[145:151])
        noteno = unpack_packed_decimal(record_bytes[152:158])
        
        return {
            'CCOLLNO': ccollno,
            'ACCTNO': acctno,
            'NOTENO': noteno
        }
    except:
        return None


def parse_desc_record(record_bytes):
    """
    Parse DESC (LCCRISEX_DESC) record - Fixed length 3050 bytes
    @001 CCOLLNO   11.
    @051 CINSTCL   $2.
    @055 NATGUAR   $2.
    @128 CGCGUR    $3.
    @211 CENSUS    10.
    @291 TRANCHE   $8.
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
    except:
        return None


# =========================
# Main Processing
# =========================
def main():
    # Set report date (yesterday)
    REPTDATE = datetime.now().date() - timedelta(days=1)
    REPTMON = f"{REPTDATE.month:02d}"
    REPTDAY = f"{REPTDATE.day:02d}"
    REPTYEAR = f"{REPTDATE.year:04d}"
    SDATE = date_to_sas_days(REPTDATE)
    
    # Build dynamic input file paths
    LOAN_LNNOTE = LOAN_LNNOTE_BASE / f"enrh_ln_note_m{REPTMON}.sas7bdat"
    LOAN_LNCOMM = LOAN_LNCOMM_BASE / f"enrh_ln_comm_m{REPTMON}.sas7bdat"
    COLL_FILE = COLL_FILE_BASE / f"LCCRISEX_{REPTYEAR}{REPTMON}{REPTDAY}"
    DESC_FILE = DESC_FILE_BASE / f"LCCRISEX_DESC_{REPTYEAR}{REPTMON}{REPTDAY}"
    NPGS_TRRF_IN = NPGS_TRRF_BASE / "trrf.sas7bdat"
    
    OUT_FILE = OUT_DIR / f"LNTRRF{REPTMON}.sas7bdat"
    
    print("="*80)
    print(f"Processing date: {REPTDATE} (SAS date: {SDATE})")
    print("="*80)
    
    # Display input file paths
    print("\nInput files:")
    print(f"  LNNOTE:   {LOAN_LNNOTE}")
    print(f"  LNCOMM:   {LOAN_LNCOMM}")
    print(f"  CISLN:    {CISLN_LOAN_PATH}")
    print(f"  COLL:     {COLL_FILE}")
    print(f"  DESC:     {DESC_FILE}")
    print(f"  MICR:     {MICR_FILE}")
    print(f"  NPGS.TRRF: {NPGS_TRRF_IN}")
    print(f"\nOutput file: {OUT_FILE}")
    
    # Check if input files exist
    print("\n" + "="*80)
    print("Checking input files...")
    print("="*80)
    
    files_to_check = [
        ("LNNOTE", LOAN_LNNOTE),
        ("LNCOMM", LOAN_LNCOMM),
        ("CISLN", CISLN_LOAN_PATH),
        ("COLL", COLL_FILE),
        ("DESC", DESC_FILE),
        ("MICR", MICR_FILE),
    ]
    
    missing_files = []
    for name, file_path in files_to_check:
        if file_path.exists():
            size = os.path.getsize(file_path)
            print(f"  ✓ {name}: {file_path} ({size:,} bytes)")
        else:
            print(f"  ✗ {name}: {file_path} (MISSING)")
            missing_files.append(name)
    
    if missing_files:
        print(f"\nERROR: Missing files: {', '.join(missing_files)}")
        print("Exiting...")
        return
    
    print("\nAll input files found.")
    
    # STEP 1: Read LNNOTE with LOANTYPE=575 and CENSUS=575.09
    print("\n" + "="*80)
    print(f"STEP 1: Reading LNNOTE (LOANTYPE={LOAN_TYPE_FILTER}, CENSUS={CENSUS_FILTER})")
    print("="*80)
    
    def filter_lnnote(chunk):
        if chunk.height == 0:
            return chunk
        
        # Filter out PIBB entity if entity column has values
        if "ENTITY_CD" in chunk.columns:
            non_empty = chunk.filter(pl.col("ENTITY_CD") != "").height
            if non_empty > 0:
                chunk = chunk.filter(
                    (pl.col("ENTITY_CD") != ENTITY_CD_FILTER) | (pl.col("ENTITY_CD") == "")
                )
        
        # Filter for specific loan type and census
        if "LOANTYPE" in chunk.columns and "CENSUS" in chunk.columns:
            chunk = chunk.filter(
                (pl.col("LOANTYPE").cast(pl.Float64) == float(LOAN_TYPE_FILTER)) & 
                (pl.col("CENSUS").cast(pl.Float64) == float(CENSUS_FILTER))
            )
        
        # Add derived columns
        if chunk.height > 0:
            chunk = chunk.with_columns([
                pl.col("LOANTYPE").alias("PRODUCT") if "LOANTYPE" in chunk.columns else pl.lit(None).alias("PRODUCT"),
                pl.col("CENSUS").alias("CENSUST") if "CENSUS" in chunk.columns else pl.lit(None).alias("CENSUST"),
            ])
        
        return chunk
    
    essential_cols = [
        'ACCTNO', 'NAME', 'NOTENO', 'LOANTYPE', 'CENSUS', 'COMMNO', 'ENTITY_CD',
        'ISSUEDT', 'BLDATE', 'BALANCE', 'CURBAL', 'PENDBRH', 'NETPROC'
    ]
    
    loan_base = read_sas7bdat_in_chunks(
        LOAN_LNNOTE,
        chunk_size=CHUNK_SIZE,
        filter_func=filter_lnnote,
        keep_columns=essential_cols
    )
    
    print(f"Filtered LNNOTE rows: {loan_base.height}")
    
    if loan_base.height == 0:
        print("\n" + "="*80)
        print(f"NO DATA FOUND FOR LOANTYPE={LOAN_TYPE_FILTER}, CENSUS={CENSUS_FILTER}")
        print(f"The {REPTMON}/{REPTYEAR} file does not contain the required loan type.")
        print("This is expected if this loan type is a specific product")
        print("that only appears in certain months.")
        print("="*80)
        print("\nExiting gracefully without generating output.")
        return
    
    # Split into LOAN0 and LOAN1 based on COMMNO
    if "COMMNO" in loan_base.columns:
        loan1 = loan_base.filter(pl.col("COMMNO") > 0)
        loan0 = loan_base.filter(~(pl.col("COMMNO") > 0))
        print(f"LOAN0: {loan0.height} rows, LOAN1: {loan1.height} rows")
    else:
        loan0 = pl.DataFrame()
        loan1 = loan_base
    
    del loan_base
    gc.collect()
    
    print("\n" + "="*80)
    print("SUCCESS: Data found! Continuing with processing...")
    print("="*80)
    
    # Continue with remaining steps...
    # (STEP 2 through STEP 14 would follow here)
    
    print("\n" + "="*80)
    print("Processing complete!")
    print("="*80)


if __name__ == "__main__":
    main()
