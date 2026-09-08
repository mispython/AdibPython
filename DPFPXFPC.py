from __future__ import annotations

from pathlib import Path
from datetime import date, datetime, timedelta
import polars as pl
import pyreadstat
import saspy
import numpy as np
import gc


# =========================
# Paths (adjust as needed)
# =========================
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLTRRF")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# Inputs
LOAN_REPTDATE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/reptdate.sas7bdat")  # LOAN.REPTDATE
LOAN_LNNOTE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat")
LOAN_LNCOMM   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat")

CISLN_LOAN    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/loan.sas7bdat")

COLL_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831")
DESC_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831")
MICR_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLTRRF/BOPESS.txt")

NPGS_TRRF_IN  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLTRRF/trrf.sas7bdat")  # SAS: NPGS.TRRF (prior file for merge)
PBBLNFMT_FILE = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/pbblnfmt.sas7bdat")  # PBBLNFMT format dataset

# Output
OUT_DIR  = BASE_OUTPUT
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = None  # set after REPTMON known

# Chunk size for reading large files (adjust based on available memory)
CHUNK_SIZE = 100000  # Read 100k rows at a time


# =========================
# Helpers
# =========================
def get_sas7bdat_info(file_path):
    """Get metadata about SAS7BDAT file without reading all data"""
    meta = pyreadstat.read_sas7bdat(str(file_path), metadataonly=True)
    return meta


def read_sas7bdat_in_chunks(file_path, chunk_size=CHUNK_SIZE, filter_func=None, keep_columns=None):
    """
    Read SAS7BDAT file in chunks and optionally filter/select columns
    
    Args:
        file_path: Path to SAS7BDAT file
        chunk_size: Number of rows to read at a time
        filter_func: Function to filter each chunk (takes polars DataFrame, returns filtered DataFrame)
        keep_columns: List of columns to keep (None for all)
    
    Returns:
        Polars DataFrame with all chunks concatenated
    """
    chunks = []
    row_offset = 0
    file_path_str = str(file_path)
    
    while True:
        try:
            # Read a chunk
            df_chunk, meta = pyreadstat.read_sas7bdat(
                file_path_str, 
                row_offset=row_offset,
                row_limit=chunk_size
            )
            
            if df_chunk is None or len(df_chunk) == 0:
                break
                
            # Convert to polars
            pl_chunk = pl.from_pandas(df_chunk)
            
            # Apply column selection if specified
            if keep_columns:
                existing_cols = [c for c in keep_columns if c in pl_chunk.columns]
                if existing_cols:
                    pl_chunk = pl_chunk.select(existing_cols)
                else:
                    pl_chunk = pl_chunk.select([])
            
            # Apply filter function if specified
            if filter_func and len(pl_chunk) > 0:
                pl_chunk = filter_func(pl_chunk)
                
            # Only keep non-empty chunks
            if len(pl_chunk) > 0:
                chunks.append(pl_chunk)
            
            # Move to next chunk
            row_offset += len(df_chunk)
            
            # Clear memory
            del df_chunk
            gc.collect()
            
            # Break if we got less than chunk_size (end of file)
            if len(pl_chunk) < chunk_size:
                break
                
        except Exception as e:
            print(f"Error reading chunk at offset {row_offset}: {e}")
            break
    
    # Concatenate all chunks
    if chunks:
        result = pl.concat(chunks, how="vertical", rechunk=True)
        del chunks
        gc.collect()
        return result
    else:
        return pl.DataFrame()


def write_sas7bdat_in_chunks(df, file_path, sas_session=None, chunk_size=CHUNK_SIZE):
    """
    Write DataFrame to SAS7BDAT format using SASPy in chunks
    """
    if sas_session is None:
        sas_session = saspy.SASsession()
    
    output_path = str(file_path)
    
    # Write in chunks to avoid memory issues
    total_rows = df.height
    print(f"Writing {total_rows} rows to {output_path}")
    
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


def mdy(month, day, year):
    """Create date from month, day, year"""
    return date(year, month, day)


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
# REPTDATE - Using datetime timedelta - 1 day
# =========================
REPTDATE = datetime.now().date() - timedelta(days=1)
REPTMON  = f"{REPTDATE.month:02d}"
REPTDAY  = f"{REPTDATE.day:02d}"
REPTYEAR = f"{REPTDATE.year:04d}"
SDATE = date_to_sas_days(REPTDATE)
OUT_FILE  = OUT_DIR / f"LNTRRF{REPTMON}.sas7bdat"


# =========================
# LOAN0 / LOAN1 from LNNOTE - Read in chunks with filtering
# =========================
print("Reading LNNOTE in chunks...")

# Define filter function to apply to each chunk
def filter_lnnote(chunk):
    """Filter LNNOTE chunks for conventional loans and specific loan types"""
    # Filter ENTITY_CD != 'PIBB' if column exists
    if "ENTITY_CD" in chunk.columns:
        chunk = chunk.filter(pl.col("ENTITY_CD") != "PIBB")
    
    # Filter for LOANTYPE=575 & CENSUS=575.09
    if "LOANTYPE" in chunk.columns and "CENSUS" in chunk.columns:
        chunk = chunk.filter((pl.col("LOANTYPE") == 575) & (pl.col("CENSUS") == 575.09))
    
    # Add derived columns
    if chunk.height > 0:
        chunk = chunk.with_columns([
            pl.col("LOANTYPE").alias("PRODUCT") if "LOANTYPE" in chunk.columns else pl.lit(None).alias("PRODUCT"),
            pl.col("CENSUS").alias("CENSUST") if "CENSUS" in chunk.columns else pl.lit(None).alias("CENSUST"),
        ])
    
    return chunk

# Read LNNOTE in chunks with filtering
loan_base = read_sas7bdat_in_chunks(
    LOAN_LNNOTE, 
    chunk_size=CHUNK_SIZE,
    filter_func=filter_lnnote
)

print(f"Filtered LNNOTE rows: {loan_base.height}")

# Split into LOAN0 and LOAN1
loan1 = loan_base.filter(pl.col("COMMNO") > 0)
loan0 = loan_base.filter(~(pl.col("COMMNO") > 0))

print(f"LOAN0: {loan0.height} rows, LOAN1: {loan1.height} rows")

# Clear memory
del loan_base
gc.collect()


# =========================
# COMM from LNCOMM - Read in chunks with filtering
# =========================
print("Reading LNCOMM in chunks...")

def filter_lncomm(chunk):
    """Filter LNCOMM chunks for conventional loans"""
    if "ENTITY_CD" in chunk.columns:
        chunk = chunk.filter(pl.col("ENTITY_CD") != "PIBB")
    return chunk

# Read LNCOMM in chunks
lncomm = read_sas7bdat_in_chunks(
    LOAN_LNCOMM, 
    chunk_size=CHUNK_SIZE,
    filter_func=filter_lncomm,
    keep_columns=["ACCTNO", "COMMNO", "CORGAMT", "INTAMT", "ENTITY_CD"]
)

print(f"LNCOMM rows after filter: {lncomm.height}")

# Process COMM
comm = (
    lncomm
    .with_columns([
        pl.when(pl.col("CORGAMT").is_null()).then(0.00).otherwise(pl.col("CORGAMT")).alias("CORGAMT"),
        pl.when(pl.col("INTAMT").is_null()).then(0.00).otherwise(pl.col("INTAMT")).alias("INTAMT"),
    ])
    .with_columns((pl.col("CORGAMT") - pl.col("INTAMT")).alias("NETPROC"))
    .select(["ACCTNO", "COMMNO", "NETPROC"])
    .sort(by=["ACCTNO", "COMMNO"])
)

# Clear memory
del lncomm
gc.collect()


# =========================
# Merge LOAN1 with COMM
# =========================
print("Merging LOAN1 with COMM...")
loan1 = loan1.join(comm, on=["ACCTNO", "COMMNO"], how="inner")
loan = pl.concat([loan0, loan1], how="vertical", rechunk=True)

print(f"Total LOAN rows after merge: {loan.height}")

# Clear memory
del loan0, loan1, comm
gc.collect()


# =========================
# Derive ISSUED, NODAYS, ARREARS, NPLDATE
# =========================
print("Deriving ISSUED, NODAYS, ARREARS, NPLDATE...")

# ISSUED date
loan = loan.with_columns([
    pl.when(pl.col("ISSUEDT").is_not_null() & (pl.col("ISSUEDT") > 0))
      .then(pl.col("ISSUEDT").cast(pl.Int64)
            .map_elements(parse_mmddyy8_from_z11_prefix_to_date, return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("ISSUED")
])

# NODAYS calculation
loan = loan.with_columns([
    pl.when((pl.col("BLDATE") > 0) & (pl.lit(SDATE) > pl.col("BLDATE")))
      .then(pl.lit(SDATE) - pl.col("BLDATE"))
      .otherwise(0)
      .alias("NODAYS"),
    pl.lit(None, dtype=pl.Date).alias("NPLDATE")
])

# Create NDAYS format mapping
ndays_map = create_ndays_format()

# Apply NDAYS informat
loan = loan.with_columns([
    pl.col("NODAYS").map_elements(
        lambda x: ndays_informat(x, ndays_map), 
        return_dtype=pl.Int64
    ).alias("ARREARS")
])

# If ARREARS=24 then recalculate
loan = loan.with_columns([
    pl.when(pl.col("ARREARS") == 24)
      .then((pl.col("NODAYS").cast(pl.Float64) / 365.0 * 12.0).round(0).cast(pl.Int64))
      .otherwise(pl.col("ARREARS"))
      .alias("ARREARS")
])

# NPLDATE when NODAYS > 89
loan = loan.with_columns([
    pl.when(pl.col("NODAYS") > 89)
      .then(pl.col("BLDATE").cast(pl.Int64)
            .map_elements(lambda d: month_end_of(sas_days_to_date(int(d) + 90)) if d is not None else None,
                          return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("NPLDATE")
])

# Sort and deduplicate
loan = loan.unique(subset=["ACCTNO", "NOTENO"], keep="first").sort(by=["ACCTNO", "NOTENO"])

print(f"LOAN rows after dedup: {loan.height}")


# =========================
# CISLN - Read in chunks with filtering
# =========================
print("Reading CISLN in chunks...")

def filter_cisln(chunk):
    """Filter CISLN chunks for SECCUST='901'"""
    if "SECCUST" in chunk.columns:
        chunk = chunk.filter(pl.col("SECCUST") == "901")
    return chunk

# Read CISLN in chunks
cisln = read_sas7bdat_in_chunks(
    CISLN_LOAN, 
    chunk_size=CHUNK_SIZE,
    filter_func=filter_cisln,
    keep_columns=["ACCTNO", "NEWIC", "CUSTNAME", "SECCUST"]
)

# Deduplicate
cisln = cisln.unique(subset=["ACCTNO"], keep="first")

print(f"CISLN rows after filter: {cisln.height}")

# Merge with LOAN
loan = loan.join(cisln, on="ACCTNO", how="left")

print(f"LOAN rows after CISLN merge: {loan.height}")

# Clear memory
del cisln
gc.collect()


# =========================
# COLL - Read fixed-width file
# =========================
print("Reading COLL file...")
# COLL file format:
# @004  CCOLLNO  PD6.
# @146  ACCTNO   PD6.
# @153  NOTENO   PD6.

# Read as fixed-width binary file for packed decimal
coll_data = []
with open(COLL_FILE, 'rb') as f:
    chunk = []
    line_count = 0
    for line in f:
        try:
            # PD6 packed decimal format - 6 bytes
            # For packed decimal, need to unpack the bytes
            ccollno_bytes = line[3:9]  # @004, 6 bytes for packed decimal
            acctno_bytes = line[145:151]  # @146, 6 bytes
            noteno_bytes = line[152:158]  # @153, 6 bytes
            
            # Unpack packed decimal (simplified - may need adjustment)
            def unpack_packed_decimal(b):
                if len(b) == 0:
                    return 0
                # Packed decimal: each byte contains 2 digits except last which has 1 digit + sign
                digits = []
                for i in range(len(b) - 1):
                    digits.append((b[i] >> 4) & 0x0F)
                    digits.append(b[i] & 0x0F)
                # Last byte
                digits.append((b[-1] >> 4) & 0x0F)
                sign = b[-1] & 0x0F
                
                # Convert to integer
                value = 0
                for digit in digits:
                    value = value * 10 + digit
                
                # Check sign (0xC, 0xF = positive, 0xD = negative)
                if sign == 0x0D:
                    value = -value
                
                return value
            
            ccollno = unpack_packed_decimal(ccollno_bytes)
            acctno = unpack_packed_decimal(acctno_bytes)
            noteno = unpack_packed_decimal(noteno_bytes)
            
            chunk.append({
                'CCOLLNO': ccollno,
                'ACCTNO': acctno,
                'NOTENO': noteno
            })
            line_count += 1
            
            # Process in chunks
            if len(chunk) >= CHUNK_SIZE:
                coll_data.extend(chunk)
                chunk = []
                gc.collect()
                
        except Exception as e:
            continue
    
    # Add remaining
    if chunk:
        coll_data.extend(chunk)

coll = pl.DataFrame(coll_data).sort(by=["CCOLLNO"])
print(f"COLL rows: {coll.height}")

# Clear memory
del coll_data
gc.collect()


# =========================
# DESC - Read fixed-width file
# =========================
print("Reading DESC file...")
# DESC file format:
# @001 CCOLLNO   11.
# @051 CINSTCL   $2.
# @055 NATGUAR   $2.
# @128 CGCGUR    $3.
# @211 CENSUS    10.
# @291 TRANCHE   $8.

desc_data = []
with open(DESC_FILE, 'rb') as f:
    chunk = []
    for line in f:
        try:
            line_str = line.decode('latin-1')
            
            ccollno = float(line_str[0:11].strip()) if line_str[0:11].strip() else 0.0
            cinstcl = line_str[50:52].strip()
            natguar = line_str[54:56].strip()
            cgcgur = line_str[127:130].strip()
            census = float(line_str[210:220].strip()) if line_str[210:220].strip() else 0.0
            tranche = line_str[290:298].strip()
            
            # Filter for CGCGUR IN ('080','090')
            if cgcgur in ('080', '090'):
                sch = '7Q' if cgcgur == '080' else '8Q'
                
                chunk.append({
                    'CCOLLNO': ccollno,
                    'CINSTCL': cinstcl,
                    'NATGUAR': natguar,
                    'CGCGUR': cgcgur,
                    'CENSUS': census,
                    'TRANCHE': tranche,
                    'SCH': sch
                })
                
            # Process in chunks
            if len(chunk) >= CHUNK_SIZE:
                desc_data.extend(chunk)
                chunk = []
                gc.collect()
                
        except Exception as e:
            continue
    
    # Add remaining
    if chunk:
        desc_data.extend(chunk)

desc = pl.DataFrame(desc_data).sort(by=["CCOLLNO"])
print(f"DESC rows after filter: {desc.height}")

# Clear memory
del desc_data
gc.collect()


# =========================
# Merge COLL and DESC
# =========================
print("Merging COLL and DESC...")
coll = coll.join(desc, on="CCOLLNO", how="inner")
coll = coll.filter((pl.col("CINSTCL") == "18") & (pl.col("NATGUAR") == "06"))
coll = coll.sort(by=["ACCTNO", "NOTENO"])

print(f"COLL rows after merge: {coll.height}")

# Clear memory
del desc
gc.collect()


# =========================
# NPGS = LOAN ⋈ COLL by ACCTNO, NOTENO (inner)
# =========================
npgs = loan.join(coll, on=["ACCTNO", "NOTENO"], how="inner")
print(f"NPGS rows after LOAN-COLL merge: {npgs.height}")

# Clear memory
del loan, coll
gc.collect()


# =========================
# MICR - Read fixed-width file
# =========================
print("Reading MICR file...")
# MICR file format:
# @001 PENDBRH    3.
# @040 MICRCD    $5.

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
npgs = npgs.join(micr, on="PENDBRH", how="left")

print(f"NPGS rows after MICR merge: {npgs.height}")

# Clear memory
del micr, micr_data
gc.collect()


# =========================
# CVAR02 from SCH, keep only non-blank CVAR02
# =========================
print("Creating CVAR02...")
npgs = npgs.with_columns([
    pl.lit("   ").alias("CVAR02")
]).with_columns([
    pl.when(pl.col("SCH") == "7Q").then(pl.lit("7Q"))
     .when(pl.col("SCH") == "8Q").then(pl.lit("8Q"))
     .otherwise(pl.col("CVAR02"))
     .alias("CVAR02")
])

npgs = npgs.filter(pl.col("CVAR02") != "   ")


# =========================
# Final CVAR fields, NORMDT, flags
# =========================
print("Creating final fields...")
NORMDT = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

npgs = npgs.with_columns([
    pl.col("CENSUS").cast(pl.Float64).alias("CVAR01"),
    pl.col("NEWIC").cast(pl.Utf8).alias("CVAR03"),
    pl.when((pl.col("CUSTNAME").is_null()) | (pl.col("CUSTNAME") == "  "))
      .then(pl.col("NAME"))
      .otherwise(pl.col("CUSTNAME"))
      .cast(pl.Utf8)
      .alias("CVAR04"),
    pl.col("ISSUED").alias("CVAR05"),
    pl.col("ACCTNO").cast(pl.Float64).alias("CVAR06"),
    pl.lit("FL").alias("CVAR07"),
    pl.col("NETPROC").cast(pl.Float64).alias("CVAR08"),
    pl.col("BALANCE").cast(pl.Float64).alias("CVAR09"),
    pl.lit(0.00).alias("CVAR10"),
    pl.col("ARREARS").cast(pl.Int64).alias("CVAR11"),
    pl.lit("   ").alias("CVAR12"),
    pl.col("NPLDATE").map_elements(
        lambda d: f"{d.day:02d}/{d.month:02d}/{d.year:04d}" if d is not None else "          ",
        return_dtype=pl.Utf8
    ).alias("CVAR13"),
    pl.lit("0233").alias("CVAR14"),
    pl.col("MICRCD").cast(pl.Utf8).alias("CVAR15"),
    pl.col("PENDBRH").cast(pl.Int64).alias("BRANCH"),
    pl.lit("TL").alias("CVAR16"),
    pl.col("CURBAL").cast(pl.Float64).alias("CVAR17"),
    pl.lit(NORMDT).alias("NORMDT"),
])

# IF ARREARS GE 3 AND NPLDATE > 0 THEN CVAR12='NPL'
npgs = npgs.with_columns([
    pl.when((pl.col("ARREARS") >= 3) & pl.col("NPLDATE").is_not_null())
      .then(pl.lit("NPL"))
      .otherwise(pl.col("CVAR12"))
      .alias("CVAR12")
])

# PROC SORT; BY CVAR06 CVAR01
npgs = npgs.sort(by=["CVAR06", "CVAR01"])


# =========================
# Read prior NPGS.TRRF as NPLA and merge
# =========================
print("Reading prior NPGS.TRRF...")
if NPGS_TRRF_IN.exists():
    # Read only needed columns
    npla = read_sas7bdat_in_chunks(
        NPGS_TRRF_IN,
        chunk_size=CHUNK_SIZE,
        keep_columns=["CVAR06", "CVAR01", "STATUS", "NDATE"]
    )
    npla = npla.sort(by=["CVAR06", "CVAR01"])
    npgs = npgs.join(npla, on=["CVAR06", "CVAR01"], how="left")
    
    # Clear memory
    del npla
    gc.collect()
else:
    # If missing, create placeholders
    for c in ["STATUS", "NDATE"]:
        if c not in npgs.columns:
            npgs = npgs.with_columns(pl.lit(None).alias(c))

print("Applying post-merge CVAR13 logic...")

# Apply post-merge CVAR13 logic using STATUS/NDATE/NORMDT
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
    else:  # CVAR12 == '   '
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

# Final sort BY CVAR01
npgs = npgs.sort(by=["CVAR01"])


# =========================
# Prepare output columns
# =========================
print("Preparing output...")
# Ensure passthrough columns exist
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

out = npgs.select(keep_cols)

# Clear memory
del npgs
gc.collect()

print(f"Final output rows: {out.height}")


# =========================
# Output: NPGS.LNTRRF&REPTMON as SAS7BDAT
# =========================
print(f"Writing output to {OUT_FILE}...")

# Start SAS session
sas = saspy.SASsession()

# Write output as SAS7BDAT
output_sas_path = write_sas7bdat_in_chunks(out, OUT_FILE, sas, chunk_size=CHUNK_SIZE)

print(f"Successfully wrote {output_sas_path}")

# End SAS session
sas.endsas()

print("Processing complete!")
