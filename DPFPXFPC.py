from __future__ import annotations

from pathlib import Path
from datetime import date, datetime, timedelta
import polars as pl
import pyreadstat
import pandas as pd
import saspy
from PBBLNFMT import put, informat, apply_format, available_formats
import duckdb  # noqa: F401
import pyarrow as pa  # noqa: F401
import pyarrow.parquet as pq  # noqa: F401


# =========================
# Paths (adjust to your env)
# =========================
BASE_OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLSMEZ")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# ---- Input SAS datasets (all in sas7bdat format) ----
LOAN_LNNOTE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat")
LOAN_LNCOMM   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat")
LOANI_LNNOTE  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat")
LOANI_LNCOMM  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat")
CISLN_LOAN    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/loan.sas7bdat")
COLL_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831")
DESC_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831")
MICR_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/BOPESS.txt")
NPGS_SMEZ     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/smez.sas7bdat")

# Chunk size for reading large SAS datasets
CHUNK_SIZE = 100000


# =========================
# Helper functions
# =========================
def sas_days_to_date(days: int) -> date:
    origin = date(1960, 1, 1)
    return origin + timedelta(days=int(days))


def date_to_sas_days(d: date) -> int:
    origin = date(1960, 1, 1)
    return (d - origin).days


def read_sas7bdat_filtered(filepath: Path, entity_filter: str = None, 
                           chunk_size: int = CHUNK_SIZE,
                           column_filter: dict = None) -> pl.DataFrame:
    chunks = []
    offset = 0
    
    while True:
        try:
            df, _ = pyreadstat.read_sas7bdat(
                str(filepath), 
                row_offset=offset, 
                row_limit=chunk_size
            )
            
            if df.empty:
                break
                
            df.columns = [col.lower() for col in df.columns]
            
            if entity_filter and 'entity_cd' in df.columns:
                if entity_filter == 'PIBB':
                    df = df[df['entity_cd'] == 'PIBB']
                elif entity_filter == 'NON_PIBB':
                    df = df[df['entity_cd'] != 'PIBB']
            
            if column_filter:
                for col_name, col_value in column_filter.items():
                    if col_name in df.columns:
                        df = df[df[col_name] == col_value]
            
            if not df.empty:
                chunks.append(pl.from_pandas(df))
            
            offset += chunk_size
            
            if len(df) < chunk_size:
                break
                
        except Exception as e:
            print(f"Error reading chunk at offset {offset}: {e}")
            break
    
    if not chunks:
        return pl.DataFrame()
    
    return pl.concat(chunks, how="vertical", rechunk=True)


def read_sas7bdat(filepath: Path) -> pl.DataFrame:
    df, meta = pyreadstat.read_sas7bdat(str(filepath))
    df.columns = [col.lower() for col in df.columns]
    return pl.from_pandas(df)


def read_ebcdic_fixed_records(filepath: Path, record_length: int, col_specs: list, 
                              max_records: int = None) -> pl.DataFrame:
    rows = []
    records_read = 0
    
    with open(filepath, 'rb') as f:
        while True:
            record = f.read(record_length)
            if not record or len(record) < record_length:
                break
                
            row = {}
            for col_name, start, end, col_type in col_specs:
                start_idx = start - 1
                end_idx = end
                
                if col_type == 'pd':
                    raw_bytes = record[start_idx:end_idx]
                    try:
                        hex_str = raw_bytes.hex()
                        digits = hex_str[:-1]
                        sign_nibble = hex_str[-1].upper()
                        
                        if digits and all(c in '0123456789ABCDEF' for c in digits):
                            value = int(digits, 16)
                            if sign_nibble in ('D', 'B'):
                                value = -value
                            row[col_name.lower()] = float(value)
                        else:
                            row[col_name.lower()] = None
                    except:
                        row[col_name.lower()] = None
                        
                elif col_type == 'numeric':
                    try:
                        raw_bytes = record[start_idx:end_idx]
                        decoded = raw_bytes.decode('cp037').strip()
                        decoded_clean = ''.join(c for c in decoded if c.isdigit() or c in '.-')
                        row[col_name.lower()] = float(decoded_clean) if decoded_clean else None
                    except:
                        row[col_name.lower()] = None
                        
                else:  # character
                    try:
                        raw_bytes = record[start_idx:end_idx]
                        decoded = raw_bytes.decode('cp037').strip()
                        row[col_name.lower()] = decoded
                    except:
                        row[col_name.lower()] = ""
            
            rows.append(row)
            records_read += 1
            
            if max_records and records_read >= max_records:
                break
    
    return pl.DataFrame(rows)


def read_fixed_width_text(filepath: Path, col_specs: list, encoding: str = 'ascii') -> pl.DataFrame:
    rows = []
    
    with open(filepath, 'r', encoding=encoding, errors='ignore') as f:
        for line in f:
            if line.strip():
                row = {}
                for col_name, start, end, col_type in col_specs:
                    if len(line) >= end:
                        value = line[start-1:end].strip()
                        if col_type == 'numeric':
                            try:
                                row[col_name.lower()] = float(value) if value else None
                            except:
                                row[col_name.lower()] = None
                        else:
                            row[col_name.lower()] = value
                    else:
                        row[col_name.lower()] = None if col_type == 'numeric' else ""
                rows.append(row)
    
    return pl.DataFrame(rows)


def parse_mmddyy8_from_z11_prefix_to_date(x) -> date | None:
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
    if d.month in (1, 3, 5, 7, 8, 10, 12):
        last = 31
    elif d.month in (4, 6, 9, 11):
        last = 30
    else:
        last = 29 if (d.year % 4 == 0) else 28
    return date(d.year, d.month, last)


def format_date_ddmmyyyy(d: date | None) -> str:
    if d is None:
        return "          "
    return f"{d.day:02d}/{d.month:02d}/{d.year:04d}"


# =========================
# Calculate REPTDATE
# =========================
REPTDATE = date.today() - timedelta(days=1)
REPTMON  = f"{REPTDATE.month:02d}"
REPTDAY  = f"{REPTDATE.day:02d}"
REPTYEAR = f"{REPTDATE.year:04d}"
SDATE_INT = date_to_sas_days(REPTDATE)
SDATE     = f"{SDATE_INT:05d}"
NORMDT = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

print(f"Report Date: {REPTDATE}")
print(f"Normalization Date: {NORMDT}")


# =========================
# Build LOAN0 / LOAN1
# =========================
print("Reading LOAN/LNNOTE datasets in chunks...")
print("Reading Islamic LNNOTE (ENTITY_CD = 'PIBB')...")
loani_ln = read_sas7bdat_filtered(LOANI_LNNOTE, entity_filter='PIBB', chunk_size=CHUNK_SIZE)
print(f"  Islamic LNNOTE rows: {loani_ln.height}")

print("Reading Conventional LNNOTE (ENTITY_CD != 'PIBB')...")
loan_ln = read_sas7bdat_filtered(LOAN_LNNOTE, entity_filter='NON_PIBB', chunk_size=CHUNK_SIZE)
print(f"  Conventional LNNOTE rows: {loan_ln.height}")

print("Combining LNNOTE datasets...")
loan_base = (
    pl.concat([loani_ln, loan_ln], how="vertical", rechunk=True)
    .with_columns([
        pl.col("loantype").alias("product"),
        pl.col("census").alias("censust"),
        pl.lit("    ").alias("sch")
    ])
)

loan_base = loan_base.with_columns([
    pl.when(pl.col("loantype") == 163).then(pl.lit("P94"))
     .when((pl.col("loantype") == 512) & (pl.col("census") == 512.01)).then(pl.lit("P93"))
     .when((pl.col("loantype") == 574) & (pl.col("census") == 574.02)).then(pl.lit("P93"))
     .when((pl.col("loantype") == 512) & (pl.col("census") == 512.00)).then(pl.lit("P101"))
     .otherwise(pl.col("sch"))
     .alias("sch")
])

loan_base = loan_base.filter(pl.col("sch") != "    ")

loan1 = loan_base.filter(pl.col("commno") > 0)
loan0 = loan_base.filter(~(pl.col("commno") > 0))

print(f"  LOAN0 rows: {loan0.height}")
print(f"  LOAN1 rows: {loan1.height}")


# =========================
# COMM processing
# =========================
print("Reading COMM datasets in chunks...")
print("Reading Islamic LNCOMM...")
loani_comm = read_sas7bdat_filtered(LOANI_LNCOMM, entity_filter='PIBB', chunk_size=CHUNK_SIZE)
print(f"  Islamic LNCOMM rows: {loani_comm.height}")

print("Reading Conventional LNCOMM...")
loan_comm = read_sas7bdat_filtered(LOAN_LNCOMM, entity_filter='NON_PIBB', chunk_size=CHUNK_SIZE)
print(f"  Conventional LNCOMM rows: {loan_comm.height}")

has_intamt = 'intamt' in loani_comm.columns or 'intamt' in loan_comm.columns

if has_intamt:
    comm = (
        pl.concat([loani_comm, loan_comm], how="vertical", rechunk=True)
        .with_columns([
            pl.when(pl.col("corgamt").is_null()).then(pl.lit(0.00)).otherwise(pl.col("corgamt")).alias("corgamt"),
            pl.when(pl.col("intamt").is_null()).then(pl.lit(0.00)).otherwise(pl.col("intamt")).alias("intamt"),
        ])
        .with_columns([
            (pl.col("corgamt") - pl.col("intamt")).alias("netproc")
        ])
        .select(["acctno", "commno", "netproc"])
    )
else:
    print("Warning: INTAMT column not found. Using CORGAMT as NETPROC.")
    comm = (
        pl.concat([loani_comm, loan_comm], how="vertical", rechunk=True)
        .with_columns([
            pl.when(pl.col("corgamt").is_null()).then(pl.lit(0.00)).otherwise(pl.col("corgamt")).alias("corgamt"),
        ])
        .with_columns([
            pl.col("corgamt").alias("netproc")
        ])
        .select(["acctno", "commno", "netproc"])
    )

if loan1.height > 0:
    loan1 = loan1.join(comm, on=["acctno", "commno"], how="inner")
else:
    loan1 = loan1.with_columns(pl.lit(None, dtype=pl.Float64).alias("netproc"))

if "netproc" not in loan0.columns:
    loan0 = loan0.with_columns(pl.lit(None, dtype=pl.Float64).alias("netproc"))

loan = pl.concat([loan0, loan1], how="vertical", rechunk=True)
print(f"Total LOAN rows after merge: {loan.height}")


# =========================
# Derive fields
# =========================
print("Calculating ISSUED, NODAYS, ARREARS, NPLDATE...")

loan = loan.with_columns([
    pl.lit(None, dtype=pl.Date).alias("issued"),
    pl.lit(0).alias("nodays"),
    pl.lit(0).alias("arrears")
])

loan = loan.with_columns([
    pl.when(pl.col("issuedt").is_not_null() & (pl.col("issuedt") > 0))
      .then(pl.col("issuedt").cast(pl.Int64)
            .map_elements(parse_mmddyy8_from_z11_prefix_to_date, return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("issued")
])

loan = loan.with_columns([
    pl.when((pl.col("bldate") > 0) & (pl.lit(SDATE_INT) > pl.col("bldate")))
      .then(pl.lit(SDATE_INT) - pl.col("bldate"))
      .otherwise(pl.lit(0))
      .alias("nodays")
])

print("Applying NDAYS format...")
loan = loan.with_columns([
    pl.col("nodays").map_elements(
        lambda x: informat(int(x) if x is not None else 0, "NDAYS", default=0), 
        return_dtype=pl.Int64
    ).alias("arrears")
])

loan = loan.with_columns([
    pl.when(pl.col("arrears") == 24)
      .then((pl.col("nodays").cast(pl.Float64) / 365.0 * 12.0).round(0).cast(pl.Int64))
      .otherwise(pl.col("arrears"))
      .alias("arrears")
])

def calculate_npldate(bldate_val, nodays_val):
    if nodays_val is None or nodays_val <= 89:
        return None
    
    adjusted_date = sas_days_to_date(int(bldate_val) + 90)
    npl_mm = adjusted_date.month
    npl_yy = adjusted_date.year
    npl_dd = month_end_of(adjusted_date).day
    
    return date(npl_yy, npl_mm, npl_dd)

loan = loan.with_columns([
    pl.struct(["bldate", "nodays"])
      .map_elements(lambda row: calculate_npldate(row["bldate"], row["nodays"]), 
                    return_dtype=pl.Date)
      .alias("npldate")
])

loan = loan.unique(subset=["acctno", "noteno"], keep="first")
print(f"LOAN rows after deduplication: {loan.height}")


# =========================
# CISLN processing
# =========================
print("Processing CISLN in chunks...")
cisln = read_sas7bdat_filtered(
    CISLN_LOAN, 
    column_filter={'seccust': '901'},
    chunk_size=CHUNK_SIZE
)

cisln = (
    cisln
      .select(["acctno", "newic", "custname"])
      .unique(subset=["acctno"], keep="first")
)
print(f"  CISLN rows after filter: {cisln.height}")

loan = loan.join(cisln, on="acctno", how="left")


# =========================
# COLL and DESC processing
# =========================
print("Processing COLL and DESC files...")

coll_specs = [
    ("ccollno", 4, 9, "pd"),
    ("acctno", 146, 151, "pd"),
    ("noteno", 153, 158, "pd")
]

desc_specs = [
    ("ccollno", 1, 11, "numeric"),
    ("cinstcl", 51, 52, "character"),
    ("natguar", 55, 56, "character"),
    ("census", 211, 220, "numeric"),
    ("tranche", 291, 298, "character")
]

# COLL: Record length 158
COLL_RECORD_LENGTH = 158

# DESC: Calculate correct record length
desc_file_size = DESC_FILE.stat().st_size
expected_desc_records = 58604
DESC_RECORD_LENGTH = desc_file_size // expected_desc_records

print(f"COLL record length: {COLL_RECORD_LENGTH}")
print(f"DESC file size: {desc_file_size}")
print(f"DESC record length: {DESC_RECORD_LENGTH}")

try:
    # Read COLL
    print("\nReading COLL file...")
    coll = read_ebcdic_fixed_records(COLL_FILE, COLL_RECORD_LENGTH, coll_specs)
    print(f"COLL rows: {coll.height}")
    
    # Read DESC with correct record length
    print("Reading DESC file...")
    desc = read_ebcdic_fixed_records(DESC_FILE, DESC_RECORD_LENGTH, desc_specs, max_records=expected_desc_records)
    print(f"DESC rows: {desc.height}")
    
    # ==========================================
    # DIAGNOSTIC: Scan DESC records for CINSTCL and NATGUAR
    # ==========================================
    print("\n=== Scanning DESC records for CINSTCL and NATGUAR positions ===")
    
    # Read first 5 DESC records to analyze structure
    with open(DESC_FILE, 'rb') as f:
        sample_records = []
        for i in range(5):
            record = f.read(DESC_RECORD_LENGTH)
            if not record or len(record) < DESC_RECORD_LENGTH:
                break
            sample_records.append(record)
    
    # For each sample record, decode and search for '18' and '06'
    for idx, record in enumerate(sample_records):
        decoded = record.decode('cp037', errors='ignore')
        
        # Search for '18' and '06' in the first 500 characters
        positions_18 = []
        positions_06 = []
        for i in range(min(len(decoded) - 1, 500)):
            if decoded[i:i+2] == '18':
                positions_18.append(i + 1)  # 1-based
            if decoded[i:i+2] == '06':
                positions_06.append(i + 1)  # 1-based
        
        print(f"\nRecord {idx + 1}:")
        print(f"  CCOLLNO (pos 1-11): '{decoded[0:11].strip()}'")
        
        if positions_18:
            print(f"  Positions with '18' in first 500 chars: {positions_18}")
        if positions_06:
            print(f"  Positions with '06' in first 500 chars: {positions_06}")
        
        # Show context around first '18' and '06'
        if positions_18:
            p = positions_18[0] - 1  # 0-based
            print(f"  Context around '18' at pos {positions_18[0]}: ...{decoded[max(0,p-10):p+12]}...")
        if positions_06:
            p = positions_06[0] - 1  # 0-based
            print(f"  Context around '06' at pos {positions_06[0]}: ...{decoded[max(0,p-10):p+12]}...")
        
        # Show positions 51-52 and 55-56
        print(f"  Pos 51-52 (CINSTCL per SAS): '{decoded[50:52]}'")
        print(f"  Pos 55-56 (NATGUAR per SAS): '{decoded[54:56]}'")
    
    print("\n=== End Diagnostic ===")
    # ==========================================
    
    # Print sample data
    print("\n=== DESC Data Sample (first 3 rows) ===")
    print(desc.head(3))
    
    # Convert types
    coll = coll.with_columns(pl.col("ccollno").cast(pl.Float64).alias("ccollno"))
    desc = desc.with_columns(pl.col("ccollno").cast(pl.Float64).alias("ccollno"))
    
    coll = coll.with_columns([
        pl.col("acctno").cast(pl.Float64).alias("acctno"),
        pl.col("noteno").cast(pl.Float64).alias("noteno")
    ])
    
    # Sort and join
    coll = coll.sort(by="ccollno")
    desc = desc.sort(by="ccollno")
    
    coll_joined = coll.join(desc, on="ccollno", how="inner")
    print(f"\nCOLL rows after join: {coll_joined.height}")
    
    # Filter (using original SAS positions)
    coll_filtered = coll_joined.filter((pl.col("cinstcl") == "18") & (pl.col("natguar") == "06"))
    print(f"COLL rows after filter: {coll_filtered.height}")
    
    coll = coll_filtered
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    coll = pl.DataFrame(schema={"ccollno": pl.Float64, "acctno": pl.Float64, "noteno": pl.Float64, 
                                "cinstcl": pl.Utf8, "natguar": pl.Utf8, "census": pl.Float64, "tranche": pl.Utf8})

print(f"\nFinal COLL rows: {coll.height}")


# =========================
# NPGS merge
# =========================
if loan.height > 0 and coll.height > 0:
    if loan.schema["acctno"] != pl.Float64:
        loan = loan.with_columns(pl.col("acctno").cast(pl.Float64).alias("acctno"))
    if loan.schema["noteno"] != pl.Float64:
        loan = loan.with_columns(pl.col("noteno").cast(pl.Float64).alias("noteno"))

npgs = loan.join(coll, on=["acctno", "noteno"], how="inner")
print(f"NPGS rows after COLL merge: {npgs.height}")

npgs = npgs.sort(by="pendbrh")


# =========================
# MICR processing
# =========================
print("Processing MICR file...")

micr_specs = [
    ("pendbrh", 1, 3, "numeric"),
    ("micrcd", 40, 44, "character")
]

try:
    micr = read_fixed_width_text(MICR_FILE, micr_specs, encoding='ascii')
    micr = micr.sort(by="pendbrh")
    print(f"  MICR rows: {micr.height}")
except Exception as e:
    print(f"Warning: Error reading MICR file: {e}")
    micr = pl.DataFrame(schema={"pendbrh": pl.Float64, "micrcd": pl.Utf8})

if npgs.height > 0 and micr.height > 0:
    if npgs.schema["pendbrh"] != micr.schema["pendbrh"]:
        npgs = npgs.with_columns(pl.col("pendbrh").cast(pl.Float64).alias("pendbrh"))
        micr = micr.with_columns(pl.col("pendbrh").cast(pl.Float64).alias("pendbrh"))

npgs = npgs.join(micr, on="pendbrh", how="left")


# =========================
# CVAR fields
# =========================
print("Creating CVAR fields...")

npgs = npgs.with_columns([pl.lit("   ").alias("cvar02")])

npgs = npgs.with_columns([
    pl.when(pl.col("sch") == "P93").then(pl.lit("93"))
     .when(pl.col("sch") == "P94").then(pl.lit("94"))
     .when(pl.col("sch") == "P101").then(pl.lit("101"))
     .otherwise(pl.col("cvar02"))
     .alias("cvar02")
])

npgs = npgs.filter(pl.col("cvar02") != "   ")

npgs = npgs.with_columns([
    pl.col("census").alias("cvar01"),
    pl.col("newic").alias("cvar03"),
    pl.col("custname").alias("cvar04"),
    pl.col("issued").alias("cvar05"),
    pl.col("acctno").alias("cvar06"),
    pl.lit("FL").alias("cvar07"),
    pl.col("netproc").alias("cvar08"),
    pl.col("balance").alias("cvar09"),
    pl.lit(0.00).alias("cvar10"),
    pl.col("arrears").alias("cvar11"),
    pl.lit("   ").alias("cvar12"),
    pl.lit("          ").alias("cvar13"),
    pl.lit("0233").alias("cvar14"),
    pl.col("micrcd").alias("cvar15"),
    pl.col("pendbrh").alias("branch"),
    pl.lit("TL").alias("cvar16"),
    pl.col("curbal").alias("cvar17"),
])

if "name" in npgs.columns:
    npgs = npgs.with_columns([
        pl.when(pl.col("cvar04") == "  ")
          .then(pl.col("name"))
          .otherwise(pl.col("cvar04"))
          .alias("cvar04")
    ])

npgs = npgs.with_columns([
    pl.when(pl.col("npldate").is_not_null())
      .then(pl.col("npldate").map_elements(format_date_ddmmyyyy, return_dtype=pl.Utf8))
      .otherwise(pl.lit("          "))
      .alias("cvar13")
])

npgs = npgs.with_columns([pl.lit(NORMDT).alias("normdt")])

npgs = npgs.with_columns([
    pl.when((pl.col("arrears") >= 3) & pl.col("npldate").is_not_null())
      .then(pl.lit("NPL"))
      .otherwise(pl.col("cvar12"))
      .alias("cvar12")
])


# =========================
# NPL status logic
# =========================
npgs = npgs.sort(by=["cvar06", "cvar01"])

if NPGS_SMEZ.exists():
    npla = read_sas7bdat(NPGS_SMEZ).sort(by=["cvar06", "cvar01"])
    npgs = npgs.join(npla, on=["cvar06", "cvar01"], how="left", suffix="_npla")
else:
    npgs = npgs.with_columns([
        pl.lit(None).alias("status"),
        pl.lit("          ").alias("ndate")
    ])

def adjust_cvar13(row):
    cvar12 = row.get("cvar12", "   ")
    status = row.get("status", "   ")
    ndate = row.get("ndate", "          ")
    cvar13 = row.get("cvar13", "          ")
    normdt = row.get("normdt", "          ")
    
    if cvar12 == "NPL":
        if status == "NPL":
            return ndate
        return cvar13
    else:
        if status == "NPL":
            return normdt
        if status == "   " and ndate != "          ":
            return ndate
        return cvar13

npgs = npgs.with_columns([
    pl.struct(["cvar12", "status", "ndate", "cvar13", "normdt"])
      .map_elements(adjust_cvar13, return_dtype=pl.Utf8)
      .alias("cvar13")
])

npgs = npgs.sort(by="cvar01")


# =========================
# Final output
# =========================
for c in ["costctr", "balance", "curbal", "accrual", "tranche", "sch", 
          "censust", "product", "natguar", "cinstcl"]:
    if c not in npgs.columns:
        npgs = npgs.with_columns(pl.lit(None).alias(c))

keep_cols = [
    "cvar01", "cvar02", "cvar03", "cvar04", "cvar05", "cvar06", "cvar07",
    "cvar08", "cvar09", "cvar10", "cvar11", "cvar12", "cvar13", "cvar14",
    "costctr", "balance", "curbal", "accrual", "tranche",
    "branch", "cvar15", "censust", "product", "natguar", "cinstcl", "sch",
    "cvar16", "cvar17"
]

out = npgs.select(keep_cols)
out = out.rename({col: col.upper() for col in out.columns})


# =========================
# Write output
# =========================
print(f"Writing NPGS.LNSMEZ{REPTMON}...")

out_pandas = out.to_pandas()

sas = saspy.SASsession(results='TEXT')

sas.submit(f"""
    libname npgs "{BASE_OUTPUT}/NPGS";
    options nofmterr;
""")

sas_df = sas.df2sd(out_pandas, table='work.temp_out')

sas.submit(f"""
    data npgs.lnsmez{REPTMON};
        set work.temp_out;
        format CVAR01 CVAR06 10. 
               CVAR03 $15. 
               CVAR04 $50. 
               CVAR14 $4.
               CVAR13 $10. 
               CVAR08 CVAR09 CVAR10 CVAR17 10.2 
               CVAR11 5.
               CVAR02 $3.
               CVAR12 $3.
               CVAR15 $5.
               CVAR16 $2.
               CVAR07 $2.;
    run;
    
    proc datasets lib=npgs nolist;
        modify lnsmez{REPTMON};
        label
            CVAR01='Census'
            CVAR02='Schedule Code'
            CVAR03='New IC'
            CVAR04='Customer Name'
            CVAR05='Issue Date'
            CVAR06='Account Number'
            CVAR07='Flag'
            CVAR08='Net Proceeds'
            CVAR09='Balance'
            CVAR10='Zero Balance'
            CVAR11='Arrears'
            CVAR12='NPL Status'
            CVAR13='NPL Date'
            CVAR14='Constant Value'
            CVAR15='MICR Code'
            CVAR16='Type'
            CVAR17='Current Balance';
    run;
""")

print(f"Successfully wrote NPGS.LNSMEZ{REPTMON} to {BASE_OUTPUT}/NPGS")

sas.endsas()
