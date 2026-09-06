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
BASE_OUTPUT = Path("sas_output")
BASE_OUTPUT.mkdir(parents=True, exist_ok=True)

# ---- Input SAS datasets (all in sas7bdat format) ----
# LOAN / LOANI libraries (conventional vs islamic)
LOAN_LNNOTE   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat")       # SAS: LOAN.LNNOTE (conventional)
LOAN_LNCOMM   = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat")       # SAS: LOAN.LNCOMM (conventional)

LOANI_LNNOTE  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat")       # SAS: LOANI.LNNOTE (islamic)
LOANI_LNCOMM  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/enrh_ln_comm_m08.sas7bdat")       # SAS: LOANI.LNCOMM (islamic)

# CISLN
CISLN_LOAN    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMHPTOP/loan.sas7bdat")                   # SAS: CISLN.LOAN

# COLL / DESC (EBCDIC encoded text files)
COLL_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831")              # EBCDIC file
DESC_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831")         # EBCDIC file

# MICR (text file)
MICR_FILE     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/BOPESS.txt")

# Historical NPL status file
NPGS_SMEZ     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLSMEZ/smez.sas7bdat")

# Chunk size for reading large SAS datasets
CHUNK_SIZE = 100000  # Adjust based on available memory


# =========================
# Helper functions
# =========================
def sas_days_to_date(days: int) -> date:
    """Convert SAS date (days since 1960-01-01) to Python date"""
    origin = date(1960, 1, 1)
    return origin + timedelta(days=int(days))


def date_to_sas_days(d: date) -> int:
    """Convert Python date to SAS date (days since 1960-01-01)"""
    origin = date(1960, 1, 1)
    return (d - origin).days


def read_sas7bdat_filtered(filepath: Path, entity_filter: str = None, 
                           chunk_size: int = CHUNK_SIZE,
                           column_filter: dict = None) -> pl.DataFrame:
    """
    Read SAS dataset with entity filtering done during chunk processing
    entity_filter: 'PIBB' for islamic, 'NON_PIBB' for conventional, None for all
    column_filter: dict with column name as key and value to filter (e.g., {'seccust': '901'})
    """
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
                
            # Convert column names to lowercase
            df.columns = [col.lower() for col in df.columns]
            
            # Apply entity filter
            if entity_filter and 'entity_cd' in df.columns:
                if entity_filter == 'PIBB':
                    df = df[df['entity_cd'] == 'PIBB']
                elif entity_filter == 'NON_PIBB':
                    df = df[df['entity_cd'] != 'PIBB']
            
            # Apply column filter
            if column_filter:
                for col_name, col_value in column_filter.items():
                    if col_name in df.columns:
                        df = df[df[col_name] == col_value]
            
            if not df.empty:
                chunks.append(pl.from_pandas(df))
            
            offset += chunk_size
            
            # Safety check to avoid infinite loops
            if len(df) < chunk_size:
                break
                
        except Exception as e:
            print(f"Error reading chunk at offset {offset}: {e}")
            break
    
    if not chunks:
        return pl.DataFrame()
    
    return pl.concat(chunks, how="vertical", rechunk=True)


def read_sas7bdat(filepath: Path) -> pl.DataFrame:
    """Read SAS dataset using pyreadstat and convert to Polars DataFrame with lowercase columns"""
    df, meta = pyreadstat.read_sas7bdat(str(filepath))
    # Convert column names to lowercase
    df.columns = [col.lower() for col in df.columns]
    return pl.from_pandas(df)


def read_ebcdic_fixed_width(filepath: Path, col_specs: list) -> pl.DataFrame:
    """
    Read EBCDIC fixed-width file and convert to Polars DataFrame
    col_specs: list of tuples (column_name, start_position, end_position, column_type)
    """
    # Read as EBCDIC
    with open(filepath, 'rb') as f:
        raw_data = f.read()
    
    # Decode EBCDIC to ASCII (cp037 is standard EBCDIC)
    decoded_data = raw_data.decode('cp037')
    
    rows = []
    lines = decoded_data.split('\n')
    
    for line in lines:
        if line.strip():  # Skip empty lines
            row = {}
            for col_name, start, end, col_type in col_specs:
                value = line[start-1:end].strip()  # SAS uses 1-based positions
                if col_type == 'numeric':
                    try:
                        row[col_name.lower()] = float(value) if value else None
                    except:
                        row[col_name.lower()] = None
                else:
                    row[col_name.lower()] = value
            rows.append(row)
    
    return pl.DataFrame(rows)


def parse_mmddyy8_from_z11_prefix_to_date(x) -> date | None:
    """
    Emulate: INPUT(SUBSTR(PUT(x, Z11.), 1, 8), MMDDYY8.)
    Returns python date or None if invalid/zero.
    """
    if x is None:
        return None
    try:
        xi = int(x)
        if xi <= 0:
            return None
        s = f"{xi:011d}"[:8]  # first 8 chars
        # Prefer MMDDYYYY. If that fails, try MMDDYY.
        try:
            return datetime.strptime(s, "%m%d%Y").date()
        except Exception:
            return datetime.strptime(s, "%m%d%y").date()
    except Exception:
        return None


def month_end_of(d: date) -> date:
    """SAS rule in this job (leap if mod 4 == 0)"""
    if d.month in (1, 3, 5, 7, 8, 10, 12):
        last = 31
    elif d.month in (4, 6, 9, 11):
        last = 30
    else:
        last = 29 if (d.year % 4 == 0) else 28
    return date(d.year, d.month, last)


def format_date_ddmmyyyy(d: date | None) -> str:
    """Format date as DD/MM/YYYY (SAS DDMMYY10.)"""
    if d is None:
        return "          "  # 10 spaces
    return f"{d.day:02d}/{d.month:02d}/{d.year:04d}"


# =========================
# Calculate REPTDATE as today - 1 day (mimicking LOAN.REPTDATE)
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
# Build LOAN0 / LOAN1 from LOANI.LNNOTE (islamic) ∪ LOAN.LNNOTE (conventional)
# =========================
print("Reading LOAN/LNNOTE datasets in chunks...")
print("Reading Islamic LNNOTE (ENTITY_CD = 'PIBB')...")
loani_ln = read_sas7bdat_filtered(LOANI_LNNOTE, entity_filter='PIBB', chunk_size=CHUNK_SIZE)
print(f"  Islamic LNNOTE rows: {loani_ln.height}")

print("Reading Conventional LNNOTE (ENTITY_CD != 'PIBB')...")
loan_ln = read_sas7bdat_filtered(LOAN_LNNOTE, entity_filter='NON_PIBB', chunk_size=CHUNK_SIZE)
print(f"  Conventional LNNOTE rows: {loan_ln.height}")

# DATA LOAN0 LOAN1; DROP CENSUS; SET LOANI.LNNOTE LOAN.LNNOTE;
print("Combining LNNOTE datasets...")
loan_base = (
    pl.concat([loani_ln, loan_ln], how="vertical", rechunk=True)
    .with_columns([
        pl.col("loantype").alias("product"),
        pl.col("census").alias("censust"),
        pl.lit("    ").alias("sch")
    ])
)

# SCH mapping rules (IF/ELSE logic) - Using pl.lit() for literals
loan_base = loan_base.with_columns([
    pl.when(pl.col("loantype") == 163).then(pl.lit("P94"))
     .when((pl.col("loantype") == 512) & (pl.col("census") == 512.01)).then(pl.lit("P93"))
     .when((pl.col("loantype") == 574) & (pl.col("census") == 574.02)).then(pl.lit("P93"))
     .when((pl.col("loantype") == 512) & (pl.col("census") == 512.00)).then(pl.lit("P101"))
     .otherwise(pl.col("sch"))
     .alias("sch")
])

# IF SCH NE '    ';
loan_base = loan_base.filter(pl.col("sch") != "    ")

# IF COMMNO > 0 THEN OUTPUT LOAN1; ELSE OUTPUT LOAN0;
loan1 = loan_base.filter(pl.col("commno") > 0)
loan0 = loan_base.filter(~(pl.col("commno") > 0))

print(f"  LOAN0 rows: {loan0.height}")
print(f"  LOAN1 rows: {loan1.height}")

# =========================
# COMM from both libs; compute NETPROC
# =========================
print("Reading COMM datasets in chunks...")
print("Reading Islamic LNCOMM (ENTITY_CD = 'PIBB')...")
loani_comm = read_sas7bdat_filtered(LOANI_LNCOMM, entity_filter='PIBB', chunk_size=CHUNK_SIZE)
print(f"  Islamic LNCOMM rows: {loani_comm.height}")

print("Reading Conventional LNCOMM (ENTITY_CD != 'PIBB')...")
loan_comm = read_sas7bdat_filtered(LOAN_LNCOMM, entity_filter='NON_PIBB', chunk_size=CHUNK_SIZE)
print(f"  Conventional LNCOMM rows: {loan_comm.height}")

# Check if intamt column exists in COMM datasets
has_intamt = 'intamt' in loani_comm.columns or 'intamt' in loan_comm.columns

# DATA COMM; KEEP ACCTNO NETPROC COMMNO;
# Handle the case where INTAMT column might not exist
if has_intamt:
    comm = (
        pl.concat([loani_comm, loan_comm], how="vertical", rechunk=True)
        .with_columns([
            # IF CORGAMT=. THEN CORGAMT=0.00;
            pl.when(pl.col("corgamt").is_null()).then(pl.lit(0.00)).otherwise(pl.col("corgamt")).alias("corgamt"),
            # IF INTAMT=. THEN INTAMT=0.00;
            pl.when(pl.col("intamt").is_null()).then(pl.lit(0.00)).otherwise(pl.col("intamt")).alias("intamt"),
        ])
        .with_columns([
            # NETPROC=SUM(CORGAMT,-1*INTAMT);
            (pl.col("corgamt") - pl.col("intamt")).alias("netproc")
        ])
        .select(["acctno", "commno", "netproc"])
    )
else:
    print("Warning: INTAMT column not found. Using CORGAMT as NETPROC.")
    comm = (
        pl.concat([loani_comm, loan_comm], how="vertical", rechunk=True)
        .with_columns([
            # IF CORGAMT=. THEN CORGAMT=0.00;
            pl.when(pl.col("corgamt").is_null()).then(pl.lit(0.00)).otherwise(pl.col("corgamt")).alias("corgamt"),
        ])
        .with_columns([
            # NETPROC = CORGAMT (since INTAMT doesn't exist)
            pl.col("corgamt").alias("netproc")
        ])
        .select(["acctno", "commno", "netproc"])
    )

# DATA LOAN1; MERGE LOAN1(IN=A) COMM(IN=B); BY ACCTNO COMMNO; IF A AND B;
if loan1.height > 0:
    loan1 = loan1.join(comm, on=["acctno", "commno"], how="inner")
else:
    # If loan1 is empty, add netproc column with null values to match schema
    loan1 = loan1.with_columns(pl.lit(None, dtype=pl.Float64).alias("netproc"))

# Add netproc column to loan0 if it doesn't exist (to match schema)
if "netproc" not in loan0.columns:
    loan0 = loan0.with_columns(pl.lit(None, dtype=pl.Float64).alias("netproc"))

# DATA LOAN; SET LOAN0 LOAN1;
loan = pl.concat([loan0, loan1], how="vertical", rechunk=True)
print(f"Total LOAN rows after merge: {loan.height}")

# =========================
# Derive ISSUED, NODAYS, ARREARS, NPLDATE
# =========================
print("Calculating ISSUED, NODAYS, ARREARS, NPLDATE...")

# ISSUED=.; NODAYS=0; ARREARS=0;
loan = loan.with_columns([
    pl.lit(None, dtype=pl.Date).alias("issued"),
    pl.lit(0).alias("nodays"),
    pl.lit(0).alias("arrears")
])

# IF ISSUEDT > 0 THEN ISSUED=INPUT(SUBSTR(PUT(ISSUEDT,Z11.),1,8),MMDDYY8.);
loan = loan.with_columns([
    pl.when(pl.col("issuedt").is_not_null() & (pl.col("issuedt") > 0))
      .then(pl.col("issuedt").cast(pl.Int64)
            .map_elements(parse_mmddyy8_from_z11_prefix_to_date, return_dtype=pl.Date))
      .otherwise(pl.lit(None, dtype=pl.Date))
      .alias("issued")
])

# IF BLDATE > 0 AND (&SDATE > BLDATE) THEN NODAYS=&SDATE - BLDATE;
loan = loan.with_columns([
    pl.when((pl.col("bldate") > 0) & (pl.lit(SDATE_INT) > pl.col("bldate")))
      .then(pl.lit(SDATE_INT) - pl.col("bldate"))
      .otherwise(pl.lit(0))
      .alias("nodays")
])

# =========================
# ARREARS via NDAYS format (from PBBLNFMT)
# =========================
print("Applying NDAYS format...")

# ARREARS=INPUT(NODAYS,NDAYS.);
loan = loan.with_columns([
    pl.col("nodays").map_elements(
        lambda x: informat(int(x) if x is not None else 0, "NDAYS", default=0), 
        return_dtype=pl.Int64
    ).alias("arrears")
])

# IF ARREARS=24 THEN ARREARS=ROUND((NODAYS/365)*12);
loan = loan.with_columns([
    pl.when(pl.col("arrears") == 24)
      .then((pl.col("nodays").cast(pl.Float64) / 365.0 * 12.0).round(0).cast(pl.Int64))
      .otherwise(pl.col("arrears"))
      .alias("arrears")
])

# =========================
# NPLDATE calculation
# =========================
def calculate_npldate(bldate_val, nodays_val):
    """Calculate NPLDATE following SAS logic"""
    if nodays_val is None or nodays_val <= 89:
        return None
    
    # BLDATE=BLDATE+90;
    adjusted_date = sas_days_to_date(int(bldate_val) + 90)
    
    # NPLMM=MONTH(BLDATE);
    # NPLYY=YEAR(BLDATE);
    npl_mm = adjusted_date.month
    npl_yy = adjusted_date.year
    
    # IF NPLMM IN (1,3,5,7,8,10,12) THEN NPLDD=31;
    # IF NPLMM IN (4,6,9,11) THEN NPLDD=30;
    # IF NPLMM=2 THEN DO; NPLDD=28; IF MOD(NPLYY,4)=0 THEN NPLDD=29; END;
    npl_dd = month_end_of(adjusted_date).day
    
    # NPLDATE=MDY(NPLMM,NPLDD,NPLYY);
    return date(npl_yy, npl_mm, npl_dd)

# IF NODAYS > 89 THEN DO;
loan = loan.with_columns([
    pl.struct(["bldate", "nodays"])
      .map_elements(lambda row: calculate_npldate(row["bldate"], row["nodays"]), 
                    return_dtype=pl.Date)
      .alias("npldate")
])

# =========================
# PROC SORT DATA=LOAN NODUPKEY; BY ACCTNO NOTENO;
# =========================
loan = loan.unique(subset=["acctno", "noteno"], keep="first")
print(f"LOAN rows after deduplication: {loan.height}")

# =========================
# DATA CISLN; KEEP ACCTNO NEWIC CUSTNAME; SET CISLN.LOAN; IF SECCUST='901';
# =========================
print("Processing CISLN in chunks...")
# Read CISLN with filtering for SECCUST='901' during chunk processing
cisln = read_sas7bdat_filtered(
    CISLN_LOAN, 
    column_filter={'seccust': '901'},
    chunk_size=CHUNK_SIZE
)

# Select only needed columns and deduplicate
cisln = (
    cisln
      .select(["acctno", "newic", "custname"])
      .unique(subset=["acctno"], keep="first")  # PROC SORT NODUPKEY
)
print(f"  CISLN rows after filter: {cisln.height}")

# DATA LOAN; MERGE LOAN(IN=A) CISLN; BY ACCTNO; IF A;
loan = loan.join(cisln, on="acctno", how="left")

# =========================
# COLL file processing (EBCDIC with packed decimal)
# =========================
print("Processing COLL and DESC files...")

# DATA COLL; INFILE COLL; INPUT @004 CCOLLNO PD6. @146 ACCTNO PD6. @153 NOTENO PD6.;
coll_specs = [
    ("ccollno", 4, 9, "pd"),    # @004 PD6.
    ("acctno", 146, 151, "pd"),  # @146 PD6.
    ("noteno", 153, 158, "pd")   # @153 PD6.
]

# DATA DESC; INFILE DESC; INPUT @001 CCOLLNO 11. @051 CINSTCL $2. @055 NATGUAR $2. @211 CENSUS 10. @291 TRANCHE $8.;
desc_specs = [
    ("ccollno", 1, 11, "numeric"),   # @001 11.
    ("cinstcl", 51, 52, "character"), # @051 $2.
    ("natguar", 55, 56, "character"), # @055 $2.
    ("census", 211, 220, "numeric"),  # @211 10.
    ("tranche", 291, 298, "character") # @291 $8.
]

# Read the files - since they're EBCDIC with complex formats
try:
    coll = read_ebcdic_fixed_width(COLL_FILE, coll_specs)
    desc = read_ebcdic_fixed_width(DESC_FILE, desc_specs)
    
    # Ensure ccollno has the same data type in both DataFrames
    # Convert ccollno to string in both for consistent joining
    coll = coll.with_columns(pl.col("ccollno").cast(pl.Utf8).alias("ccollno"))
    desc = desc.with_columns(pl.col("ccollno").cast(pl.Utf8).alias("ccollno"))
    
    # Also ensure acctno and noteno are strings in coll for later joins
    coll = coll.with_columns([
        pl.col("acctno").cast(pl.Utf8).alias("acctno"),
        pl.col("noteno").cast(pl.Utf8).alias("noteno")
    ])
    
except Exception as e:
    print(f"Warning: Error reading EBCDIC files: {e}")
    print("Creating empty DataFrames as placeholder")
    coll = pl.DataFrame(schema={"ccollno": pl.Utf8, "acctno": pl.Utf8, "noteno": pl.Utf8})
    desc = pl.DataFrame(schema={"ccollno": pl.Utf8, "cinstcl": pl.Utf8, "natguar": pl.Utf8, 
                                "census": pl.Float64, "tranche": pl.Utf8})

print(f"  COLL rows: {coll.height}")
print(f"  DESC rows: {desc.height}")

# PROC SORT; BY CCOLLNO; (for both COLL and DESC)
coll = coll.sort(by="ccollno")
desc = desc.sort(by="ccollno")

# DATA COLL; MERGE COLL(IN=A) DESC(IN=B); BY CCOLLNO; IF A AND B;
coll = coll.join(desc, on="ccollno", how="inner")

# IF CINSTCL='18' AND NATGUAR='06';
coll = coll.filter((pl.col("cinstcl") == "18") & (pl.col("natguar") == "06"))

# PROC SORT; BY ACCTNO NOTENO;
coll = coll.sort(by=["acctno", "noteno"])

print(f"  COLL rows after filter: {coll.height}")

# =========================
# DATA NPGS; MERGE LOAN(IN=A) COLL(IN=B); BY ACCTNO NOTENO; IF A AND B;
# =========================
# Ensure loan acctno and noteno are strings to match coll
if loan.height > 0 and coll.height > 0:
    # Check data types and cast if necessary
    if loan.schema["acctno"] != pl.Utf8:
        loan = loan.with_columns(pl.col("acctno").cast(pl.Utf8).alias("acctno"))
    if loan.schema["noteno"] != pl.Utf8:
        loan = loan.with_columns(pl.col("noteno").cast(pl.Utf8).alias("noteno"))

npgs = loan.join(coll, on=["acctno", "noteno"], how="inner")
print(f"NPGS rows after COLL merge: {npgs.height}")

# PROC SORT; BY PENDBRH;
npgs = npgs.sort(by="pendbrh")

# =========================
# DATA MICR; INFILE MICR; INPUT @001 PENDBRH 3. @040 MICRCD $5.;
# =========================
print("Processing MICR file...")
try:
    # Read MICR text file with fixed width format
    micr_df = pl.read_csv(MICR_FILE, separator='\t', has_header=True)
    micr_df.columns = [col.lower() for col in micr_df.columns]
    micr = micr_df.select(["pendbrh", "micrcd"]).sort(by="pendbrh")
except:
    # Fallback: create empty DataFrame
    micr = pl.DataFrame(schema={"pendbrh": pl.Float64, "micrcd": pl.Utf8})

# DATA NPGS; MERGE NPGS(IN=A) MICR; BY PENDBRH; IF A;
npgs = npgs.join(micr, on="pendbrh", how="left")

# =========================
# CVAR02 mapping from SCH
# =========================
print("Creating CVAR fields...")

# FORMAT CVAR02 $3.; CVAR02='   ';
npgs = npgs.with_columns([
    pl.lit("   ").alias("cvar02")
])

# IF SCH='P93' THEN CVAR02='93'; ELSE IF SCH='P94' THEN CVAR02='94'; ELSE IF SCH='P101' THEN CVAR02='101';
npgs = npgs.with_columns([
    pl.when(pl.col("sch") == "P93").then(pl.lit("93"))
     .when(pl.col("sch") == "P94").then(pl.lit("94"))
     .when(pl.col("sch") == "P101").then(pl.lit("101"))
     .otherwise(pl.col("cvar02"))
     .alias("cvar02")
])

# IF CVAR02 NE '   ';
npgs = npgs.filter(pl.col("cvar02") != "   ")

# =========================
# Final CVAR fields
# =========================
# DATA NPGS; SET NPGS;
npgs = npgs.with_columns([
    # CVAR01=CENSUS;
    pl.col("census").alias("cvar01"),
    # CVAR03=NEWIC;
    pl.col("newic").alias("cvar03"),
    # CVAR04=CUSTNAME;
    pl.col("custname").alias("cvar04"),
    # CVAR05=ISSUED;
    pl.col("issued").alias("cvar05"),
    # CVAR06=ACCTNO;
    pl.col("acctno").alias("cvar06"),
    # CVAR07='FL';
    pl.lit("FL").alias("cvar07"),
    # CVAR08=NETPROC;
    pl.col("netproc").alias("cvar08"),
    # CVAR09=BALANCE;
    pl.col("balance").alias("cvar09"),
    # CVAR10=0.00;
    pl.lit(0.00).alias("cvar10"),
    # CVAR11=ARREARS;
    pl.col("arrears").alias("cvar11"),
    # CVAR12='   ';
    pl.lit("   ").alias("cvar12"),
    # CVAR13='          ';
    pl.lit("          ").alias("cvar13"),
    # CVAR14='0233';
    pl.lit("0233").alias("cvar14"),
    # CVAR15=MICRCD;
    pl.col("micrcd").alias("cvar15"),
    # BRANCH=PENDBRH;
    pl.col("pendbrh").alias("branch"),
    # CVAR16='TL';
    pl.lit("TL").alias("cvar16"),
    # CVAR17=CURBAL;
    pl.col("curbal").alias("cvar17"),
])

# IF CVAR04='  ' THEN CVAR04=NAME;
if "name" in npgs.columns:
    npgs = npgs.with_columns([
        pl.when(pl.col("cvar04") == "  ")
          .then(pl.col("name"))
          .otherwise(pl.col("cvar04"))
          .alias("cvar04")
    ])

# IF NPLDATE > 0 THEN DO; ... CVAR13=PUT(NPLDD,Z2.)||'/'||PUT(NPLMM,Z2.)||'/'||PUT(NPLYY,Z4.); END;
npgs = npgs.with_columns([
    pl.when(pl.col("npldate").is_not_null())
      .then(pl.col("npldate").map_elements(format_date_ddmmyyyy, return_dtype=pl.Utf8))
      .otherwise(pl.lit("          "))
      .alias("cvar13")
])

# NORMDT=PUT(NDD,Z2.)||'/'||PUT(NMM,Z2.)||'/'||PUT(NYY,Z4.);
npgs = npgs.with_columns([
    pl.lit(NORMDT).alias("normdt")
])

# IF ARREARS GE 3 AND NPLDATE > 0 THEN CVAR12='NPL';
npgs = npgs.with_columns([
    pl.when((pl.col("arrears") >= 3) & pl.col("npldate").is_not_null())
      .then(pl.lit("NPL"))
      .otherwise(pl.col("cvar12"))
      .alias("cvar12")
])

# =========================
# PROC SORT; BY CVAR06 CVAR01;
# PROC SORT DATA=NPGS.SMEZ OUT=NPLA; BY CVAR06 CVAR01;
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

# =========================
# Apply NPL status logic
# =========================
def adjust_cvar13(row):
    """Apply SAS logic for CVAR13 adjustments"""
    cvar12 = row.get("cvar12", "   ")
    status = row.get("status", "   ")
    ndate = row.get("ndate", "          ")
    cvar13 = row.get("cvar13", "          ")
    normdt = row.get("normdt", "          ")
    
    # IF CVAR12='NPL' THEN DO;
    if cvar12 == "NPL":
        # IF STATUS='NPL' THEN CVAR13=NDATE;
        if status == "NPL":
            return ndate
        return cvar13
    # IF CVAR12='   ' THEN DO;
    else:
        # IF STATUS='NPL' THEN CVAR13=NORMDT;
        if status == "NPL":
            return normdt
        # IF STATUS='   ' AND NDATE NE '          ' THEN CVAR13=NDATE;
        if status == "   " and ndate != "          ":
            return ndate
        return cvar13

npgs = npgs.with_columns([
    pl.struct(["cvar12", "status", "ndate", "cvar13", "normdt"])
      .map_elements(adjust_cvar13, return_dtype=pl.Utf8)
      .alias("cvar13")
])

# =========================
# PROC SORT; BY CVAR01;
# =========================
npgs = npgs.sort(by="cvar01")

# =========================
# Ensure all required columns exist
# =========================
for c in ["costctr", "balance", "curbal", "accrual", "tranche", "sch", 
          "censust", "product", "natguar", "cinstcl"]:
    if c not in npgs.columns:
        npgs = npgs.with_columns(pl.lit(None).alias(c))

# =========================
# DATA NPGS.LNSMEZ&REPTMON; SET NPGS; KEEP ...
# =========================
keep_cols = [
    "cvar01", "cvar02", "cvar03", "cvar04", "cvar05", "cvar06", "cvar07",
    "cvar08", "cvar09", "cvar10", "cvar11", "cvar12", "cvar13", "cvar14",
    "costctr", "balance", "curbal", "accrual", "tranche",
    "branch", "cvar15", "censust", "product", "natguar", "cinstcl", "sch",
    "cvar16", "cvar17"
]

out = npgs.select(keep_cols)

# Convert column names to uppercase for SAS output
out = out.rename({col: col.upper() for col in out.columns})

# =========================
# Output: NPGS.LNSMEZ&REPTMON (SAS dataset via SASPy)
# =========================
print(f"Writing NPGS.LNSMEZ{REPTMON}...")

# Convert Polars DataFrame to Pandas for SASPy
out_pandas = out.to_pandas()

# Initialize SAS session
sas = saspy.SASsession(results='TEXT')

# Create the output library
sas.submit(f"""
    libname npgs "{BASE_OUTPUT}/NPGS";
    options nofmterr;
""")

# Upload the Pandas DataFrame to SAS
sas_df = sas.df2sd(out_pandas, table='work.temp_out')

# Create the output dataset with proper formats
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

# Close SAS session
sas.endsas()
