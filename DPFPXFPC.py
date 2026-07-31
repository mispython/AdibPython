import polars as pl
from pathlib import Path
import datetime
import pyreadstat
import gc
from typing import Iterator
import re

# Configuration
loan_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOS")
arrear_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOS")
output_path = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIMREPOS")
output_path.mkdir(exist_ok=True)

# ============================================
# 1. BRANCH CODE MAPPINGS (from SAS format catalogs)
# ============================================
# These mappings should come from your SAS format catalogs BRCHCD. and CACNAME.
# You need to export these from your SAS environment or create them based on your data
# This is a sample - YOU MUST REPLACE WITH ACTUAL MAPPINGS FROM YOUR SAS ENVIRONMENT

BRANCH_CODE_MAP = {
    # Format: NTBRCH: "BRABBR"
    1: "H01", 2: "H02", 3: "H03", 4: "H04", 5: "H05",
    6: "H06", 7: "H07", 8: "H08", 9: "H09", 10: "H10",
    11: "H11", 12: "H12", 13: "H13", 14: "H14", 15: "H15",
    16: "H16", 17: "H17", 18: "H18", 19: "H19", 20: "H20",
    21: "H21", 22: "H22", 23: "H23", 24: "H24", 25: "H25",
    26: "H26", 27: "H27", 28: "H28", 29: "H29", 30: "H30",
    # Add more as needed based on your actual data
    77: "H08",  # Example from your output
    112: "H11",
    143: "H05",
    261: "H13",
    800: "H08",
    801: "H05",
    802: "H05",
    806: "H07",
    807: "H07",
    811: "H11",
    814: "H14",
    816: "H16",
    818: "H18",
    819: "H19",
    820: "H20",
    821: "H21",
    822: "H22",
    824: "H24",
    825: "H25",
    826: "H26",
    827: "H27",
    856: "H09",
    857: "H05",
    862: "H05",
}

CAC_NAME_MAP = {
    # Format: NTBRCH: "CAC NAME"
    1: "CAC-JOHOR BAHRU",
    2: "CAC-KELANG",
    3: "CAC-PENANG",
    4: "CAC-K. LUMPUR",
    5: "CAC-CITY CENTRE",
    6: "CAC-KELANG",
    7: "CAC-K. LUMPUR",
    8: "CAC-K. LUMPUR",
    9: "CAC-PENANG",
    10: "CAC-K. LUMPUR",
    11: "CAC-CITY CENTRE",
    12: "CAC-K. LUMPUR",
    13: "CAC-KELANG",
    14: "CAC-KELANG",
    15: "CAC-PENANG",
    16: "CAC-K. LUMPUR",
    17: "CAC-KELANG",
    18: "CAC-KELANG",
    19: "CAC-KELANG",
    20: "CAC-KELANG",
    21: "CAC-CITY CENTRE",
    22: "CAC-CITY CENTRE",
    23: "CAC-KELANG",
    24: "CAC-KELANG",
    25: "CAC-CITY CENTRE",
    26: "CAC-KELANG",
    27: "CAC-KELANG",
    28: "CAC-KELANG",
    29: "CAC-KELANG",
    30: "CAC-KELANG",
    # Add more as needed based on your actual data
    77: "NON CAC",
    112: "CAC-CITY CENTRE",
    143: "CAC-JOHOR BAHRU",
    261: "CAC-KELANG",
    800: "NON CAC",
    801: "CAC-JOHOR BAHRU",
    802: "CAC-JOHOR BAHRU",
    806: "NON CAC",
    807: "NON CAC",
    811: "CAC-CITY CENTRE",
    814: "CAC-KELANG",
    816: "CAC-K. LUMPUR",
    818: "CAC-KELANG",
    819: "CAC-KELANG",
    820: "CAC-KELANG",
    821: "CAC-CITY CENTRE",
    822: "CAC-K. LUMPUR",
    824: "CAC-KELANG",
    825: "CAC-KELANG",
    826: "CAC-KELANG",
    827: "CAC-KELANG",
    856: "CAC-PENANG",
    857: "CAC-JOHOR BAHRU",
    862: "CAC-JOHOR BAHRU",
}

# ============================================
# 2. HP LOAN TYPE CODES - REPLACE WITH YOUR ACTUAL &HP VALUES
# ============================================
# Get these from your SAS environment with: %PUT &HP;
# This is a sample - REPLACE WITH YOUR ACTUAL HP CODES
HP_VALUES = [
    102.0, 103.0, 104.0, 105.0, 110.0, 111.0, 112.0, 113.0, 114.0, 115.0,
    116.0, 120.0, 124.0, 127.0, 128.0, 133.0, 134.0, 135.0, 136.0, 138.0,
    141.0, 184.0, 412.0, 413.0, 422.0, 654.0, 663.0,
    # Add 983.0 and 993.0 if they're in your HP list
    983.0, 993.0
]

print(f"Using HP loan type codes: {HP_VALUES}")

# ============================================
# 3. DATE CALCULATIONS (matching SAS REPTDATE logic)
# ============================================
today = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)

# Determine week parameters based on yesterday's date
day = yesterday.day
month = yesterday.month
year = yesterday.year

if day == 8:
    sdd, wk, wk1 = 1, '1', '4'
elif day == 15:
    sdd, wk, wk1 = 9, '2', '1'
elif day == 22:
    sdd, wk, wk1 = 16, '3', '2'
else:
    sdd, wk, wk1 = 23, '4', '3'

mm = month
if wk == '1':
    mm1 = mm - 1
    if mm1 == 0:
        mm1 = 12
else:
    mm1 = mm

sdate = datetime.date(year, mm, sdd)

# Extract parameters
nowk = wk
nowk1 = wk1
reptmon = f"{mm:02d}"
reptmon1 = f"{mm1:02d}"
reptyear = str(year)
reptday = f"{day:02d}"
rdate = yesterday.strftime('%d%m%y')  # DDMMYY8 format
sdate_str = sdate.strftime('%d%m%y')

print(f"NOWK: {nowk}, NOWK1: {nowk1}, REPTMON: {reptmon}, REPTMON1: {reptmon1}")
print(f"REPTYEAR: {reptyear}, REPTDAY: {reptday}, RDATE: {rdate}, SDATE: {sdate_str}")

# ============================================
# 4. FUNCTION TO READ SAS IN CHUNKS
# ============================================
def read_sas_chunked(file_path: Path, columns: list = None, chunksize: int = 100000) -> Iterator[pl.DataFrame]:
    """Read SAS file in chunks to handle large files efficiently"""
    try:
        reader = pyreadstat.read_file_in_chunks(
            pyreadstat.read_sas7bdat,
            str(file_path),
            chunksize=chunksize,
            usecols=columns
        )
        
        for df_chunk, meta in reader:
            yield pl.from_pandas(df_chunk)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        raise

# ============================================
# 5. PROCESS LNNOTE
# ============================================
print("Processing LNNOTE (large file) in chunks...")
lnnote_chunks = []
chunk_count = 0

try:
    for chunk in read_sas_chunked(
        loan_path / "lnnote.sas7bdat",
        columns=['ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR', 'BALANCE', 'BORSTAT'],
        chunksize=50000  # Reduced chunksize for memory efficiency
    ):
        chunk_count += 1
        if chunk_count % 10 == 0:
            print(f"Processed {chunk_count} chunks from LNNOTE...")
            gc.collect()
        
        # WHERE LOANTYPE IN &HP AND BALANCE GT 0 AND BORSTAT NOT IN ('F','I','R')
        filtered_chunk = chunk.filter(
            (pl.col('LOANTYPE').is_in(HP_VALUES)) &
            (pl.col('BALANCE') > 0) &
            (~pl.col('BORSTAT').is_in(['F', 'I', 'R']))
        ).select([
            'ACCTNO', 'LOANTYPE', 'NTBRCH', 'COLLDESC', 'COLLYEAR'
        ])
        
        if filtered_chunk.height > 0:
            lnnote_chunks.append(filtered_chunk)
            
            # Free up memory
            del chunk, filtered_chunk
            if len(lnnote_chunks) % 20 == 0:
                gc.collect()
                
except Exception as e:
    print(f"Error processing LNNOTE: {e}")
    raise

if lnnote_chunks:
    lnnote_df = pl.concat(lnnote_chunks).sort('ACCTNO')
    del lnnote_chunks
else:
    lnnote_df = pl.DataFrame(schema={
        'ACCTNO': pl.Float64, 
        'LOANTYPE': pl.Float64, 
        'NTBRCH': pl.Float64, 
        'COLLDESC': pl.Utf8, 
        'COLLYEAR': pl.Float64
    })

print(f"LNNOTE records: {lnnote_df.height}")
gc.collect()

# ============================================
# 6. PROCESS NAME8
# ============================================
print("Processing NAME8...")
try:
    name8_data, name8_meta = pyreadstat.read_sas7bdat(
        str(loan_path / "name8.sas7bdat"),
        usecols=['ACCTNO', 'LINETHRE', 'LINEFOUR']
    )
    name8_df = pl.from_pandas(name8_data).select([
        'ACCTNO', 'LINETHRE', 'LINEFOUR'
    ]).sort('ACCTNO')
    print(f"NAME8 records: {name8_df.height}")
    del name8_data
except Exception as e:
    print(f"Error reading NAME8: {e}")
    name8_df = pl.DataFrame(schema={
        'ACCTNO': pl.Float64,
        'LINETHRE': pl.Utf8,
        'LINEFOUR': pl.Utf8
    })
gc.collect()

# ============================================
# 7. PROCESS LOANTEMP (ARREAR)
# ============================================
print("Processing LOANTEMP...")
try:
    arrear_data, arrear_meta = pyreadstat.read_sas7bdat(
        str(arrear_path / "loantemp.sas7bdat"),
        usecols=['ACCTNO', 'ARREAR']
    )
    arrear_df = pl.from_pandas(arrear_data).select([
        'ACCTNO', 'ARREAR'
    ]).sort('ACCTNO')
    print(f"LOANTEMP records: {arrear_df.height}")
    del arrear_data
except Exception as e:
    print(f"Warning: LOANTEMP not found - {e}")
    arrear_df = pl.DataFrame(schema={
        'ACCTNO': pl.Float64,
        'ARREAR': pl.Float64
    })
gc.collect()

# ============================================
# 8. MERGE DATASETS
# ============================================
print("Merging datasets...")

# First merge LNNOTE with NAME8 (INNER JOIN - only keep LNNOTE records)
repo_df = lnnote_df.join(
    name8_df.rename({'LINETHRE': 'ENGINE', 'LINEFOUR': 'CHASSIS'}),
    on='ACCTNO',
    how='inner'  # This matches SAS IN=AA condition
)

print(f"After NAME8 merge: {repo_df.height}")

# Then merge with ARREAR (LEFT JOIN)
repo_df = repo_df.join(
    arrear_df,
    on='ACCTNO',
    how='left'
)

# ============================================
# 9. TRANSFORM DATA WITH PROPER MAPPINGS
# ============================================
print("Transforming data...")

# Add branch code and CAC name mappings
repo_df = repo_df.with_columns([
    # BRABBR - map NTBRCH to branch code
    pl.col('NTBRCH')
    .cast(pl.Int64)
    .map_dict(BRANCH_CODE_MAP, default=pl.col('NTBRCH').cast(pl.Utf8))
    .alias('BRABBR'),
    
    # CAC - map NTBRCH to CAC name
    pl.col('NTBRCH')
    .cast(pl.Int64)
    .map_dict(CAC_NAME_MAP, default="NON CAC")
    .alias('CAC'),
    
    # Clean and parse COLLDESC
    pl.col('COLLDESC')
    .str.replace_all(r'\s+', ' ')  # Normalize whitespace
    .str.strip_chars()
    .alias('COLLDESC_CLEAN'),
])

# Parse COLLDESC based on content type
def parse_coll_desc(desc: str) -> dict:
    """Parse COLLDESC field into MAKE, MODEL, REGNO based on pattern"""
    if not desc or desc == "":
        return {"MAKE": "", "MODEL": "", "REGNO": ""}
    
    desc = desc.strip()
    
    # Check if it's a vehicle description (contains vehicle makes)
    vehicle_makes = ['PROTON', 'PERODUA', 'TOYOTA', 'HONDA', 'NISSAN', 'MITSUBISHI', 
                     'MAZDA', 'FORD', 'BMW', 'MERCEDES', 'AUDI', 'ISUZU', 'HYUNDAI']
    
    is_vehicle = any(make in desc.upper() for make in vehicle_makes)
    
    if is_vehicle:
        # Try to extract make, model, regno based on common patterns
        # Pattern 1: "MAKE MODEL REGNO" or "MAKE MODEL"
        parts = desc.split()
        if len(parts) >= 2:
            # First part is usually MAKE
            make = parts[0]
            # Try to find REGNO (alphanumeric with no spaces or with pattern like AA1234)
            regno = ""
            model_parts = []
            
            for part in parts[1:]:
                # Check if part looks like a registration number
                # Registration numbers often have letters and numbers, no spaces
                if re.match(r'^[A-Z0-9]+$', part) and len(part) >= 4:
                    regno = part
                else:
                    model_parts.append(part)
            
            model = " ".join(model_parts) if model_parts else ""
            
            return {
                "MAKE": make[:16] if make else "",
                "MODEL": model[:21] if model else "",
                "REGNO": regno[:13] if regno else ""
            }
        else:
            return {"MAKE": desc[:16], "MODEL": "", "REGNO": ""}
    else:
        # Non-vehicle - put everything in MAKE
        return {"MAKE": desc[:16], "MODEL": "", "REGNO": ""}

# Apply parsing using map_elements (more flexible than str.slice)
parsed_data = repo_df.select([
    pl.col('ACCTNO'),
    pl.col('COLLDESC').map_elements(
        lambda x: parse_coll_desc(x)['MAKE'], 
        return_dtype=pl.Utf8
    ).alias('MAKE'),
    pl.col('COLLDESC').map_elements(
        lambda x: parse_coll_desc(x)['MODEL'], 
        return_dtype=pl.Utf8
    ).alias('MODEL'),
    pl.col('COLLDESC').map_elements(
        lambda x: parse_coll_desc(x)['REGNO'], 
        return_dtype=pl.Utf8
    ).alias('REGNO'),
])

# Add the parsed columns back to repo_df
repo_df = repo_df.with_columns([
    parsed_data['MAKE'],
    parsed_data['MODEL'],
    parsed_data['REGNO'],
])

# Handle missing ARREAR and ensure it's numeric
repo_df = repo_df.with_columns([
    pl.col('ARREAR').fill_null(0).cast(pl.Float64)
])

# Drop temporary column
repo_df = repo_df.drop('COLLDESC_CLEAN')

print(f"Merged REPO records: {repo_df.height}")

# ============================================
# 10. FILTER FOR REPO (ARREAR >= 10)
# ============================================
repo_filtered = repo_df.filter(pl.col('ARREAR') >= 10)

# Filter REPO1 for loan types 983 and 993
repo1_filtered = repo_filtered.filter(pl.col('LOANTYPE').is_in([983.0, 993.0]))

print(f"REPO records (ARREAR >= 10): {repo_filtered.height}")
print(f"REPO1 records (983,993): {repo1_filtered.height}")

# ============================================
# 11. SORT DATA
# ============================================
repo_sorted = repo_filtered.sort('REGNO')
repo1_sorted = repo1_filtered.sort('REGNO')

# ============================================
# 12. GENERATE OUTPUT FILES
# ============================================
print("Generating REPOTXT.txt...")

def format_field(value, max_len, default=""):
    """Format field with proper length and default"""
    if value is None:
        return default[:max_len].ljust(max_len)
    str_val = str(value).strip()
    return str_val[:max_len].ljust(max_len)

with open(output_path / "repotxt.txt", "w") as f:
    # Header
    f.write(f"{rdate}-REPOSSESSION LISTING\n")
    
    for row in repo_sorted.iter_rows(named=True):
        try:
            # Format each field with proper widths matching SAS PUT statement
            # @001 BRABBR $3. -> 3 characters
            # @009 CAC $20. -> 20 characters
            # @029 REGNO $13. -> 13 characters
            # @043 MAKE $16. -> 16 characters
            # @060 MODEL $21. -> 21 characters
            # @082 ENGINE $40. -> 40 characters
            # @123 CHASSIS $40. -> 40 characters
            # @164 COLLYEAR $4. -> 4 characters
            
            brabbr = str(row.get('BRABBR', ''))[:3].ljust(3)
            cac = str(row.get('CAC', ''))[:20].ljust(20)
            regno = str(row.get('REGNO', ''))[:13].ljust(13)
            make = str(row.get('MAKE', ''))[:16].ljust(16)
            model = str(row.get('MODEL', ''))[:21].ljust(21)
            engine = str(row.get('ENGINE', ''))[:40].ljust(40)
            chassis = str(row.get('CHASSIS', ''))[:40].ljust(40)
            collyear = str(row.get('COLLYEAR', ''))[:4].ljust(4)
            
            line = f"{brabbr}{cac}{regno}{make}{model}{engine}{chassis}{collyear}\n"
            f.write(line)
            
        except Exception as e:
            print(f"Error formatting row: {e}")
            continue

print("REPOTXT.txt generated successfully")

# Generate REPOTXT1.txt
print("Generating REPOTXT1.txt...")
with open(output_path / "repotxt1.txt", "w") as f:
    f.write(f"{rdate}-REPOSSESSION LISTING (983,993)\n")
    
    for row in repo1_sorted.iter_rows(named=True):
        try:
            brabbr = str(row.get('BRABBR', ''))[:3].ljust(3)
            cac = str(row.get('CAC', ''))[:20].ljust(20)
            regno = str(row.get('REGNO', ''))[:13].ljust(13)
            make = str(row.get('MAKE', ''))[:16].ljust(16)
            model = str(row.get('MODEL', ''))[:21].ljust(21)
            engine = str(row.get('ENGINE', ''))[:40].ljust(40)
            chassis = str(row.get('CHASSIS', ''))[:40].ljust(40)
            collyear = str(row.get('COLLYEAR', ''))[:4].ljust(4)
            
            line = f"{brabbr}{cac}{regno}{make}{model}{engine}{chassis}{collyear}\n"
            f.write(line)
            
        except Exception as e:
            print(f"Error formatting row: {e}")
            continue

print("REPOTXT1.txt generated successfully")

# ============================================
# 13. SUMMARY STATISTICS
# ============================================
print("\n" + "="*50)
print("PROCESSING COMPLETED SUCCESSFULLY")
print("="*50)
print(f"Total LNNOTE records: {lnnote_df.height}")
print(f"Total REPO records (ARREAR >= 10): {repo_filtered.height}")
print(f"Total REPO1 records (983,993): {repo1_filtered.height}")
print(f"Output files: {output_path / 'repotxt.txt'}, {output_path / 'repotxt1.txt'}")

# Clean up
del lnnote_df, name8_df, arrear_df, repo_df, repo_filtered, repo1_filtered
gc.collect()
print("\nCleanup completed")
