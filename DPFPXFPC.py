import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import pyreadstat
import saspy
import os
import tempfile

# =========================
# CONFIG (SAS7BDAT INPUTS)
# =========================
CURRENT_DF  = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/intg_dp_acct_current_m08.sas7bdat")
LIMIT_DF    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/intg_dp_acct_overdft_m08.sas7bdat")
CISDP_DF    = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cisdp/deposit.sas7bdat")
NPLA_DF     = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/npla.sas7bdat")

# TEXT FILES (UNCHANGED)
GP3_FILE  = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDNPGS/GP3.txt"
COLL_FILE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_20260831"
DESC_FILE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/LCCRISEX_DESC_20260831"
MICR_FILE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBLTRRF/BOPESS.txt"

OUTPUT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBDNPGS")
OUTPUT_FILE = f"DPNPGS_{datetime.now().strftime('%m')}.sas7bdat"

# Chunk size for processing large files
CHUNK_SIZE = 100000  # Adjust based on your memory constraints

# =========================
# STEP 1: REPORT DATE
# =========================
# Use yesterday's date as report date
reptdate = datetime.now() - timedelta(days=1)

REPTDAY  = reptdate.day
REPTMON  = reptdate.month
REPTYEAR = reptdate.year
SDATE    = reptdate.toordinal()

# =========================
# STEP 2: READ SAS DATASETS
# =========================
print("Reading SAS datasets...")

# Read CURRENT dataset
current_df, current_meta = pyreadstat.read_sas7bdat(CURRENT_DF)
print(f"CURRENT dataset: {current_df.shape[0]} rows before filtering")

# Apply entity filter
if 'ENTITY_CD' in current_df.columns:
    current_df = current_df[current_df['ENTITY_CD'] != 'PIBB'].copy()
    print(f"CURRENT dataset: {current_df.shape[0]} rows after filtering (ENTITY_CD != 'PIBB')")
else:
    print("Warning: ENTITY_CD column not found in CURRENT dataset")

# Read LIMIT dataset
limit_df, limit_meta = pyreadstat.read_sas7bdat(LIMIT_DF)
print(f"LIMIT dataset: {limit_df.shape[0]} rows before filtering")

# Apply entity filter
if 'ENTITY_CD' in limit_df.columns:
    limit_df = limit_df[limit_df['ENTITY_CD'] != 'PIBB'].copy()
    print(f"LIMIT dataset: {limit_df.shape[0]} rows after filtering (ENTITY_CD != 'PIBB')")
else:
    print("Warning: ENTITY_CD column not found in LIMIT dataset")

# Read NPLA dataset
npla_df, npla_meta = pyreadstat.read_sas7bdat(NPLA_DF)
print(f"NPLA dataset: {npla_df.shape[0]} rows")

# =========================
# STEP 2B: READ CISDP IN CHUNKS (LARGE FILE)
# =========================
print("Reading CISDP dataset in chunks...")

# First, read only the header to get column names
cisdp_header, _ = pyreadstat.read_sas7bdat(CISDP_DF, row_limit=1)
print(f"CISDP columns available: {len(cisdp_header.columns)}")

# Check if required columns exist
required_cols = ['ACCTNO']
if 'NEWIC' in cisdp_header.columns:
    required_cols.append('NEWIC')
else:
    print("Warning: NEWIC column not found in CISDP dataset")

if 'CUSTNAME' in cisdp_header.columns:
    required_cols.append('CUSTNAME')
else:
    print("Warning: CUSTNAME column not found in CISDP dataset")

# Initialize empty list to store filtered chunks
cisdp_chunks = []

# Read in chunks using pyreadstat's row_offset and row_limit
row_offset = 0
chunk_count = 0

while True:
    try:
        # Read a chunk of data
        chunk, _ = pyreadstat.read_sas7bdat(
            CISDP_DF, 
            row_offset=row_offset, 
            row_limit=CHUNK_SIZE
        )
        
        if len(chunk) == 0:
            break
            
        chunk_count += 1
        
        # Filter only needed columns and rows
        if 'SECCUST' in chunk.columns:
            filtered_chunk = chunk[chunk['SECCUST'] == '901'][required_cols].copy()
            if len(filtered_chunk) > 0:
                cisdp_chunks.append(filtered_chunk)
        
        if chunk_count % 10 == 0:  # Print progress every 10 chunks
            print(f"Processed {chunk_count} chunks, {row_offset + len(chunk)} rows total")
        
        # Update offset
        row_offset += CHUNK_SIZE
        
        # Break if we've read all data
        if len(chunk) < CHUNK_SIZE:
            break
            
    except Exception as e:
        print(f"Error reading chunk at offset {row_offset}: {e}")
        break

# Combine all chunks
if cisdp_chunks:
    cisdp_df = pd.concat(cisdp_chunks, ignore_index=True)
    cisdp_df = cisdp_df.drop_duplicates()
    print(f"CISDP dataset: {cisdp_df.shape[0]} rows after filtering (SECCUST == '901')")
    
    # Free memory
    del cisdp_chunks
else:
    cisdp_df = pd.DataFrame(columns=required_cols)
    print("Warning: No CISDP data found for SECCUST == '901'")

# =========================
# STEP 3: CURRENT → CA
# =========================
ca = current_df.copy()

def map_sch(row):
    if row.PRODUCT == 108 and row.CENSUST == 305: return 'P85'
    if row.PRODUCT == 112 and row.CENSUST == 301: return 'P70'
    if row.PRODUCT == 112 and row.CENSUST == 300: return 'P51'
    if row.PRODUCT == 112 and row.CENSUST == 302: return 'P72'
    if row.PRODUCT == 112 and row.CENSUST == 306: return 'P53'
    if row.PRODUCT == 114 and row.CENSUST == 303: return 'P72'
    if row.PRODUCT == 108 and row.CENSUST == 304: return 'P65'
    return None

ca['SCH'] = ca.apply(map_sch, axis=1)
ca = ca[ca['SCH'].notna()].copy()
print(f"CA after SCH mapping: {ca.shape[0]} rows")

# Check if CA is empty
if ca.empty:
    print("ERROR: No records after SCH mapping. Check PRODUCT and CENSUST values.")
    print("Sample PRODUCT values:", current_df['PRODUCT'].value_counts().head(10))
    print("Sample CENSUST values:", current_df['CENSUST'].value_counts().head(10))
    exit(1)

# =========================
# STEP 4A: LIMIT
# =========================
def convert_lmtstart(x):
    """Convert LMTSTART to datetime with robust error handling"""
    if pd.isna(x):
        return pd.NaT
    
    try:
        # Handle different numeric formats
        if isinstance(x, (int, float)):
            if x <= 0:
                return pd.NaT
            
            # Convert to string and pad
            x_str = str(int(x)).zfill(8)
        else:
            x_str = str(x).strip().zfill(8)
        
        # Try different date formats
        formats_to_try = [
            "%m%d%Y",    # MMDDYYYY
            "%d%m%Y",    # DDMMYYYY
            "%Y%m%d",    # YYYYMMDD
            "%Y%d%m",    # YYYYDDMM
        ]
        
        for fmt in formats_to_try:
            try:
                return datetime.strptime(x_str[:8], fmt)
            except ValueError:
                continue
        
        # If all formats fail, try to handle YYMMDD format
        try:
            year = int(x_str[0:2])
            month = int(x_str[2:4])
            day = int(x_str[4:6])
            
            # Assume 20xx for years less than 50, 19xx for 50+
            if year < 50:
                year += 2000
            else:
                year += 1900
            
            return datetime(year, month, day)
        except:
            return pd.NaT
            
    except Exception as e:
        return pd.NaT

print("Processing LIMIT data...")
limit_processed = limit_df.copy()

# Check LMTSTART data type and sample values
print(f"LMTSTART dtype: {limit_processed['LMTSTART'].dtype}")
print(f"LMTSTART non-null count: {limit_processed['LMTSTART'].notna().sum()}")

# Apply conversion with progress tracking
limit_processed['LMTSTART'] = limit_processed['LMTSTART'].apply(convert_lmtstart)
limit_processed = limit_processed[['ACCTNO','LMTSTART']].drop_duplicates()

print(f"LIMIT processed: {limit_processed.shape[0]} unique records")

ca = ca.merge(limit_processed, on='ACCTNO', how='left')
print(f"CA after LIMIT merge: {ca.shape[0]} rows")

# =========================
# STEP 4B: GP3 (FIXED WIDTH)
# =========================
gp3 = pd.read_fwf(
    GP3_FILE,
    colspecs=[(3,13),(18,20),(20,22),(22,26)],
    names=['ACCTNO','RPTDAY','RPTMON','RPTYEAR']
)

gp3['NPLDATE'] = pd.to_datetime(
    dict(year=gp3.RPTYEAR, month=gp3.RPTMON, day=gp3.RPTDAY),
    errors='coerce'
)

ca = ca.merge(gp3[['ACCTNO','NPLDATE']], on='ACCTNO', how='left')
print(f"CA after GP3 merge: {ca.shape[0]} rows")

# =========================
# STEP 4C: CISDP MERGE
# =========================
ca = ca.merge(cisdp_df, on='ACCTNO', how='left')
print(f"CA after CISDP merge: {ca.shape[0]} rows")

# =========================
# STEP 4D: COLL + DESC (EBCDIC FILES)
# =========================
def read_ebcdic_fwf(file_path, colspecs, names):
    """Read fixed-width EBCDIC file and convert to ASCII"""
    # Read the file in binary mode
    with open(file_path, 'rb') as f:
        raw_data = f.read()
    
    # Try different EBCDIC encodings
    encodings = ['cp037', 'cp500', 'cp1047', 'cp1140', 'cp1141', 'cp273', 'cp277', 'cp278', 'cp280', 'cp284', 'cp285', 'cp297']
    
    decoded_data = None
    for encoding in encodings:
        try:
            decoded_data = raw_data.decode(encoding)
            print(f"Successfully decoded with {encoding}")
            break
        except:
            continue
    
    if decoded_data is None:
        # Fallback to cp037 with error replacement
        decoded_data = raw_data.decode('cp037', errors='replace')
        print("Warning: Using cp037 with error replacement")
    
    # Write to temporary file for pd.read_fwf
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp:
        tmp.write(decoded_data)
        tmp_path = tmp.name
    
    # Read the decoded file
    df = pd.read_fwf(
        tmp_path,
        colspecs=colspecs,
        names=names
    )
    
    # Clean up temp file
    os.unlink(tmp_path)
    
    return df

def clean_ebcdic_strings(df):
    """Clean EBCDIC artifacts from string columns"""
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(lambda x: ''.join(
            char for char in str(x) if char.isprintable() or char.isspace()
        ).strip() if pd.notna(x) else x)
    return df

print("Reading COLL file (EBCDIC)...")
coll = read_ebcdic_fwf(
    COLL_FILE,
    colspecs=[(3,9),(145,151)],
    names=['CCOLLNO','ACCTNO']
)
print(f"COLL file: {coll.shape[0]} rows")

print("Reading DESC file (EBCDIC)...")
desc = read_ebcdic_fwf(
    DESC_FILE,
    colspecs=[(0,11),(50,52),(54,56),(210,220)],
    names=['CCOLLNO','CINSTCL','NATGUAR','CENSUS']
)
print(f"DESC file: {desc.shape[0]} rows")

# Clean up EBCDIC artifacts
coll = clean_ebcdic_strings(coll)
desc = clean_ebcdic_strings(desc)

# Debug: Print sample data from DESC
print("\n=== DESC SAMPLE DATA ===")
print(desc.head(20))
print("\n=== DESC DATA TYPES ===")
print(desc.dtypes)
print("\n=== CENSUS SAMPLE VALUES ===")
print(desc['CENSUS'].head(50))
print("\n=== CENSUS UNIQUE VALUES (first 100) ===")
print(desc['CENSUS'].value_counts().head(100))
print("\n=== CINSTCL SAMPLE VALUES ===")
print(desc['CINSTCL'].value_counts().head(20))
print("\n=== NATGUAR SAMPLE VALUES ===")
print(desc['NATGUAR'].value_counts().head(20))

# Convert CENSUS to numeric with better handling
# First, try to extract numeric values from the string
desc['CENSUS_CLEAN'] = desc['CENSUS'].str.extract(r'(\d+)').astype(float)
desc['CENSUS'] = pd.to_numeric(desc['CENSUS'], errors='coerce')

# If CENSUS is all NaN, use the cleaned version
if desc['CENSUS'].isna().all() and desc['CENSUS_CLEAN'].notna().any():
    print("Using cleaned CENSUS values...")
    desc['CENSUS'] = desc['CENSUS_CLEAN']

print(f"\nCENSUS after conversion - non-null: {desc['CENSUS'].notna().sum()}")

# Debug: Check CENSUS value ranges
if desc['CENSUS'].notna().any():
    print(f"CENSUS value range: {desc['CENSUS'].min()} to {desc['CENSUS'].max()}")
    print(f"CENSUS sample values (first 20 non-null): {desc[desc['CENSUS'].notna()]['CENSUS'].head(20).tolist()}")

def map_cr(census):
    if pd.isna(census):
        return None
    
    # Try to convert to int if float
    try:
        census_int = int(census)
    except:
        return None
    
    if 51000000 <= census_int <= 51999999: return '51'
    if 63000000 <= census_int <= 63999999: return '63'
    if 70000000 <= census_int <= 70999999: return '70'
    if 71000000 <= census_int <= 71999999: return '71'
    if 72000000 <= census_int <= 72999999: return '72'
    if 1000000000 <= census_int <= 1099999999: return '10'
    
    # Also try 8-digit versions
    if 510000 <= census_int <= 519999: return '51'
    if 630000 <= census_int <= 639999: return '63'
    if 700000 <= census_int <= 709999: return '70'
    if 710000 <= census_int <= 719999: return '71'
    if 720000 <= census_int <= 729999: return '72'
    
    return None

desc['CR'] = desc['CENSUS'].apply(map_cr)
desc_filtered = desc[desc['CR'].notna()]
print(f"DESC after CR mapping: {desc_filtered.shape[0]} rows")

# If still 0 rows, try alternative mapping
if desc_filtered.empty:
    print("\nTrying alternative CR mapping...")
    
    # Try to map based on first 2 digits
    def map_cr_alt(census):
        if pd.isna(census):
            return None
        
        census_str = str(census).zfill(8)
        first_two = census_str[:2]
        
        if first_two == '51': return '51'
        if first_two == '63': return '63'
        if first_two == '70': return '70'
        if first_two == '71': return '71'
        if first_two == '72': return '72'
        if first_two == '10': return '10'
        return None
    
    desc['CR'] = desc['CENSUS'].apply(map_cr_alt)
    desc_filtered = desc[desc['CR'].notna()]
    print(f"DESC after alternative CR mapping: {desc_filtered.shape[0]} rows")

coll = coll.merge(desc_filtered, on='CCOLLNO')
print(f"COLL after merge with DESC: {coll.shape[0]} rows")

# Debug: Check CINSTCL and NATGUAR values
print("\n=== COLL SAMPLE DATA AFTER MERGE ===")
print(coll.head(20))
print("\nSample CINSTCL values:", coll['CINSTCL'].value_counts().head(10))
print("Sample NATGUAR values:", coll['NATGUAR'].value_counts().head(10))

# Try different filter criteria if needed
coll_filtered = coll[(coll['CINSTCL']=='18') & (coll['NATGUAR']=='06')]
print(f"COLL after filtering (CINSTCL=18, NATGUAR=06): {coll_filtered.shape[0]} rows")

# If no records, try just CINSTCL or NATGUAR alone
if coll_filtered.empty:
    print("\nNo records with CINSTCL=18 AND NATGUAR=06")
    coll_filtered = coll[coll['CINSTCL']=='18']
    print(f"COLL with just CINSTCL=18: {coll_filtered.shape[0]} rows")
    
    if coll_filtered.empty:
        coll_filtered = coll[coll['NATGUAR']=='06']
        print(f"COLL with just NATGUAR=06: {coll_filtered.shape[0]} rows")

dep = ca.merge(coll_filtered, on='ACCTNO')
print(f"DEP after COLL merge: {dep.shape[0]} rows")

# Check if dep is empty
if dep.empty:
    print("WARNING: No records after merging with COLL data.")
    print("Sample ACCTNO from CA:", ca['ACCTNO'].head(10).tolist())
    print("Sample ACCTNO from COLL:", coll_filtered['ACCTNO'].head(10).tolist())
    
    # Continue with CA as dep (without collateral data)
    dep = ca.copy()
    # Add missing columns with None values
    if 'CR' not in dep.columns:
        dep['CR'] = None
    if 'CENSUS' not in dep.columns:
        dep['CENSUS'] = None
    print("Continuing with CA data without collateral merge...")

# =========================
# STEP 4E: MICR
# =========================
micr = pd.read_fwf(
    MICR_FILE,
    colspecs=[(0,3),(39,44)],
    names=['BRANCH','MICRCD']
)

dep = dep.merge(micr, on='BRANCH', how='left')
print(f"DEP after MICR merge: {dep.shape[0]} rows")

# =========================
# STEP 5: ARREARS + NPL
# =========================
def calc_arrears(row):
    if row.get('CURBAL', 0) >= 0:
        return 0, pd.NaT

    dates = []

    for col in ['EXODDATE','TEMPODDT']:
        val = row.get(col, 0)
        if pd.notna(val) and val > 0:
            try:
                if isinstance(val, (int, float)):
                    d = datetime.strptime(str(int(val)).zfill(8)[:8], "%m%d%Y")
                else:
                    d = datetime.strptime(str(val).strip()[:8], "%m%d%Y")
                dates.append(d)
            except:
                continue

    if not dates:
        return 0, pd.NaT

    oddays = min(dates)
    nodays = (reptdate - oddays).days + 1

    arrears = nodays // 30

    npldate = pd.NaT
    if arrears >= 3:
        npldate = oddays + pd.DateOffset(days=90)
        npldate = npldate + pd.offsets.MonthEnd(0)

    return arrears, npldate

# Check if dep is not empty before applying calc_arrears
if dep.empty:
    print("ERROR: No data to process after all merges.")
    print("Creating empty output with proper structure...")
    
    # Create empty dataframe with required output structure
    empty_data = {
        'CVAR01': [], 'CVAR02': [], 'CVAR03': [], 'CVAR04': [],
        'CVAR05': [], 'CVAR06': [], 'CVAR07': [], 'CVAR08': [],
        'CVAR09': [], 'CVAR10': [], 'CVAR11': [], 'CVAR12': [],
        'CVAR13': [], 'CVAR14': [], 'CVAR15': []
    }
    npgs = pd.DataFrame(empty_data)
else:
    # Apply calc_arrears only if dep is not empty
    dep[['ARREARS','NPLDATE_CALC']] = dep.apply(
        lambda x: pd.Series(calc_arrears(x)), axis=1
    )

    dep['NPLDATE'] = dep['NPLDATE_CALC'].combine_first(dep['NPLDATE'])

    # =========================
    # STEP 6: CVAR02
    # =========================
    def map_cvar02(row):
        if row.SCH=='P51' and row.CR in ['10','51']: return '51'
        if row.SCH=='P65' and row.CR=='10': return '65'
        if row.SCH=='P53' and row.CR=='10': return '53'
        if row.SCH=='P85' and row.CR=='10': return '85'
        if row.SCH=='P70' and row.CR=='70': return '70'
        if row.SCH=='P70' and row.CR=='71': return '71'
        if row.SCH=='P72' and row.CR in ['10','72']: return '72'
        if row.SCH=='P70' and row.CR=='10': return 'XX'
        return None

    dep['CVAR02'] = dep.apply(map_cvar02, axis=1)
    dep_filtered = dep[dep['CVAR02'].notna()]
    print(f"DEP after CVAR02 mapping: {dep_filtered.shape[0]} rows")
    
    # If all records filtered out, use original dep
    if dep_filtered.empty:
        print("WARNING: No records after CVAR02 mapping. Using all records...")
        # Set CVAR02 to a default value
        dep['CVAR02'] = 'XX'  # Default value
        dep_filtered = dep.copy()

    # =========================
    # STEP 7: OUTPUT STRUCTURE
    # =========================
    dep_filtered['CVAR01'] = dep_filtered['CENSUS']
    dep_filtered['CVAR03'] = dep_filtered['NEWIC']
    dep_filtered['CVAR04'] = dep_filtered['CUSTNAME']
    dep_filtered['CVAR05'] = dep_filtered['LMTSTART']
    dep_filtered['CVAR06'] = dep_filtered['ACCTNO']
    dep_filtered['CVAR07'] = 'OD'
    dep_filtered['CVAR08'] = dep_filtered['APPRLIMT'].fillna(0)

    dep_filtered['CVAR09'] = np.where(dep_filtered['LEDGBAL'] < 0, -dep_filtered['LEDGBAL'], 0)
    dep_filtered['CVAR10'] = np.where(dep_filtered['LEDGBAL'] >= 0, dep_filtered['LEDGBAL'], 0)

    dep_filtered['CVAR11'] = dep_filtered['ARREARS']
    dep_filtered['CVAR12'] = np.where(dep_filtered['ARREARS'] >= 3, 'NPL', '   ')
    dep_filtered['CVAR13'] = dep_filtered['NPLDATE'].dt.strftime('%d/%m/%Y')

    dep_filtered['CVAR14'] = '0233'
    dep_filtered['CVAR15'] = dep_filtered['MICRCD']

    # =========================
    # STEP 8: HISTORY MERGE
    # =========================
    npgs = dep_filtered.merge(npla_df, on=['CVAR06','CVAR01'], how='left')

    npgs.loc[
        (npgs['CVAR12']=='NPL') & (npgs['STATUS']=='NPL'),
        'CVAR13'
    ] = npgs['NDATE']

# =========================
# STEP 9: OUTPUT (SAS7BDAT)
# =========================
print(f"Writing output to {OUTPUT_FILE}...")
print(f"Final dataset: {npgs.shape[0]} rows")

if npgs.empty:
    print("WARNING: Output is empty. Creating empty SAS dataset...")
    # Create a minimal empty dataset
    empty_output = pd.DataFrame({
        'CVAR06': ['0'],  # At least one row
        'CVAR02': ['XX'],
        'CVAR07': ['OD'],
        'CVAR14': ['0233']
    })
    npgs = empty_output

# Initialize SAS session
sas = saspy.SASsession(cfgname='default')  # You may need to adjust the configuration name

# Convert pandas DataFrame to SAS dataset
sas.df2sd(npgs, table='npgs_output', libref='WORK')

# Write SAS dataset to sas7bdat file
sas_code = f"""
PROC EXPORT DATA=WORK.npgs_output 
    OUTFILE="{OUTPUT / OUTPUT_FILE}" 
    DBMS=SAS7BDAT REPLACE;
RUN;
"""

sas.submit(sas_code)

# Close SAS session
sas.endsas()

print(f"Output written: {OUTPUT / OUTPUT_FILE}")
print(f"Total records: {len(npgs)}")
