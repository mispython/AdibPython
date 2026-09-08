import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import pyreadstat
import saspy
import os
import tempfile
import re

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
CHUNK_SIZE = 100000

# =========================
# STEP 1: REPORT DATE
# =========================
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

if 'ENTITY_CD' in current_df.columns:
    current_df = current_df[current_df['ENTITY_CD'] != 'PIBB'].copy()
    print(f"CURRENT dataset: {current_df.shape[0]} rows after filtering")

# Read LIMIT dataset
limit_df, limit_meta = pyreadstat.read_sas7bdat(LIMIT_DF)
print(f"LIMIT dataset: {limit_df.shape[0]} rows before filtering")

if 'ENTITY_CD' in limit_df.columns:
    limit_df = limit_df[limit_df['ENTITY_CD'] != 'PIBB'].copy()
    print(f"LIMIT dataset: {limit_df.shape[0]} rows after filtering")

# Read NPLA dataset
npla_df, npla_meta = pyreadstat.read_sas7bdat(NPLA_DF)
print(f"NPLA dataset: {npla_df.shape[0]} rows")

# =========================
# STEP 2B: READ CISDP IN CHUNKS
# =========================
print("Reading CISDP dataset in chunks...")

cisdp_header, _ = pyreadstat.read_sas7bdat(CISDP_DF, row_limit=1)
required_cols = ['ACCTNO']
if 'NEWIC' in cisdp_header.columns:
    required_cols.append('NEWIC')
if 'CUSTNAME' in cisdp_header.columns:
    required_cols.append('CUSTNAME')

cisdp_chunks = []
row_offset = 0
chunk_count = 0

while True:
    try:
        chunk, _ = pyreadstat.read_sas7bdat(
            CISDP_DF, 
            row_offset=row_offset, 
            row_limit=CHUNK_SIZE
        )
        
        if len(chunk) == 0:
            break
            
        chunk_count += 1
        
        if 'SECCUST' in chunk.columns:
            filtered_chunk = chunk[chunk['SECCUST'] == '901'][required_cols].copy()
            if len(filtered_chunk) > 0:
                cisdp_chunks.append(filtered_chunk)
        
        if chunk_count % 10 == 0:
            print(f"Processed {chunk_count} chunks, {row_offset + len(chunk)} rows total")
        
        row_offset += CHUNK_SIZE
        
        if len(chunk) < CHUNK_SIZE:
            break
            
    except Exception as e:
        print(f"Error reading chunk at offset {row_offset}: {e}")
        break

if cisdp_chunks:
    cisdp_df = pd.concat(cisdp_chunks, ignore_index=True)
    cisdp_df = cisdp_df.drop_duplicates()
    print(f"CISDP dataset: {cisdp_df.shape[0]} rows after filtering")
    del cisdp_chunks
else:
    cisdp_df = pd.DataFrame(columns=required_cols)
    print("Warning: No CISDP data found")

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

# =========================
# STEP 4A: LIMIT
# =========================
def convert_lmtstart(x):
    if pd.isna(x):
        return pd.NaT
    
    try:
        if isinstance(x, (int, float)):
            if x <= 0:
                return pd.NaT
            x_str = str(int(x)).zfill(8)
        else:
            x_str = str(x).strip().zfill(8)
        
        formats_to_try = ["%m%d%Y", "%d%m%Y", "%Y%m%d", "%Y%d%m"]
        
        for fmt in formats_to_try:
            try:
                return datetime.strptime(x_str[:8], fmt)
            except ValueError:
                continue
        
        try:
            year = int(x_str[0:2])
            month = int(x_str[2:4])
            day = int(x_str[4:6])
            
            if year < 50:
                year += 2000
            else:
                year += 1900
            
            return datetime(year, month, day)
        except:
            return pd.NaT
            
    except Exception:
        return pd.NaT

print("Processing LIMIT data...")
limit_processed = limit_df.copy()
limit_processed['LMTSTART'] = limit_processed['LMTSTART'].apply(convert_lmtstart)
limit_processed = limit_processed[['ACCTNO','LMTSTART']].drop_duplicates()
print(f"LIMIT processed: {limit_processed.shape[0]} unique records")

ca = ca.merge(limit_processed, on='ACCTNO', how='left')
print(f"CA after LIMIT merge: {ca.shape[0]} rows")

# =========================
# STEP 4B: GP3
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
def read_ebcdic_file(file_path):
    """Read EBCDIC file and return decoded string"""
    with open(file_path, 'rb') as f:
        raw_data = f.read()
    
    encodings = ['cp037', 'cp500', 'cp1047', 'cp1140']
    
    for encoding in encodings:
        try:
            decoded_data = raw_data.decode(encoding)
            print(f"Successfully decoded {os.path.basename(file_path)} with {encoding}")
            return decoded_data
        except:
            continue
    
    decoded_data = raw_data.decode('cp037', errors='replace')
    print(f"Warning: Using cp037 with error replacement for {os.path.basename(file_path)}")
    return decoded_data

print("\nReading COLL file (EBCDIC)...")
coll_raw = read_ebcdic_file(COLL_FILE)
coll_lines = coll_raw.split('\n')
print(f"COLL file: {len(coll_lines)} lines")

# Parse COLL file - extract account numbers and collateral numbers
coll_data = []
for line in coll_lines:
    if len(line) >= 151:
        # Extract based on observed patterns
        # Look for patterns like '00000156331000' (collateral number)
        # and account numbers (typically 10-13 digits)
        
        # Extract collateral number (positions 3-9 as per original spec)
        coll_no = line[3:9].strip()
        
        # Extract account number - look for patterns in the line
        # Account numbers appear to be 10-13 digit numbers
        acct_match = re.search(r'(\d{10,13})', line[145:151] if len(line) > 145 else '')
        
        if coll_no and acct_match:
            coll_data.append({
                'CCOLLNO': coll_no,
                'ACCTNO': acct_match.group(1)
            })

coll = pd.DataFrame(coll_data)
print(f"COLL parsed: {coll.shape[0]} rows")
if not coll.empty:
    print("COLL sample:")
    print(coll.head())

print("\nReading DESC file (EBCDIC)...")
desc_raw = read_ebcdic_file(DESC_FILE)
desc_lines = desc_raw.split('\n')
print(f"DESC file: {len(desc_lines)} lines")

# Parse DESC file - look for patterns like '18RI' and '16IC'
desc_data = []
current_record = {}

for line in desc_lines:
    # Look for collateral number pattern (11 digits followed by record type)
    # Pattern: '00000156331000 18RI02MY' - collateral number, record type, state code
    coll_match = re.search(r'(\d{11})\d{3}\s+(\d{2})([A-Z]{2})(\d{2})([A-Z]{2})', line)
    
    if coll_match:
        if current_record:
            desc_data.append(current_record)
        
        current_record = {
            'CCOLLNO': coll_match.group(1),
            'CINSTCL': coll_match.group(2),  # Institution class (18)
            'NATGUAR': coll_match.group(3),  # Nature of guarantee (RI)
            'STATE': coll_match.group(4),     # State code (02)
            'COUNTRY': coll_match.group(5)    # Country code (MY)
        }
    elif '18RI' in line:
        # Individual record with RI (Relationship/Individual)
        ri_match = re.search(r'(\d{11})\d{3}\s+18RI', line)
        if ri_match:
            if current_record:
                desc_data.append(current_record)
            current_record = {
                'CCOLLNO': ri_match.group(1),
                'CINSTCL': '18',
                'NATGUAR': 'RI'
            }
    elif '16IC' in line:
        # IC record
        ic_match = re.search(r'(\d{11})\d{3}\s+16IC', line)
        if ic_match:
            if current_record:
                desc_data.append(current_record)
            current_record = {
                'CCOLLNO': ic_match.group(1),
                'CINSTCL': '16',
                'NATGUAR': 'IC'
            }

# Add last record
if current_record:
    desc_data.append(current_record)

desc = pd.DataFrame(desc_data)
print(f"DESC parsed: {desc.shape[0]} rows")
if not desc.empty:
    print("DESC sample:")
    print(desc.head())
    print("\nCINSTCL values:", desc['CINSTCL'].value_counts().head(10))
    print("NATGUAR values:", desc['NATGUAR'].value_counts().head(10))

# Merge COLL with DESC
if not coll.empty and not desc.empty:
    # Clean collateral numbers for matching
    coll['CCOLLNO_CLEAN'] = coll['CCOLLNO'].str.lstrip('0')
    desc['CCOLLNO_CLEAN'] = desc['CCOLLNO'].str.lstrip('0')
    
    # Merge
    coll_merged = coll.merge(
        desc[['CCOLLNO_CLEAN', 'CINSTCL', 'NATGUAR']],
        on='CCOLLNO_CLEAN',
        how='inner'
    )
    print(f"COLL merged with DESC: {coll_merged.shape[0]} rows")
    
    # Filter for individual records (RI = Relationship/Individual)
    coll_filtered = coll_merged[coll_merged['NATGUAR'] == 'RI']
    print(f"COLL filtered (NATGUAR=RI): {coll_filtered.shape[0]} rows")
    
    # Merge with CA
    if not coll_filtered.empty:
        # Add CR column (will be populated later)
        coll_filtered['CR'] = None
        
        dep = ca.merge(
            coll_filtered[['ACCTNO', 'CR']].drop_duplicates(),
            on='ACCTNO',
            how='left'
        )
        print(f"DEP after COLL merge: {dep.shape[0]} rows")
    else:
        print("WARNING: No records after COLL filtering")
        dep = ca.copy()
        dep['CR'] = None
else:
    print("WARNING: No valid COLL or DESC records")
    dep = ca.copy()
    dep['CR'] = None

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

if not dep.empty:
    dep[['ARREARS','NPLDATE_CALC']] = dep.apply(
        lambda x: pd.Series(calc_arrears(x)), axis=1
    )
    dep['NPLDATE'] = dep['NPLDATE_CALC'].combine_first(dep['NPLDATE'])

# =========================
# STEP 6: CVAR02
# =========================
def map_cvar02(row):
    if row.SCH=='P51': return '51'
    if row.SCH=='P65': return '65'
    if row.SCH=='P53': return '53'
    if row.SCH=='P85': return '85'
    if row.SCH=='P70': return '70'
    if row.SCH=='P72': return '72'
    return None

if not dep.empty:
    dep['CVAR02'] = dep.apply(map_cvar02, axis=1)
    dep_filtered = dep[dep['CVAR02'].notna()]
    print(f"DEP after CVAR02 mapping: {dep_filtered.shape[0]} rows")
    
    if dep_filtered.empty:
        print("WARNING: No records after CVAR02 mapping. Using all records...")
        dep['CVAR02'] = 'XX'
        dep_filtered = dep.copy()
else:
    dep_filtered = dep.copy()
    dep_filtered['CVAR02'] = 'XX'

# =========================
# STEP 7: OUTPUT STRUCTURE
# =========================
# Use ACCTNO as CVAR01 if CENSUS is not available
if 'CENSUS' not in dep_filtered.columns:
    dep_filtered['CENSUS'] = dep_filtered['ACCTNO']  # Temporary fallback

dep_filtered['CVAR01'] = dep_filtered['CENSUS'].astype(str)
dep_filtered['CVAR03'] = dep_filtered['NEWIC'] if 'NEWIC' in dep_filtered.columns else ''
dep_filtered['CVAR04'] = dep_filtered['CUSTNAME'] if 'CUSTNAME' in dep_filtered.columns else ''
dep_filtered['CVAR05'] = dep_filtered['LMTSTART'] if 'LMTSTART' in dep_filtered.columns else pd.NaT
dep_filtered['CVAR06'] = dep_filtered['ACCTNO'].astype(str)
dep_filtered['CVAR07'] = 'OD'
dep_filtered['CVAR08'] = dep_filtered['APPRLIMT'].fillna(0) if 'APPRLIMT' in dep_filtered.columns else 0

dep_filtered['CVAR09'] = np.where(dep_filtered['LEDGBAL'] < 0, -dep_filtered['LEDGBAL'], 0) if 'LEDGBAL' in dep_filtered.columns else 0
dep_filtered['CVAR10'] = np.where(dep_filtered['LEDGBAL'] >= 0, dep_filtered['LEDGBAL'], 0) if 'LEDGBAL' in dep_filtered.columns else 0

dep_filtered['CVAR11'] = dep_filtered['ARREARS'] if 'ARREARS' in dep_filtered.columns else 0
dep_filtered['CVAR12'] = np.where(dep_filtered['ARREARS'] >= 3, 'NPL', '   ') if 'ARREARS' in dep_filtered.columns else '   '
dep_filtered['CVAR13'] = dep_filtered['NPLDATE'].dt.strftime('%d/%m/%Y') if 'NPLDATE' in dep_filtered.columns else ''

dep_filtered['CVAR14'] = '0233'
dep_filtered['CVAR15'] = dep_filtered['MICRCD'] if 'MICRCD' in dep_filtered.columns else ''

# =========================
# STEP 8: HISTORY MERGE
# =========================
# Convert keys to string for consistent merging
dep_filtered['CVAR06'] = dep_filtered['CVAR06'].astype(str)
dep_filtered['CVAR01'] = dep_filtered['CVAR01'].astype(str)

if not npla_df.empty:
    if 'CVAR06' in npla_df.columns:
        npla_df['CVAR06'] = npla_df['CVAR06'].astype(str)
    if 'CVAR01' in npla_df.columns:
        npla_df['CVAR01'] = npla_df['CVAR01'].astype(str)
    
    npgs = dep_filtered.merge(npla_df, on=['CVAR06','CVAR01'], how='left')
    
    if 'STATUS' in npgs.columns and 'NDATE' in npgs.columns:
        npgs.loc[
            (npgs['CVAR12']=='NPL') & (npgs['STATUS']=='NPL'),
            'CVAR13'
        ] = npgs['NDATE']
else:
    npgs = dep_filtered.copy()

# =========================
# STEP 9: OUTPUT (SAS7BDAT)
# =========================
print(f"\nWriting output to {OUTPUT_FILE}...")
print(f"Final dataset: {npgs.shape[0]} rows")

# Select only the required columns for output
output_columns = ['CVAR01', 'CVAR02', 'CVAR03', 'CVAR04', 'CVAR05', 
                  'CVAR06', 'CVAR07', 'CVAR08', 'CVAR09', 'CVAR10',
                  'CVAR11', 'CVAR12', 'CVAR13', 'CVAR14', 'CVAR15']

# Ensure all required columns exist
for col in output_columns:
    if col not in npgs.columns:
        npgs[col] = ''

npgs_output = npgs[output_columns].copy()

# Initialize SAS session
sas = saspy.SASsession(cfgname='default')

# Convert pandas DataFrame to SAS dataset
sas.df2sd(npgs_output, table='npgs_output', libref='WORK')

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
print(f"Total records: {len(npgs_output)}")
