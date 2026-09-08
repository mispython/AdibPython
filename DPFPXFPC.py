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

# Read CISDP dataset
cisdp_df, cisdp_meta = pyreadstat.read_sas7bdat(CISDP_DF)
print(f"CISDP dataset: {cisdp_df.shape[0]} rows")

# Read NPLA dataset
npla_df, npla_meta = pyreadstat.read_sas7bdat(NPLA_DF)
print(f"NPLA dataset: {npla_df.shape[0]} rows")

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
    if pd.isna(x) or x <= 0:
        return pd.NaT
    s = str(int(x)).zfill(11)[:8]
    return datetime.strptime(s, "%m%d%Y")

limit_processed = limit_df.copy()
limit_processed['LMTSTART'] = limit_processed['LMTSTART'].apply(convert_lmtstart)
limit_processed = limit_processed[['ACCTNO','LMTSTART']].drop_duplicates()

ca = ca.merge(limit_processed, on='ACCTNO', how='left')

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

# =========================
# STEP 4C: CISDP
# =========================
cisdp = cisdp_df[cisdp_df['SECCUST'] == '901'][['ACCTNO','NEWIC','CUSTNAME']]
cisdp = cisdp.drop_duplicates()

ca = ca.merge(cisdp, on='ACCTNO', how='left')

# =========================
# STEP 4D: COLL + DESC (EBCDIC FILES)
# =========================
def read_ebcdic_fwf(file_path, colspecs, names):
    """Read fixed-width EBCDIC file and convert to ASCII"""
    # Read the file in binary mode
    with open(file_path, 'rb') as f:
        raw_data = f.read()
    
    # Decode EBCDIC to string (cp037 is common EBCDIC encoding for mainframes)
    # Alternative encodings: cp500 (International), cp1047 (Latin-1)
    decoded_data = raw_data.decode('cp037', errors='replace')
    
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

print("Reading DESC file (EBCDIC)...")
desc = read_ebcdic_fwf(
    DESC_FILE,
    colspecs=[(0,11),(50,52),(54,56),(210,220)],
    names=['CCOLLNO','CINSTCL','NATGUAR','CENSUS']
)

# Clean up EBCDIC artifacts
coll = clean_ebcdic_strings(coll)
desc = clean_ebcdic_strings(desc)

# Convert CENSUS to numeric, handling EBCDIC numeric fields
desc['CENSUS'] = pd.to_numeric(desc['CENSUS'], errors='coerce')

def map_cr(census):
    if pd.isna(census):
        return None
    if 51000000 <= census <= 51999999: return '51'
    if 63000000 <= census <= 63999999: return '63'
    if 70000000 <= census <= 70999999: return '70'
    if 71000000 <= census <= 71999999: return '71'
    if 72000000 <= census <= 72999999: return '72'
    if 1000000000 <= census <= 1099999999: return '10'
    return None

desc['CR'] = desc['CENSUS'].apply(map_cr)
desc = desc[desc['CR'].notna()]

coll = coll.merge(desc, on='CCOLLNO')
coll = coll[(coll['CINSTCL']=='18') & (coll['NATGUAR']=='06')]

dep = ca.merge(coll, on='ACCTNO')

# =========================
# STEP 4E: MICR
# =========================
micr = pd.read_fwf(
    MICR_FILE,
    colspecs=[(0,3),(39,44)],
    names=['BRANCH','MICRCD']
)

dep = dep.merge(micr, on='BRANCH', how='left')

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
            d = datetime.strptime(str(int(val)).zfill(11)[:8], "%m%d%Y")
            dates.append(d)

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
dep = dep[dep['CVAR02'].notna()]

# =========================
# STEP 7: OUTPUT STRUCTURE
# =========================
dep['CVAR01'] = dep['CENSUS']
dep['CVAR03'] = dep['NEWIC']
dep['CVAR04'] = dep['CUSTNAME']
dep['CVAR05'] = dep['LMTSTART']
dep['CVAR06'] = dep['ACCTNO']
dep['CVAR07'] = 'OD'
dep['CVAR08'] = dep['APPRLIMT'].fillna(0)

dep['CVAR09'] = np.where(dep['LEDGBAL'] < 0, -dep['LEDGBAL'], 0)
dep['CVAR10'] = np.where(dep['LEDGBAL'] >= 0, dep['LEDGBAL'], 0)

dep['CVAR11'] = dep['ARREARS']
dep['CVAR12'] = np.where(dep['ARREARS'] >= 3, 'NPL', '   ')
dep['CVAR13'] = dep['NPLDATE'].dt.strftime('%d/%m/%Y')

dep['CVAR14'] = '0233'
dep['CVAR15'] = dep['MICRCD']

# =========================
# STEP 8: HISTORY MERGE
# =========================
npgs = dep.merge(npla_df, on=['CVAR06','CVAR01'], how='left')

npgs.loc[
    (npgs['CVAR12']=='NPL') & (npgs['STATUS']=='NPL'),
    'CVAR13'
] = npgs['NDATE']

# =========================
# STEP 9: OUTPUT (SAS7BDAT)
# =========================
print(f"Writing output to {OUTPUT_FILE}...")

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
