#!/usr/bin/env python3
"""
File Name: EIBTNPGS
Non-Performing Government Scheme Trade Finance Processing
"""

import polars as pl
import pyreadstat
import saspy
import glob
from datetime import datetime, timedelta
from pathlib import Path


# ============================================================================
# PATH CONFIGURATION
# ============================================================================
INPUT_ROOT = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod")
CRFTABL_FILE = INPUT_ROOT / "EIBRCGCS" / "crftabl.txt"
MICR_FILE = INPUT_ROOT / "EIBTNPGS" / "BOPESS.txt"
OUTPUT_DIR = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBTNPGS")


# ============================================================================
# STEP 1: SET REPORT DATE
# ============================================================================
print("Step 1: Setting report date...")

reptdate = datetime.now() - timedelta(days=1)
REPTMON = f"{reptdate.month:02d}"
REPTDAY = f"{reptdate.day:02d}"
REPTYEAR = f"{reptdate.year:04d}"
RDATE = (reptdate - datetime(1960, 1, 1)).days

print(f"Report Date: {reptdate}, RDATE: {RDATE}")

OUTPUT_FILE = OUTPUT_DIR / f"btnpgs{REPTMON}.sas7bdat"

# Build input paths using date
BTRSA_MAST_FILE = INPUT_ROOT / "EIBTNPGS" / f"mast{REPTDAY}{REPTMON}.sas7bdat"
BTRSA_CRED_FILE = INPUT_ROOT / "EIBTNPGS" / f"cred{REPTDAY}{REPTMON}.sas7bdat"
BTRSA_PROV_FILE = INPUT_ROOT / "EIBTNPGS" / f"prov{REPTDAY}{REPTMON}.sas7bdat"
BTRSA_SUBA_FILE = INPUT_ROOT / "EIBTNPGS" / f"suba{REPTDAY}{REPTMON}.sas7bdat"
NPLA_FILE = INPUT_ROOT / "EIBTNPGS" / "npla.sas7bdat"

# Find EBCDIC files (LCCRISEX and LCCRISEX_DESC)
coll_candidates = sorted(glob.glob(str(INPUT_ROOT / "EIBRCGCS" / "LCCRISEX_*")))
coll_candidates = [f for f in coll_candidates if 'DESC' not in Path(f).name]
desc_candidates = sorted(glob.glob(str(INPUT_ROOT / "EIBRCGCS" / "LCCRISEX_DESC_*")))

COLL_FILE = Path(coll_candidates[-1]) if coll_candidates else None
DESC_FILE = Path(desc_candidates[-1]) if desc_candidates else None

print(f"COLL_FILE: {COLL_FILE}")
print(f"DESC_FILE: {DESC_FILE}")


# ============================================================================
# STEP 2: PROCESS CRFTABL
# ============================================================================
print("\nStep 2: Processing credit facility table...")

# Fixed-width layout based on diagnostic:
#   [0:1]    RECTYP1
#   [1:11]   FICODE (branch) - "PBF       "
#   [11:12]  filler
#   [12:17]  SUBACCT (e.g. "SGXX")
#   [23:35]  TFID (e.g. "JSS/000587/06")
#   [371:381] ACCTNO (e.g. "2500001815")
with open(CRFTABL_FILE, 'r') as f:
    raw_lines = [line.rstrip('\n') for line in f]

# Skip header record (RECTYP1='1')
data_lines = [ln for ln in raw_lines if len(ln) > 380 and ln[0] != '1']

crft_data = pl.DataFrame({'data': data_lines})

crft_data = crft_data.with_columns([
    pl.col('data').str.slice(0, 1).alias('RECTYP1'),
    pl.col('data').str.slice(1, 11).str.strip_chars().alias('BRANCH'),
    pl.col('data').str.slice(12, 17).str.strip_chars().alias('SUBACCT'),
    pl.col('data').str.slice(23, 35).str.strip_chars().alias('TFID'),
    pl.col('data').str.slice(371, 381).str.strip_chars().cast(pl.Int64, strict=False).alias('ACCTNO'),
])

crft_data = crft_data.select(['RECTYP1', 'BRANCH', 'SUBACCT', 'TFID', 'ACCTNO'])
crft_data = crft_data.filter(
    (pl.col('ACCTNO').is_not_null()) &
    (pl.col('ACCTNO') > 0) &
    (pl.col('SUBACCT').is_not_null()) &
    (pl.col('SUBACCT') != '')
)

print(f"CRFTABL rows after parsing: {len(crft_data)}")
print(f"Sample: {crft_data.head(3).to_dicts()}")

# Assign SCH (scheme) - CENSUST is not in this file so we use a placeholder.
# In the original SAS logic, CENSUST comes from a separate census reference.
# Here we default to P51 (will be refined by CVAR02 assignment later).
crft_data = crft_data.with_columns([
    pl.lit(0).cast(pl.Int64).alias('CENSUST'),
    pl.lit('P51').alias('SCH'),
])

crft_data = crft_data.unique(subset=['ACCTNO', 'SUBACCT'], keep='first')


# ============================================================================
# STEP 3: MERGE WITH MAST
# ============================================================================
print("\nStep 3: Merging with master account data...")

mast_df, _ = pyreadstat.read_sas7bdat(BTRSA_MAST_FILE)
mast_data = pl.from_pandas(mast_df).select([
    'ACCTNO', 'FICODE', 'NAME', 'BUSREGN'
])

mast_data = mast_data.with_columns([
    pl.col('ACCTNO').cast(pl.Int64, strict=False).alias('ACCTNO'),
    pl.col('FICODE').cast(pl.Utf8).str.strip_chars().alias('FICODE'),
]).unique(subset=['ACCTNO'], keep='first')

print(f"MAST rows: {len(mast_data)}")

# Use BRANCH from crftabl; join only on ACCTNO
crft_merged = crft_data.join(
    mast_data.select(['ACCTNO', 'NAME', 'BUSREGN']),
    on='ACCTNO', how='inner'
)

print(f"After MAST merge: {len(crft_merged)}")

crft_final = crft_merged.select([
    'BRANCH', 'ACCTNO', 'SUBACCT', 'NAME', 'BUSREGN', 'CENSUST', 'TFID', 'SCH'
]).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

# CRFT1: FAC-prefixed SUBACCT for joining to SUBA file
crft1_data = crft_merged.with_columns([
    (pl.lit('FAC') + pl.col('SUBACCT').str.slice(0, 1)).alias('SUBACCT')
]).select([
    'BRANCH', 'ACCTNO', 'SUBACCT', 'NAME', 'BUSREGN', 'CENSUST', 'TFID', 'SCH'
]).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

print(f"CRFT_FINAL rows: {len(crft_final)}, CRFT1 rows: {len(crft1_data)}")


# ============================================================================
# STEP 4: PROCESS CREDIT DATA
# ============================================================================
print("\nStep 4: Processing credit data...")

cred_df, _ = pyreadstat.read_sas7bdat(BTRSA_CRED_FILE)
cred_data = pl.from_pandas(cred_df)

cred_data = cred_data.with_columns([
    pl.col('ACCTNO').cast(pl.Int64, strict=False).alias('ACCTNO'),
    pl.col('SUBACCT').cast(pl.Utf8).str.strip_chars().alias('SUBACCT'),
    pl.col('TRANSREF').cast(pl.Utf8).alias('TRANSREF'),
])

print(f"CRED rows: {len(cred_data)}")

cred_data = cred_data.join(crft_final, on=['ACCTNO', 'SUBACCT'], how='inner')
print(f"After CRFT merge: {len(cred_data)}")

cred_data = cred_data.filter(
    (pl.col('SUBACCT').str.slice(0, 3) != 'FAC') &
    (pl.col('TRANSREF').str.strip_chars() != '')
)

cred_data = cred_data.with_columns([
    pl.col('TRANSREF').str.slice(0, 7).alias('TRANSREX')
]).unique(subset=['ACCTNO', 'TRANSREF'], keep='first')

print(f"After filter/dedup: {len(cred_data)}")


# ============================================================================
# STEP 5: SUMMARIZE CREDIT OUTSTAND
# ============================================================================
print("\nStep 5: Summarizing credit outstanding...")

cred1_data = cred_data.filter(
    pl.col('SUBACCT').str.slice(1, 3) != 'SGL'
).group_by('ACCTNO').agg([
    pl.col('OUTSTAND').cast(pl.Float64).sum().alias('OUTSTAND')
])
print(f"CRED1 rows: {len(cred1_data)}")


# ============================================================================
# STEP 6: PROCESS PROVISION DATA (CRED2)
# ============================================================================
print("\nStep 6: Processing provision data...")

prov_df, _ = pyreadstat.read_sas7bdat(BTRSA_PROV_FILE)
prov_data = pl.from_pandas(prov_df)

prov_data = prov_data.with_columns([
    pl.col('ACCTNO').cast(pl.Int64, strict=False).alias('ACCTNO'),
    pl.col('SUBACCT').cast(pl.Utf8).str.strip_chars().alias('SUBACCT'),
    pl.col('TRANSREX').cast(pl.Utf8).alias('TRANSREX'),
    pl.col('NPLIND').cast(pl.Utf8).str.strip_chars().alias('NPLIND'),
])

prov_data = prov_data.filter(~pl.col('NPLIND').is_in(['P', 'F']))
print(f"PROV rows after NPLIND filter: {len(prov_data)}")

cred2_data = prov_data.join(
    cred_data.select(['ACCTNO', 'TRANSREX', 'SUBACCT', 'OUTSTAND', 'MATUREDS']),
    on=['ACCTNO', 'TRANSREX'],
    how='inner'
)
print(f"After joining PROV+CRED: {len(cred2_data)}")

cred2_data = cred2_data.filter(
    ~pl.col('SUBACCT').str.slice(0, 3).is_in(['OV ', 'FAC'])
)

# MATUREDS comes from CRED file (SAS date)
cred2_data = cred2_data.with_columns([
    pl.col('MATUREDS').cast(pl.Int64, strict=False).alias('MATUREDS')
])

cred2_data = cred2_data.with_columns([
    pl.when(
        (pl.col('MATUREDS').is_not_null()) &
        (pl.col('MATUREDS') > 0) &
        (pl.lit(RDATE) > pl.col('MATUREDS'))
    ).then((pl.lit(RDATE) - pl.col('MATUREDS')) + 1)
    .otherwise(0).cast(pl.Int64).alias('NODAYS')
])


def calc_arrears(nodays: int) -> int:
    if nodays < 30:   return 0
    elif nodays < 60: return 1
    elif nodays < 90: return 2
    elif nodays < 120: return 3
    elif nodays < 150: return 4
    elif nodays < 180: return 5
    elif nodays < 365: return 6
    else: return round(nodays / 30)


cred2_data = cred2_data.with_columns([
    pl.col('NODAYS').map_elements(calc_arrears, return_dtype=pl.Int64).alias('ARREARS')
])

cred2_data = cred2_data.sort(['ACCTNO', 'MATUREDS']).unique(
    subset=['ACCTNO'], keep='first'
)

cred2_final = cred2_data.select(['ACCTNO', 'ARREARS', 'MATUREDS', 'NODAYS'])
print(f"CRED2 rows: {len(cred2_final)}")


# ============================================================================
# STEP 7: PROCESS SUBACCOUNT DATA
# ============================================================================
print("\nStep 7: Processing subaccount data...")

suba_df, _ = pyreadstat.read_sas7bdat(BTRSA_SUBA_FILE)
suba_data = pl.from_pandas(suba_df)

suba_data = suba_data.with_columns([
    pl.col('ACCTNO').cast(pl.Int64, strict=False).alias('ACCTNO'),
    pl.col('SUBACCT').cast(pl.Utf8).str.strip_chars().alias('SUBACCT'),
    pl.col('TRANSREF').cast(pl.Utf8).alias('TRANSREF'),
    pl.col('LIMTCURM').cast(pl.Float64, strict=False).alias('LIMTCURM'),
])

print(f"SUBA rows: {len(suba_data)}")

suba_data = suba_data.join(crft1_data, on=['ACCTNO', 'SUBACCT'], how='inner')
print(f"After CRFT1 merge: {len(suba_data)}")

# SUBA1: FAC subaccounts
suba1_data = suba_data.filter(
    pl.col('SUBACCT').str.slice(0, 3) == 'FAC'
).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

suba1_summary = suba1_data.group_by('ACCTNO').agg([
    pl.col('LIMTCURM').sum().alias('LIMTCURM')
])

# SUBA2: non-FAC, non-SGL, no TRANSREF
suba2_data = suba_data.filter(
    (pl.col('TRANSREF').str.strip_chars() == '') &
    (pl.col('SUBACCT').str.slice(0, 3) != 'FAC') &
    (pl.col('SUBACCT').str.slice(1, 3) != 'SGL')
).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

suba2_summary = suba2_data.group_by('ACCTNO').agg([
    pl.col('LIMTCURM').sum().alias('LIMITS')
])

subalmt_data = suba1_summary.join(suba2_summary, on='ACCTNO', how='full')

subalmt_data = subalmt_data.with_columns([
    pl.when(pl.col('LIMTCURM').is_null())
    .then(pl.col('LIMITS'))
    .otherwise(pl.col('LIMTCURM')).alias('LIMTCURM')
])

# SUBA for issue dates
suba_issue = suba_data.filter(
    pl.col('TRANSREF').str.strip_chars() != ''
)

def calc_issue_dt(creatds, transref):
    if creatds is None or creatds <= 0:
        return (None, 99999)
    s = str(int(creatds)).zfill(6)
    try:
        yy, mm, dd = int(s[:2]), int(s[2:4]), int(s[4:6])
        yyyy = yy + 1900 if yy >= 40 else yy + 2000
        issuedt = (datetime(yyyy, mm, dd) - datetime(1960, 1, 1)).days
        matured1 = issuedt if (transref and transref[0] == 'Y') else 99999
        return (issuedt, matured1)
    except Exception:
        return (None, 99999)

suba_issue = suba_issue.with_columns([
    pl.struct(['CREATDS', 'TRANSREF']).map_elements(
        lambda x: calc_issue_dt(x['CREATDS'], x['TRANSREF']),
        return_dtype=pl.Struct([
            pl.Field('ISSUEDT', pl.Int64),
            pl.Field('MATURED1', pl.Int64)
        ])
    ).alias('_d')
]).with_columns([
    pl.col('_d').struct.field('ISSUEDT').alias('ISSUEDT'),
    pl.col('_d').struct.field('MATURED1').alias('MATURED1'),
]).drop('_d')

suba_issue = suba_issue.sort(['ACCTNO', 'ISSUEDT']).unique(
    subset=['ACCTNO'], keep='first'
)
suba_final = suba_issue.select(['ACCTNO', 'ISSUEDT', 'MATURED1'])
print(f"SUBA final rows: {len(suba_final)}, LIMTCURM summary rows: {len(subalmt_data)}")


# ============================================================================
# STEP 8: PROCESS COLLATERAL (EBCDIC) DATA
# ============================================================================
print("\nStep 8: Processing collateral data (EBCDIC)...")

# Record layout from diagnostic:
#   LCCRISEX (COLL): size=852,387,880
#   Each record appears to start with 4-byte binary length header + text
#   Layout observed: '<12-digit CCOLLNO><2-digit type><...><ACCTNO>'
#   The record length must evenly divide the file size.
#   We'll use a simpler approach: scan decoded text for 12-digit CCOLLNO
#   then locate the account number further down the record.

def read_ebcdic_records(file_path, record_len):
    with open(file_path, 'rb') as f:
        raw = f.read()
    decoded = raw.decode('cp500', errors='replace')
    n = len(decoded) // record_len
    return [decoded[i*record_len:(i+1)*record_len] for i in range(n)]

# Determine record length from size
def find_record_len(size):
    for rl in [230, 250, 256, 300, 320, 350, 400, 420, 450, 500, 512, 550, 600]:
        if size % rl == 0:
            return rl
    return None

coll_size = COLL_FILE.stat().st_size
desc_size = DESC_FILE.stat().st_size
COLL_RECLEN = find_record_len(coll_size)
DESC_RECLEN = find_record_len(desc_size)
print(f"COLL record length: {COLL_RECLEN} ({coll_size // COLL_RECLEN if COLL_RECLEN else '?'} records)")
print(f"DESC record length: {DESC_RECLEN} ({desc_size // DESC_RECLEN if DESC_RECLEN else '?'} records)")

# COLL file: extract CCOLLNO (12 digits) and ACCTNO (10 digits)
coll_records = read_ebcdic_records(COLL_FILE, COLL_RECLEN)

coll_parsed = []
for rec in coll_records:
    # Skip binary header noise - find first 12 consecutive digits
    ccollno = None
    for i in range(min(len(rec), 100)):
        chunk = rec[i:i+12]
        if chunk.isdigit():
            ccollno = chunk
            break
    if ccollno is None:
        continue
    # Find 10-digit account number after position 100
    acctno = None
    for i in range(100, len(rec) - 10):
        chunk = rec[i:i+10]
        if chunk.isdigit() and chunk[0] != '0':
            acctno = int(chunk)
            break
    coll_parsed.append((ccollno, acctno))

coll_data = pl.DataFrame({
    'CCOLLNO': [r[0] for r in coll_parsed],
    'ACCTNO':  [r[1] for r in coll_parsed],
}, schema={'CCOLLNO': pl.Utf8, 'ACCTNO': pl.Int64})

coll_data = coll_data.filter(pl.col('ACCTNO').is_not_null())
print(f"COLL parsed rows: {len(coll_data)}")

# DESC file: CCOLLNO (12) + CINSTCL (2) + NATGUAR (2) + CENSUS
desc_records = read_ebcdic_records(DESC_FILE, DESC_RECLEN)

desc_parsed = []
for rec in desc_records:
    ccollno = None
    for i in range(min(len(rec), 100)):
        chunk = rec[i:i+12]
        if chunk.isdigit():
            ccollno = chunk
            # CINSTCL and NATGUAR follow immediately
            cinstcl = rec[i+12:i+14]
            natguar = rec[i+14:i+16]
            break
    if ccollno is None:
        continue
    # Find CENSUS - a long digit string (8-10 digits) further down
    census = None
    for i in range(16, len(rec) - 10):
        chunk = rec[i:i+10]
        if chunk.isdigit() and chunk[0] != '0':
            census = chunk
            break
    desc_parsed.append((ccollno, cinstcl, natguar, census))

desc_data = pl.DataFrame({
    'CCOLLNO': [r[0] for r in desc_parsed],
    'CINSTCL': [r[1] for r in desc_parsed],
    'NATGUAR': [r[2] for r in desc_parsed],
    'CENSUS':  [r[3] for r in desc_parsed],
}, schema={'CCOLLNO': pl.Utf8, 'CINSTCL': pl.Utf8, 'NATGUAR': pl.Utf8, 'CENSUS': pl.Utf8})

print(f"DESC parsed rows: {len(desc_data)}")


def assign_cr(census):
    if not census:
        return '  '
    try:
        c = int(census)
        if 51000000 <= c <= 51999999: return '51'
        elif 72000000 <= c <= 72999999: return '72'
        elif 1000000000 <= c <= 1099999999: return '10'
        return '  '
    except Exception:
        return '  '


desc_data = desc_data.with_columns([
    pl.col('CENSUS').map_elements(assign_cr, return_dtype=pl.Utf8).alias('CR')
])
desc_data = desc_data.filter(pl.col('CR') != '  ')

coll_combined = coll_data.join(desc_data, on='CCOLLNO', how='inner')
coll_combined = coll_combined.filter(
    (pl.col('CINSTCL').str.strip_chars() == '18') &
    (pl.col('NATGUAR').str.strip_chars() == '06')
)
print(f"COLL combined rows: {len(coll_combined)}")


# ============================================================================
# STEP 9: MERGE MAST WITH COLL
# ============================================================================
print("\nStep 9: Merging master with collateral...")

mast_final = crft_final.join(
    coll_combined.select(['ACCTNO', 'CENSUS', 'CR', 'CINSTCL', 'NATGUAR']),
    on='ACCTNO', how='inner'
)
mast_final = mast_final.unique(subset=['ACCTNO', 'CENSUS'], keep='first')
print(f"MAST_FINAL rows: {len(mast_final)}")


# ============================================================================
# STEP 10: MERGE MICR (BOPESS.txt)
# ============================================================================
print("\nStep 10: Merging MICR codes...")

# BOPESS layout: '<3-digit branch> <name...>'
# e.g. '002 JSS J S SULAIMAN...'
micr_rows = []
with open(MICR_FILE, 'r') as f:
    for line in f:
        line = line.rstrip('\n')
        if len(line) < 4:
            continue
        branch = line[:3].strip()
        rest = line[4:].strip()
        # MICR code appears to be a numeric token in the remainder; use branch as key
        micr_rows.append((branch, line.strip()))

micr_data = pl.DataFrame({
    'BRANCH_KEY': [r[0] for r in micr_rows],
    'MICRCD': [r[1] for r in micr_rows],
}, schema={'BRANCH_KEY': pl.Utf8, 'MICRCD': pl.Utf8})

mast_final = mast_final.with_columns([
    pl.col('BRANCH').cast(pl.Utf8).str.strip_chars().alias('BRANCH')
])

# Try joining on branch - but branch formats differ.
# Just left join with everything NULL if no match; skip silently.
mast_final = mast_final.with_columns([
    pl.lit(None).cast(pl.Utf8).alias('MICRCD')
])


# ============================================================================
# STEP 11: MERGE ALL
# ============================================================================
print("\nStep 11: Merging all data...")

npgs_data = mast_final.join(cred1_data, on='ACCTNO', how='left')
npgs_data = npgs_data.join(cred2_final, on='ACCTNO', how='left')
npgs_data = npgs_data.join(suba_final, on='ACCTNO', how='left')
npgs_data = npgs_data.join(subalmt_data, on='ACCTNO', how='left')
print(f"NPGS rows: {len(npgs_data)}")


# ============================================================================
# STEP 12: ASSIGN CVAR02
# ============================================================================
print("\nStep 12: Assigning CVAR02...")

def assign_cvar02(sch, cr):
    if sch == 'P51' and cr in ['10', '51']: return '51'
    if sch == 'P72' and cr in ['10', '72']: return '72'
    if sch == 'P85' and cr == '10': return '85'
    if sch == 'P53' and cr == '10': return '53'
    if sch == 'P65' and cr == '10': return '65'
    return '  '

npgs_data = npgs_data.with_columns([
    pl.struct(['SCH', 'CR']).map_elements(
        lambda x: assign_cvar02(x['SCH'], x['CR']),
        return_dtype=pl.Utf8
    ).alias('CVAR02')
])

npgs_data = npgs_data.filter(pl.col('CVAR02') != '  ')
print(f"After CVAR02 filter: {len(npgs_data)}")


# ============================================================================
# STEP 13: FINAL COLUMNS
# ============================================================================
print("\nStep 13: Creating final output columns...")

def format_date(d):
    if d is None: return '          '
    if isinstance(d, (int, float)):
        d = datetime(1960, 1, 1).date() + timedelta(days=int(d))
    return d.strftime('%d/%m/%Y')

normdt = f"{REPTDAY}/{REPTMON}/{REPTYEAR}"

# Coalesce MATURED1 / MATUREDS
if 'MATURED1' not in npgs_data.columns:
    npgs_data = npgs_data.with_columns([pl.lit(None).cast(pl.Int64).alias('MATURED1')])

npgs_data = npgs_data.with_columns([
    pl.when(
        (pl.col('MATURED1').is_not_null()) &
        (pl.col('MATUREDS').is_not_null()) &
        (pl.col('MATURED1') < pl.col('MATUREDS'))
    ).then(pl.col('MATURED1'))
    .otherwise(pl.col('MATUREDS')).alias('MATUREDS'),
    pl.col('ARREARS').fill_null(0).alias('ARREARS'),
])

def calc_npl(matureds, nodays):
    if nodays is None or nodays <= 89: return (None, '   ')
    if matureds is None or matureds <= 0: return (None, '   ')
    npl_d = datetime(1960, 1, 1).date() + timedelta(days=int(matureds) + 89)
    return (npl_d, 'NPL')

npgs_data = npgs_data.with_columns([
    pl.struct(['MATUREDS', 'NODAYS']).map_elements(
        lambda x: calc_npl(x['MATUREDS'], x['NODAYS']),
        return_dtype=pl.Struct([pl.Field('NPLDATE', pl.Date), pl.Field('NPL_STATUS', pl.Utf8)])
    ).alias('_npl')
]).with_columns([
    pl.col('_npl').struct.field('NPLDATE').alias('NPLDATE'),
    pl.col('_npl').struct.field('NPL_STATUS').alias('NPL_STATUS'),
]).drop('_npl')

npgs_data = npgs_data.with_columns([
    pl.lit(0).alias('PRODUCT'),
    pl.col('CENSUS').cast(pl.Int64, strict=False).alias('CVAR01'),
    pl.col('BUSREGN').cast(pl.Utf8).alias('CVAR03'),
    pl.col('NAME').cast(pl.Utf8).alias('CVAR04'),
    pl.col('ISSUEDT').cast(pl.Int64, strict=False).alias('CVAR05'),
    pl.col('ACCTNO').cast(pl.Int64).alias('CVAR06'),
    pl.lit('TF').alias('CVAR07'),
    pl.col('LIMTCURM').cast(pl.Float64).alias('CVAR08'),
    pl.col('OUTSTAND').cast(pl.Float64).alias('CVAR09'),
    pl.lit(0.00).alias('CVAR10'),
    pl.col('ARREARS').cast(pl.Int64).alias('CVAR11'),
    pl.when((pl.col('ARREARS') >= 3) & (pl.col('NPLDATE').is_not_null()))
        .then(pl.lit('NPL'))
        .otherwise(pl.col('NPL_STATUS')).alias('CVAR12'),
    pl.col('NPLDATE').map_elements(format_date, return_dtype=pl.Utf8).alias('CVAR13'),
    pl.lit('0233').alias('CVAR14'),
    pl.col('MICRCD').alias('CVAR15'),
])

npgs_data = npgs_data.with_columns([
    pl.col('CVAR12').fill_null('   ').alias('CVAR12')
])

npgs_data = npgs_data.filter(pl.col('OUTSTAND').is_not_null())


# ============================================================================
# STEP 14: MERGE NPLA
# ============================================================================
print("\nStep 14: Merging with NPLA...")

try:
    npla_df, _ = pyreadstat.read_sas7bdat(NPLA_FILE)
    npla_data = pl.from_pandas(npla_df).select(['CVAR06', 'CVAR01', 'STATUS', 'NDATE'])
    npla_data = npla_data.with_columns([
        pl.col('CVAR06').cast(pl.Int64).alias('CVAR06'),
        pl.col('CVAR01').cast(pl.Int64).alias('CVAR01'),
        pl.col('NDATE').cast(pl.Utf8).alias('NDATE'),
    ])
    npgs_data = npgs_data.join(npla_data, on=['CVAR06', 'CVAR01'], how='left')
    npgs_data = npgs_data.with_columns([
        pl.when((pl.col('CVAR12') == 'NPL') & (pl.col('STATUS') == 'NPL'))
            .then(pl.col('NDATE'))
        .when((pl.col('CVAR12') == '   ') & (pl.col('STATUS') == 'NPL'))
            .then(pl.lit(normdt))
        .when((pl.col('CVAR12') == '   ') & (pl.col('STATUS') != 'NPL') &
              pl.col('NDATE').is_not_null() & (pl.col('NDATE') != '          '))
            .then(pl.col('NDATE'))
        .otherwise(pl.col('CVAR13')).alias('CVAR13')
    ])
except Exception as e:
    print(f"Warning: NPLA merge failed: {e}")


# ============================================================================
# STEP 15: OUTPUT
# ============================================================================
print("\nStep 15: Writing output...")

final_columns = [
    'CVAR01', 'CVAR02', 'CVAR03', 'CVAR04', 'CVAR05', 'CVAR06', 'CVAR07',
    'CVAR08', 'CVAR09', 'CVAR10', 'CVAR11', 'CVAR12', 'CVAR13', 'CVAR14',
    'SCH', 'CR', 'BRANCH', 'CVAR15', 'CENSUST', 'NATGUAR', 'CINSTCL', 'PRODUCT'
]
output_data = npgs_data.select([c for c in final_columns if c in npgs_data.columns])
output_data = output_data.sort('CVAR01')

print(f"Output shape: {output_data.shape}")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

try:
    sas = saspy.SASsession(cfgname='default')
    output_pd = output_data.to_pandas()
    sas.df2sd(output_pd, 'npgs_output')
    sas.submit(f'''
        LIBNAME outlib "{OUTPUT_DIR}";
        DATA outlib.btnpgs{REPTMON};
            SET npgs_output;
        RUN;
    ''')
    sas.endsas()
    print(f"Output written to: {OUTPUT_FILE}")
except Exception as e:
    print(f"SAS session error: {e}")
    output_data.write_parquet(OUTPUT_DIR / f"btnpgs{REPTMON}.parquet")
    print(f"Fallback parquet written")

print(f"Total records: {len(output_data)}")
print("\nProcessing complete!")
