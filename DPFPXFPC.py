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

BTRSA_MAST_FILE = INPUT_ROOT / "EIBTNPGS" / f"mast{REPTDAY}{REPTMON}.sas7bdat"
BTRSA_CRED_FILE = INPUT_ROOT / "EIBTNPGS" / f"cred{REPTDAY}{REPTMON}.sas7bdat"
BTRSA_PROV_FILE = INPUT_ROOT / "EIBTNPGS" / f"prov{REPTDAY}{REPTMON}.sas7bdat"
BTRSA_SUBA_FILE = INPUT_ROOT / "EIBTNPGS" / f"suba{REPTDAY}{REPTMON}.sas7bdat"
NPLA_FILE = INPUT_ROOT / "EIBTNPGS" / "npla.sas7bdat"

coll_candidates = sorted(glob.glob(str(INPUT_ROOT / "EIBRCGCS" / "LCCRISEX_*")))
coll_candidates = [f for f in coll_candidates if 'DESC' not in Path(f).name]
desc_candidates = sorted(glob.glob(str(INPUT_ROOT / "EIBRCGCS" / "LCCRISEX_DESC_*")))

COLL_FILE = Path(coll_candidates[-1]) if coll_candidates else None
DESC_FILE = Path(desc_candidates[-1]) if desc_candidates else None

print(f"COLL_FILE: {COLL_FILE}")
print(f"DESC_FILE: {DESC_FILE}")


# ============================================================================
# STEP 1b: CONVERT EBCDIC FILES TO SAS7BDAT USING SAS
# ============================================================================
print("\nStep 1b: Converting EBCDIC files to sas7bdat via SAS...")

COLL_SAS7BDAT = Path("/tmp") / "coll_converted.sas7bdat"
DESC_SAS7BDAT = Path("/tmp") / "desc_converted.sas7bdat"

# Only convert if not already cached
if not COLL_SAS7BDAT.exists() or not DESC_SAS7BDAT.exists():
    sas = saspy.SASsession(cfgname='default')

    convert_code = f'''
    /* Inspect the EBCDIC file structure first */
    filename rawfile "{COLL_FILE}" recfm=f lrecl=400;
    data _null_;
        infile rawfile obs=1;
        input @1 line $char400.;
        put "First record length: " +(-1) length(line);
        put "First 100 chars: " line $char100.;
    run;
    '''

    # First, let's have SAS determine the RECFM and LRECL
    # Most SAS-generated EBCDIC files are RECFM=F with fixed LRECL
    # Try common lengths

    # Actually, let's just do a PROC CONTENTS or use SAS to read with a guessed LRECL
    # Most likely these are RECFM=FB with LRECL=400 (matching DESC)
    # or perhaps VB (variable blocked)

    convert_code = f'''
    /* Try reading as variable-length records (RDW) first */
    filename raw1 "{COLL_FILE}" recfm=vb lrecl=32760;
    data coll_work;
        infile raw1 obs=1000000;
        length rec $400;
        input rec $char400.;
    run;

    proc contents data=coll_work; run;
    '''

    result = sas.submit(convert_code)
    print("SAS inspection result:")
    print(result['LOG'][-3000:])

    sas.endsas()
    raise SystemExit("Stopped for inspection - check SAS log above to determine correct RECFM/LRECL")


# ============================================================================
# STEP 2: PROCESS CRFTABL
# ============================================================================
print("\nStep 2: Processing credit facility table...")

with open(CRFTABL_FILE, 'r') as f:
    raw_lines = [line.rstrip('\n') for line in f]

data_lines = [ln for ln in raw_lines if len(ln) > 380 and ln[0] != '1']
crft_data = pl.DataFrame({'data': data_lines})

crft_data = crft_data.with_columns([
    pl.col('data').str.slice(0, 1).alias('RECTYP1'),
    pl.col('data').str.slice(2, 4).str.strip_chars().alias('BRANCH'),  # 2-char branch
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

print(f"CRFTABL rows: {len(crft_data)}")
print(f"Sample SUBACCT: {crft_data['SUBACCT'].head(10).to_list()}")
print(f"SUBACCT lengths: {crft_data.with_columns(pl.col('SUBACCT').str.len_chars().alias('L'))['L'].unique().to_list()}")

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
mast_data = pl.from_pandas(mast_df).select(['ACCTNO', 'FICODE', 'NAME', 'BUSREGN'])
mast_data = mast_data.with_columns([
    pl.col('ACCTNO').cast(pl.Int64, strict=False).alias('ACCTNO'),
]).unique(subset=['ACCTNO'], keep='first')

crft_merged = crft_data.join(
    mast_data.select(['ACCTNO', 'NAME', 'BUSREGN']),
    on='ACCTNO', how='inner'
)

crft_final = crft_merged.select([
    'BRANCH', 'ACCTNO', 'SUBACCT', 'NAME', 'BUSREGN', 'CENSUST', 'TFID', 'SCH'
]).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

# CRFT1: FAC + first char of SUBACCT
crft1_data = crft_merged.with_columns([
    (pl.lit('FAC') + pl.col('SUBACCT').str.slice(0, 1)).alias('SUBACCT')
]).select([
    'BRANCH', 'ACCTNO', 'SUBACCT', 'NAME', 'BUSREGN', 'CENSUST', 'TFID', 'SCH'
]).unique(subset=['ACCTNO', 'SUBACCT'], keep='first')

print(f"CRFT_FINAL: {len(crft_final)}, CRFT1: {len(crft1_data)}")


# ============================================================================
# STEP 4: DEBUG CRED SUBACCT FORMAT BEFORE JOINING
# ============================================================================
print("\nStep 4: Debugging CRED SUBACCT format...")

cred_df, _ = pyreadstat.read_sas7bdat(BTRSA_CRED_FILE)
cred_data = pl.from_pandas(cred_df)

print(f"CRED columns: {cred_data.columns}")
print(f"CRED sample SUBACCT: {cred_data['SUBACCT'].head(10).to_list()}")
print(f"CRED SUBACCT lengths: {cred_data.with_columns(pl.col('SUBACCT').str.len_chars().alias('L'))['L'].unique().to_list()}")
print(f"CRED sample ACCTNO: {cred_data['ACCTNO'].head(5).to_list()}")
print(f"CRED ACCTNO dtype: {cred_data['ACCTNO'].dtype}")
print(f"CRFT sample ACCTNO: {crft_final['ACCTNO'].head(5).to_list()}")
print(f"CRFT sample SUBACCT: {crft_final['SUBACCT'].head(10).to_list()}")

# Check for any overlap
common_acct = set(cred_data['ACCTNO'].drop_nulls().cast(pl.Int64).unique().to_list()) & \
              set(crft_final['ACCTNO'].unique().to_list())
print(f"Common ACCTNO count: {len(common_acct)}")

if len(common_acct) > 0:
    sample_acct = list(common_acct)[:5]
    print(f"Sample common accounts: {sample_acct}")
    print(f"CRED rows for those: {cred_data.filter(pl.col('ACCTNO').cast(pl.Int64).is_in(sample_acct)).select(['ACCTNO', 'SUBACCT']).head(20)}")
    print(f"CRFT rows for those: {crft_final.filter(pl.col('ACCTNO').is_in(sample_acct)).select(['ACCTNO', 'SUBACCT']).head(20)}")

raise SystemExit("Stopped for CRED/CRFT join debugging")


# ... rest of script unchanged until we fix the joins ...
