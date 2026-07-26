# ============================================================
# JOB NAME : EIBNMMFR (Python)
# INPUT    : SAS datasets (.sas7bdat)
# OUTPUT   : SAS dataset (.sas7bdat) and Parquet
# PURPOSE  : Replace JCL + SAS job
# ============================================================

import pandas as pd
import pyreadstat
from datetime import date, timedelta
import os

# ============================================================
# 1. REPORT DATE LOGIC (SAS REPTDATE)
# ============================================================

today = date.today()
first_of_this_month = today.replace(day=1)
REPTDATE = first_of_this_month - timedelta(days=1)

REPTYEAR = REPTDATE.strftime("%y")
REPTMON  = REPTDATE.strftime("%m")
REPTDAY  = REPTDATE.strftime("%d")
RDATE    = REPTDATE.strftime("%d%m%Y")

# ============================================================
# 2. INPUT SAS DATASET PATHS
# ============================================================

INPUT_BASE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBNMMFR"

PBB_ALM_CR_PATH   = f"{INPUT_BASE}/pbb_alm_cr.sas7bdat"
PBB_MAST_BR_PATH  = f"{INPUT_BASE}/pbb_mast_br.sas7bdat"
PIBB_ALM_CR_PATH  = f"{INPUT_BASE}/pibb_alm_cr.sas7bdat"
PIBB_MAST_BR_PATH = f"{INPUT_BASE}/pibb_mast_br.sas7bdat"

# ============================================================
# 3. OUTPUT PATHS
# ============================================================

OUTPUT_BASE = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/ioutput/EIBNMMFR"

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_BASE, exist_ok=True)

OUTPUT_SAS   = f"{OUTPUT_BASE}/SAP.MTH.MFRS.BNM01.DTLFTP.sas7bdat"
OUTPUT_PARQUET = f"{OUTPUT_BASE}/SAP.MTH.MFRS.BNM01.DTLFTP.parquet"

# ============================================================
# 4. LOAD SAS DATASETS WITH PYREADSTAT
# ============================================================

def load_sas(path):
    """Load SAS dataset using pyreadstat"""
    df, meta = pyreadstat.read_sas7bdat(path)
    return df

print("Loading SAS datasets...")
pbb_alm_cr   = load_sas(PBB_ALM_CR_PATH)
pbb_mast_br  = load_sas(PBB_MAST_BR_PATH)
pibb_alm_cr  = load_sas(PIBB_ALM_CR_PATH)
pibb_mast_br = load_sas(PIBB_MAST_BR_PATH)

# ============================================================
# 5. FILTER + PREPARE DATA (SAS DATA STEP)
# ============================================================

VALID_PRODESC = [
    "BILLS RETAIL",
    "TOTAL COMMERCIAL RETAILS"
]

def prepare_df(df1, df2):
    """Combine two dataframes, filter, and add REPTDATE"""
    df = pd.concat([df1, df2], ignore_index=True)
    df = df[df["PRODESC"].isin(VALID_PRODESC)].copy()
    df["REPTDATE"] = REPTDATE
    return df

print("Preparing PBB data...")
pbb = prepare_df(pbb_alm_cr, pbb_mast_br)

print("Preparing PIBB data...")
pibb = prepare_df(pibb_alm_cr, pibb_mast_br)

# ============================================================
# 6. COMBINE PBB + PIBB
# ============================================================

print("Combining datasets...")
crl = pd.concat([pbb, pibb], ignore_index=True)

# Keep only required columns
crl = crl[["ACCTNO", "NOTENO", "PRODESC", "REPTDATE"]]

# ============================================================
# 7. WRITE OUTPUT AS SAS DATASET USING SASPY
# ============================================================

try:
    import saspy
    print("Using saspy to write SAS dataset...")
    
    # Start SAS session
    sas = saspy.SASsession()
    
    # Convert datetime to SAS date (numeric days since 1960-01-01)
    sas_date = (REPTDATE - date(1960, 1, 1)).days
    crl['REPTDATE'] = sas_date
    
    # Write to SAS dataset
    sas.dataframe2sasdata(
        crl,
        table='SAP_MTH_MFRS_BNM01_DTLFTP',
        libref='WORK'
    )
    
    # Copy to output location using SAS
    sas.submit(f'''
        proc copy in=work out="{OUTPUT_BASE}";
            select SAP_MTH_MFRS_BNM01_DTLFTP;
        run;
    ''')
    
    # Rename to match expected format
    os.rename(
        f"{OUTPUT_BASE}/sap_mth_mfrs_bnm01_dtlfpt.sas7bdat",
        OUTPUT_SAS
    )
    
    sas.endsas()
    
except ImportError:
    print("saspy not available. Only Parquet output will be created.")
    print("To install saspy: pip install saspy")
    print("Note: saspy requires SAS installed on the system")

# ============================================================
# 8. WRITE OUTPUT AS PARQUET
# ============================================================

print(f"Writing Parquet output to: {OUTPUT_PARQUET}")
crl.to_parquet(
    OUTPUT_PARQUET,
    engine="pyarrow",
    compression="snappy",
    index=False
)

# ============================================================
# 9. PRINT SUMMARY
# ============================================================

print("\n" + "="*60)
print("EIBNMMFR job completed successfully!")
print("="*60)
print(f"Report Date    : {REPTDATE.strftime('%d-%b-%Y')}")
print(f"Total records  : {len(crl):,}")
if os.path.exists(OUTPUT_SAS):
    print(f"SAS output     : {OUTPUT_SAS}")
print(f"Parquet output : {OUTPUT_PARQUET}")
print("="*60)

# ============================================================
# END OF JOB
# ============================================================
