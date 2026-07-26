# ============================================================
# JOB NAME : EIBNMMFR (Python)
# INPUT    : Binary datasets
# OUTPUT   : Parquet
# PURPOSE  : Replace JCL + SAS job
# ============================================================

import pandas as pd
import pickle
from datetime import date, timedelta
from ftplib import FTP
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
# 2. INPUT BINARY FILE PATHS
# (sas dataset)
# ============================================================

PBB_ALM_CR_BIN   = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBNMMFR/pbb_alm_cr.sas7bdat"
PBB_MAST_BR_BIN  = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBNMMFR/pbb_mast_br.sas7bdat"
PIBB_ALM_CR_BIN  = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBNMMFR/pibb_alm_cr.sas7bdat"
PIBB_MAST_BR_BIN = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBNMMFR/pibb_mast_br.sas7bdat"

# ============================================================
# 3. LOAD BINARY INPUT
# ============================================================

def load_binary(path):
    with open(path, "rb") as f:
        return pickle.load(f)

pbb_alm_cr   = load_binary(PBB_ALM_CR_BIN)
pbb_mast_br  = load_binary(PBB_MAST_BR_BIN)
pibb_alm_cr  = load_binary(PIBB_ALM_CR_BIN)
pibb_mast_br = load_binary(PIBB_MAST_BR_BIN)

# ============================================================
# 4. FILTER + PREPARE DATA (SAS DATA STEP)
# ============================================================

VALID_PRODESC = [
    "BILLS RETAIL",
    "TOTAL COMMERCIAL RETAILS"
]

def prepare_df(df1, df2):
    df = pd.concat([df1, df2], ignore_index=True)
    df = df[df["PRODESC"].isin(VALID_PRODESC)].copy()
    df["REPTDATE"] = pd.to_datetime(REPTDATE)
    return df

pbb  = prepare_df(pbb_alm_cr,  pbb_mast_br)
pibb = prepare_df(pibb_alm_cr, pibb_mast_br)

# ============================================================
# 5. COMBINE PBB + PIBB
# ============================================================

crl = pd.concat([pbb, pibb], ignore_index=True)

crl = crl[
    ["ACCTNO", "NOTENO", "PRODESC", "REPTDATE"]
]

# ============================================================
# 6. WRITE OUTPUT AS PARQUET
# (SAS: SAP.MTH.MFRS.BNM01.DTLFTP)
# ============================================================

OUTPUT_PARQUET = "SAP.MTH.MFRS.BNM01.DTLFTP.parquet"

crl.to_parquet(
    OUTPUT_PARQUET,
    engine="pyarrow",
    compression="snappy",
    index=False
)



# ============================================================
# END OF JOB
# ============================================================

print("EIBNMMFR job completed successfully (Binary → Parquet).")


input is in sas7bdat, can use pyreadstat. output in sas7bdat and parquet
