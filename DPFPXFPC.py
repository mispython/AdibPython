import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

# =========================
# CONFIG (PARQUET INPUTS)
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

# =========================
# STEP 1: REPORT DATE
# =========================
reptdate = pd.to_datetime(REPTDATE_DF['REPTDATE'].iloc[0])

REPTDAY  = reptdate.day
REPTMON  = reptdate.month
REPTYEAR = reptdate.year
SDATE    = reptdate.toordinal()

# =========================
# STEP 2: CURRENT → CA
# =========================
ca = CURRENT_DF.copy()

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

# =========================
# STEP 3A: LIMIT
# =========================
def convert_lmtstart(x):
    if pd.isna(x) or x <= 0:
        return pd.NaT
    s = str(int(x)).zfill(11)[:8]
    return datetime.strptime(s, "%m%d%Y")

limit_df = LIMIT_DF.copy()
limit_df['LMTSTART'] = limit_df['LMTSTART'].apply(convert_lmtstart)
limit_df = limit_df[['ACCTNO','LMTSTART']].drop_duplicates()

ca = ca.merge(limit_df, on='ACCTNO', how='left')

# =========================
# STEP 3B: GP3 (FIXED WIDTH)
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
# STEP 3C: CISDP
# =========================
cisdp = CISDP_DF[CISDP_DF['SECCUST'] == '901'][['ACCTNO','NEWIC','CUSTNAME']]
cisdp = cisdp.drop_duplicates()

ca = ca.merge(cisdp, on='ACCTNO', how='left')

# =========================
# STEP 3D: COLL + DESC
# =========================
coll = pd.read_fwf(
    COLL_FILE,
    colspecs=[(3,9),(145,151)],
    names=['CCOLLNO','ACCTNO']
)

desc = pd.read_fwf(
    DESC_FILE,
    colspecs=[(0,11),(50,52),(54,56),(210,220)],
    names=['CCOLLNO','CINSTCL','NATGUAR','CENSUS']
)

def map_cr(census):
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
# STEP 3E: MICR
# =========================
micr = pd.read_fwf(
    MICR_FILE,
    colspecs=[(0,3),(39,44)],
    names=['BRANCH','MICRCD']
)

dep = dep.merge(micr, on='BRANCH', how='left')

# =========================
# STEP 4: ARREARS + NPL
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
# STEP 5: CVAR02
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
# STEP 6: OUTPUT STRUCTURE
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
# STEP 7: HISTORY MERGE
# =========================
npgs = dep.merge(NPLA_DF, on=['CVAR06','CVAR01'], how='left')

npgs.loc[
    (npgs['CVAR12']=='NPL') & (npgs['STATUS']=='NPL'),
    'CVAR13'
] = npgs['NDATE']

# =========================
# OUTPUT (PARQUET)
# =========================
output_file = f"DPNPGS_{REPTMON:02d}.parquet"
npgs.to_parquet(output_file, index=False)

print(f"Output written: {output_file}")


for CURRENT nad OVERDRAFT dataset, need to add filter "WHERE ENTITY_CD != 'PIBB'" (conventional) in order to read the dataset
all inputs are in sas7bdat sas dataset.
use pyreadstat to read.
remove reptdate, use datetime timedelta - 1 instead. 
output in sas7bdat. 
write out using saspy
