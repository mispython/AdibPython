import pyreadstat
import pandas as pd

# Load both
lnnote, _ = pyreadstat.read_sas7bdat(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m09.sas7bdat",
    usecols=['ACCTNO', 'LOANTYPE', 'NTBRCH', 'BALANCE', 'BORSTAT'],
)
lnnote.columns = lnnote.columns.str.lower()
lnnote['lt'] = pd.to_numeric(lnnote['loantype'], errors='coerce')

lt, _ = pyreadstat.read_sas7bdat(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/loantemp_iccd2.sas7bdat",
    usecols=['ACCTNO', 'ARREAR', 'PRODUCT', 'BRANCH'],
)
lt.columns = lt.columns.str.lower()

# Normalize ACCTNO on both sides
def norm(x):
    if pd.isna(x): return None
    try: return str(int(float(x)))
    except: return str(x).strip()

lnnote['acctno_s'] = lnnote['acctno'].apply(norm)
lt['acctno_s'] = lt['acctno'].apply(norm)

# Filter LNNOTE to HP
hp = lnnote[
    lnnote['lt'].isin([128,130,131,132,380,381,700,705,720,725,983,993,996,678,679,698,699])
    & (lnnote['balance'] > 0)
    & (~lnnote['borstat'].isin(['F','I','R']))
].copy()
print(f"LNNOTE filtered: {len(hp):,}")

# How many of those ACCTNOs exist in LOANTEMP?
hp_acctnos = set(hp['acctno_s'].dropna())
lt_acctnos = set(lt['acctno_s'].dropna())
print(f"LNNOTE filtered ACCTNOs unique: {len(hp_acctnos):,}")
print(f"LOANTEMP unique ACCTNOs:        {len(lt_acctnos):,}")
print(f"Overlap:                        {len(hp_acctnos & lt_acctnos):,}")

# Merge and show product distribution where arrear >= 10
merged = hp.merge(lt[['acctno_s', 'arrear', 'product', 'branch']], on='acctno_s', how='left')
arr10 = merged[merged['arrear'] >= 10]
print(f"\nAfter merge, arrear >= 10: {len(arr10):,}")
print(f"By LNNOTE loantype:")
print(arr10['lt'].value_counts().to_string())
print(f"\nBy LOANTEMP product:")
print(arr10['product'].value_counts().head(10).to_string())
print(f"\nBy branch:")
print(arr10['branch'].value_counts().head(10).to_string())
