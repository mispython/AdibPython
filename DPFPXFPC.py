import pyreadstat
import pandas as pd

lnnote, _ = pyreadstat.read_sas7bdat(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m09.sas7bdat",
    usecols=['LOANTYPE', 'BALANCE', 'BORSTAT'],
)
lnnote.columns = lnnote.columns.str.lower()
lnnote['lt'] = pd.to_numeric(lnnote['loantype'], errors='coerce')

hp = lnnote[lnnote['lt'].isin([128,130,131,132,380,381,700,705,720,725,983,993,996,678,679,698,699])]

print("HP portfolio breakdown:")
print(hp['lt'].value_counts().sort_index().to_string())
