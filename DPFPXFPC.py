import pyreadstat
import pandas as pd

lt, _ = pyreadstat.read_sas7bdat(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/loantemp_iccd2.sas7bdat",
)
lt.columns = lt.columns.str.lower()
print("LOANTEMP shape:", lt.shape)
print("LOANTEMP columns:", lt.columns.tolist())
print("\nFirst 5 ACCTNOs:", lt['acctno'].head().tolist())

# Look for KKI-type accounts (branch 117)
# These won't have a branch column in LOANTEMP — but we can check overlap
# with LNNOTE accounts that ARE branch 117
