import pyreadstat
import pandas as pd

# 1. What does a raw REPOTXT line look like byte-for-byte?
with open('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/REPOTXT_sample.txt', 'rb') as f:
    for i, line in enumerate(f):
        stripped = line.rstrip(b'\r\n')
        print(f"Line {i}: len={len(stripped)}  repr={line!r}")
        if i >= 2:
            break

# 2. Check the production SAS source:
#    grep for 'FILE REPOTXT' in the SAS .sas file and paste it here.

# 3. What does NAME8 look like for a sample account?
name8, _ = pyreadstat.read_sas7bdat(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/stg_ln_nam8file.sas7bdat",
    usecols=['ACCTNO', 'LINETHRE', 'LINEFOUR'],
)
name8.columns = name8.columns.str.lower()
print("\nNAME8 columns:", name8.columns.tolist())
print("NAME8 sample rows:")
print(name8.head(5))
