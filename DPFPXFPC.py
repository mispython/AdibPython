import pyreadstat
from pathlib import Path

filepath = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMBRAS/pbb/lnnote.sas7bdat")

# 1. Metadata - value label catalog attached directly to the sas7bdat, if any
_, meta = pyreadstat.read_sas7bdat(str(filepath), metadataonly=True)

print("variable_to_label (format name attached to LOANTYPE):",
      meta.variable_to_label.get('LOANTYPE') if hasattr(meta, 'variable_to_label') else 'n/a')
print("variable_value_labels['LOANTYPE']:", meta.variable_value_labels.get('LOANTYPE'))

# 2. If the block above is empty, the labels likely live in a separate
#    catalog file (commonly named the same as the library, e.g. FORMATS.sas7bcat,
#    sitting alongside the .sas7bdat). List the directory to check.
print("\nFiles alongside lnnote.sas7bdat:")
for f in filepath.parent.iterdir():
    print(" ", f.name)

# 3. Raw sample of LOANTYPE values regardless, so we can see the actual codes in use
df_sample, _ = pyreadstat.read_sas7bdat(str(filepath), usecols=['LOANTYPE'], row_limit=5000)
print("\nDistinct LOANTYPE codes seen in first 5000 rows:")
print(sorted(df_sample['LOANTYPE'].dropna().unique().tolist()))
