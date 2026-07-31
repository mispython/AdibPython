import pyreadstat
from pathlib import Path

filepath = Path("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMBRAS/pbb/lnnote.sas7bdat")

# 1. Metadata only - tells us the SAS format attached to ISSUEDT (if any),
#    and the raw storage type pyreadstat detected.
_, meta = pyreadstat.read_sas7bdat(str(filepath), metadataonly=True)

print("Column names:", meta.column_names[:20])
print("readstat_variable_types['ISSUEDT']:", meta.readstat_variable_types.get('ISSUEDT'))
print("original_variable_types['ISSUEDT']:", meta.original_variable_types.get('ISSUEDT'))
print("variable_format['ISSUEDT']:", meta.variable_format.get('ISSUEDT') if hasattr(meta, 'variable_format') else meta.__dict__.get('variable_format'))

# 2. Pull a tiny real sample (first 20 rows) of the raw column so we can see
#    the actual values and python dtype pyreadstat hands back.
df_sample, _ = pyreadstat.read_sas7bdat(str(filepath), usecols=['ISSUEDT'], row_limit=20)
print("\nSample ISSUEDT values and dtype:")
print(df_sample['ISSUEDT'].dtype)
print(df_sample['ISSUEDT'].to_list())
