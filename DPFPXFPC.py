import pyreadstat

path = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/loan0904.sas7bdat"

# metadata only — no data load
df, meta = pyreadstat.read_sas7bdat(path, metadataonly=True)
print("Columns:", [c.lower() for c in meta.column_names])
print("Num rows:", meta.number_rows)
