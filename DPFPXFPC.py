import pyreadstat

path = "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/loan0904.sas7bdat"

# metadata only — no data load
df, meta = pyreadstat.read_sas7bdat(path, metadataonly=True)
print("Columns:", [c.lower() for c in meta.column_names])
print("Num rows:", meta.number_rows)

df, meta = pyreadstat.read_sas7bdat(path, row_limit=10)
df.columns = [c.lower() for c in df.columns]
print(df)
print("Unique 'product':", df['product'].unique() if 'product' in df.columns else "n/a")
print("Unique 'prodcd' :", df['prodcd'].unique()  if 'prodcd'  in df.columns else "n/a")

