import pyreadstat
df, meta = pyreadstat.read_sas7bdat("/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/pba01260803.sas7bdat")
print(df.columns.tolist())
