import pyreadstat
_, meta = pyreadstat.read_sas7bdat(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP01/loan104.sas7bdat",
    metadataonly=True,
)
for c in meta.column_names:
    if 'iss' in c.lower() or 'date' in c.lower() or 'dte' in c.lower():
        print(c)
