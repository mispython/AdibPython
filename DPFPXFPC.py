import pyreadstat
df, meta = pyreadstat.read_sas7bdat(
    "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note_m10.sas7bdat",
    usecols=['ISSUEDT', 'LASTTRAN', 'PAIDIND'],
    row_limit=20,
)
print(df[['ISSUEDT', 'LASTTRAN', 'PAIDIND']].head(20))
print(df.dtypes)
print(meta.original_variable_types.get('ISSUEDT'),
      meta.original_variable_types.get('LASTTRAN'))
