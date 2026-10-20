import pyreadstat
from datetime import date, timedelta

df, _ = pyreadstat.read_sas7bdat(
    '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m09.sas7bdat',
    usecols=['ACCTNO', 'ISSUEDT'],
    row_limit=10,
)
for _, r in df.iterrows():
    n = int(r['ISSUEDT'])
    print(f"ACCTNO={r['ACCTNO']:>15}  ISSUEDT={n:>8}  "
          f"+1960={date(1960,1,1)+timedelta(days=n)}  "
          f"+1960-9d={date(1960,1,1)+timedelta(days=n-9)}")
