Reading DPTRBL Parquet file...
  Loaded 11,857,406 records from DPTRBL
  Columns: ['BANKNO', 'REPTNO', 'FMTCODE', 'BRANCH', 'ACCTNO', 'NAME', 'TAXNO', 'DEBIT', 'CREDIT', 'CLOSEDT']...
Error reading DPTRBL Parquet: unconverted data remains: 165
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDDEPE.py", line 87, in <module>
    reptdate = datetime.strptime(str(int(tbdate_val)), '%Y%m%d').date()
  File "/usr/lib64/python3.9/_strptime.py", line 568, in _strptime_datetime
    tt, fraction, gmtoff_fraction = _strptime(data_string, format)
  File "/usr/lib64/python3.9/_strptime.py", line 352, in _strptime
    raise ValueError("unconverted data remains: %s" %
ValueError: unconverted data remains: 165
