Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/EIBDFDHQ_DAILY_FD_HQ_REPORT.py", line 37, in <module>
    reptdate = dt.datetime.strptime(str(int(tbd)), "%Y%m%d")
  File "/usr/lib64/python3.9/_strptime.py", line 568, in _strptime_datetime
    tt, fraction, gmtoff_fraction = _strptime(data_string, format)
  File "/usr/lib64/python3.9/_strptime.py", line 352, in _strptime
    raise ValueError("unconverted data remains: %s" %
ValueError: unconverted data remains: 5343
