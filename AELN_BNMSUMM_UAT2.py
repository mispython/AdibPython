Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/EGOLD/EIBDEGLD.py", line 20, in <module>
    REPTDATE = datetime.strptime(str(REPTDATE_value), "%Y-%m-%d")
  File "/usr/lib64/python3.9/_strptime.py", line 568, in _strptime_datetime
    tt, fraction, gmtoff_fraction = _strptime(data_string, format)
  File "/usr/lib64/python3.9/_strptime.py", line 349, in _strptime
    raise ValueError("time data %r does not match format %r" %
ValueError: time data '24084.0' does not match format '%Y-%m-%d'
