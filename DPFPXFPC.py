REPORT ID : EIBDLNSA
Report Date: 12/09/2026
Traceback (most recent call last):
  File "/stgsrcsys/host/uat/python/EIBDLNS2.py", line 85, in <module>
    con.execute(f"""
_duckdb.InvalidInputException: Invalid Input Error: CSV Error on Line: 1
Invalid unicode (byte sequence mismatch) detected. This file is not utf-8 encoded.

Possible Solution: Set the correct encoding, if available, to read this CSV File (e.g., encoding='UTF-16')
Possible Solution: Enable ignore errors (ignore_errors=true) to skip this row

  file = /host_pq/dwh/input/LOAN/NFEEFILE_20260912
  delimiter = \x01 (Set By User)
  quote = (empty) (Set By User)
  escape = (empty) (Auto-Detected)
  new_line = Single-Line File (Auto-Detected)
  header = false (Set By User)
2                                                          The SAS System                           16:11 Sunday, September 13, 2026

  skip_rows = 0 (Auto-Detected)
  comment = (empty) (Auto-Detected)
  strict_mode = true (Auto-Detected)
  date_format =  (Auto-Detected)
  timestamp_format =  (Auto-Detected)
  null_padding = 0
  sample_size = 20480
  ignore_errors = false
  all_varchar = 0
