REPORT ID : EIBDLNSA
Report Date: 12/09/2026
Prev   Date: 11/09/2026
Decoding NFEEFILE: /host_pq/dwh/input/LOAN/NFEEFILE_20260912
  ... 1,000,000 records
  ... 2,000,000 records
  ... 3,000,000 records
  ... 4,000,000 records
  ... 5,000,000 records
  ... 6,000,000 records
  ... 7,000,000 records
  ... 8,000,000 records
  ... 9,000,000 records
  ... 10,000,000 records
  ... 11,000,000 records
  ... 12,000,000 records
  ... 13,000,000 records
2                                                          The SAS System                           16:11 Sunday, September 13, 2026

  ... 14,000,000 records
  ... 15,000,000 records
  ... 16,000,000 records
  ... 17,000,000 records
  ... 18,000,000 records
  ... 19,000,000 records
  ... 20,000,000 records
  ... 21,000,000 records
  ... 22,000,000 records
  ... 23,000,000 records
  ... 24,000,000 records
  ... 25,000,000 records
  ... 26,000,000 records
  ... 27,000,000 records
  ... 28,000,000 records
  ... 29,000,000 records
  ... 30,000,000 records
  ... 31,000,000 records
  ... 32,000,000 records
  ... 33,000,000 records
  ... 34,000,000 records
  ... 35,000,000 records
  ... 36,000,000 records
  ... 37,000,000 records
  ... 38,000,000 records
  ... 39,000,000 records
  ... 40,000,000 records
  ... 41,000,000 records
  ... 42,000,000 records
  ... 43,000,000 records
  ... 44,000,000 records
  ... 45,000,000 records
  ... 46,000,000 records
  ... 47,000,000 records
  ... 48,000,000 records
  ... 49,000,000 records
  ... 50,000,000 records
  ... 51,000,000 records
  ... 52,000,000 records
  ... 53,000,000 records
  502 rows kept -> /stgsrcsys/host/holding/NFEEFILE_20260912.parquet
Decoding ACCTFILE: /host_pq/dwh/input/LOAN/ACCTFILE_20260912
  ... 500,000 records
  ... 1,000,000 records
  ... 1,500,000 records
  ... 2,000,000 records
  ... 2,500,000 records
  ... 3,000,000 records
  ... 3,500,000 records
  ... 4,000,000 records
  ... 4,500,000 records
  ... 5,000,000 records
  ... 5,500,000 records
  ... 6,000,000 records
  ... 6,500,000 records
  ... 7,000,000 records
  ... 7,500,000 records
  ... 8,000,000 records
3                                                          The SAS System                           16:11 Sunday, September 13, 2026

  249,984 rows kept -> /stgsrcsys/host/holding/ACCTFILE_20260912.parquet
  376 branch rows -> /stgsrcsys/host/holding/LKP_BRANCH.parquet
Traceback (most recent call last):
  File "/stgsrcsys/host/uat/python/EIBDLNS2.py", line 362, in <module>
    today_df   = con.execute("SELECT * FROM loan_summary").to_df()
AttributeError: '_duckdb.DuckDBPyConnection' object has no attribute 'to_df'
