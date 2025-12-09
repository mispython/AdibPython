Today: 2025-12-09
Processing date: 2025-12-08 (yesterday)
Report month: 12, Report year: 25

CIS file: /host/cis/parquet/year=2025/month=12/day=08/CUST_GROUPING_OUT.parquet
RCIS file: /host/cis/parquet/year=2025/month=12/day=08/CUST_GROUPING_RPTCC.parquet

Reading ICISBASEL data from: /host/cis/parquet/year=2025/month=12/day=08/CUST_GROUPING_OUT.parquet
Data types before conversion:
  GRPING: String
  GROUPNO: String
  CUSTNO: String
  FULLNAME: String
  ACCTNO: String
  NOTENO: String
  PRODUCT: String
  AMTINDC: String
  BALAMT: String
  TOTAMT: Float64
  RLENCODE: String
  PRIMSEC: String
Traceback (most recent call last):
  File "/pythonITD/mis_dev/sas_migration/CIGR/EIIWCIGR_ISLAMIC.py", line 39, in <module>
    pl.when(pl.col("BALAMT").dtype == pl.Utf8)
AttributeError: 'Expr' object has no attribute 'dtype'
