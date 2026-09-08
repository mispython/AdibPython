============================================================
Processing date: 2026-08-31 (SAS date: 24349)
============================================================

============================================================
STEP 1: Reading LNNOTE (using LOANTYPE=570)
============================================================
Filtered LNNOTE rows: 122
LOAN0: 116 rows, LOAN1: 6 rows

============================================================
STEP 6: Reading COLL file (EBCDIC, FB 380)
============================================================
  File: LCCRISEX_20260831
  File size: 852387880 bytes
  Record length: 380 bytes
  Expected records: 2243126
  Processed 100000 records...
  Processed 200000 records...
  Processed 300000 records...
  Processed 400000 records...
  Processed 500000 records...
  Processed 600000 records...
  Processed 700000 records...
  Processed 800000 records...
  Processed 900000 records...
  Processed 1000000 records...
  Processed 1100000 records...
  Processed 1200000 records...
  Processed 1300000 records...
  Processed 1400000 records...
  Processed 1500000 records...
  Processed 1600000 records...
  Processed 1700000 records...
  Processed 1800000 records...
  Processed 1900000 records...
  Processed 2000000 records...
  Processed 2100000 records...
  Processed 2200000 records...
  Total records read: 2243126
  Records parsed: 2243126

COLL rows: 2243126
Sample: shape: (3, 3)
┌─────────┬────────────┬────────────┐
│ CCOLLNO ┆ ACCTNO     ┆ NOTENO     │
│ ---     ┆ ---        ┆ ---        │
│ i64     ┆ i64        ┆ i64        │
╞═════════╪════════════╪════════════╡
│ 133     ┆ 3078959107 ┆ 3078959107 │
│ 943     ┆ 3078848615 ┆ 3078848615 │
│ 1297    ┆ 3093159115 ┆ 3093159115 │
└─────────┴────────────┴────────────┘

============================================================
STEP 7: Reading DESC file (EBCDIC, FB 3050)
============================================================
  File: LCCRISEX_DESC_20260831
  File size: 4962984400 bytes
  Record length: 3050 bytes
  Expected records: 1627208
  Total records read: 1627208
  Records parsed: 2395

DESC rows: 2395
Sample: shape: (3, 7)
┌─────────┬─────────┬─────────┬────────┬─────────────┬─────────┬─────┐
│ CCOLLNO ┆ CINSTCL ┆ NATGUAR ┆ CGCGUR ┆ CENSUS      ┆ TRANCHE ┆ SCH │
│ ---     ┆ ---     ┆ ---     ┆ ---    ┆ ---         ┆ ---     ┆ --- │
│ i64     ┆ str     ┆ str     ┆ str    ┆ f64         ┆ str     ┆ str │
╞═════════╪═════════╪═════════╪════════╪═════════════╪═════════╪═════╡
│ 6454177 ┆ 18      ┆ 06      ┆ 080    ┆ 1.0006e9    ┆         ┆ 7Q  │
│ 7076854 ┆ 18      ┆ 06      ┆ 080    ┆ 6.3003682e7 ┆         ┆ 7Q  │
│ 7400641 ┆ 18      ┆ 06      ┆ 080    ┆ 7.2000316e7 ┆         ┆ 7Q  │
└─────────┴─────────┴─────────┴────────┴─────────────┴─────────┴─────┘

============================================================
STEP 8: Merging COLL and DESC
============================================================
COLL rows after merge: 1999
Sample: shape: (3, 9)
┌──────────┬────────────┬────────┬─────────┬───┬────────┬─────────────┬─────────┬─────┐
│ CCOLLNO  ┆ ACCTNO     ┆ NOTENO ┆ CINSTCL ┆ … ┆ CGCGUR ┆ CENSUS      ┆ TRANCHE ┆ SCH │
│ ---      ┆ ---        ┆ ---    ┆ ---     ┆   ┆ ---    ┆ ---         ┆ ---     ┆ --- │
│ i64      ┆ i64        ┆ i64    ┆ str     ┆   ┆ str    ┆ f64         ┆ str     ┆ str │
╞══════════╪════════════╪════════╪═════════╪═══╪════════╪═════════════╪═════════╪═════╡
│ 7400641  ┆ 2000355628 ┆ 17     ┆ 18      ┆ … ┆ 080    ┆ 7.2000316e7 ┆         ┆ 7Q  │
│ 7076854  ┆ 2057253718 ┆ 10     ┆ 18      ┆ … ┆ 080    ┆ 6.3003682e7 ┆         ┆ 7Q  │
│ 10078012 ┆ 2057778213 ┆ 13     ┆ 18      ┆ … ┆ 080    ┆ 1.0005e9    ┆         ┆ 7Q  │
└──────────┴────────────┴────────┴─────────┴───┴────────┴─────────────┴─────────┴─────┘

============================================================
STEP 9: Creating NPGS
============================================================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBLTRRF.py", line 520, in <module>
    npgs = loan1.join(coll, on=["ACCTNO", "NOTENO"], how="inner")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 128, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 8242, in join
    self.lazy()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.SchemaError: datatypes of join keys don't match - `ACCTNO`: f64 on left does not match `ACCTNO`: i64 on right (and no other type was available to cast to)
