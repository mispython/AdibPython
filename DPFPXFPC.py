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
  Total records read: 2243126
  Records parsed successfully: 2243123
  Parse errors: 0

COLL rows parsed: 2243123
Sample COLL records:
shape: (10, 3)
┌─────────┬────────────┬────────────┐
│ CCOLLNO ┆ ACCTNO     ┆ NOTENO     │
│ ---     ┆ ---        ┆ ---        │
│ i64     ┆ i64        ┆ i64        │
╞═════════╪════════════╪════════════╡
│ 133     ┆ 3078959107 ┆ 3078959107 │
│ 943     ┆ 3078848615 ┆ 3078848615 │
│ 1297    ┆ 3093159115 ┆ 3093159115 │
│ 1701    ┆ 3077416629 ┆ 3077416629 │
│ 1719    ┆ 3077416629 ┆ 3077416629 │
│ 1727    ┆ 3077416629 ┆ 3077416629 │
│ 1750    ┆ 3077416629 ┆ 3077416629 │
│ 1776    ┆ 3077416629 ┆ 3077416629 │
│ 1784    ┆ 3077416629 ┆ 3077416629 │
│ 2469    ┆ 3094562608 ┆ 3094562608 │
└─────────┴────────────┴────────────┘

============================================================
STEP 7: Reading DESC file (EBCDIC, FB 3050)
============================================================
  File: LCCRISEX_DESC_20260831
  File size: 4962984400 bytes
  Record length: 3050 bytes
  Expected records: 1627208
  Total records read: 1627208
  Records parsed successfully: 2395
  Parse errors: 0

DESC rows parsed: 2395
Sample DESC records:
shape: (10, 7)
┌────────────┬─────────┬─────────┬────────┬─────────────┬─────────┬─────┐
│ CCOLLNO    ┆ CINSTCL ┆ NATGUAR ┆ CGCGUR ┆ CENSUS      ┆ TRANCHE ┆ SCH │
│ ---        ┆ ---     ┆ ---     ┆ ---    ┆ ---         ┆ ---     ┆ --- │
│ f64        ┆ str     ┆ str     ┆ str    ┆ f64         ┆ str     ┆ str │
╞════════════╪═════════╪═════════╪════════╪═════════════╪═════════╪═════╡
│ 6.454177e6 ┆ 18      ┆ 06      ┆ 080    ┆ 1.0006e9    ┆         ┆ 7Q  │
│ 7.076854e6 ┆ 18      ┆ 06      ┆ 080    ┆ 6.3003682e7 ┆         ┆ 7Q  │
│ 7.400641e6 ┆ 18      ┆ 06      ┆ 080    ┆ 7.2000316e7 ┆         ┆ 7Q  │
│ 8.242505e6 ┆ 18      ┆ 06      ┆ 080    ┆ 1.0005e9    ┆         ┆ 7Q  │
│ 8.938789e6 ┆ 18      ┆ 06      ┆ 080    ┆ 1.0005e9    ┆         ┆ 7Q  │
│ 8.938888e6 ┆ 18      ┆ 06      ┆ 080    ┆ 1.0005e9    ┆         ┆ 7Q  │
│ 9.134453e6 ┆ 18      ┆ 02      ┆ 080    ┆ 1.0005e9    ┆         ┆ 7Q  │
│ 9.134461e6 ┆ 18      ┆ 02      ┆ 080    ┆ 1.0005e9    ┆         ┆ 7Q  │
│ 9.134503e6 ┆ 18      ┆ 02      ┆ 080    ┆ 1.0005e9    ┆         ┆ 7Q  │
│ 9.134842e6 ┆ 18      ┆ 02      ┆ 080    ┆ 1.0005e9    ┆         ┆ 7Q  │
└────────────┴─────────┴─────────┴────────┴─────────────┴─────────┴─────┘

============================================================
STEP 8: Merging COLL and DESC
============================================================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBLTRRF.py", line 592, in <module>
    coll = coll.join(desc, on="CCOLLNO", how="inner")
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
polars.exceptions.SchemaError: datatypes of join keys don't match - `CCOLLNO`: i64 on left does not match `CCOLLNO`: f64 on right (and no other type was available to cast to)
