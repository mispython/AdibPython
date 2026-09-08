Report Date: 2026-08-31
Normalization Date: 31/08/2026
Reading LOAN/LNNOTE datasets in chunks...
Reading Islamic LNNOTE (ENTITY_CD = 'PIBB')...
  Islamic LNNOTE rows: 6
Reading Conventional LNNOTE (ENTITY_CD != 'PIBB')...
  Conventional LNNOTE rows: 99994
Combining LNNOTE datasets...
  LOAN0 rows: 6
  LOAN1 rows: 0
Reading COMM datasets in chunks...
Reading Islamic LNCOMM...
  Islamic LNCOMM rows: 1066036
Reading Conventional LNCOMM...
  Conventional LNCOMM rows: 1066036
Warning: INTAMT column not found. Using CORGAMT as NETPROC.
Total LOAN rows after merge: 6
Calculating ISSUED, NODAYS, ARREARS, NPLDATE...
Applying NDAYS format...
LOAN rows after deduplication: 6
Processing CISLN in chunks...
  CISLN rows after filter: 63752
Processing COLL and DESC files...

Reading COLL file...
COLL rows: 5394860
Reading DESC file as line-delimited text...
DESC rows: 58608

=== DESC Data Sample (first 5 rows) ===
shape: (5, 5)
┌─────────┬─────────┬─────────┬────────┬─────────┐
│ ccollno ┆ cinstcl ┆ natguar ┆ census ┆ tranche │
│ ---     ┆ ---     ┆ ---     ┆ ---    ┆ ---     │
│ f64     ┆ str     ┆ str     ┆ f64    ┆ str     │
╞═════════╪═════════╪═════════╪════════╪═════════╡
│ 133.0   ┆ 29      ┆         ┆ null   ┆         │
│ null    ┆         ┆         ┆ null   ┆         │
│ null    ┆ 15      ┆         ┆ null   ┆ 37634   │
│ null    ┆         ┆         ┆ null   ┆         │
│ null    ┆         ┆         ┆ 1.0    ┆         │
└─────────┴─────────┴─────────┴────────┴─────────┘

Unique CINSTCL values (first 20): ['1', 'UG', '3B', 'OP', '22', 'A)', '15', 'DO', 'MP', 'HI', 'ID', 'IH', 'RI', 'KH', '0U', 'VA', 'T8', 'OS', '23', '6\x00']
Unique NATGUAR values (first 20): ['90', '26', 'R', 'EB', 'S', 'SB', 'EW', '2A', 'AS', 'RL', 'EM', 'IO', 'DR', 'EA', 'F', 'ZA', '\x00', 'PH', 'HJ', '10']
Rows with CINSTCL='18': 84
Rows with NATGUAR='06': 0

COLL rows after join: 3293031
COLL rows after filter: 0

Final COLL rows: 0
NPGS rows after COLL merge: 0
Processing MICR file...
  MICR rows: 300
Creating CVAR fields...
Writing NPGS.LNSMEZ08...
Using SAS Config named: default
SAS Connection established. Subprocess id is 422933

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Successfully wrote NPGS.LNSMEZ08 to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLSMEZ
SAS Connection terminated. Subprocess id was 422933
