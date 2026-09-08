Report Date: 2026-09-01
Normalization Date: 01/09/2026
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
COLL record length: 158
DESC file size: 4962984400
DESC record length: 84686

Reading COLL file...
COLL rows: 5394860
Reading DESC file...
DESC rows: 58604

=== DESC Data Sample (first 3 rows) ===
shape: (3, 5)
┌─────────┬─────────┬─────────┬────────┬─────────┐
│ ccollno ┆ cinstcl ┆ natguar ┆ census ┆ tranche │
│ ---     ┆ ---     ┆ ---     ┆ ---    ┆ ---     │
│ f64     ┆ str     ┆ str     ┆ f64    ┆ str     │
╞═════════╪═════════╪═════════╪════════╪═════════╡
│ 133.0   ┆ 29      ┆         ┆ null   ┆         │
│ null    ┆         ┆         ┆ null   ┆         │
│ null    ┆         ┆         ┆ null   ┆         │
└─────────┴─────────┴─────────┴────────┴─────────┘

Unique CINSTCL values (first 20): ['PT', '/0', '65', 'à', 'B1', '14', 'G', '-3', 'BH', '2M', '0K', 'I/', ')N', 'TU', '4,', '27', 'N3', 'N,', '3C', 'AH']
Unique NATGUAR values (first 20): ['\x9d', 'TI', 'C0', 'T,', 'UC', '/3', 'FS', 'LU', 'YP', 'QU', 'NY', '/Y', 'J4', '\x03\x95', 'A)', '00', '/B', 'HU', 'PR', 'UB']
Rows with CINSTCL='18': 17
Rows with NATGUAR='06': 16

COLL rows after join: 593672197
COLL rows after filter: 0

Final COLL rows: 0
NPGS rows after COLL merge: 0
Processing MICR file...
  MICR rows: 300
Creating CVAR fields...
Writing NPGS.LNSMEZ09...
Using SAS Config named: default
SAS Connection established. Subprocess id is 412979

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Successfully wrote NPGS.LNSMEZ09 to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLSMEZ
SAS Connection terminated. Subprocess id was 412979
