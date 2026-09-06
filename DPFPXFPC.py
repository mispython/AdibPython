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
Reading Islamic LNCOMM (ENTITY_CD = 'PIBB')...
  Islamic LNCOMM rows: 1066036
Reading Conventional LNCOMM (ENTITY_CD != 'PIBB')...
  Conventional LNCOMM rows: 1066036
Warning: INTAMT column not found. Using CORGAMT as NETPROC.
Total LOAN rows after merge: 6
Calculating ISSUED, NODAYS, ARREARS, NPLDATE...
Applying NDAYS format...
LOAN rows after deduplication: 6
Processing CISLN in chunks...
  CISLN rows after filter: 63752
Processing COLL and DESC files...
  COLL rows: 1
  DESC rows: 1

=== COLL Data Sample (first 5 rows) ===
shape: (1, 3)
┌─────────┬──────────┬──────────┐
│ ccollno ┆ acctno   ┆ noteno   │
│ ---     ┆ ---      ┆ ---      │
│ f64     ┆ f64      ┆ f64      │
╞═════════╪══════════╪══════════╡
│ 133.0   ┆ 3.0790e9 ┆ 3.0790e9 │
└─────────┴──────────┴──────────┘

=== DESC Data Sample (first 5 rows) ===
shape: (1, 5)
┌─────────┬─────────┬─────────┬────────┬─────────┐
│ ccollno ┆ cinstcl ┆ natguar ┆ census ┆ tranche │
│ ---     ┆ ---     ┆ ---     ┆ ---    ┆ ---     │
│ f64     ┆ str     ┆ str     ┆ null   ┆ str     │
╞═════════╪═════════╪═════════╪════════╪═════════╡
│ 133.0   ┆ 29      ┆         ┆ null   ┆         │
└─────────┴─────────┴─────────┴────────┴─────────┘

Unique CINSTCL values: ['29']

Unique NATGUAR values: ['']

  COLL rows: 1
  DESC rows: 1
  COLL rows after join with DESC: 1

=== COLL after join (first 5 rows) ===
shape: (1, 7)
┌─────────┬──────────┬──────────┬─────────┬─────────┬────────┬─────────┐
│ ccollno ┆ acctno   ┆ noteno   ┆ cinstcl ┆ natguar ┆ census ┆ tranche │
│ ---     ┆ ---      ┆ ---      ┆ ---     ┆ ---     ┆ ---    ┆ ---     │
│ f64     ┆ f64      ┆ f64      ┆ str     ┆ str     ┆ null   ┆ str     │
╞═════════╪══════════╪══════════╪═════════╪═════════╪════════╪═════════╡
│ 133.0   ┆ 3.0790e9 ┆ 3.0790e9 ┆ 29      ┆         ┆ null   ┆         │
└─────────┴──────────┴──────────┴─────────┴─────────┴────────┴─────────┘

Unique CINSTCL values after join: ['29']
Unique NATGUAR values after join: ['']

  COLL rows after filter: 0
NPGS rows after COLL merge: 0
Processing MICR file...
Creating CVAR fields...
Writing NPGS.LNSMEZ08...
Using SAS Config named: default
SAS Connection established. Subprocess id is 133202

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Successfully wrote NPGS.LNSMEZ08 to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLSMEZ/NPGS
SAS Connection terminated. Subprocess id was 133202
