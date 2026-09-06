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

=== COLL Data Sample (first 5 rows) ===
shape: (5, 3)
┌─────────┬────────┬────────┐
│ ccollno ┆ acctno ┆ noteno │
│ ---     ┆ ---    ┆ ---    │
│ str     ┆ str    ┆ str    │
╞═════════╪════════╪════════╡
│   ┆        ┆        │
│ X       ┆        ┆        │
│     ┆        ┆        │
│  %  ┆        ┆        │
│    ┆  ┆  │
└─────────┴────────┴────────┘

COLL columns: ['ccollno', 'acctno', 'noteno']
COLL dtypes: [String, String, String]

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

DESC columns: ['ccollno', 'cinstcl', 'natguar', 'census', 'tranche']
DESC dtypes: [Float64, String, String, Float64, String]

Unique CINSTCL values: ['RH', '0C', 'JE', 'QB', 'LO', '58', 'TA', '81', '-,', '@K', 'T', 'NC', '6B', 'OM', 'RD', '93', 'PA', 'AH', 'GH', 'GL']

Unique NATGUAR values: ['NJ', 'SW', '@K', 'DU', 'PA', 'M)', 'OW', ',2', 'IB', 'MF', '2,', 'NY', '1D', 'HO', '4F', ':', '-0', '10', '90', 'J']

  COLL rows: 1913966
  DESC rows: 58604
  COLL rows after join with DESC: 0

  COLL rows after filter: 0
NPGS rows after COLL merge: 0
Processing MICR file...
Creating CVAR fields...
Writing NPGS.LNSMEZ08...
Using SAS Config named: default
SAS Connection established. Subprocess id is 132862

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Successfully wrote NPGS.LNSMEZ08 to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLSMEZ
SAS Connection terminated. Subprocess id was 132862
