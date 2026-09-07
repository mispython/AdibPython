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
  Using known COLL record length: 158
Detecting DESC record length (previous code guessed this from an assumed row count -- that produced a non-integer remainder and misaligned every field)...
  Record length candidates (length, quality score):
      134200  ->  1.000
       62525  ->  0.999
       16775  ->  0.999
       67100  ->  0.998
      125050  ->  0.998
       24400  ->  0.998
       33550  ->  0.998
       12200  ->  0.998
  Selected record length: 134200 (score 1.000)
COLL record length: 158
DESC record length: 134200

Reading COLL file...
  COLL: read 5394860 records (expected ~5394860)
       ccollno: 3654264/5394860 decode failures (67.7%)  <-- HIGH FAILURE RATE
        acctno: 3654292/5394860 decode failures (67.7%)  <-- HIGH FAILURE RATE
        noteno: 3587980/5394860 decode failures (66.5%)  <-- HIGH FAILURE RATE
Reading DESC file...
  DESC: read 36982 records (expected ~36982)
       ccollno: 0/36982 decode failures (0.0%)
       cinstcl: 0/36982 decode failures (0.0%)
       natguar: 0/36982 decode failures (0.0%)
        census: 27045/36982 decode failures (73.1%)  <-- HIGH FAILURE RATE
       tranche: 0/36982 decode failures (0.0%)

=== COLL Data Sample (first 3 rows) ===
shape: (3, 3)
┌─────────┬───────────┬───────────┐
│ ccollno ┆ acctno    ┆ noteno    │
│ ---     ┆ ---       ┆ ---       │
│ f64     ┆ f64       ┆ f64       │
╞═════════╪═══════════╪═══════════╡
│ 307.0   ┆ 2.0818e11 ┆ 2.0818e11 │
│ null    ┆ null      ┆ null      │
│ 0.0     ┆ null      ┆ null      │
└─────────┴───────────┴───────────┘

=== DESC Data Sample (first 3 rows) ===
shape: (3, 5)
┌─────────┬─────────┬─────────┬────────┬─────────┐
│ ccollno ┆ cinstcl ┆ natguar ┆ census ┆ tranche │
│ ---     ┆ ---     ┆ ---     ┆ ---    ┆ ---     │
│ f64     ┆ str     ┆ str     ┆ f64    ┆ str     │
╞═════════╪═════════╪═════════╪════════╪═════════╡
│ 133.0   ┆ 29      ┆         ┆ null   ┆         │
│ 10868.0 ┆ 35      ┆         ┆ null   ┆         │
│ 18457.0 ┆ 65      ┆         ┆ null   ┆         │
└─────────┴─────────┴─────────┴────────┴─────────┘

Validating decoded code fields before filtering...
  Top values for 'cinstcl':
shape: (15, 2)
┌─────────┬───────┐
│ cinstcl ┆ count │
│ ---     ┆ ---   │
│ str     ┆ u32   │
╞═════════╪═══════╡
│ 10      ┆ 13058 │
│ 16      ┆ 11762 │
│ 18      ┆ 10770 │
│ 27      ┆ 1047  │
│ 12      ┆ 128   │
│ …       ┆ …     │
│ PB      ┆ 5     │
│ 19      ┆ 4     │
│ 20      ┆ 3     │
│ 29      ┆ 3     │
│ 17      ┆ 3     │
└─────────┴───────┘
  'cinstcl' field-quality score: 1.000
  Top values for 'natguar':
shape: (15, 2)
┌─────────┬───────┐
│ natguar ┆ count │
│ ---     ┆ ---   │
│ str     ┆ u32   │
╞═════════╪═══════╡
│         ┆ 16733 │
│ IC      ┆ 9177  │
│ 02      ┆ 6614  │
│ 01      ┆ 3093  │
│ 05      ┆ 603   │
│ …       ┆ …     │
│ 09      ┆ 15    │
│ 03      ┆ 15    │
│ 00      ┆ 14    │
│ OT      ┆ 5     │
│ SI      ┆ 4     │
└─────────┴───────┘
  'natguar' field-quality score: 0.997
Rows with CINSTCL='18': 10770
Rows with NATGUAR='06': 79

COLL rows after join: 39
COLL rows after filter: 0

Final COLL rows: 0
NPGS rows after COLL merge: 0
Processing MICR file...
Creating CVAR fields...
Writing NPGS.LNSMEZ08...
Using SAS Config named: default
SAS Connection established. Subprocess id is 371721

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Successfully wrote NPGS.LNSMEZ08 to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLSMEZ
SAS Connection terminated. Subprocess id was 371721
