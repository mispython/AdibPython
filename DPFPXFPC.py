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
Checking whether COLL is VB (variable-length, RDW-prefixed) format...
  Not VB-framed (or framing didn't hold) -- trying fixed-length detection.
Detecting COLL record length...
  Record length candidates near estimate (length, quality score):
         190  ->  0.500
          95  ->  0.167
         152  ->  0.133
          76  ->  0.067
         158  ->  0.058
          79  ->  0.019
  No candidate near 158 scored well (best 0.500). Widening sweep to divisors of file size in [100, 4000] -- the estimate used to seed this search was likely wrong.
  Top candidates from wide sweep:
         380  ->  1.000
         760  ->  1.000
         190  ->  0.500
         152  ->  0.133
        3002  ->  0.133
        1580  ->  0.089
        3160  ->  0.089
        1501  ->  0.083
         790  ->  0.063
         395  ->  0.061
  Selected record length: 380 (score 1.000)
Detecting DESC record length (previous code guessed this from an assumed row count -- that produced a non-integer remainder and misaligned every field)...
  Record length candidates near estimate (length, quality score):
      134200  ->  1.000
       62525  ->  0.999
       16775  ->  0.999
       67100  ->  0.998
      125050  ->  0.998
       24400  ->  0.998
       33550  ->  0.998
       12200  ->  0.998
  Selected record length: 134200 (score 1.000)
COLL record length: 380
DESC record length: 134200

Reading COLL file...
  COLL: read 2243126 records (expected ~2243126)
       ccollno: 0/2243126 decode failures (0.0%)
        acctno: 0/2243126 decode failures (0.0%)
        noteno: 0/2243126 decode failures (0.0%)
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
│ 2371.0  ┆ 2.0818e11 ┆ 2.0818e11 │
│ 4759.0  ┆ 2.0863e11 ┆ 2.0863e11 │
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
│ 17      ┆ 3     │
│ 29      ┆ 3     │
│ 20      ┆ 3     │
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
│ 03      ┆ 15    │
│ 09      ┆ 15    │
│ 00      ┆ 14    │
│ OT      ┆ 5     │
│ SI      ┆ 4     │
└─────────┴───────┘
  'natguar' field-quality score: 0.997
Rows with CINSTCL='18': 10770
Rows with NATGUAR='06': 79

COLL rows after join: 27
  Distinct CCOLLNO -- COLL: 1530738, DESC: 36982, overlap: 11
  Overlap as % of DESC's distinct CCOLLNO: 0.0%
  DESC rows matching filter criteria independently: CINSTCL='18' -> 10770, NATGUAR='06' -> 79, BOTH -> 79
COLL rows after filter: 0

Final COLL rows: 0
NPGS rows after COLL merge: 0
  (of 6 LOAN rows, this used a COLL universe already filtered to CINSTCL='18' AND NATGUAR='06' -- a small or zero result here can be a genuine data outcome on a tiny LOAN sample like this one, not necessarily a decode bug. Check the LOAN acctno/noteno pairs against COLL's full (unfiltered) acctno/noteno set if you need to confirm.)
Processing MICR file...
Creating CVAR fields...
Writing NPGS.LNSMEZ08...
Using SAS Config named: default
SAS Connection established. Subprocess id is 417272

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Successfully wrote NPGS.LNSMEZ08 to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLSMEZ
SAS Connection terminated. Subprocess id was 417272
