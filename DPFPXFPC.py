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
DESC file size: 4962984400 bytes
Using default DESC record length: 512

COLL rows: 5394860
DESC rows: 9693328

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
│ null    ┆         ┆         ┆ null   ┆         │
│ null    ┆         ┆         ┆ null   ┆         │
└─────────┴─────────┴─────────┴────────┴─────────┘

Unique CINSTCL values (first 20): ['\x86Ç', 'ÅÍ', 'ìø', 'Í\x03', '\x13b', '\x91Ç', 'hñ', 'À\x10', 'È\x90', 'c\x9c', 'Å\x9c', 'ä°', '\x82o', 'KY', 'qß', '4G', '\x02/', 'e\x84', '\x12o', 'ti']
Unique NATGUAR values (first 20): ['4W', 'Ïã', 'bð', 'T@', 'lt', 'Íë', '/V', '\x94\x7f', '\x88\x16', '5-', 'FR', ',L', '\x98\x1b', 'MU', 'î\x1b', '\x04&', '\x82k', '\x7f\x13', '(F', 'Y.']
Rows with CINSTCL='18': 4587
Rows with NATGUAR='06': 3436
