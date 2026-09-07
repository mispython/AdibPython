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
COLL record length: 158
DESC record length: 298

Reading COLL file...
COLL rows: 5394860
Reading DESC file...
DESC rows: 16654310

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

Unique CINSTCL values (first 20): ['a\x13', 'ñÍ', 'MY', '\x00Â', '\x1b\x81', '3F', 'EC', '\x13\x03', '\x8dÈ', 'î\x80', '(8', '\x16d', 'lÁ', 'p\x80', 'Á\x90', 'IS', '5*', '\x81&', 'CN', 'FG']
Unique NATGUAR values (first 20): ['ÄÊ', '8K', 'Ìa', '5Q', 'i\x88', 'Ç\x19', '\x9c\x91', 'AH', 'É-', 'î\x90', '9N', 'k\x90', 'áø', '\x17Ë', '\x8d\x02', 'T', 'RL', '\x9co', 'E:', '\x84\x02']
Rows with CINSTCL='18': 7988
Rows with NATGUAR='06': 5956
Killed
