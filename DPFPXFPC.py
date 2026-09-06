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
COLL file size: 852387880 bytes
DESC file size: 4962984400 bytes
COLL record length 158 divides evenly (5394860 records)
  COLL rows: 5394860
  DESC rows: 16654310

=== COLL Data Sample (first 5 rows) ===
shape: (5, 3)
┌───────────┬──────────┬──────────┐
│ ccollno   ┆ acctno   ┆ noteno   │
│ ---       ┆ ---      ┆ ---      │
│ f64       ┆ f64      ┆ f64      │
╞═══════════╪══════════╪══════════╡
│ 133.0     ┆ 3.0790e9 ┆ 3.0790e9 │
│ null      ┆ null     ┆ null     │
│ 0.0       ┆ null     ┆ null     │
│ 130.0     ┆ null     ┆ null     │
│ 4.0404e10 ┆ null     ┆ null     │
└───────────┴──────────┴──────────┘

=== DESC Data Sample (first 5 rows) ===
shape: (5, 5)
┌─────────┬─────────┬─────────┬────────┬─────────┐
│ ccollno ┆ cinstcl ┆ natguar ┆ census ┆ tranche │
│ ---     ┆ ---     ┆ ---     ┆ ---    ┆ ---     │
│ f64     ┆ str     ┆ str     ┆ f64    ┆ str     │
╞═════════╪═════════╪═════════╪════════╪═════════╡
│ 133.0   ┆ 29      ┆         ┆ null   ┆         │
│ null    ┆         ┆         ┆ null   ┆         │
│ null    ┆         ┆         ┆ null   ┆         │
│ null    ┆         ┆         ┆ null   ┆         │
│ null    ┆         ┆         ┆ null   ┆         │
└─────────┴─────────┴─────────┴────────┴─────────┘

Unique CINSTCL values (first 20): ['\x90@', 'VD', 'KO', '/-', 'ñe', '.6', 'Z8', ',"', '\x81\x88', '\x86\x01', '\x9d\x82', '\x9c\x1b', '`\x80', 'Ïm', 'id', 'añ', '\x98È', 'ë\x12', '<', '54']
Unique NATGUAR values (first 20): ['çç', 'Í', "'H", '\x00', '\x96k', 'É\x94', '°\x10', '\x13°', 'ë\x84', '\x10ß', 'í\x84', '\x80Ø', 'FL', '`l', 'ã\x80', 'ë\x82', '76', '\x16\x82', '3M', '\x17ç']

  COLL rows: 5394860
  DESC rows: 16654310
