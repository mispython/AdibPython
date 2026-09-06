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
Expected DESC records: 58604
Calculated DESC record length: 84686

Reading COLL file...
COLL rows: 70001
Reading DESC file...
DESC rows: 58604

=== DESC Record Analysis ===
Record length: 84686
Positions with '18': [12251, 18352, 57959, 61051]
Positions with '06': [27503, 45760]

Position 51-52: '29'
Position 55-56: '  '

=== First 300 characters of first DESC record ===
Pos   1- 50: [00000000133008 ]
Pos  51-100: [2976                                              ]
Pos 101-150: [                              DNP                 ]
Pos 151-200: [                                                  ]
Pos 201-250: [                                                  ]
Pos 251-300: [                                                  ]

=== End Diagnostic ===

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

Unique CINSTCL values (first 20): ['T0', 'CB', 'A,', 'IB', '08', '3,', 'TD', '/D', '17', 'AD', 'LG', 'CG', 'JO', '\x00\x00', 'U1', 'OC', 'OP', 'HO', 'G4', '\x88\x00']
Unique NATGUAR values (first 20): ['.7', 'NE', 'MP', 'Ø', 'PJ', 'RT', 'C\x00', 'OJ', 'RA', 'L.', '00', 'PE', '22', '8,', '06', 'MU', '-U', 'RC', '\x00', '\x88\x03']
Rows with CINSTCL='18': 17
Rows with NATGUAR='06': 16

COLL rows after join: 8332090
COLL rows after filter: 0

Final COLL rows: 0
NPGS rows after COLL merge: 0
Processing MICR file...
  MICR rows: 300
Creating CVAR fields...
Writing NPGS.LNSMEZ08...
Using SAS Config named: default
SAS Connection established. Subprocess id is 135564

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Successfully wrote NPGS.LNSMEZ08 to /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBLSMEZ
SAS Connection terminated. Subprocess id was 135564
