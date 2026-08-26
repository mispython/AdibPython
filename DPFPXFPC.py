REPTMON: 07, RDATE: 310726
Processing date: 2026-07-31
Successfully read 875 rows from /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRSRGF/lnnpgs07.sas7bdat

=== DEBUG: CVAR02 values ===
CVAR02 dtype: String
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSRGF.py:52: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  value_counts = cgcs_df.group_by('cvar02').agg(pl.count().alias('count')).sort('count', descending=True)

CVAR02 value counts:
  '1Z': 492 rows
  '3Z': 171 rows
  '1H': 65 rows
  'F5': 50 rows
  '2Z': 32 rows
  'H6': 26 rows
  '53': 14 rows
  '4Z': 8 rows
  '5H': 4 rows
  'E6': 3 rows
  '3H': 2 rows
  '6H': 2 rows
  '5S': 2 rows
  '5Z': 1 rows
  '2H': 1 rows
  'XX': 1 rows
  '81': 1 rows

=== Checking for '10' and '63' in different formats ===
As-is match: 0 rows
Stripped match: 0 rows

Contains '10': 0 rows
Contains '63': 0 rows

First 2 chars match: 0 rows

=== Sample data (first 10 rows) ===
shape: (10, 6)
┌──────────┬────────┬────────────┬─────────────────────────────────┬──────────┬────────┐
│ cvar01   ┆ cvar02 ┆ cvar03     ┆ cvar04                          ┆ cvar06   ┆ cvar07 │
│ ---      ┆ ---    ┆ ---        ┆ ---                             ┆ ---      ┆ ---    │
│ f64      ┆ str    ┆ str        ┆ str                             ┆ f64      ┆ str    │
╞══════════╪════════╪════════════╪═════════════════════════════════╪══════════╪════════╡
│ 1.0005e9 ┆ XX     ┆ 476732A    ┆ PISTON INDUSTRY SDN BHD         ┆ 2.0670e9 ┆ FL     │
│ 1.0005e9 ┆ 53     ┆ 795669K    ┆ INCOMM MARKETING SDN. BHD.      ┆ 2.0672e9 ┆ FL     │
│ 1.0005e9 ┆ 53     ┆ 571370V    ┆ BENGKEL MENGIMPAL KERETA KK SD… ┆ 2.0578e9 ┆ FL     │
│ 1.0006e9 ┆ 53     ┆ 672849M    ┆ HANG SENG GOLD TECHNOLOGY SDN … ┆ 2.0702e9 ┆ FL     │
│ 1.0006e9 ┆ E6     ┆ G202710    ┆ TEXON PEMBORONG BUMIPUTRA       ┆ 2.0782e9 ┆ FL     │
│ 1.0006e9 ┆ 53     ┆ MRI2013134 ┆ MYPAGES MEDIA ADVERTISING ENTE… ┆ 2.1289e9 ┆ FL     │
│ 1.0006e9 ┆ 53     ┆ 842014M    ┆ MUN MEDICO SDN. BHD.            ┆ 2.1263e9 ┆ FL     │
│ 1.0006e9 ┆ 81     ┆ 355699M    ┆ CEL LOGISTICS SDN. BHD.         ┆ 2.1304e9 ┆ FL     │
│ 1.0006e9 ┆ 53     ┆ 922595W    ┆ MONT TIARA DEVELOPMENT SDN. BH… ┆ 2.1451e9 ┆ FL     │
│ 1.0006e9 ┆ 53     ┆ AS0150589U ┆ TROPICAL RESORT                 ┆ 2.1388e9 ┆ FL     │
└──────────┴────────┴────────────┴─────────────────────────────────┴──────────┴────────┘

=== CVAR07 values ===
CVAR07 dtype: String
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBRSRGF.py:95: DeprecationWarning: `pl.count()` is deprecated. Please use `pl.len()` instead.
(Deprecated in version 0.20.5)
  cvar07_counts = cgcs_df.group_by('cvar07').agg(pl.count().alias('count')).sort('count', descending=True)
  'FL': 875 rows
