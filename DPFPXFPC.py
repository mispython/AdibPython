SDATE = 2026-10-01
  [enrh_ln_note_m10.sas7bdat] 8,125,473 rows, 495 cols
  [enrh_ln_note_m10.sas7bdat] entity column = 'ENTITY_CD'
    filter entity_cd != 'PIBB': dropped 15 rows
    HPSETTLE cumulative kept = 6,767
    filter entity_cd != 'PIBB': dropped 12 rows
    HPSETTLE cumulative kept = 11,664
    filter entity_cd != 'PIBB': dropped 4 rows
    HPSETTLE cumulative kept = 14,405
    filter entity_cd != 'PIBB': dropped 10 rows
    HPSETTLE cumulative kept = 16,405
    filter entity_cd != 'PIBB': dropped 3 rows
    HPSETTLE cumulative kept = 17,684
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 18,730
    HPSETTLE cumulative kept = 18,925
    HPSETTLE cumulative kept = 18,962
    filter entity_cd != 'PIBB': dropped 241,641 rows
    HPSETTLE cumulative kept = 18,962
    filter entity_cd != 'PIBB': dropped 249,993 rows
    HPSETTLE cumulative kept = 18,967
    filter entity_cd != 'PIBB': dropped 72,227 rows
    HPSETTLE cumulative kept = 100,230
    filter entity_cd != 'PIBB': dropped 7 rows
    HPSETTLE cumulative kept = 212,035
    filter entity_cd != 'PIBB': dropped 14 rows
    HPSETTLE cumulative kept = 315,796
    filter entity_cd != 'PIBB': dropped 68 rows
    HPSETTLE cumulative kept = 381,630
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 411,240
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 522,103
    HPSETTLE cumulative kept = 698,897
    HPSETTLE cumulative kept = 936,144
    HPSETTLE cumulative kept = 1,149,499
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 1,354,291
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 1,548,413
    HPSETTLE cumulative kept = 1,768,741
    HPSETTLE cumulative kept = 2,006,550
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 2,240,245
    HPSETTLE cumulative kept = 2,467,951
    HPSETTLE cumulative kept = 2,672,198
    filter entity_cd != 'PIBB': dropped 1 rows
    HPSETTLE cumulative kept = 2,835,705
    filter entity_cd != 'PIBB': dropped 126,109 rows
    HPSETTLE cumulative kept = 2,901,105
    filter entity_cd != 'PIBB': dropped 249,998 rows
    HPSETTLE cumulative kept = 2,901,106
    filter entity_cd != 'PIBB': dropped 249,997 rows
    HPSETTLE cumulative kept = 2,901,106
    filter entity_cd != 'PIBB': dropped 249,996 rows
    HPSETTLE cumulative kept = 2,901,106
    filter entity_cd != 'PIBB': dropped 249,991 rows
    HPSETTLE cumulative kept = 2,901,107
    filter entity_cd != 'PIBB': dropped 125,469 rows
    HPSETTLE cumulative kept = 2,901,107
  [enrh_ln_note_m10.sas7bdat] read 33 chunk(s), 8,125,473 raw rows
HPSETTLE rows: 2901107

First 20 rows:
shape: (20, 3)
┌──────────┬────────────┬────────────┐
│ acctno   ┆ issdte     ┆ closedte   │
│ ---      ┆ ---        ┆ ---        │
│ f64      ┆ date       ┆ date       │
╞══════════╪════════════╪════════════╡
│ 2.0000e9 ┆ 2002-08-26 ┆ 2012-06-24 │
│ 2.0000e9 ┆ 2015-08-28 ┆ 2023-12-04 │
│ 2.0000e9 ┆ 1997-12-12 ┆ 2004-01-06 │
│ 2.0000e9 ┆ 2004-01-19 ┆ 2010-12-02 │
│ 2.0000e9 ┆ 2001-05-28 ┆ 2005-01-12 │
│ …        ┆ …          ┆ …          │
│ 2.0000e9 ┆ 2005-03-25 ┆ 2010-03-29 │
│ 2.0000e9 ┆ 2010-06-11 ┆ 2015-06-25 │
│ 2.0000e9 ┆ 1999-04-01 ┆ 2006-11-24 │
│ 2.0000e9 ┆ 2007-01-04 ┆ 2015-12-21 │
│ 2.0000e9 ┆ 2009-12-29 ┆ 2015-12-21 │
└──────────┴────────────┴────────────┘

Row counts:
  issdte >= 2026-10-01:           0
  closedte >= 2026-10-01:          0
  both issdte AND closedte >=:  0
