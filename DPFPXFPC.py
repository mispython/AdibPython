Scanning: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note_m09.sas7bdat
(streaming in 1M-row chunks, applying only cheap filters)

[chunk 0] ISSUEDT dtype: Float64
[chunk 0] ISSUEDT sample: [14633.0, 14435.0, 14432.0, 15578.0, 16714.0]
[chunk 0] LOANTYPE dtype: Float64
[chunk 0] ENTITY_CD sample: ['', '', '', '', '']

  scanned 4,000,000 rows ...
  scanned 8,000,000 rows ...

Total rows scanned: 8,125,473
Active rows (paid != P & bal > 0): 2,724,311

======================================================================
TOP 40 LOANTYPE in ACTIVE book (paid!=P, bal>0), by row count
======================================================================
  LOANTYPE 700: 1,065,977
  LOANTYPE 128: 380,506
  LOANTYPE 245: 362,998
  LOANTYPE 720: 162,022
  LOANTYPE 363: 99,915
  LOANTYPE 152: 98,785
  LOANTYPE 993: 74,870
  LOANTYPE 247: 55,620
  LOANTYPE 705: 50,536
  LOANTYPE 135: 35,052
  LOANTYPE 983: 31,335
  LOANTYPE 311: 25,411
  LOANTYPE 131: 21,336
  LOANTYPE 154: 17,912
  LOANTYPE 235: 17,836
  LOANTYPE 246: 14,875
  LOANTYPE 175: 14,599
  LOANTYPE 248: 12,205
  LOANTYPE 142: 12,047
  LOANTYPE 359: 12,018
  LOANTYPE 301: 11,952
  LOANTYPE 303: 11,906
  LOANTYPE 315: 11,129
  LOANTYPE 151: 8,918
  LOANTYPE 250: 7,959
  LOANTYPE 234: 7,794
  LOANTYPE 368: 6,997
  LOANTYPE 348: 6,356
  LOANTYPE 5: 5,842
  LOANTYPE 244: 5,516
  LOANTYPE 575: 5,257
  LOANTYPE 392: 4,696
  LOANTYPE 422: 4,284
  LOANTYPE 409: 3,828
  LOANTYPE 600: 3,571
  LOANTYPE 15: 3,534
  LOANTYPE 413: 3,488
  LOANTYPE 518: 3,137
  LOANTYPE 464: 2,899
  LOANTYPE 410: 2,787

======================================================================
YEAR-MONTH distribution of ACTIVE rows (top 30)
======================================================================
  2026-09: 37,213
  2026-08: 32,147
  2026-07: 34,209
  2026-06: 28,423
  2026-05: 30,284
  2026-04: 32,740
  2026-03: 30,159
  2026-02: 27,378
  2026-01: 41,537
  2025-12: 29,975
  2025-11: 29,853
  2025-10: 33,999
  2025-09: 30,087
  2025-08: 31,831
  2025-07: 33,541
  2025-06: 30,990
  2025-05: 34,687
  2025-04: 32,318
  2025-03: 36,502
  2025-02: 28,992
  2025-01: 40,135
  2024-12: 27,999
  2024-11: 29,943
  2024-10: 33,062
  2024-09: 30,634
  2024-08: 32,526
  2024-07: 32,717
  2024-06: 28,138
  2024-05: 30,174
  2024-04: 28,489

======================================================================
ACTIVE rows per candidate HPD list (all months combined)
======================================================================
  PBBLNFMT.HPD (10 codes)       : 1,682,322 rows
  PBBLNFMT.HP (17 codes)        : 1,788,728 rows
  LNFMT.LSTAFF (mass retail)    : 9,957 rows
  LNFMT.LHOUSE (housing)        : 496,662 rows
  LNFMT.LHPPRD (HP product)     : 1,767,185 rows

======================================================================
MOST-RECENT year-month with data, per candidate list
======================================================================
  PBBLNFMT.HPD (10 codes)       : 2026-09(27182), 2026-08(24623), 2026-07(26811)
  PBBLNFMT.HP (17 codes)        : 2026-09(27182), 2026-08(24623), 2026-07(26811)
  LNFMT.LSTAFF (mass retail)    : 2026-09(57), 2026-08(44), 2026-07(56)
  LNFMT.LHOUSE (housing)        : 2026-09(2704), 2026-08(2464), 2026-07(2650)
  LNFMT.LHPPRD (HP product)     : 2026-09(19684), 2026-08(18235), 2026-07(21100)

======================================================================
REPORTING MONTH 2026-09 — how many active rows per candidate list?
======================================================================
  PBBLNFMT.HPD (10 codes)       : 27,182 rows
  PBBLNFMT.HP (17 codes)        : 27,182 rows
  LNFMT.LSTAFF (mass retail)    : 57 rows
  LNFMT.LHOUSE (housing)        : 2,704 rows
  LNFMT.LHPPRD (HP product)     : 19,684 rows

======================================================================
ENTITY_CD distribution of ACTIVE rows
======================================================================
  '': 2,064,086
  'PIBB': 660,225

Done.
