=== Diagnostic run for reptmon=09 (reptdate=2026-09-30) ===

HP codes from PBBLNFMT.MACROS['HP'] (17): [128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 983, 993, 996, 678, 679, 698, 699]

======================================================================
A. LNNOTE filtering stages
======================================================================
Reading enrh_ln_note_m09.sas7bdat ...
  Total rows                        : 8,125,473

  LOANTYPE dtype: float64
  LOANTYPE first 10 raw values:
     600.0  (type=float)
     521.0  (type=float)
     521.0  (type=float)
     15.0  (type=float)
     25.0  (type=float)
     15.0  (type=float)
     4.0  (type=float)
     5.0  (type=float)
     25.0  (type=float)
     32.0  (type=float)

  LOANTYPE first 10 normalized:
     ['600', '521', '521', '15', '25', '15', '4', '5', '25', '32']

  Stage 1: LOANTYPE IN &HP          : 5,485,086
  Stage 2: BALANCE > 0              : 3,420,376
  Stage 3: BORSTAT NOT IN (F,I,R)   : 8,033,991
  ALL THREE (the real filter)       : 1,729,861
  Of the filtered rows, 983/993      : 47,792

  Loan type distribution after all filters:
        128:  379,652
        130:      355
        131:   21,340
        132:        6
        380:        1
        678:      105
        700: 1,063,989
        705:   50,490
        720:  164,546
        725:    1,585
        983:   14,076  <-- 983/993
        993:   33,716  <-- 983/993

  Raw LOANTYPE value counts (top 20) — before any filtering:
          '700':  3,707,243
          '128':  1,183,771
          '245':    613,353
          '705':    246,398
          '135':    241,198
          '392':    223,237
          '247':    210,087
          '363':    187,575
          '720':    174,788
          '152':    120,131
          '359':    109,741
          '993':    102,919  <-- 983/993
          '301':     95,897
          '234':     58,126
          '246':     55,565
          '228':     52,457
          '235':     48,879
          '983':     41,862  <-- 983/993
          '201':     35,113
          '311':     35,103

======================================================================
B. LOANTEMP filtering stages
======================================================================
Reading loantemp_iccd2.sas7bdat ...
  Total rows                        : 631,802
  ARREAR dtype                      : float64
  ARREAR >= 10                      : 4,077
  ARREAR null                       : 0
  ACCTNO dtype                      : float64
  ACCTNO first 5 raw                : [2900189623.0, 2900189914.0, 2900190005.0, 2900190333.0, 2900190818.0]

======================================================================
C. Intersection (the ceiling for REPO1)
======================================================================
  LNNOTE filtered acctnos           : 1,729,838
  LOANTEMP acctnos w/ ARREAR >= 10  : 4,049
  LNNOTE 983/993 acctnos            : 47,789

  INTERSECTION (ARREAR>=10 AND in LNNOTE filtered): 301   <- max REPO size
  INTERSECTION (ARREAR>=10 AND 983/993):            1   <- max REPO1 size

======================================================================
VERDICT
======================================================================
  => DATA LIMITATION: only 1 account(s) in the input
     satisfy (ARREAR>=10 AND LOANTYPE IN 983/993).
     Your code is CORRECT. REPO1 is intentionally a tiny subset.

Done.
