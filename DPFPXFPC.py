EIMREPOI REPOTXT Parity Diagnostic
========================================================================
LNNOTE:   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m09.sas7bdat
LOANTEMP: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMREPOI/loantemp_iccd2.sas7bdat
HP set (17): [128, 130, 131, 132, 380, 381, 700, 705, 720, 725, 983, 993, 996, 678, 679, 698, 699]

========================================================================
  SECTION 1: Load LNNOTE (filtered to HP + BALANCE > 0 + BORSTAT)
========================================================================
Reading LNNOTE...
  Total LNNOTE rows: 8,125,473
  LOANTYPE IN HP:           5,485,086
  BALANCE > 0:              3,420,376
  BORSTAT NOT IN F,I,R:     8,033,991
  ALL THREE (filtered):     1,729,861
  Filtered LNNOTE retained: 1,729,861

--- Loan type distribution in filtered LNNOTE ---
       128:    379,652
       130:        355
       131:     21,340
       132:          6
       380:          1
       678:        105
       700:  1,063,989
       705:     50,490
       720:    164,546
       725:      1,585
       983:     14,076
       993:     33,716

========================================================================
  SECTION 2: KKI branch (117) accounts — prod has these
========================================================================
  KKI accounts in raw LNNOTE: 10,646

--- KKI loan type distribution (raw LNNOTE) ---
       128:  5,393 <-- HP
       983:    766 <-- HP
       247:    677
       135:    673
       700:    371 <-- HP
       359:    318
       142:    230
       175:    212
       705:    204 <-- HP
       234:    155
       993:    150 <-- HP
       152:    136
       246:    102
       301:     97
       303:     93

  KKI after ALL filters:     680

--- KKI loan types after ALL filters ---
       983:    399
       128:    186
       993:     75
       700:     13
       705:      7

========================================================================
  SECTION 3: Load LOANTEMP (iccd2)
========================================================================
Reading LOANTEMP...
  LOANTEMP total rows: 631,802
  ARREAR >= 10:        4,077
  product=700 count:   0

--- LOANTEMP: ARREAR >= 10 by branch (top 20) ---
    branch    701:    160
    branch     30:    123
    branch     13:    107
    branch     34:     90
    branch    802:     81
    branch    173:     68
    branch    827:     66
    branch     56:     62
    branch      8:     60
    branch     89:     59
    branch     10:     53
    branch    801:     53
    branch    702:     51
    branch      3:     48
    branch    143:     48
    branch    820:     42
    branch     57:     42
    branch    168:     41
    branch    808:     40
    branch     94:     40

  KKI (branch 117) with ARREAR >= 10: 13
  KKI products with arrears:
    product    651:      3
    product    652:      3
    product    666:      2
    product    128:      2
    product    664:      1
    product    657:      1
    product    655:      1

========================================================================
  SECTION 4: Merge LNNOTE ↔ LOANTEMP on ACCTNO
========================================================================
  LNNOTE filtered unique acctnos: 1,729,838
  LOANTEMP unique acctnos:        629,380
  Overlap (matched):              400,046 (23.1% of LNNOTE)

  After merge, ARREAR >= 10: 301

--- Merged ARREAR >= 10 — by LNNOTE loantype ---
    LNNOTE loantype    128:    300
    LNNOTE loantype    983:      1

--- Merged ARREAR >= 10 — by LOANTEMP product ---
    LOANTEMP product    128:    300
    LOANTEMP product    983:      1

--- Merged ARREAR >= 10 — by branch (top 20) ---
    branch    827:     44
    branch    801:     28
    branch    862:     28
    branch    802:     25
    branch    819:     17
    branch    818:     17
    branch    822:     11
    branch    826:     10
    branch    814:     10
    branch    820:      9
    branch    809:      9
    branch    806:      8
    branch    821:      8
    branch    811:      8
    branch     34:      7
    branch    800:      7
    branch    816:      6
    branch    857:      6
    branch    808:      4
    branch    825:      4

========================================================================
  SECTION 5: Trace prod REGNOs missing from Python REPOTXT
========================================================================

--- REGNO = AAM8888 ---
  Found 2 row(s) in raw LNNOTE:
    ACCTNO=8960559813  LOANTYPE=128.0  NTBRCH=209.0
      BALANCE=-3199.709999999999  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: False  |  BORSTAT ok: True
      LOANTEMP: ✗ NOT FOUND for acctno 8960559813
    ACCTNO=8960559813  LOANTYPE=983.0  NTBRCH=857.0
      BALANCE=80794.87  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: ✗ NOT FOUND for acctno 8960559813

--- REGNO = ABF5555 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8826480007  LOANTYPE=983.0  NTBRCH=809.0
      BALANCE=5173.67  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: ✗ NOT FOUND for acctno 8826480007

--- REGNO = ABT3993 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8837931705  LOANTYPE=983.0  NTBRCH=801.0
      BALANCE=34908.64  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: ✗ NOT FOUND for acctno 8837931705

--- REGNO = ACN712 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8978533325  LOANTYPE=983.0  NTBRCH=857.0
      BALANCE=14196.41  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: ✗ NOT FOUND for acctno 8978533325

--- REGNO = ACV7557 ---
  Found 2 row(s) in raw LNNOTE:
    ACCTNO=8151094605  LOANTYPE=700.0  NTBRCH=9.0
      BALANCE=-1366.07  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: False  |  BORSTAT ok: True
      LOANTEMP: ✗ NOT FOUND for acctno 8151094605
    ACCTNO=8968201720  LOANTYPE=983.0  NTBRCH=809.0
      BALANCE=30546.02  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: ✗ NOT FOUND for acctno 8968201720

--- REGNO = ACW6576 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8977552232  LOANTYPE=983.0  NTBRCH=857.0
      BALANCE=28740.05  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: ✗ NOT FOUND for acctno 8977552232

--- REGNO = ACA4798 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8994108018  LOANTYPE=983.0  NTBRCH=828.0
      BALANCE=12041.32  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: ✗ NOT FOUND for acctno 8994108018

--- REGNO = ACM6676 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8871689225  LOANTYPE=983.0  NTBRCH=34.0
      BALANCE=30678.400000000005  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: ✗ NOT FOUND for acctno 8871689225

========================================================================
  VERDICT
========================================================================
  Python REPOTXT expected rows: 301
  Prod  REPOTXT reported rows:  1000+ (per user)
  Gap:                         699+

  🔴 LOANTEMP.iccd2 has ZERO product=700 rows.
     If prod REPOTXT contains 700-type auto-HP, prod's LOANTEMP
     is DIFFERENT from iccd2.

  ✅ 2 KKI records survive to merged ARREAR>=10.

  Next steps based on trace results above:
  - If prod REGNOs found in LNNOTE but NOT in LOANTEMP: prod uses different LOANTEMP
  - If found in LOANTEMP but ARREAR < 10: prod's ARREAR definition differs
  - If found in both but not in merged output: ACCTNO format mismatch bug

Diagnostic complete.
