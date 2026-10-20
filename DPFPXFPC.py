EIMREPOI REPOTXT Parity Diagnostic
========================================================================
LNNOTE:   /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m09.sas7bdat
LOANTEMP: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMRESHI/loantemp.sas7bdat
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
  LOANTEMP total rows: 663,747
  ARREAR >= 10:        48,360
  product=700 count:   0

--- LOANTEMP: ARREAR >= 10 by branch (top 20) ---
    branch    802:  3,659
    branch    820:  2,930
    branch    821:  2,136
    branch    806:  2,023
    branch    801:  1,978
    branch    805:  1,897
    branch    826:  1,739
    branch    808:  1,734
    branch    824:  1,715
    branch    811:  1,647
    branch    814:  1,645
    branch    809:  1,609
    branch    827:  1,545
    branch    804:  1,514
    branch    818:  1,507
    branch    816:  1,257
    branch    862:  1,073
    branch    812:    968
    branch    822:    839
    branch    844:    797

  KKI (branch 117) with ARREAR >= 10: 758
  KKI products with arrears:
    product    983:    744
    product    651:      3
    product    652:      3
    product    666:      2
    product    128:      2
    product    154:      1
    product    664:      1
    product    657:      1
    product    655:      1

========================================================================
  SECTION 4: Merge LNNOTE ↔ LOANTEMP on ACCTNO
========================================================================
  LNNOTE filtered unique acctnos: 1,729,838
  LOANTEMP unique acctnos:        643,549
  Overlap (matched):              394,422 (22.8% of LNNOTE)

  After merge, ARREAR >= 10: 20,806

--- Merged ARREAR >= 10 — by LNNOTE loantype ---
    LNNOTE loantype    983: 20,725
    LNNOTE loantype    128:     81

--- Merged ARREAR >= 10 — by LOANTEMP product ---
    LOANTEMP product    983: 20,584
    LOANTEMP product    128:    222

--- Merged ARREAR >= 10 — by branch (top 20) ---
    branch    802:  1,431
    branch    820:  1,244
    branch    827:  1,127
    branch    821:    884
    branch    806:    866
    branch    824:    849
    branch    805:    820
    branch    804:    816
    branch    826:    787
    branch    801:    769
    branch    862:    762
    branch    814:    761
    branch    808:    704
    branch    811:    661
    branch    809:    625
    branch    818:    598
    branch    117:    488
    branch    816:    440
    branch    844:    401
    branch    812:    384

========================================================================
  SECTION 5: Trace prod REGNOs missing from Python REPOTXT
========================================================================

--- REGNO = AAM8888 ---
  Found 2 row(s) in raw LNNOTE:
    ACCTNO=8960559813  LOANTYPE=128.0  NTBRCH=209.0
      BALANCE=-3199.709999999999  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: False  |  BORSTAT ok: True
      LOANTEMP: product=983.0  arrear=16.0  branch=857.0
        → WOULD APPEAR in REPOTXT if ACCTNO joined
    ACCTNO=8960559813  LOANTYPE=983.0  NTBRCH=857.0
      BALANCE=80794.87  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: product=983.0  arrear=16.0  branch=857.0
        → WOULD APPEAR in REPOTXT if ACCTNO joined

--- REGNO = ABF5555 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8826480007  LOANTYPE=983.0  NTBRCH=809.0
      BALANCE=5173.67  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: product=983.0  arrear=16.0  branch=809.0
        → WOULD APPEAR in REPOTXT if ACCTNO joined
      LOANTEMP: product=983.0  arrear=16.0  branch=809.0
        → WOULD APPEAR in REPOTXT if ACCTNO joined

--- REGNO = ABT3993 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8837931705  LOANTYPE=983.0  NTBRCH=801.0
      BALANCE=34908.64  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: product=983.0  arrear=16.0  branch=801.0
        → WOULD APPEAR in REPOTXT if ACCTNO joined

--- REGNO = ACN712 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8978533325  LOANTYPE=983.0  NTBRCH=857.0
      BALANCE=14196.41  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: product=983.0  arrear=16.0  branch=857.0
        → WOULD APPEAR in REPOTXT if ACCTNO joined

--- REGNO = ACV7557 ---
  Found 2 row(s) in raw LNNOTE:
    ACCTNO=8151094605  LOANTYPE=700.0  NTBRCH=9.0
      BALANCE=-1366.07  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: False  |  BORSTAT ok: True
      LOANTEMP: ✗ NOT FOUND for acctno 8151094605
    ACCTNO=8968201720  LOANTYPE=983.0  NTBRCH=809.0
      BALANCE=30546.02  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: product=983.0  arrear=16.0  branch=809.0
        → WOULD APPEAR in REPOTXT if ACCTNO joined

--- REGNO = ACW6576 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8977552232  LOANTYPE=983.0  NTBRCH=857.0
      BALANCE=28740.05  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: product=983.0  arrear=16.0  branch=857.0
        → WOULD APPEAR in REPOTXT if ACCTNO joined

--- REGNO = ACA4798 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8994108018  LOANTYPE=983.0  NTBRCH=828.0
      BALANCE=12041.32  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: product=983.0  arrear=16.0  branch=828.0
        → WOULD APPEAR in REPOTXT if ACCTNO joined

--- REGNO = ACM6676 ---
  Found 1 row(s) in raw LNNOTE:
    ACCTNO=8871689225  LOANTYPE=983.0  NTBRCH=34.0
      BALANCE=30678.400000000005  BORSTAT=''
      LOANTYPE IN HP: True  |  BALANCE>0: True  |  BORSTAT ok: True
      LOANTEMP: product=983.0  arrear=16.0  branch=844.0
        → WOULD APPEAR in REPOTXT if ACCTNO joined
      LOANTEMP: product=983.0  arrear=16.0  branch=844.0
        → WOULD APPEAR in REPOTXT if ACCTNO joined

========================================================================
  VERDICT
========================================================================
  Python REPOTXT expected rows: 20806
  Prod  REPOTXT reported rows:  1000+ (per user)
  Gap:                         -19806+

  🔴 LOANTEMP.iccd2 has ZERO product=700 rows.
     If prod REPOTXT contains 700-type auto-HP, prod's LOANTEMP
     is DIFFERENT from iccd2.

  ✅ 488 KKI records survive to merged ARREAR>=10.

  Next steps based on trace results above:
  - If prod REGNOs found in LNNOTE but NOT in LOANTEMP: prod uses different LOANTEMP
  - If found in LOANTEMP but ARREAR < 10: prod's ARREAR definition differs
  - If found in both but not in merged output: ACCTNO format mismatch bug

Diagnostic complete.
