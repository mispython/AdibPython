File: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note.sas7bdat
Exists: True
Size: 10,059,644,928 bytes

============================================================
1. METADATA
============================================================
Total rows (from metadata): 8,149,154
Total columns: 499
  ENTITY_CD: type=NULL, label=None
  BORSTAT: type=NULL, label=None
  LOANTYPE: type=NULL, label=None
  ACCTNO: type=NULL, label=None
  NOTENO: type=NULL, label=None

============================================================
2. ENTITY_CD DISTRIBUTION (full file)
============================================================
Read 8,149,154 rows in 188.6s

Raw ENTITY_CD value counts (top 30):
                    '' :    6,327,413
                'PIBB' :    1,821,741

Stripped ENTITY_CLEAN value counts (top 30):
                    '' :    6,327,413
                'PIBB' :    1,821,741

PIBB matches:
  exact 'PIBB'          : 1,821,741
  stripped == 'PIBB'    : 1,821,741
  contains 'PIBB'       : 1,821,741

============================================================
3. BORSTAT DISTRIBUTION
============================================================
Read 8,149,154 rows in 203.5s

BORSTAT across WHOLE file (stripped):
          '' :    7,988,790
         'F' :       81,106
         'W' :       19,768
         'C' :       17,052
         'A' :       10,917
         'R' :       10,653
         'E' :        4,624
         'B' :        4,149
         'M' :        3,720
         'X' :        2,407
         'Z' :        1,980
         'S' :        1,752
         'G' :          474
         'Y' :          364
         'P' :          341
         'N' :          258
         'D' :          221
         'K' :          214
         'I' :          170
         'T' :          158
         'Q' :           30
         '0' :            6

Rows with ENTITY_CD='PIBB': 1,821,741
  BORSTAT distribution within PIBB:
            '' :    1,779,787
           'F' :       23,280
           'W' :        7,070
           'R' :        2,811
           'C' :        2,735
           'A' :        2,182
           'B' :        1,486
           'M' :        1,241
           'P' :          251
           'X' :          242
           'E' :          238
           'G' :          141
           'N' :           76
           'K' :           52
           'T' :           42
           'D' :           35
           'I' :           29
           'S' :           20
           'Z' :           17
           'Y' :            5
           '0' :            1

  LOANTYPE distribution within PIBB (top 20):
    128.0 :    1,183,890
    135.0 :      241,803
    152.0 :      120,635
    983.0 :       41,862
    131.0 :       26,104
    142.0 :       25,788
    154.0 :       22,674
    175.0 :       20,731
    151.0 :       13,679
    113.0 :       13,113
    112.0 :       12,670
    136.0 :        9,243
    652.0 :        7,886
    422.0 :        5,891
    409.0 :        5,654
    413.0 :        4,962
    464.0 :        4,578
    195.0 :        4,433
    176.0 :        4,381
    194.0 :        4,180

  PIBB & BORSTAT='A': 2,182
  PIBB & BORSTAT='A' & LOANTYPE not excluded: 297

============================================================
4. FIRST 500K ROWS vs FULL FILE
============================================================
PIBB in first 500k rows : 27
PIBB in whole file      : 1,821,741
Test-mode captures      : 0.001% of all PIBB rows

ENTITY_CD in first 500k (top 10):
                    '' :      499,973
                'PIBB' :           27

============================================================
5. PHYSICAL POSITION OF PIBB ROWS
============================================================
PIBB row positions (physical):
  min : 814
  max : 8,149,153
  mean: 5,899,877
  total file rows: 8,149,154
  PIBB rows in first 500k: 27
