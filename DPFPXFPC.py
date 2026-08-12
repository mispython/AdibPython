============================================================
EIIMTLCR - Top Depositors Report (Islamic Banking)
============================================================
Start time: 2026-08-12 11:55:19

Available disk space: 71.10 MB
WARNING: Low disk space!

Report Date: 31/07/2026
Report Month: 07
Output directory: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTLCR/

Reading CIS exclusion list: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/list/keep_top_dep_excl_pibb.sas7bdat
  Read keep_top_dep_excl_pibb.sas7bdat: 99 rows, 4 columns
  CIS exclusions: 99
Reading EQU exclusion list: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/list/keep_top_dep_excl_equ_pibb.sas7bdat
  Read keep_top_dep_excl_equ_pibb.sas7bdat: 75 rows, 2 columns
  EQU exclusions: 75

============================================================
M&I PROCESSING
============================================================

Reading CMM file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/cmm07.sas7bdat
  Read cmm07.sas7bdat: 8968532 rows, 26 columns
CMM columns: ['branch', 'acctno', 'cdno', 'amount', 'product', 'intplan', 'curcode', 'matdt', 'remmth', 'rem30d', 'bnmcode', 'custcd', 'billerind', 'nid_cdno', 'custno', 'oldic', 'newic', 'custname', 'pbmerch', 'ecp', 'sme_tag', 'bic', 'cmmcode', 'toticbal', 'toticeqbal', 'totdpbal']
CMM NEWIC sample: ['AA003674', 'AA004130', 'AA012903', 'AA016213', 'AA018277']
CMM CUSTNO sample: [3575167.0, 10015615.0, 2732154.0, 5758918.0, 2670106.0]
Records excluded: 752/8968532

Reading COF file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/list/icof_mni_depositor_list.sas7bdat
  Read icof_mni_depositor_list.sas7bdat: 2401 rows, 11 columns
COF columns: ['depid', 'depgrp', 'custname', 'custno', 'bussreg', 'acctno', 'mniaddl1', 'mniaddl2', 'ubo', 'depid_ubo', 'depgrp_ubo']
COF unique BUSSREG: 1238
COF BUSSREG sample: ['RTPK2157', '924363W', '790895D', 'EQ116078A', '388561T']

Merging CMM (8968532) with COF by NEWIC...
Matched by NEWIC: 6296
Unmatched by NEWIC: 8962236

Second merge: Unmatched (8962236) with COF by CUSTNO...
Matched by CUSTNO: 260

Assigning new DEPID for 4340961 remaining unique customers...
New DEPID range: 5001 to 4345961
  Group 1 (matched by NEWIC): 6296
  Group 2 (matched by CUSTNO): 260
  Group 3 (new DEPID): 8961976

Total M&I records: 8968532

Classifying products...
After removing non-M&I BICs: 8968532 (removed 0)

Summarizing by DEPID/DEPGRP/CUSTYPE...
M&I summary: 4341078 groups
Sample:
   depid                        depgrp custype   fd   sa           ca  rnid  fd2  sa2          ca2  rnid2
0    1.0                     AIA GROUP       C  0.0  0.0  31017315.96   0.0  0.0  0.0  31017315.96    0.0
1    2.0  BANK PEMBANGUNAN M'SIA GROUP       C  0.0  0.0    206123.97   0.0  0.0  0.0    206123.97    0.0
2    6.0                 SHIMANO GROUP       C  0.0  0.0  17137502.14   0.0  0.0  0.0  17137502.14    0.0
CUSTYPE distribution: {'I': 3906571, 'C': 434507}

============================================================
EQUITY PROCESSING
============================================================

Reading EQU file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/equ07.sas7bdat
  Read equ07.sas7bdat: 6455 rows, 21 columns
EQU columns: ['amount', 'curcode', 'dealtype', 'dealref', 'matdt', 'custfiss', 'remmth', 'rem30d', 'ori30d', 'bnmcode', 'custno', 'custname', 'acctno', 'custid', 'cisno', 'cisname', 'icno', 'bic', 'cmmcode', 'nsfcode', 'icgrp']
EQU CUSTNO sample: ['BBMB', 'BNP', 'CHASE', 'AMBANK', 'AMBANK']
After filtering empty CUSTNO: 6283 (removed 172)
Excluded EQU records: 1204/6283

Reading EQU COF file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIMTLCR/list/icof_equ_depositor_list.sas7bdat
  Read icof_equ_depositor_list.sas7bdat: 2182 rows, 9 columns
EQU COF columns: ['depid', 'depgrp', 'custname', 'custno', 'linkid', 'ubo', 'depid_ubo', 'depgrp_ubo', 'linkid_ubo']
EQU COF unique CUSTNO: 2148

Merging EQU (6283) with COF by CUSTNO...
Matched by CUSTNO: 3240
Unmatched by CUSTNO: 3043

Assigning new DEPID for 924 unique EQU customers...
New DEPID range: 50005001 to 50005924
  Matched records: 3240
  New DEPID records: 3043

Total EQU records: 6283

Classifying Equity products...

Summarizing Equity by LINKID/DEPGRP/CUSTYPE...
Equity summary: 967 groups
Sample:
   depid                        depgrp custype  std  nid  ibb  repo  std2  nid2
0    1.0                     AIA GROUP       C  0.0  0.0  0.0   0.0   0.0   0.0
1    2.0  BANK PEMBANGUNAN M'SIA GROUP       C  0.0  0.0  0.0   0.0   0.0   0.0
2    3.0                DEUTSCHE GROUP       C  0.0  0.0  0.0   0.0   0.0   0.0
CUSTYPE distribution: {'C': 919, 'I': 48}

============================================================
CONSOLIDATION
============================================================
M&I groups: 4341078
Equity groups: 967

Merging by DEPID...
After merge: 4342019 rows

Final consolidated groups: 4342019
CUSTYPE distribution:
custype
I    3906619
C     435400
Name: count, dtype: int64

Top 5 by TOT2:
               depid                  depgrp custype          tot2  mni           equ
4341384  100005297.0      TEH LI SHIAN DIONA       I  7.735634e+09  0.0  7.735634e+09
4342007  100005913.0  YAO SU JUNG-SAVINGS AC       I  1.652374e+09  0.0  1.652374e+09
4341902  100005810.0    TEH LEE PANG WILLIAM       I  1.248374e+09  0.0  1.248374e+09
4341222  100005136.0     ANGELA TEH JIA YING       I  6.277710e+08  0.0  6.277710e+08
4341928  100005836.0     TEH LI MING LILLIAN       I  4.822450e+08  0.0  4.822450e+08

============================================================
GENERATING REPORTS
============================================================

============================================================
TOP 50 INDIVIDUAL REPORT
============================================================
Total Individual groups: 3906619
Top 50 TOT2 range: 6,523,470.41 to 7,735,633,500.00
Report written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTLCR/COFOUTI.txt
File size: 3,002 bytes

Generating detail listing for 50 depositors...
  Processing 1/50: TEH LI SHIAN DIONA
  Processing 11/50: TAN SRI DATO SRI TAY AH LE
  Processing 21/50: LIM SIEW SOOI
  Processing 31/50: ESA BIN MOHAMED
  Processing 41/50: LEU HUANG DING
Detail listing completed: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTLCR/COFOUTI.txt
File size: 80,859 bytes

============================================================
TOP 50 CORPORATE REPORT
============================================================
Total Corporate groups: 435400
Top 50 TOT2 range: 34,548,745.56 to 358,391,505.10
Report written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTLCR/COFOUTC.txt
File size: 3,407 bytes

Generating detail listing for 50 depositors...
  Processing 1/50: KHAZANAH NASIONAL BERHAD
  Processing 11/50: IJM PROPERTIES SDN BHD
  Processing 21/50: FLP REALTY SDN. BHD.
  Processing 31/50: DUPLAS MARKETING SDN BHD
  Processing 41/50: LYNAS MALAYSIA SDN BHD
Detail listing completed: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTLCR/COFOUTC.txt
File size: 444,734 bytes

============================================================
TOP 100 BY PRODUCT REPORT
============================================================
Top 100 TOT range: 29,277,202.96 to 7,735,633,500.00
Report written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTLCR/COFOUT1.txt
File size: 9,068 bytes

Generating detail listing for 100 depositors...
  Processing 1/100: TEH LI SHIAN DIONA
  Processing 11/100: STANDARD CHARTERED GROUP
  Processing 21/100: CHINA CONSTRUCTION BANK (M
  Processing 31/100: SUNWAY GROUP
  Processing 41/100: LBS BINA GROUP BERHAD
  Processing 51/100: TAIWAN BUSINESS BANK TW
  Processing 61/100: AKAUNTAN NEGARA MALAYSIA
  Processing 71/100: BE INTERNATIONAL SDN BHD
  Processing 81/100: SOUTH ASIA FIBRE INDUSTRIES SD
  Processing 91/100: WONG AND LIM CONTRACTORS
Detail listing completed: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTLCR/COFOUT2.txt
File size: 549,932 bytes

============================================================
MATURITY REPORT
============================================================
  Processing maturity 1/100: TEH LI SHIAN DIONA
  Processing maturity 11/100: STANDARD CHARTERED GROUP
  Processing maturity 21/100: CHINA CONSTRUCTION BANK (M
  Processing maturity 31/100: SUNWAY GROUP
  Processing maturity 41/100: LBS BINA GROUP BERHAD
  Processing maturity 51/100: TAIWAN BUSINESS BANK TW
  Processing maturity 61/100: AKAUNTAN NEGARA MALAYSIA
  Processing maturity 71/100: BE INTERNATIONAL SDN BHD
  Processing maturity 81/100: SOUTH ASIA FIBRE INDUSTRIES SD
  Processing maturity 91/100: WONG AND LIM CONTRACTORS
Report written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIMTLCR/COFOUT3.txt
File size: 21,254 bytes

============================================================
CLEANUP
============================================================

Output files:
  COFOUTI.txt: 79.0 KB
  COFOUTC.txt: 434.3 KB
  COFOUT1.txt: 8.9 KB
  COFOUT2.txt: 537.0 KB
  COFOUT3.txt: 20.8 KB

End time: 2026-08-12 12:04:28
============================================================
✓ EIIMTLCR Complete
