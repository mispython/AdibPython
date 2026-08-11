✓ PBBLNFMT module loaded successfully
============================================================
EIBMTLCR - Top Depositors Report
============================================================

Report Date: 31/07/2026
Report Month: 07

Loading exclusion lists...
  Loaded CIS exclusions: 99 records
  Loaded EQU exclusions: 62 records
Exclusions: CIS=99, EQU=62

========================================
Processing M&I...
========================================
  Reading CMM: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTLCR/cmm07.sas7bdat
  CMM loaded: 8968532 records, 26 columns
  Reading VOSTRO: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTLCR/vostro.sas7bdat
  VOSTRO loaded: 26 records, 6 columns
  CISINFO loaded: 9771249 records
  VOSTRO after CISINFO merge: 26 records
  Combined CMM+VOSTRO: 8968558 records, 26 columns
  COF_MNI_DEPOSITOR_LIST loaded: 2558 records
  COF_IDNO for NEWIC merge: 1236 records
  After NEWIC merge: 8968558 records
  First match: 6477 matched, 8962081 unmatched
  Second match: 916 matched, 8961165 unmatched
  Assigned new DEPIDs: 8961165 records
  Total M&I records: 8968558
  M&I records after product filter: 8968558
  M&I summary: 4341087 groups
M&I Summary: 4341087 groups
M&I Detail: 8968558 records

========================================
Processing Equity...
========================================
  Reading EQU: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBMTLCR/equ07.sas7bdat
  EQU loaded: 6455 records, 21 columns
  EQU after CUSTNO filter: 6283 records
  COF_EQU_DEPOSITOR_LIST loaded: 2750 records
  COF_EQU for merge: 2749 records
  EQU match: 4869 matched, 1414 unmatched
  Assigned new EQU DEPIDs: 1414 records
  Total EQU records: 6283
  EQU records after product filter: 4723
  EQU summary: 655 groups
Equity Summary: 655 groups
Equity Detail: 4723 records

========================================
Consolidating...
========================================
  TOT2 summary: 4341678 groups
  Product summary: 4341654 groups
Consolidated Detail: 4341686 records
TOT2 Summary: 4341678 groups
Product Summary: 4341654 groups

========================================
Generating reports...
========================================
  Generated 50 Individual records
  Generated 50 Corporate records
/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTLCR.py:834: UserWarning: Comparisons with None always result in null. Consider using `.is_null()` or `.is_not_null()`.
  (pl.col('DEPID') == depid) &
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTLCR.py", line 1160, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTLCR.py", line 1114, in main
    prod_lines, prod_top = generate_top100_by_product(alltot, mni_detail, equ_detail, rep_vars, f"{PATHS['OUTPUT']}COFOUT1.txt")
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTLCR.py", line 925, in generate_top100_by_product
    lines.append(f"{row['RANK']}{dlm}{row['DEPGRP']}{dlm}"
TypeError: unsupported format string passed to list.__format__
