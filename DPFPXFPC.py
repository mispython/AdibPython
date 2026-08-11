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
  Second match: 0 matched, 8962081 unmatched
  Assigned new DEPIDs: 8962081 records
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTLCR.py", line 1200, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTLCR.py", line 1087, in main
    mni_sum, mni_detail = process_mni(rep_vars, excl_cis)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMTLCR.py", line 335, in process_mni
    mni_all = pl.concat(dfs_to_concat)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 234, in concat
    out = wrap_df(plr.concat_df(elems))
polars.exceptions.SchemaError: type String is incompatible with expected type Null
