============================================================
Processing RPVBDATA dates
============================================================
✓ TBDATE: 20260501 → REPTDT: 0426, PREVDT: 0326

============================================================
Processing SRSDATA dates
============================================================
✓ TBDATE: 20260519 → SRSTDT: 0526

============================================================
Date validation
============================================================
✗ THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:0526)
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMREPO_UAT.py", line 209, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/LOAN/EIBMREPO_UAT.py", line 155, in main
    raise RuntimeError(error_msg)
RuntimeError: THE SAP.PBB.RPVB.TEXT IS NOT DATED (MMYY:0526)
