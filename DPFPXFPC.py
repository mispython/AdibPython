============================================================
EIIDLCRM - BNM LCR Reporting (Islamic Banking)
============================================================
SAS Connection established. Subprocess id is 3920786


Date: 05/10/2026 Week:1 Mon:10
Template: 70 items
  CIS equity warning: No such file or directory (os error 2): ...s/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cis/CIS.CUST.DAILY.parquet (set POLARS_VERBOSE=1 to see full path)

This error occurred with the following context stack:
        [1] 'parquet scan'
        [2] 'sink'

CIS: 0 records

Treasury...
  k1tbl: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k1tbl101.sas7bdat
  k3tbl: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k3tbl101.sas7bdat
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py", line 572, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py", line 330, in main
    k_records = process_treasury(rep_date)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py", line 125, in process_treasury
    ktbl, _dist = build_kalmliq(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/KALMLIQ.py", line 245, in build_kalmliq
    matdt = _parse_date(r.get("MATDT"))
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/KALMLIQ.py", line 45, in _parse_date
    y, m, d = s.split("-")[:3]
ValueError: not enough values to unpack (expected 3, got 1)
SAS Connection terminated. Subprocess id was 3920786
