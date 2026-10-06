KALMLIQ loaded from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/KALMLIQ.py
============================================================
EIIDLCRM - BNM LCR Reporting (Islamic Banking)
============================================================
SAS Connection established. Subprocess id is 3989103


Date: 05/10/2026 Week:1 Mon:10
Template: 70 items
  CIS file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBDLCRM/cis/CIS_CUST_DAILY.parquet
CIS: 17611 records

Treasury...
  k1tbl: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k1tbl101.sas7bdat
  k3tbl: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k3tbl101.sas7bdat
  k1tbl exists: True
  k3tbl exists: True
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py", line 672, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py", line 421, in main
    k_records = process_treasury(rep_date)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLCRM.py", line 141, in process_treasury
    ktbl, _dist = build_kalmliq(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/KALMLIQ.py", line 236, in build_kalmliq
    k1tbl = _build_k1tbl(k1tbl_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/KALMLIQ.py", line 60, in _build_k1tbl
    raw = _read_sas_kapiti(k1tbl_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/KALMLIQ.py", line 30, in _read_sas_kapiti
    df = con.execute(f"SELECT * FROM read_parquet('{Path(path).as_posix()}')").pl()
_duckdb.InvalidInputException: Invalid Input Error: No magic bytes found at end of file '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLCRM/bnmk/k1tbl101.sas7bdat'

LINE 1: SELECT * FROM read_parquet('/sas/python/virt_edw/Data_Warehouse/MIS/XMIS...
                      ^
SAS Connection terminated. Subprocess id was 3989103
