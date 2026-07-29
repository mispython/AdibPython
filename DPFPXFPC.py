========== START JOB EIBWHP03 ==========
Report Date: 2026-07-28
Report Month: 07
Report Week: 4
Python version: 3.9.25 (main, Nov 26 2025, 08:47:37) 
[GCC 8.5.0 20210514 (Red Hat 8.5.0-28)]
[SHR] Validated input dataset: LOAN_CURRENT - loan074.sas7bdat
[SHR] Validated input dataset: LOAN_PREVIOUS - loan073.sas7bdat
[SHR] Validated input dataset: ULOAN_CURRENT - uloan074.sas7bdat
[EXEC] Executing EIBWHP03 business logic...
[READ] Successfully read: loan074.sas7bdat
[INFO] Records: 2636878, Columns: 412
[INFO] Column names: ACCTNO, NAME, CUSTCODE, NOTENO, ASSMDATE...
[JOB FAILED] Error reading SAS7BDAT file /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP03/loan074.sas7bdat with pyreadstat: 'metadata_container' object has no attribute 'encoding'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP03.py", line 186, in read_sas7bdat
    print(f"[INFO] File encoding: {meta.encoding}")
AttributeError: 'metadata_container' object has no attribute 'encoding'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP03.py", line 396, in <module>
    run_job()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP03.py", line 352, in run_job
    processed_records, spool_lines = execute_business_logic()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP03.py", line 234, in execute_business_logic
    data_frames[name] = read_sas7bdat(path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP03.py", line 193, in read_sas7bdat
    raise Exception(f"Error reading SAS7BDAT file {file_path} with pyreadstat: {e}")
Exception: Error reading SAS7BDAT file /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP03/loan074.sas7bdat with pyreadstat: 'metadata_container' object has no attribute 'encoding'
