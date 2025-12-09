Processing BRH...
Extracting ELDS dates...
ELDSDT1: 2025-09-05, ELDSDT2: 2025-09-05
Processing ELN1...
Error: 'ExprStringNameSpace' object has no attribute 'strip'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBWSIBC_SIB.py", line 305, in <module>
    result = process_loan_reports(BASE_PATH)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBWSIBC_SIB.py", line 242, in process_loan_reports
    eln1_df = process_eln1(eldstxt_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/EIBWSIBC_SIB.py", line 92, in process_eln1
    pl.col('raw_line').str.slice(0, 13).str.strip().alias('AANO'),
AttributeError: 'ExprStringNameSpace' object has no attribute 'strip'
