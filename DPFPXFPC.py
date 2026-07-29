========== START JOB EIBWHP02 ==========
[INFO] REPTDATE=2026-07-28 -> {'NOWK': '4', 'NOWK1': '3', 'REPTMON': '07', 'REPTMON1': '07', 'REPTYEAR': '2026', 'REPTDAY': '28', 'RDATE': '28/07/26', 'SDATE': '23/07/26'}
[SHR] Validated input dataset: loan073.sas7bdat
[SHR] Validated input dataset: loan074.sas7bdat
[SHR] Validated input dataset: uloan074.sas7bdat
[WARN] SECTA_FORMAT / SECTB_FORMAT are both empty. Every row will be dropped by expand_sector_formats(). Fill in the real format mappings before trusting this report.
[JOB FAILED] Error reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP02/loan073.sas7bdat: read_sas7bdat() got an unexpected keyword argument 'num_processes'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 141, in read_sas_dataset
    df, meta = pyreadstat.read_sas7bdat(
  File "pyreadstat/pyreadstat.pyx", line 42, in pyreadstat.pyreadstat.read_sas7bdat
TypeError: read_sas7bdat() got an unexpected keyword argument 'num_processes'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 352, in <module>
    run_job()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 334, in run_job
    alw = build_alw(loan_prev_path, loan_curr_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 178, in build_alw
    alw1 = read_sas_dataset(loan_prev_path, usecols=ALW1_COLS, num_processes=NUM_READ_PROCESSES)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP02.py", line 152, in read_sas_dataset
    raise Exception(f"Error reading {file_path}: {e}")
Exception: Error reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWHP02/loan073.sas7bdat: read_sas7bdat() got an unexpected keyword argument 'num_processes'
