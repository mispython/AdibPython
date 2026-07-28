========= START JOB EIBWHP01 ==========
[DATE] Report date: 27-07-2026
[DATE] REPTMON: 202607
[DATE] NOWK: 31
[JOB FAILED] 'PosixPath' object does not support item assignment
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 386, in <module>
    run_job()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 326, in run_job
    INPUT_DIR["BNM"] = INPUT_DIR / f"loan{REPTMON}{NOWK}.sas7bdat"
TypeError: 'PosixPath' object does not support item assignment
