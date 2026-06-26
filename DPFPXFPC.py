PROCESSING CONVENTIONAL BANKING FLOAT DATA
==================================================
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBFLOAT_FLOAT.py", line 455, in <module>
    conventional_result = process_conventional_float()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBFLOAT_FLOAT.py", line 41, in process_conventional_float
    fdmthly_df = read_sas_file(mni_path / "fdmthly.sas7bdat")
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBFLOAT_FLOAT.py", line 18, in read_sas_file
    with sas7bdat.SAS7BDAT(file_path) as reader:
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/sas7bdat.py", line 334, in __init__
    self.logger = self._make_logger(level=log_level)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/sas7bdat.py", line 418, in _make_logger
    logger = logging.getLogger(self.path)
  File "/usr/lib64/python3.9/logging/__init__.py", line 2042, in getLogger
    return Logger.manager.getLogger(name)
  File "/usr/lib64/python3.9/logging/__init__.py", line 1297, in getLogger
    raise TypeError('A logger name must be a string')
TypeError: A logger name must be a string
