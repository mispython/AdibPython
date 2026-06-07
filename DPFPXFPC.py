Starting EIBWSIBC Report Processing
============================================================
Processing for period: 05/26, Week: 4

------------------------------------------------------------
Extracting ELDS dates...
Warning: Could not extract date from SAS file /stgsrcsys/host/uat/sibc05264.sas7bdat: A logger name must be a string
First line of ELDSTX2: "******* File 2 ************* SAS Datawarehouse for 31-05-2026 ********************"                ...
ELDSDT1 (from SAS): None
ELDSDT2: 2026-05-31

------------------------------------------------------------
Reading BRH from: /sasdata/rawdata/lookup/LKP_BRANCH
BRH records after filtering BRSTAT='C': 376

------------------------------------------------------------
Processing ELN1 from SAS dataset: /stgsrcsys/host/uat/sibc05264.sas7bdat
ERROR processing ELN1 from SAS: A logger name must be a string
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/CONVERTED JOBS/EIBWSIBC.py", line 178, in process_eln1_from_sas
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

------------------------------------------------------------
Processing ELN2 from: /stgsrcsys/host/uat/BNMSIBC2.TXT
Total lines in ELDSTX2: 66
ELN2 records processed: 65
ERROR: No data processed from ELN1 or ELN2

Processing failed. Please check error messages above.
