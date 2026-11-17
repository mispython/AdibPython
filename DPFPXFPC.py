Reading DATEFILE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/DATEFILE
Islamic Daily Loan Movement - 31/08/2026
  EXTDATE  : 8312026255
  REPTDATE : 2026-08-31  (num: 260831)
  PREVDATE : 2026-08-30
  DLETDATE : 2026-08-28
  REPTMON  : 08
  REPTDAY  : 31
  LNDLY_PREVYEAR : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/lndly30.sas7bdat
  *** TESTING MODE: row cap = 1,000,000 per SAS7BDAT input ***
Using SAS Config named: default
SAS Connection established. Subprocess id is 407206

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
---- LIBREF PROBE FAILED — SAS cannot write to OUTPUT_DIR ----

21   ods listing close;ods html5 (id=saspy_internal) file=stdout options(bitmap_mode='inline') device=svg style=HTMLBlue; ods
21 ! graphics on / outputfmt=png;
NOTE: Writing HTML5(SASPY_INTERNAL) Body file: STDOUT
22   
23   libname _eiidprobe '/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN';
ERROR: _eiidprobe is not a valid SAS name.
ERROR: Error in the LIBNAME statement.
24   
25   ods html5 (id=saspy_internal) close;ods listing;

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLOAN.py", line 121, in <module>
    raise RuntimeError(
RuntimeError: SAS process cannot create libref at /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN. Check mount/permissions between the Python and SAS subprocesses.
SAS Connection terminated. Subprocess id was 407206
