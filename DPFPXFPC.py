EIMRESHI - HP Loan Summary & Detail Report
============================================================
============================================================
Report Date: 21/07/2026
Week: 4
RDATE: 210726
============================================================

Reading loan data from SAS files...
  Reading loantemp.sas7bdat...
  LOANTEMP raw rows: 663,747
  LOANTEMP after filtering: 387,612 rows
  Reading lnnote.sas7bdat (chunked, filtered as it streams)...
  Error: too many values to unpack (expected 2)
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHI.py", line 226, in <module>
    df_lnnote = read_lnnote_filtered(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMRESHI.py", line 140, in read_lnnote_filtered
    chunk_iter, meta = pyreadstat.read_file_in_chunks(
ValueError: too many values to unpack (expected 2)
