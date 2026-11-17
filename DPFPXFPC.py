Reading DATEFILE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/DATEFILE
Islamic Daily Loan Movement - 16/11/2026
  EXTDATE  : 11162026255
  REPTDATE : 2026-11-16  (num: 261116)
  PREVDATE : 2026-11-15
  DLETDATE : 2026-11-13
  REPTMON  : 11
  REPTDAY  : 16
  LNDLY_PREVYEAR : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/lndly15.sas7bdat
  *** TESTING MODE: row cap = 1,000,000 per SAS7BDAT input ***
Using SAS Config named: default
SAS Connection established. Subprocess id is 440843

SAS libref probe OK for /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN
Reading LNNOTE (chunked): /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note_d16.sas7bdat
  Ingesting enrh_ln_note_d16.sas7bdat: 8,125,473 rows available (TESTING cap -> reading 1,000,000)
    chunk 1: rows 500,000 / 1,000,000
    chunk 2: rows 1,000,000 / 1,000,000
  Finished loan_raw: 1,000,000 rows (cap 1,000,000)
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/lndly16.sas7bdat (1,000,000 rows)
Reading previous day (SAS7BDAT): /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/lndly15.sas7bdat
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLOAN.py", line 293, in <module>
    ingest_sas7bdat_to_table(
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIDLOAN.py", line 173, in ingest_sas7bdat_to_table
    raise ValueError(f"{sas_file.name}: missing columns {miss}")
ValueError: lndly15.sas7bdat: missing columns ['PENDBRH']
SAS Connection terminated. Subprocess id was 440843
