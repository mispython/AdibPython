Reading DATEFILE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/DATEFILE
Islamic Daily Loan Movement - 16/11/2026
  EXTDATE  : 11162026255
  REPTDATE : 2026-11-16  (num: 261116)
  PREVDATE : 2026-11-15
  DLETDATE : 2026-11-13
  REPTMON  : 11
  REPTDAY  : 16
  LNDLY_PREVYEAR : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/lndly15.sas7bdat
  *** TESTING MODE: row cap = 500,000 per SAS7BDAT input ***
Using SAS Config named: default
SAS Connection established. Subprocess id is 531249

SAS libref probe OK for /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN
Reading LNNOTE (chunked): /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note_d16.sas7bdat
  Ingesting enrh_ln_note_d16.sas7bdat: 8,125,473 rows available (TESTING cap -> reading 500,000); branch col = PENDBRH
    chunk 1: rows 500,000 / 500,000
  Finished loan_raw: 500,000 rows (cap 500,000)
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/lndly16.sas7bdat (500,000 rows)
Reading previous day (SAS7BDAT): /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/lndly15.sas7bdat
  Ingesting lndly15.sas7bdat: 1,744,761 rows available (TESTING cap -> reading 500,000); branch col = BRANCH
    chunk 1: rows 500,000 / 500,000
  Finished prev_loan: 500,000 rows (cap 500,000)
  Using previous-day source: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/lndly15.sas7bdat
Reading LKP_BRANCH: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/LKP_BRANCH
  Loaded 376 branch records
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/mloan_16-11-2026.sas7bdat (317 rows)
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/mcred_16-11-2026.sas7bdat (47 rows)
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/mhp_16-11-2026.sas7bdat (81 rows)
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/dmloan_16-11-2026.sas7bdat (1,923 rows)
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/dmcred_16-11-2026.sas7bdat (13 rows)
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/dmhp_16-11-2026.sas7bdat (0 rows)

Islamic Daily Loan Movement Report Complete
Date: 16/11/2026

Previous-day source used: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/lndly15.sas7bdat

Branch Summaries:
  1. MLOAN - Term Loan Outstanding by Branch
  2. MCRED - Revolving Credit Outstanding by Branch
  3. MHP   - HP Outstanding by Branch

Customer Movements (>= RM 500K):
  - Term Loans       : 1923 accounts
  - Revolving Credit : 13 accounts
  - HP Loans         : 0 accounts

Output Files (SAS7BDAT):
  - mloan_16-11-2026.sas7bdat
  - mcred_16-11-2026.sas7bdat
  - mhp_16-11-2026.sas7bdat
  - dmloan_16-11-2026.sas7bdat   (emitted only if previous-day source present)
  - dmcred_16-11-2026.sas7bdat   (emitted only if previous-day source present)
  - dmhp_16-11-2026.sas7bdat     (emitted only if previous-day source present)
  - lndly16.sas7bdat

Testing mode: ON (MAX_ROWS = 500000)

SAS Connection terminated. Subprocess id was 531249
Completed: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN
