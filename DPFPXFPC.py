Using SAS Config named: default
SAS Connection established. Subprocess id is 3644795

Reading DATEFILE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/DATEFILE
Islamic Daily Loan Movement - 31/08/2026
  EXTDATE  : 8312026255
  REPTDATE : 2026-08-31  (num: 260831)
  PREVDATE : 2026-08-30
  DLETDATE : 2026-08-28
  REPTMON  : 08
  LNDLY_PREVYEAR : /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/lndly{prevday}.sas7bdat
  *** TESTING MODE: row cap = 1,000,000 per SAS7BDAT input ***
Reading LNNOTE (chunked): /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/a_lnnote/enrh_ln_note.sas7bdat
  Ingesting enrh_ln_note.sas7bdat: 8,149,154 rows available (TESTING cap -> reading 1,000,000)
    chunk 1: rows 500,000 / 1,000,000
    chunk 2: rows 1,000,000 / 1,000,000
  Finished loan_raw: 1,000,000 rows (cap 1,000,000)
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/lndly31.sas7bdat (1,000,000 rows)
Reading previous day (SAS7BDAT): /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/lndly{prevday}.sas7bdat
  WARNING: no previous-day SAS7BDAT found — previous-day tables will be empty.
Reading LKP_BRANCH: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/LKP_BRANCH
  Loaded 376 branch records
error occured in SAS during data transfer. Check the LOG for issues.
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/mloan_31-08-2026.sas7bdat (320 rows)
error occured in SAS during data transfer. Check the LOG for issues.
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/mcred_31-08-2026.sas7bdat (60 rows)
error occured in SAS during data transfer. Check the LOG for issues.
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/mhp_31-08-2026.sas7bdat (119 rows)
  Skipping movement reports (no previous-day source found).

Islamic Daily Loan Movement Report Complete
Date: 31/08/2026

Previous-day source used: NONE (movement reports skipped)

Branch Summaries:
  1. MLOAN - Term Loan Outstanding by Branch
  2. MCRED - Revolving Credit Outstanding by Branch
  3. MHP   - HP Outstanding by Branch

Customer Movements (>= RM 500K):
  - Term Loans       : 0 accounts
  - Revolving Credit : 0 accounts
  - HP Loans         : 0 accounts

Output Files (SAS7BDAT):
  - mloan_31-08-2026.sas7bdat
  - mcred_31-08-2026.sas7bdat
  - mhp_31-08-2026.sas7bdat
  - dmloan_31-08-2026.sas7bdat   (emitted only if previous-day source present)
  - dmcred_31-08-2026.sas7bdat   (emitted only if previous-day source present)
  - dmhp_31-08-2026.sas7bdat     (emitted only if previous-day source present)
  - lndly31.sas7bdat

Testing mode: ON (MAX_ROWS = 1000000)

SAS Connection terminated. Subprocess id was 3644795
Completed: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN
