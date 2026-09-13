Reading DATEFILE: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/DATEFILE
Islamic Daily Loan Movement - 31/08/2026
  EXTDATE  : 8312026255
  REPTDATE : 2026-08-31  (num: 260831)
  PREVDATE : 2026-08-30
  DLETDATE : 2026-08-28
  REPTMON  : 08
Reading LNNOTE (chunked, chunk_size=500,000): /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBRCGCS/enrh_ln_note_m08.sas7bdat
  Total rows in file: 8,086,749
  Ingested chunk 1: rows 500,000 / 8,086,749
  Ingested chunk 2: rows 1,000,000 / 8,086,749
  Ingested chunk 3: rows 1,500,000 / 8,086,749
  Ingested chunk 4: rows 2,000,000 / 8,086,749
  Ingested chunk 5: rows 2,500,000 / 8,086,749
  Ingested chunk 6: rows 3,000,000 / 8,086,749
  Ingested chunk 7: rows 3,500,000 / 8,086,749
  Ingested chunk 8: rows 4,000,000 / 8,086,749
  Ingested chunk 9: rows 4,500,000 / 8,086,749
  Ingested chunk 10: rows 5,000,000 / 8,086,749
  Ingested chunk 11: rows 5,500,000 / 8,086,749
  Ingested chunk 12: rows 6,000,000 / 8,086,749
  Ingested chunk 13: rows 6,500,000 / 8,086,749
  Ingested chunk 14: rows 7,000,000 / 8,086,749
  Ingested chunk 15: rows 7,500,000 / 8,086,749
  Ingested chunk 16: rows 8,000,000 / 8,086,749
  Ingested chunk 17: rows 8,086,749 / 8,086,749
  Finished ingesting 8,086,749 rows into loan_raw
100% ▕██████████████████████████████████████▏ (00:00:02.28 elapsed)     
  Wrote /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/lndly31.parquet
Reading previous day: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/lndly30.parquet
  WARNING: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN/lndly30.parquet not found — previous-day tables will be empty.
Reading LKP_BRANCH: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIDLOAN/LKP_BRANCH
  Loaded 376 branch records
  Wrote mloan_31-08-2026.csv, mcred_31-08-2026.csv, mhp_31-08-2026.csv
  Skipping movement reports (no previous-day file).

Islamic Daily Loan Movement Report Complete
Date: 31/08/2026

Branch Summaries:
  1. MLOAN - Term Loan Outstanding by Branch
  2. MCRED - Revolving Credit Outstanding by Branch
  3. MHP   - HP Outstanding by Branch

Customer Movements (>= RM 500K):
  - Term Loans       : 0 accounts
  - Revolving Credit : 0 accounts
  - HP Loans         : 0 accounts

Output Files:
  - mloan_31-08-2026.csv
  - mcred_31-08-2026.csv
  - mhp_31-08-2026.csv
  - dmloan_31-08-2026.csv   (emitted only if previous-day parquet present)
  - dmcred_31-08-2026.csv   (emitted only if previous-day parquet present)
  - dmhp_31-08-2026.csv     (emitted only if previous-day parquet present)
  - lndly31.parquet

Completed: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIDLOAN
