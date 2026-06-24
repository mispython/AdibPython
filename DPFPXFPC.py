Using SAS Config named: default
SAS Connection established. Subprocess id is 76551

SAS session initialized successfully
NOWK: 4, REPTMON: 05, REPTYEAR: 2026
SDESC: PUBLIC BANK BERHAD
Report Date: 2026-05-31
Start Date: 2026-05-23
------------------------------------------------------------
Created REPTDATE.parquet and REPTDATE.csv

============================================================
Processing PBB_ICR.txt...
============================================================
ERROR: Input file PBB_ICR.txt not found
Creating empty dataframe for ICLPBB
Created empty ICLPBB052026.parquet
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
Created empty SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/ICR_PIDM/ICLPBB052026.sas7bdat

============================================================
Processing PIBB_ICR.txt...
============================================================
ERROR: Input file PIBB_ICR.txt not found
Creating empty dataframe for ICLPIBB
Created empty ICLPIBB052026.parquet
Created empty SAS dataset: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/ICR_PIDM/ICLPIBB052026.sas7bdat

============================================================
PROCESSING COMPLETED SUCCESSFULLY
============================================================

Output files created:
  Parquet files (deposit path): /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/ICR_PIDM
    - ICLPBB052026.parquet (0 records)
    - ICLPIBB052026.parquet (0 records)

  SAS datasets (deposit path): /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/ICR_PIDM
    - ICLPBB052026.sas7bdat (0 records)
    - ICLPIBB052026.sas7bdat (0 records)

  Additional outputs (output path): /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQPICL
    - REPTDATE.parquet
    - REPTDATE.csv
SAS Connection terminated. Subprocess id was 76551

SAS session closed successfully

============================================================
