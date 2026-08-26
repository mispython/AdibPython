SAS Connection established. Subprocess id is 2294564

REPTMON: 07, RDATE: 310726
Available columns: ['curbal', 'costctr', 'accrual', 'balance', 'product', 'censust', 'sch', 'cinstcl', 'natguar', 'tranche', 'cvar02', 'cvar01', 'cvar06', 'cvar03', 'cvar04', 'cvar14', 'cvar13', 'cvar08', 'cvar09', 'cvar10', 'cvar11', 'cvar05', 'cvar07', 'cvar12', 'cvar15', 'branch']
Number of rows: 0
SMEE dataset created with 0 rows
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS ERROR detected in SMEE export:
  ERROR: DBMS type SAS7BDAT not valid for export.
  NOTE: The SAS System stopped processing this step because of errors.
After filtering: 0 rows remaining
SC5T file created: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRSMEE/sc5t.txt
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS ERROR detected in NPGS export:
  ERROR: DBMS type SAS7BDAT not valid for export.
  NOTE: The SAS System stopped processing this step because of errors.
============================================================
PUBLIC BANK BERHAD
DETAIL OF ACCTS (SCH=E5) FOR SUBMISSION TO CGC @ 310726
============================================================
Report generated: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRSMEE/sc5r.txt

Processing complete. Files created in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBRSMEE:
- sc5t.txt (text format)
- sc5r.txt (report)
- smee.parquet, smee.sas7bdat, smee.csv
- npgs_filtered.parquet, npgs_filtered.sas7bdat
SAS Connection terminated. Subprocess id was 2294564
