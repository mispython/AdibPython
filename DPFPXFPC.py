SAS Connection established. Subprocess id is 700344

✓ SAS session initialized successfully
Report Date: 31/05/26
Start Date: 23/05/26
Week: 4
Month: 05
Year: 2026

Loading UMA data...
✓ Loaded 31915 UMA records
Processing Saving Accounts...
✓ Processed 4282476 saving accounts
✓ Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/savg054.parquet
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
✓ Saved SAS: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/savg054.sas7bdat
Processing Current Accounts...
✓ Processed 919361 current accounts
  - Regular: 852110
  - FCY: 67251
✓ Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/curn054.parquet
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
✓ Saved SAS: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/curn054.sas7bdat
✓ Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/fcy054.parquet
✓ Saved SAS: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/fcy054.sas7bdat
Creating branch-level summaries...
✓ Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/dept054.parquet
✓ Saved SAS: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/dept054.sas7bdat
✓ Created department summary
Processing account count summaries...
✓ Account count summaries created
Processing Fixed Deposits...
✓ Processed 2680347 fixed deposit accounts
✓ Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/fdmthly.parquet
/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
✓ Saved SAS: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/fdmthly.sas7bdat

======================================================================
EIBQDISE Deposit Processing Complete!
======================================================================

Output Files Created (Parquet and SAS):
  1. Savings: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/savg054
  2. Current: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/curn054
  3. FCY: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/fcy054
  4. Department: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/dept054
  5. FD Monthly: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/fdmthly

Record Counts:
  Savings:        4,282,476
  Current:        919,361
  FCY:            67,251
  Fixed Deposit:  2,680,347

SAS Connection terminated. Subprocess id was 700344
✓ SAS session closed
