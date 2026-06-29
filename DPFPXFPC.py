SAS Connection established. Subprocess id is 690229

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
⚠ Error saving SAS file using saspy: 'SASsession' object has no attribute 'dataframe'
⚠ Error saving SAS file with pyreadstat: module 'pyreadstat' has no attribute 'write'
⚠ SAS write failed, saved CSV instead: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/savg054.csv
Processing Current Accounts...
✓ Processed 919361 current accounts
  - Regular: 852110
  - FCY: 67251
✓ Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/curn054.parquet
⚠ Error saving SAS file using saspy: 'SASsession' object has no attribute 'dataframe'
⚠ Error saving SAS file with pyreadstat: module 'pyreadstat' has no attribute 'write'
⚠ SAS write failed, saved CSV instead: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/curn054.csv
✓ Saved Parquet: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/fcy054.parquet
⚠ Error saving SAS file using saspy: 'SASsession' object has no attribute 'dataframe'
⚠ Error saving SAS file with pyreadstat: module 'pyreadstat' has no attribute 'write'
⚠ SAS write failed, saved CSV instead: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/fcy054.csv
Creating branch-level summaries...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQDISE.py", line 605, in <module>
    dept_curn = current_all.group_by(["BRANCH", "STATECD", "PRODCD", "CUSTCD", "SECTOR", "AMTIND"]).agg([
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/group_by.py", line 296, in agg
    self._lgb()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/deprecation.py", line 97, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/opt_flags.py", line 328, in wrapper
    return function(*args, **kwargs)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/lazyframe/frame.py", line 2429, in collect
    return wrap_df(ldf.collect(engine, callback))
polars.exceptions.InvalidOperationError: `sum` operation not supported for dtype `str`

Resolved plan until failure:

        ---> FAILED HERE RESOLVING 'sink' <---
DF ["APPRLIMT", "INTPLAN_IBCA", "POST_IND", "LEDGBAL", ...]; PROJECT */163 COLUMNS
SAS Connection terminated. Subprocess id was 690229
