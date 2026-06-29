SAS Connection established. Subprocess id is 692235

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
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQDISE.py", line 666, in <module>
    dept_all = pl.concat([dept_savg, dept_curn], how="diagonal")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 247, in concat
    out = wrap_df(plr.concat_df_diagonal(elems))
polars.exceptions.SchemaError: type String is incompatible with expected type Float64
SAS Connection terminated. Subprocess id was 692235
