SAS Connection established. Subprocess id is 688794

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
⚠ Error saving SAS file using saspy: expected str, bytes or os.PathLike object, not DataFrame
⚠ Error saving SAS file with pyreadstat: module 'pyreadstat' has no attribute 'write_sas7bdat'
⚠ SAS write failed, saved CSV instead: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIBQDISE/savg054.csv
Processing Current Accounts...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQDISE.py", line 539, in <module>
    current_all = pl.concat([current_regular, current_fcy], how="diagonal")
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/functions/eager.py", line 247, in concat
    out = wrap_df(plr.concat_df_diagonal(elems))
polars.exceptions.SchemaError: type Int64 is incompatible with expected type String
SAS Connection terminated. Subprocess id was 688794
