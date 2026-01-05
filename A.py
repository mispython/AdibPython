MONTHLY JOB: Processing December 2025
Processing: 2025-12-31 | Accumulating with: 2025-11

============================================================
MONTHLY PROCESSING STARTED
============================================================

1. CHANNEL SUMMARY

2. OTC DETAIL
  Processed 376 branches, TOLPROMPT sum: 22,115

3. CHANNEL UPDATE
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/CIS/MIS_CRM_CHANNEL_UAT.py", line 195, in <module>
    update_df = process_channel_update()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/CIS/MIS_CRM_CHANNEL_UAT.py", line 155, in process_channel_update
    return accumulate_monthly_data(df_new, "CHANNEL_UPDATE", schema, date_field="DATE")
  File "/sas/python/virt_edw/Data_Warehouse/MIS/Job/CIS/MIS_CRM_CHANNEL_UAT.py", line 74, in accumulate_monthly_data
    if current_value in df_current[month_field if month_field == "MONTH" else date_field].unique().to_list():
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 1395, in __getitem__
    return get_df_item_by_key(self, key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/_utils/getitem.py", line 163, in get_df_item_by_key
    return df.get_column(key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 8735, in get_column
    return wrap_s(self._df.get_column(name))
polars.exceptions.ColumnNotFoundError: "MONTH" not found
