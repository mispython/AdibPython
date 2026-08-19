============================================================
EIIQINST - Islamic Trustee and Client Account Reporting
============================================================

Report Period: 12/2025 (Week: 4)
SDESC: PUBLIC BANK BERHAD

Processing Trustee Accounts...
  FLOAT: 18927 rows
  IBGPIDM: 7609 rows
  REMIT: 6385 rows
  SA/CA/FD: 9 rows
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQINST.py", line 745, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQINST.py", line 472, in main
    trustee = trustee.merge(remit_df[remit_cols], on='acctno', how='left')
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 10832, in merge
    return merge(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/reshape/merge.py", line 170, in merge
    op = _MergeOperation(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/reshape/merge.py", line 807, in __init__
    self._maybe_coerce_merge_keys()
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/reshape/merge.py", line 1508, in _maybe_coerce_merge_keys
    raise ValueError(msg)
ValueError: You are trying to merge on float64 and object columns for key 'acctno'. If you wish to proceed you should use pd.concat
