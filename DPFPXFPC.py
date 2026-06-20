*** TEST MODE - Date: 2026-12-23 (Dec Week 4) ***
NOWK: 4, REPTMON: 12, REPTYEAR: 2026
Parsed 11825 records from MAREMORE
REMIT: 8976, NONDEBIT: 2849
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQREMT.py", line 140, in <module>
    if any([savg, curn, isavg, icurn]):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/polars/dataframe/frame.py", line 1164, in __bool__
    raise TypeError(msg)
TypeError: the truth value of a DataFrame is ambiguous

Hint: to check if a DataFrame contains any values, use `is_empty()`.
