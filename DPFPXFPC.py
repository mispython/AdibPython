SAS Connection established. Subprocess id is 3022090

EIMBNM01: Starting Public Bank Berhad loan summary reports...
  Report date: 2026-08-31  MM=08 YY=2026 WK=4
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMBNM01.py", line 1288, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMBNM01.py", line 932, in main
    almnew_df = pd.concat([f for f in [alm_df, pbif_df] if not f.empty],
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/reshape/concat.py", line 382, in concat
    op = _Concatenator(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/reshape/concat.py", line 445, in __init__
    objs, keys = self._clean_keys_and_objs(objs, keys)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/reshape/concat.py", line 507, in _clean_keys_and_objs
    raise ValueError("No objects to concatenate")
ValueError: No objects to concatenate
SAS Connection terminated. Subprocess id was 3022090
