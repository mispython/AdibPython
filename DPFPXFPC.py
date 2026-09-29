SAS Connection established. Subprocess id is 3019457

EIMBNM01: Starting Public Bank Berhad loan summary reports...
  Report date: 2026-08-31  MM=08 YY=2026 WK=4
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMBNM01.py", line 1273, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMBNM01.py", line 905, in main
    if not pbif_df.empty:
AttributeError: 'DataFrame' object has no attribute 'empty'
SAS Connection terminated. Subprocess id was 3019457
