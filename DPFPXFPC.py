REPTMON=09, NOWK=4, REPTYEAR=2026, RDATE=30/09/26, SDATE=23/09/26, SUFFIX=094
L124PBBD DEBUG: reptmon='09' nowk='4' sfx='094'
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/loan094.sas7bdat ...
WARNING: 'l124094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3437819

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1839: UserWarning: Note that Indexes are not transferred over as columns. Only actual columns are transferred
  warnings.warn("Note that Indexes are not transferred over as columns. Only actual columns are transferred")
SAS Connection terminated. Subprocess id was 3437819
L124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.sas7bdat  (0 rows)
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/uloan094.sas7bdat ...
WARNING: 'ul124094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3437866

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1839: UserWarning: Note that Indexes are not transferred over as columns. Only actual columns are transferred
  warnings.warn("Note that Indexes are not transferred over as columns. Only actual columns are transferred")
SAS Connection terminated. Subprocess id was 3437866
UL124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.sas7bdat  (0 rows)
WARNING: 'lalw094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3437903

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1118: UserWarning: Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem
  warnings.warn("Noticed 'ERROR:' in LOG, you ought to take a look and see if there was a problem")
SAS Connection terminated. Subprocess id was 3437903
LALW written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/lalw094.sas7bdat  (0 rows)
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 181, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 168, in main
    alw_df = read_sas7bdat_ci(bnmx_alw_path)
TypeError: read_sas7bdat_ci() missing 1 required positional argument: 'filename'
