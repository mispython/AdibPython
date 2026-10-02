REPTMON=09, NOWK=4, REPTYEAR=2026, RDATE=30/09/26, SDATE=23/09/26, SUFFIX=094
L124PBBD DEBUG: reptmon='09' nowk='4' sfx='094'
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/loan094.sas7bdat ...
WARNING: 'l124094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3437442

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1839: UserWarning: Note that Indexes are not transferred over as columns. Only actual columns are transferred
  warnings.warn("Note that Indexes are not transferred over as columns. Only actual columns are transferred")
SAS Connection terminated. Subprocess id was 3437442
L124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/l124094.sas7bdat  (0 rows)
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/uloan094.sas7bdat ...
WARNING: 'ul124094' has 0 rows — writing empty dataset.
SAS Connection established. Subprocess id is 3437490

/sas/python/virt_edw_dev/lib64/python3.9/site-packages/saspy/sasiostdio.py:1839: UserWarning: Note that Indexes are not transferred over as columns. Only actual columns are transferred
  warnings.warn("Note that Indexes are not transferred over as columns. Only actual columns are transferred")
SAS Connection terminated. Subprocess id was 3437490
UL124 written: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm/ul124094.sas7bdat  (0 rows)
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 181, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 159, in main
    run_lalwp124()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/LALWP124.py", line 245, in main
    write_sas_and_txt(lalw_final, BNM_PATH, f"lalw{sfx}", verbose=False)
TypeError: write_sas_and_txt() got an unexpected keyword argument 'verbose'
