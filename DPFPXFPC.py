REPTMON=09, NOWK=4, REPTYEAR=2026, RDATE=29/09/26, SDATE=23/09/26
L124PBBD: reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/bnm1/loan094.sas7bdat ...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 214, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 188, in main
    run_lalwp124()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/LALWP124.py", line 138, in main
    run_l124pbbd()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/L124PBBD.py", line 187, in main
    write_via_saspy(l124_df, BNM_PATH, l124_base)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/L124PBBD.py", line 93, in write_via_saspy
    raise ValueError(
ValueError: Refusing to write empty/column-less dataset 'l124094'. Filtered input produced 0 rows — verify the source file's columns and the PRODUCT filter.
