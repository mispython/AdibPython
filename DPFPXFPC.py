1. Processing FD_SACA...
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMDIAM.py", line 170, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMDIAM.py", line 135, in main
    saca_data = read_fixed_width(CFG["input"]["saca"], flatfile_cols)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMDIAM.py", line 55, in read_fixed_width
    record[name] = dtype(val) if dtype != float else float(val)
ValueError: could not convert string to float: '.'
