Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMADRE.py", line 78, in <module>
    records = read_fixed_width(DPADDR_FILE, fwf_layout)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBMADRE.py", line 70, in read_fixed_width
    row[name] = int(raw or 0)
ValueError: invalid literal for int() with base 10: '\x03\x92'
