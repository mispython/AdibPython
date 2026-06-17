Report Parameters: {'REPTYEAR': '26', 'REPTMON': '06', 'REPTDAY': '16', 'PREVMON': '05', 'PREVDAY': '31', 'RDATE': '16-06-2026', 'RDATEX': '0626', 'SDATE': '60701'}
Skipping header line 1

Parsing summary:
  Valid records: 0
  Skipped records: 870
Error reading /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/BTPM12.txt: No valid data found in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/BTPM12.txt
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDBT12.py", line 158, in <module>
    btdtl = read_btdtl_text(INPUT_BTPM12_FILE)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDBT12.py", line 136, in read_btdtl_text
    raise ValueError(f"No valid data found in {filepath}")
ValueError: No valid data found in /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/BTPM12.txt
