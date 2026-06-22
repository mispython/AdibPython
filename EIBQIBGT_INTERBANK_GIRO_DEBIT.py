NOWK: 4, REPTMON: 12, REPTYEAR: 2025
SDESC: PUBLIC BANK BERHAD
Reading IBG file from: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/MNI/IBG_YEAREND.txt
Error reading IBG_YEAREND.txt: 'ExprStringNameSpace' object has no attribute 'strip'
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBQIBGT.py", line 83, in <module>
    pl.col('raw_line').str.slice(0, 10).str.strip().alias('PAYMODE'),      # @01 PAYMODE $10.
AttributeError: 'ExprStringNameSpace' object has no attribute 'strip'
