Connecting to SAS...
SAS Connection established. Subprocess id is 2696363

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBDDEPE.py", line 53, in <module>
    print(f"SAS session: {sas.accessMethod}")   # Access Method only, no full repr
AttributeError: 'SASsession' object has no attribute 'accessMethod'
SAS Connection terminated. Subprocess id was 2696363
