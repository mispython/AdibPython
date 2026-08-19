============================================================
EIIQINST - Islamic Trustee and Client Account Reporting
============================================================

Report Period: 12/2025 (Week: 4)
SDESC: PUBLIC BANK BERHAD

============================================================
INPUT FILES
============================================================

PIDMS directory (/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/):
  - IBGPIDM.txt
  - client.sas7bdat
  - curn124.sas7bdat
  - current.sas7bdat
  - fd.sas7bdat
  - fdmthly.sas7bdat
  - float.sas7bdat
  - ibgpidm.sas7bdat
  - remit.sas7bdat
  - savg124.sas7bdat
  - saving.sas7bdat
  - si.sas7bdat
  - uma.sas7bdat
  - unclaim2025.sas7bdat

SACA directory (/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/):
  - IBGPIDM.txt
  - client.sas7bdat
  - curn124.sas7bdat
  - current.sas7bdat
  - fd.sas7bdat
  - fdmthly.sas7bdat
  - float.sas7bdat
  - ibgpidm.sas7bdat
  - remit.sas7bdat
  - savg124.sas7bdat
  - saving.sas7bdat
  - si.sas7bdat
  - uma.sas7bdat
  - unclaim2025.sas7bdat

DEPOSIT directory (/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/):
  - IBGPIDM.txt
  - client.sas7bdat
  - curn124.sas7bdat
  - current.sas7bdat
  - fd.sas7bdat
  - fdmthly.sas7bdat
  - float.sas7bdat
  - ibgpidm.sas7bdat
  - remit.sas7bdat
  - savg124.sas7bdat
  - saving.sas7bdat
  - si.sas7bdat
  - uma.sas7bdat
  - unclaim2025.sas7bdat

UNCLAIM directory (/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/):
  - IBGPIDM.txt
  - client.sas7bdat
  - curn124.sas7bdat
  - current.sas7bdat
  - fd.sas7bdat
  - fdmthly.sas7bdat
  - float.sas7bdat
  - ibgpidm.sas7bdat
  - remit.sas7bdat
  - savg124.sas7bdat
  - saving.sas7bdat
  - si.sas7bdat
  - uma.sas7bdat
  - unclaim2025.sas7bdat

Processing Trustee Accounts...
  FLOAT: 18927 rows
  IBGPIDM: 7609 rows
  REMIT: 6385 rows
  SA/CA/FD: 9 rows
  Trustee >60k: 0 accounts
  Trustee <=60k: 1 accounts
  Output written to: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/output/EIIQINST/islamic_trustee_low.txt

TRUSTEE <=60000 by Branch:
  Branch 161.0: RM 18,305.23

Processing Client Accounts...
  Found CLIENT file: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIIQINST/client.sas7bdat
  CLIENT master: 617 rows
  SASA: 1 rows
Traceback (most recent call last):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 3805, in get_loc
    return self._engine.get_loc(casted_key)
  File "index.pyx", line 167, in pandas._libs.index.IndexEngine.get_loc
  File "index.pyx", line 196, in pandas._libs.index.IndexEngine.get_loc
  File "pandas/_libs/hashtable_class_helper.pxi", line 7081, in pandas._libs.hashtable.PyObjectHashTable.get_item
  File "pandas/_libs/hashtable_class_helper.pxi", line 7089, in pandas._libs.hashtable.PyObjectHashTable.get_item
KeyError: 'avbal'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQINST.py", line 876, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIIQINST.py", line 706, in main
    client['avbaltt'] = client['avbal'] + client['intpaybl']
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4102, in __getitem__
    indexer = self.columns.get_loc(key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 3812, in get_loc
    raise KeyError(key) from err
KeyError: 'avbal'
