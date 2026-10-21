[FMT] PBBLNFMT loaded (SECTCD / SECTA / SECTB functions available)
========== START JOB EIBWHP01 ==========
[DATE] 15/10/26  REPTMON=10 NOWK=2
[DATE] REPTMON1=10 NOWK1=1
[INFO] Current  : loan102.sas7bdat
[INFO] Previous : loan101.sas7bdat
[INFO] LNNOTE   : enrh_ln_note.sas7bdat
[PROCESS] Reading current loan : loan102.sas7bdat
  Current after PRODUCT filter: 197,720
[PROCESS] Reading previous loan: loan101.sas7bdat
  Previous after PRODUCT filter: 192,327
  Distinct (ACCTNO, NOTENO) keys needing LNNOTE: 89,291
[PROCESS] Reading LNNOTE – pruned columns, matching keys, excluding ENTITY_CD='PIBB'...
    LNNOTE chunks read: 5
    LNNOTE chunks read: 10
    LNNOTE chunks read: 15
  LNNOTE filtered: 447,025
[PROCESS] Mapping SECTOR -> SECTORCD via PBBLNFMT.SECTCD...
  LNNOTE after SECTCD mapping: 447,025
[PROCESS] Computing EFFAPR...
  Current merged with EFFAPR: 1,023,809
  Outer merged: 2,909,945
[JOB FAILED] 'EFFAPR_curr'
Traceback (most recent call last):
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 3805, in get_loc
    return self._engine.get_loc(casted_key)
  File "index.pyx", line 167, in pandas._libs.index.IndexEngine.get_loc
  File "index.pyx", line 196, in pandas._libs.index.IndexEngine.get_loc
  File "pandas/_libs/hashtable_class_helper.pxi", line 7081, in pandas._libs.hashtable.PyObjectHashTable.get_item
  File "pandas/_libs/hashtable_class_helper.pxi", line 7089, in pandas._libs.hashtable.PyObjectHashTable.get_item
KeyError: 'EFFAPR_curr'

The above exception was the direct cause of the following exception:

Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 372, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 355, in main
    expanded = process(curr_path, prev_path, lnnote_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 262, in process
    merged['EFFAPR'] = merged['EFFAPR_curr'].fillna(merged['EFFAPR_prev']).fillna(0.0)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/frame.py", line 4102, in __getitem__
    indexer = self.columns.get_loc(key)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/core/indexes/base.py", line 3812, in get_loc
    raise KeyError(key) from err
KeyError: 'EFFAPR_curr'
