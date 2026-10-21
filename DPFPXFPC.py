[WARN] format call failed: SECTCD() missing 1 required positional argument: 'value'
[WARN] format call failed: SECTA() missing 1 required positional argument: 'value'
[WARN] format call failed: SECTB() missing 1 required positional argument: 'value'
[FMT] SECTCD=0 SECTA=0 SECTB=0
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
[JOB FAILED] __enter__
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 367, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 350, in main
    expanded = process(curr_path, prev_path, lnnote_path)
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWHP01.py", line 190, in process
    with pyreadstat.read_file_in_chunks(
AttributeError: __enter__
