SAS Connection terminated. Subprocess id was 3434853
DEBUG: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.sas7bdat did not appear after 30s.
DEBUG: directory listing of /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output:
   PBBRDAL.txt
   pbbrdal.sas7bdat
Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 181, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIBWP124.py", line 177, in main
    run_p124rdal()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/P124RDAL.py", line 222, in main
    import PBBMRDLF  # noqa: F401
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/PBBMRDLF.py", line 126, in <module>
    build()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/PBBMRDLF.py", line 118, in build
    raise RuntimeError(
RuntimeError: PBBMRDLF: /sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIBWP124/output/PBBRDAL.sas7bdat never appeared on disk.
