Traceback (most recent call last):
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR201.py", line 155, in <module>
    main()
  File "/sas/python/virt_edw/Data_Warehouse/MIS/XMIS/EIMIR201.py", line 35, in main
    brhdata_df = pd.read_fwf(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/io/parsers/readers.py", line 1565, in read_fwf
    return _read(filepath_or_buffer, kwds)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/io/parsers/readers.py", line 620, in _read
    parser = TextFileReader(filepath_or_buffer, **kwds)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/io/parsers/readers.py", line 1620, in __init__
    self._engine = self._make_engine(f, self.engine)
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/io/parsers/readers.py", line 1880, in _make_engine
    self.handles = get_handle(
  File "/sas/python/virt_edw_dev/lib64/python3.9/site-packages/pandas/io/common.py", line 873, in get_handle
    handle = open(
FileNotFoundError: [Errno 2] No such file or directory: 'sas/python/virt_edw/Data_Warehouse/MIS/XMIS/input/prod/EIMIR201/LKP_BRANCH'
You have mail in /var/spool/mail/sas_edw_dev
