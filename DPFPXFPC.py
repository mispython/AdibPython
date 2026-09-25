Reading  : /host_pq/dwh/input/DEPOSIT/DPDARPGS_FB_20260924
Writing  : /stgsrcsys/host/holding/DPDARPGS_FB_20260924.parquet
Traceback (most recent call last):
  File "/stgsrcsys/host/uat/python/flatfile_to_parquet.py", line 122, in <module>
    main()
  File "/stgsrcsys/host/uat/python/flatfile_to_parquet.py", line 106, in main
    writer = pq.ParquetWriter(
  File "/sas/python/virt_edw/lib/python3.9/site-packages/pyarrow/parquet/core.py", line 1021, in __init__
    self.writer = _parquet.ParquetWriter(
  File "pyarrow/_parquet.pyx", line 2138, in pyarrow._parquet.ParquetWriter.__cinit__
TypeError: __cinit__() got an unexpected keyword argument 'row_group_size'
NOTE: 11 records were read from the infile "cd /sas/python/virt_edw;source bin/activate;python 
      /stgsrcsys/host/uat/python/flatfile_to_parquet.py".
      The minimum record length was 10.
      The maximum record length was 105.
NOTE: DATA statement used (Total process time):
      real time           4.43 seconds
2                                                          The SAS System                           11:58 Friday, September 25, 2026

      cpu time            0.00 seconds
