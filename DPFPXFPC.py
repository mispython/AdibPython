Reading  : /host_pq/dwh/input/DEPOSIT/DPDARPGS_FB_20260924
Writing  : /stgsrcsys/host/uat/maa/python/input/DPDARPGS_FB_20260924.parquet
Traceback (most recent call last):
  File "/stgsrcsys/host/uat/python/flatfile_to_parquet.py", line 122, in <module>
    main()
  File "/stgsrcsys/host/uat/python/flatfile_to_parquet.py", line 106, in main
    writer = pq.ParquetWriter(
  File "/sas/python/virt_edw/lib/python3.9/site-packages/pyarrow/parquet/core.py", line 1015, in __init__
    sink = self.file_handle = filesystem.open_output_stream(
  File "pyarrow/_fs.pyx", line 887, in pyarrow._fs.FileSystem.open_output_stream
  File "pyarrow/error.pxi", line 155, in pyarrow.lib.pyarrow_internal_check_status
  File "pyarrow/error.pxi", line 92, in pyarrow.lib.check_status
PermissionError: [Errno 13] Failed to open local file '/stgsrcsys/host/uat/maa/python/input/DPDARPGS_FB_20260924.parquet'. Detail: [
errno 13] Permission denied
NOTE: 13 records were read from the infile "cd /sas/python/virt_edw;source bin/activate;python 
      /stgsrcsys/host/uat/python/flatfile_to_parquet.py".
      The minimum record length was 10.
