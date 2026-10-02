NOTE: The infile "cd /sas/python/virt_edw;source bin/activate;python /stgsrcsys/host/uat/python/EIBDFALE.py" is:
      Pipe command="cd /sas/python/virt_edw;source bin/activate;python /stgsrcsys/host/uat/python/EIBDFALE.py"

Step A: importing pyarrow
  pyarrow imported in 0.0s
Step B: opening ONE part file
  opened in 0.0s
  rows: 309679
  row groups: 1
Step C: reading metadata only
  metadata in 0.0s
Step D: reading ONE row group
  read in 0.0s, 309679 rows
Step E: importing polars
  polars imported in 0.3s
/stgsrcsys/host/uat/python/EIBDFALE.py:33: DeprecationWarning: `threadpool_size` was renamed; use `thread_pool_size` instead.
  print(f"  threadpool: {pl.threadpool_size()}", flush=True)
  threadpool: 80
Step F: scan one part file
  read in 0.2s, 309679 rows
2                                                          The SAS System                              17:35 Friday, October 2, 2026

DONE
NOTE: 18 records were read from the infile "cd /sas/python/virt_edw;source bin/activate;python 
      /stgsrcsys/host/uat/python/EIBDFALE.py".
      The minimum record length was 4.
      The maximum record length was 125.
NOTE: DATA statement used (Total process time):
      real time           1.94 seconds
      cpu time            0.00 seconds
