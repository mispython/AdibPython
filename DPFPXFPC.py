TEST1 :

Found 147 part files
  [  0/147] part-00000.parquet: 309,679 rows in 0.001s
  [  1/147] part-00001.parquet: 309,680 rows in 0.002s
  [  2/147] part-00002.parquet: 309,680 rows in 0.001s
  [  3/147] part-00003.parquet: 309,680 rows in 0.001s
  [  4/147] part-00004.parquet: 309,680 rows in 0.001s
  [ 20/147] part-00020.parquet: 309,680 rows in 0.001s
  [ 40/147] part-00040.parquet: 309,680 rows in 0.001s
  [ 60/147] part-00060.parquet: 309,680 rows in 0.001s
  [ 80/147] part-00080.parquet: 309,680 rows in 0.001s
  [100/147] part-00100.parquet: 309,680 rows in 0.001s
  [120/147] part-00120.parquet: 309,680 rows in 0.001s
  [140/147] part-00140.parquet: 309,680 rows in 0.001s

Total 45,447,948 rows across 147 parts in 0.2s
NOTE: 15 records were read from the infile "cd /sas/python/virt_edw;source bin/activate;python 
      /stgsrcsys/host/uat/python/EIBDFALE.py".
2                                                          The SAS System                              17:35 Friday, October 2, 2026

      The minimum record length was 0.
      The maximum record length was 54.
NOTE: DATA statement used (Total process time):
      real time           1.56 seconds
      cpu time            0.00 seconds


TEST 2:

Threads: 8
Read 45,447,948 rows in 0.8s
NOTE: 2 records were read from the infile "cd /sas/python/virt_edw;source bin/activate;python 
      /stgsrcsys/host/uat/python/EIBDFALE.py".
      The minimum record length was 10.
      The maximum record length was 28.
NOTE: DATA statement used (Total process time):
      real time           2.60 seconds
      cpu time            0.00 seconds



TEST3 :

Threads: 8
/stgsrcsys/host/uat/python/EIBDFALE.py:12: DeprecationWarning: the `streaming` parameter was deprecated in 1.25.0; use `engine` inst
ead.
  n = pl.scan_parquet(PDIR).select(pl.len()).collect(streaming=True).item()
Counted 45,447,948 rows in 0.0s
NOTE: 4 records were read from the infile "cd /sas/python/virt_edw;source bin/activate;python 
      /stgsrcsys/host/uat/python/EIBDFALE.py".
      The minimum record length was 10.
      The maximum record length was 136.
NOTE: DATA statement used (Total process time):
      real time           1.55 seconds
      cpu time            0.00 seconds


TEST 4:

Threads: 8
/stgsrcsys/host/uat/python/EIBDFALE.py:12: DeprecationWarning: the `streaming` parameter was deprecated in 1.25.0; use `engine` inst
ead.
  n = pl.scan_parquet(PDIR).select(pl.len()).collect(streaming=True).item()
Counted 45,447,948 rows in 0.0s
MemTotal:       1056702836 kB
MemAvailable:   1033889952 kB
SwapTotal:      33554428 kB
SwapFree:       22388784 kB
NOTE: 8 records were read from the infile "cd /sas/python/virt_edw;source bin/activate;python 
      /stgsrcsys/host/uat/python/EIBDFALE.py".
      The minimum record length was 10.
      The maximum record length was 136.
NOTE: DATA statement used (Total process time):
      real time           1.41 seconds
      cpu time            0.00 seconds
      
