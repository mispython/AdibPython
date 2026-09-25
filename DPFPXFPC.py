Reading  : /host_pq/dwh/input/DEPOSIT/DPDARPGS_FB_20260924
Writing  : /stgsrcsys/host/uat/maa/python/input/DPDARPGS_FB_20260924.parquet
Traceback (most recent call last):
  File "/stgsrcsys/host/uat/python/flatfile_to_parquet.py", line 120, in <module>
    main()
  File "/stgsrcsys/host/uat/python/flatfile_to_parquet.py", line 101, in main
    table = decode_chunk(arr)
  File "/stgsrcsys/host/uat/python/flatfile_to_parquet.py", line 70, in decode_chunk
    cols[name] = arr[:, a].view("S1").astype("U1")
UnicodeDecodeError: 'ascii' codec can't decode byte 0x80 in position 0: ordinal not in range(128)
